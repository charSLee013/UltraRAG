from __future__ import annotations

import logging
from typing import List

from ..types import ChunkRecord, RawDocument
from .sqlite import SQLiteStore
from .chroma import ChromaStore

logger = logging.getLogger("ingestion.ingestor")


class SqliteChromaIngestor:
    """Two-phase ingest across SQLite + Chroma with rollback semantics."""

    def __init__(self, sqlite: SQLiteStore, chroma: ChromaStore) -> None:
        self.sqlite = sqlite
        self.chroma = chroma

    def ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:
        if not records:
            # [块] 空记录：仅更新 repo 表，维持 fetched_at 与内容哈希
            logger.info("[ingest] repo=%s no records, upserting repo metadata only", raw.repo_id)
            self.sqlite.begin()
            try:
                self.sqlite.upsert_repo(raw)
                self.sqlite.commit()
            except Exception:
                self.sqlite.rollback()
                raise
            return

        repo_id = raw.repo_id

        for r in records:
            if getattr(r, "fetched_at", None) is None:
                r.fetched_at = raw.fetched_at

        # [块] 两阶段写入：先 SQLite（同事务替换），再 Chroma（删除→upsert），失败回滚
        self.sqlite.begin()
        try:
            self.sqlite.upsert_repo(raw)
            self.sqlite.delete_repo_chunks(repo_id)
            self.sqlite._insert_records(records)
            try:
                self.chroma.delete_repo(repo_id)
                self.chroma.upsert_records(records)
            except Exception:
                try:
                    self.chroma.delete_repo(repo_id)
                finally:
                    raise
            self.sqlite.commit()
        except Exception:
            self.sqlite.rollback()
            raise

    def ingest_many(self, items: List[tuple[List[ChunkRecord], RawDocument]]) -> None:
        if not items:
            return
        # Normalize fetched_at
        for records, raw in items:
            for r in records:
                if getattr(r, "fetched_at", None) is None:
                    r.fetched_at = raw.fetched_at

        # Phase 1: SQLite in single transaction
        self.sqlite.begin()
        try:
            for records, raw in items:
                repo_id = raw.repo_id
                self.sqlite.upsert_repo(raw)
                self.sqlite.delete_repo_chunks(repo_id)
                if records:
                    self.sqlite._insert_records(records)
            self.sqlite.commit()
        except Exception:
            self.sqlite.rollback()
            raise

        # Phase 2: Chroma — delete then upsert in batches
        try:
            for _records, raw in items:
                self.chroma.delete_repo(raw.repo_id)
            all_records: List[ChunkRecord] = [r for records, _ in items for r in records]
            if all_records:
                self.chroma.upsert_records(all_records)
        except Exception:
            # best-effort rollback on Chroma side per repo
            for _records, raw in items:
                try:
                    self.chroma.delete_repo(raw.repo_id)
                except Exception:
                    pass
            raise
