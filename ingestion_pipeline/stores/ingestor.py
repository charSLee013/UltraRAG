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
