from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv

from ..types import ChunkRecord, RawDocument


load_dotenv()


class SQLiteStore:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or os.environ.get(
            "INGESTION_SQLITE_PATH", "output/ingestion/sqlite/docs.sqlite"
        )
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=3000")
        # repo 表：用于审计/重放的最小元数据
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS repo (
                repo_id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                owner_repo TEXT NOT NULL,
                source_url TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        # chunks 表：严格最小列集（不存 locator_* / embedding）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_uuid TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                text TEXT NOT NULL
            )
            """
        )
        # 校验 schema：如存在多余/缺失列则 fail-fast（禁止自动迁移）
        cur.execute("PRAGMA table_info(chunks)")
        cols = [row[1] for row in cur.fetchall()]
        minimal = ["chunk_uuid", "repo_id", "chunk_index", "text"]
        if set(cols) != set(minimal):
            cur.close()
            raise RuntimeError(
                "[sqlite] chunks schema mismatch; expected columns="
                f"{minimal}, found={cols}. Please recreate the DB explicitly."
            )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_repo ON chunks(repo_id, chunk_index)"
        )
        cur.close()

    def delete_repo_chunks(self, repo_id: str) -> None:
        self.conn.execute(
            "DELETE FROM chunks WHERE repo_id=?",
            (repo_id,),
        )

    def _insert_records(self, records: Iterable[ChunkRecord]) -> None:
        # 仅写入最小列集；locator/embedding 写入由 Chroma 负责
        rows = [
            (
                r.chunk_uuid,
                r.repo_id,
                r.chunk_index,
                r.text,
            )
            for r in records
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO chunks(
                chunk_uuid, repo_id, chunk_index, text
            ) VALUES (?, ?, ?, ?)
            """,
            rows,
        )

    def upsert_repo(self, raw: RawDocument) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO repo(repo_id, source_type, owner_repo, source_url, content_hash, fetched_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                raw.repo_id,
                raw.locator.source_type.value,
                raw.locator.owner_repo,
                raw.locator.source_url,
                raw.content_hash,
                raw.fetched_at.isoformat(),
            ),
        )

    def get_repo_content_hash(self, repo_id: str) -> Optional[str]:
        cur = self.conn.execute(
            "SELECT content_hash FROM repo WHERE repo_id=?",
            (repo_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    # Transaction controls for two-phase ingest
    def begin(self) -> None:
        self.conn.execute("BEGIN")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    def replace_chunks(self, records: List[ChunkRecord]) -> None:
        if not records:
            return
        repo_id = records[0].repo_id
        self.begin()
        try:
            self.delete_repo_chunks(repo_id)
            self._insert_records(records)
            self.commit()
        except Exception:
            self.rollback()
            raise

    def close(self) -> None:
        self.conn.close()
