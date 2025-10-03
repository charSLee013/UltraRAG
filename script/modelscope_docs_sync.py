#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Tuple
import uuid
import time

import aiosqlite
import chromadb
from chromadb import PersistentClient
import httpx
from dotenv import load_dotenv

from modelscope_client import DEFAULT_ENDPOINT, Document, ModelScopeClient

load_dotenv()

DATABASE_PATH = Path("output/modelscope_docs/docs.sqlite")
DEFAULT_CHECKPOINT_INTERVAL = 60  # seconds
DOC_QUEUE_SIZE = 64
CHUNK_QUEUE_SIZE = 64
MAX_TEXT_BYTES = 512 * 1024
CHROMA_PATH = Path(os.environ.get("CHROMA_PATH", "output/modelscope_docs/chroma"))
CHROMA_COLLECTION = os.environ.get("CHROMA_COLLECTION", "modelscope_docs")
EMBEDDING_API_URL = os.environ["EMBEDDING_API_URL"]
EMBEDDING_API_KEY = os.environ["EMBEDDING_API_KEY"]
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-m3")
EMBEDDING_BATCH = int(os.environ.get("EMBEDDING_BATCH", "64"))
EMBEDDING_TIMEOUT = int(os.environ.get("EMBEDDING_TIMEOUT", "60"))
CHUNK_FLUSH_THRESHOLD = int(os.environ.get("CHUNK_FLUSH_THRESHOLD", "20"))
DEBUG_MODE = os.environ.get("INGEST_DEBUG", "0") == "1"

def debug(message: str) -> None:
    if DEBUG_MODE:
        print(f"[DEBUG] {message}", flush=True)

CHUNK_WORKERS = int(os.environ.get("CHUNK_WORKERS", "64"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def split_text(text: str, max_chars: int = 800, overlap: int = 100) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    start = 0
    length = len(text)
    while start < length:
        end = min(length, start + max_chars)
        chunk = text[start:end]
        chunks.append(chunk)
        if end == length:
            break
        start = max(0, end - overlap)
    return chunks


class TokenBucket:
    def __init__(self, min_capacity: int, max_capacity: int) -> None:
        self._min_capacity = max(1, min_capacity)
        self._max_capacity = max(self._min_capacity, max_capacity)
        self._capacity = self._max_capacity
        self._available = self._capacity
        self._success_streak = 0
        self._cond = asyncio.Condition()

    def snapshot(self) -> tuple[int, int]:
        return self._capacity, self._available

    async def acquire(self) -> None:
        async with self._cond:
            while self._available <= 0:
                await self._cond.wait()
            self._available -= 1

    async def succeed(self) -> None:
        async with self._cond:
            self._available = min(self._capacity, self._available + 1)
            self._success_streak += 1
            if self._capacity < self._max_capacity and self._success_streak >= self._capacity:
                self._capacity += 1
                self._available = min(self._available + 1, self._capacity)
                self._success_streak = 0
            self._cond.notify()

    async def fail(self) -> None:
        async with self._cond:
            self._capacity = max(self._min_capacity, max(1, self._capacity // 2))
            self._available = min(self._available + 1, self._capacity)
            self._success_streak = 0
            self._cond.notify_all()


EMBEDDING_LIMITER = TokenBucket(1, 32)


async def ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=3000")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
                repo_type   TEXT,
                owner       TEXT,
                name        TEXT,
                path        TEXT,
                sha256      TEXT,
                size        INTEGER,
                fetched_at  TEXT,
                revision    TEXT,
                source_url  TEXT,
                content     TEXT,
                PRIMARY KEY (repo_type, owner, name, path)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                repo_type   TEXT,
                owner       TEXT,
                name        TEXT,
                path        TEXT,
                chunk_index INTEGER,
                chunk_sha256 TEXT,
                content     TEXT,
                length      INTEGER,
                embedding   TEXT,
                created_at  TEXT,
                PRIMARY KEY (repo_type, owner, name, path, chunk_index)
            )
            """
        )
        try:
            await conn.execute("ALTER TABLE chunks ADD COLUMN embedding TEXT")
        except aiosqlite.OperationalError:
            pass
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS repo_state (
                repo_type TEXT,
                owner     TEXT,
                name      TEXT,
                revision  TEXT,
                PRIMARY KEY (repo_type, owner, name)
            )
            """
        )
        await conn.commit()


def batched(iterable: list[str], batch_size: int) -> list[list[str]]:
    return [iterable[i : i + batch_size] for i in range(0, len(iterable), batch_size)]


def _pick_model_revision(record: dict[str, object]) -> Optional[str]:
    for key in (
        "Revision",
        "RepoRevision",
        "LatestRevision",
        "LastCommitId",
        "LastModifiedTime",
        "LastUpdatedTime",
        "GmtModified",
        "UpdatedAt",
    ):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _pick_dataset_revision(record: Optional[dict[str, object]]) -> Optional[str]:
    if not record:
        return None
    for key in ("Revision", "GmtModified", "UpdatedAt"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return None


async def embed_texts(client: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    payload = {
        "model": EMBEDDING_MODEL,
        "input": texts,
        "encoding_format": "float",
    }
    await EMBEDDING_LIMITER.acquire()
    try:
        resp = await client.post(
            EMBEDDING_API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {EMBEDDING_API_KEY}"},
            timeout=EMBEDDING_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        embeddings = [item["embedding"] for item in data["data"]]
    except Exception:  # noqa: BLE001
        await EMBEDDING_LIMITER.fail()
        raise
    else:
        await EMBEDDING_LIMITER.succeed()
        return embeddings




async def fetch_documents(
    client: ModelScopeClient,
    doc_queue: asyncio.Queue,
    db_path: Path,
    max_models: Optional[int],
    max_datasets: Optional[int],
) -> None:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT repo_type, owner, name, revision FROM repo_state"
        )
        rows = await cursor.fetchall()
    repo_cache: dict[tuple[str, str, str], Optional[str]] = {
        (row["repo_type"], row["owner"], row["name"]): row["revision"]
        for row in rows
    }
    debug(f"fetch bootstrap: cached {len(repo_cache)} entries")

    async def queue_doc(doc: Document) -> None:
        await doc_queue.put(doc)

    queued_models = skipped_models = 0
    async for model in client.iter_models(limit=max_models):
        owner = model.get("Path")
        name = model.get("Name")
        if not owner or not name:
            continue
        revision = _pick_model_revision(model)
        key = ("model", owner, name)
        if repo_cache.get(key) == revision and revision is not None:
            skipped_models += 1
            continue
        readme, source_url = await client.fetch_summary_fallback("model", owner, name)
        readme = readme.strip()
        if not readme:
            continue
        encoded = readme.encode("utf-8")
        if len(encoded) > MAX_TEXT_BYTES:
            continue
        sha = sha256_text(readme)
        doc = Document(
            repo_type="model",
            owner=owner,
            name=name,
            path="README.md",
            content=readme,
            size=len(encoded),
            sha256=sha,
            revision=(revision or sha),
            source_url=source_url,
        )
        repo_cache[key] = doc.revision
        queued_models += 1
        if queued_models % 100 == 0:
            debug(f"queued models: {queued_models} (skipped {skipped_models})")
        await queue_doc(doc)
    debug(f"fetch models: queued={queued_models}, skipped={skipped_models}")

    queued_datasets = skipped_datasets = 0
    async for dataset in client.iter_datasets(limit=max_datasets):
        owner = dataset.get("Namespace") or dataset.get("Owner")
        name = dataset.get("Name")
        if not owner or not name:
            continue
        detail = await client.fetch_dataset_detail(owner, name)
        revision = _pick_dataset_revision(detail)
        key = ("dataset", owner, name)
        if repo_cache.get(key) == revision and revision is not None:
            skipped_datasets += 1
            continue
        content = ""
        source_url = f"{client.endpoint}/datasets/{owner}/{name}"
        if detail and isinstance(detail.get("ReadmeContent"), str) and detail["ReadmeContent"].strip():
            content = detail["ReadmeContent"].strip()
        else:
            fallback = await client.fetch_summary_fallback("dataset", owner, name)
            content = fallback[0]
            source_url = fallback[1]
        content = content.strip()
        if not content:
            continue
        encoded = content.encode("utf-8")
        if len(encoded) > MAX_TEXT_BYTES:
            continue
        sha = sha256_text(content)
        doc = Document(
            repo_type="dataset",
            owner=owner,
            name=name,
            path="README.md",
            content=content,
            size=len(encoded),
            sha256=sha,
            revision=(revision or sha),
            source_url=source_url,
        )
        repo_cache[key] = doc.revision
        queued_datasets += 1
        if queued_datasets % 100 == 0:
            debug(f"queued datasets: {queued_datasets} (skipped {skipped_datasets})")
        await queue_doc(doc)
    debug(f"fetch datasets: queued={queued_datasets}, skipped={skipped_datasets}")

async def ingestor(db_path: Path,
                   doc_queue: asyncio.Queue,
                   chunk_queue: asyncio.Queue,
                   run_id: str,
                   db_lock: asyncio.Lock) -> dict[str, int]:
    stats = {"insert": 0, "update": 0, "skip": 0, "error": 0}
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=3000")
        while True:
            doc = await doc_queue.get()
            if doc is None:
                await chunk_queue.put(None)
                break
            try:
                key = doc.key()
                async with db_lock:
                    cursor = await conn.execute(
                        "SELECT rowid, sha256 FROM docs WHERE repo_type=? AND owner=? AND name=? AND path=?",
                        key
                    )
                    row = await cursor.fetchone()
                    if row and row["sha256"] == doc.sha256:
                        stats["skip"] += 1
                        continue
                    if row:
                        await conn.execute(
                            "UPDATE docs SET sha256=?, size=?, fetched_at=?, revision=?, source_url=?, content=? "
                            "WHERE repo_type=? AND owner=? AND name=? AND path=?",
                            (
                                doc.sha256,
                                doc.size,
                                ModelScopeClient.now_utc(),
                                doc.revision,
                                doc.source_url,
                                doc.content,
                                doc.repo_type,
                                doc.owner,
                                doc.name,
                                doc.path,
                            )
                        )
                        action = "update"
                    else:
                        await conn.execute(
                            "INSERT INTO docs (repo_type, owner, name, path, sha256, size, fetched_at, revision, source_url, content) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                doc.repo_type,
                                doc.owner,
                                doc.name,
                                doc.path,
                                doc.sha256,
                                doc.size,
                                ModelScopeClient.now_utc(),
                                doc.revision,
                                doc.source_url,
                                doc.content,
                            )
                        )
                        action = "insert"
                    await conn.commit()
                stats[action] += 1
                await chunk_queue.put(doc)
            except Exception as exc:  # noqa: BLE001
                debug(f"ingestor error: {exc}")
                stats["error"] += 1
        return stats


async def chunk_worker(db_path: Path,
                       chunk_queue: asyncio.Queue,
                       run_id: str,
                       db_lock: asyncio.Lock) -> dict[str, int]:
    stats = {"chunked": 0, "chunk_failed": 0}
    chroma_client: PersistentClient = PersistentClient(path=str(CHROMA_PATH))
    collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)
    async with aiosqlite.connect(db_path) as conn, httpx.AsyncClient(
        timeout=EMBEDDING_TIMEOUT,
        headers={"Authorization": f"Bearer {EMBEDDING_API_KEY}"},
    ) as embed_client:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA busy_timeout=3000")
        pending: list[
            tuple[
                Document,
                list[tuple[str, str, str, str, int, str, str, int, str, str]],
                list[list[float]],
                list[str],
                list[str],
                list[dict[str, object]],
                str,
            ]
        ] = []

        async def flush_pending() -> None:
            if not pending:
                return
            total_rows = sum(len(entry[1]) for entry in pending)
            async with db_lock:
                write_start = time.perf_counter()
                try:
                    await conn.execute("BEGIN")
                    for doc, chunk_rows, _, _, _, _, log_time in pending:
                        await conn.execute(
                            "DELETE FROM chunks WHERE repo_type=? AND owner=? AND name=? AND path=?",
                            (doc.repo_type, doc.owner, doc.name, doc.path),
                        )
                        if chunk_rows:
                            await conn.executemany(
                                "INSERT INTO chunks (repo_type, owner, name, path, chunk_index, chunk_sha256, content, length, embedding, created_at) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                chunk_rows,
                            )
                        revision_value = doc.revision or doc.sha256
                        await conn.execute(
                            "INSERT OR REPLACE INTO repo_state (repo_type, owner, name, revision) VALUES (?, ?, ?, ?)",
                            (doc.repo_type, doc.owner, doc.name, revision_value),
                        )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise
            write_duration = time.perf_counter() - write_start
            debug(f"flush {len(pending)} docs / {total_rows} chunks in {write_duration:.2f}s")
            for doc, _, embeddings, ids, documents, metadatas, _ in pending:
                collection.upsert(
                    ids=ids,
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas,
                )
            pending.clear()

        while True:
            doc = await chunk_queue.get()
            if doc is None:
                await flush_pending()
                break
            chunks = split_text(doc.content)
            try:
                embed_start = time.perf_counter()
                batches = list(batched(chunks, EMBEDDING_BATCH))
                batch_results = await asyncio.gather(
                    *(embed_texts(embed_client, batch) for batch in batches)
                )
                embeddings: list[list[float]] = [vec for group in batch_results for vec in group]
                embed_duration = time.perf_counter() - embed_start
                if len(embeddings) != len(chunks):
                    raise RuntimeError("embedding count mismatch")
                cap, avail = EMBEDDING_LIMITER.snapshot()
                debug(f"embed {len(chunks)} chunks in {embed_duration:.2f}s (cap={cap}, avail={avail})")
                ids: list[str] = []
                metadatas: list[dict[str, object]] = []
                documents: list[str] = []
                chunk_rows: list[tuple[str, str, str, str, int, str, str, int, str, str]] = []
                timestamp = ModelScopeClient.now_utc()
                for idx, (chunk_text, embedding) in enumerate(zip(chunks, embeddings, strict=True)):
                    chunk_sha = sha256_text(chunk_text)
                    chunk_rows.append(
                        (
                            doc.repo_type,
                            doc.owner,
                            doc.name,
                            doc.path,
                            idx,
                            chunk_sha,
                            chunk_text,
                            len(chunk_text),
                            json.dumps(embedding),
                            timestamp,
                        )
                    )
                    chunk_id = f"{doc.repo_type}:{doc.owner}:{doc.name}:{doc.path}:{idx}"
                    ids.append(chunk_id)
                    documents.append(chunk_text)
                    metadatas.append(
                        {
                            "repo_type": doc.repo_type,
                            "owner": doc.owner,
                            "name": doc.name,
                            "path": doc.path,
                            "chunk_index": idx,
                            "sha256": doc.sha256,
                            "fetched_at": timestamp,
                        }
                    )
                pending.append((doc, chunk_rows, embeddings, ids, documents, metadatas, timestamp))
                if len(pending) >= CHUNK_FLUSH_THRESHOLD:
                    await flush_pending()
                stats["chunked"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["chunk_failed"] += 1
        return stats



async def periodic_checkpoint(db_path: Path, interval: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            async with aiosqlite.connect(db_path) as conn:
                await conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                await conn.commit()
        except Exception:  # noqa: BLE001
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def run_pipeline(endpoint: str,
                       db_path: Path,
                       max_models: Optional[int],
                       max_datasets: Optional[int],
                       checkpoint_interval: int) -> None:
    await ensure_schema(db_path)
    doc_queue: asyncio.Queue = asyncio.Queue(maxsize=DOC_QUEUE_SIZE)
    chunk_queue: asyncio.Queue = asyncio.Queue(maxsize=CHUNK_QUEUE_SIZE)
    db_lock = asyncio.Lock()
    run_id = uuid.uuid4().hex

    async with ModelScopeClient(endpoint=endpoint) as client:
        fetch_task = asyncio.create_task(fetch_documents(client, doc_queue, db_path, max_models, max_datasets))
        ingestor_task = asyncio.create_task(ingestor(db_path, doc_queue, chunk_queue, run_id, db_lock))
        chunk_tasks = [asyncio.create_task(chunk_worker(db_path, chunk_queue, run_id, db_lock)) for _ in range(CHUNK_WORKERS)]
        stop_event = asyncio.Event()
        checkpoint_task = asyncio.create_task(periodic_checkpoint(db_path, checkpoint_interval, stop_event))

        await fetch_task
        await doc_queue.put(None)
        ingest_stats = await ingestor_task
        for _ in range(CHUNK_WORKERS):
            await chunk_queue.put(None)
        chunk_stats_list = await asyncio.gather(*chunk_tasks)
        stop_event.set()
        await checkpoint_task

    chunked = sum(stats.get("chunked", 0) for stats in chunk_stats_list)
    chunk_failed = sum(stats.get("chunk_failed", 0) for stats in chunk_stats_list)

    inserted = ingest_stats.get("insert", 0)
    updated = ingest_stats.get("update", 0)
    skipped = ingest_stats.get("skip", 0)
    errors = ingest_stats.get("error", 0)
    print(f"Run {run_id} complete")
    print(f"  inserted: {inserted}")
    print(f"  updated: {updated}")
    print(f"  skipped: {skipped}")
    print(f"  ingestion errors: {errors}")
    print(f"  chunked: {chunked}")
    print(f"  chunk errors: {chunk_failed}")
    summary = {
        "run_id": run_id,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "ingestion_errors": errors,
        "chunked": chunked,
        "chunk_errors": chunk_failed,
    }
    print(json.dumps(summary, ensure_ascii=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ModelScope documentation ingestion pipeline")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--max-models", type=int, default=None)
    parser.add_argument("--max-datasets", type=int, default=None)
    parser.add_argument("--checkpoint-interval", type=int, default=DEFAULT_CHECKPOINT_INTERVAL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_pipeline(
        endpoint=args.endpoint,
        db_path=args.database,
        max_models=args.max_models,
        max_datasets=args.max_datasets,
        checkpoint_interval=args.checkpoint_interval,
    ))


if __name__ == "__main__":
    main()
