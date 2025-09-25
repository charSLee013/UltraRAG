#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Optional
import uuid

import aiosqlite
import chromadb
from chromadb import PersistentClient
import httpx
from dotenv import load_dotenv

from modelscope_client import DEFAULT_ENDPOINT, Document, ModelScopeClient, is_textual_file

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
EMBEDDING_BATCH = int(os.environ.get("EMBEDDING_BATCH", "16"))
EMBEDDING_TIMEOUT = int(os.environ.get("EMBEDDING_TIMEOUT", "60"))


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


async def ensure_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
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
            CREATE TABLE IF NOT EXISTS sync_log (
                run_id     TEXT,
                repo_type  TEXT,
                owner      TEXT,
                name       TEXT,
                path       TEXT,
                action     TEXT,
                message    TEXT,
                logged_at  TEXT
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
        await conn.commit()


def batched(iterable: list[str], batch_size: int) -> list[list[str]]:
    return [iterable[i:i + batch_size] for i in range(0, len(iterable), batch_size)]


async def embed_texts(client: httpx.AsyncClient, texts: list[str]) -> list[list[float]]:
    payload = {
        "model": EMBEDDING_MODEL,
        "input": texts,
        "encoding_format": "float",
    }
    backoff = 1.0
    for attempt in range(5):
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
            return embeddings
        except Exception as exc:  # noqa: BLE001
            if attempt == 4:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2


async def fetch_documents(client: ModelScopeClient,
                          doc_queue: asyncio.Queue,
                          max_models: Optional[int],
                          max_datasets: Optional[int]) -> None:
    # models
    async for model in client.iter_models(limit=max_models):
        owner = model.get("Path")
        name = model.get("Name")
        if not owner or not name:
            continue
        files = await client.fetch_model_files(owner, name)
        for file in files:
            if not is_textual_file(file.path, file.size):
                continue
            if isinstance(file.size, int) and file.size > MAX_TEXT_BYTES:
                continue
            fetched = await client.fetch_model_file_content(file)
            if not fetched:
                continue
            content, source_url = fetched
            if not content.strip():
                continue
            encoded = content.encode("utf-8")
            if len(encoded) > MAX_TEXT_BYTES:
                continue
            sha = sha256_text(content)
            doc = Document(
                repo_type="model",
                owner=owner,
                name=name,
                path=file.path,
                content=content,
                size=len(encoded),
                sha256=sha,
                revision=file.revision,
                source_url=source_url or file.source_url or ""
            )
            await doc_queue.put(doc)
    # datasets
    async for dataset in client.iter_datasets(limit=max_datasets):
        owner = dataset.get("Namespace") or dataset.get("Owner")
        name = dataset.get("Name")
        if not owner or not name:
            continue
        detail = await client.fetch_dataset_detail(owner, name)
        content = ""
        source_url = f"{client.endpoint}/datasets/{owner}/{name}"
        if detail and isinstance(detail.get("ReadmeContent"), str) and detail["ReadmeContent"].strip():
            content = detail["ReadmeContent"].strip()
        else:
            fallback = await client.fetch_summary_fallback("dataset", owner, name)
            content = fallback[0]
            source_url = fallback[1]
        if not content.strip():
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
            revision=detail.get("Revision") if detail else None,
            source_url=source_url,
        )
        await doc_queue.put(doc)


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
                        await conn.execute(
                            "INSERT INTO sync_log (run_id, repo_type, owner, name, path, action, message, logged_at) "
                            "VALUES (?, ?, ?, ?, ?, 'skip', '', ?)",
                            (run_id, doc.repo_type, doc.owner, doc.name, doc.path, ModelScopeClient.now_utc())
                        )
                        await conn.commit()
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
                    await conn.execute(
                        "INSERT INTO sync_log (run_id, repo_type, owner, name, path, action, message, logged_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, '', ?)",
                        (run_id, doc.repo_type, doc.owner, doc.name, doc.path, action, ModelScopeClient.now_utc())
                    )
                    await conn.commit()
                stats[action] += 1
                await chunk_queue.put(doc)
            except Exception as exc:  # noqa: BLE001
                async with db_lock:
                    await conn.execute(
                        "INSERT INTO sync_log (run_id, repo_type, owner, name, path, action, message, logged_at) "
                        "VALUES (?, ?, ?, ?, ?, 'error', ?, ?)",
                        (run_id, doc.repo_type, doc.owner, doc.name, doc.path, str(exc), ModelScopeClient.now_utc())
                    )
                    await conn.commit()
                stats["error"] += 1
        return stats


async def chunk_worker(db_path: Path,
                       chunk_queue: asyncio.Queue,
                       run_id: str,
                       db_lock: asyncio.Lock) -> dict[str, int]:
    stats = {"chunked": 0, "chunk_failed": 0}
    chroma_client: PersistentClient = PersistentClient(path=str(CHROMA_PATH))
    collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION)
    async with aiosqlite.connect(db_path) as conn, httpx.AsyncClient(timeout=EMBEDDING_TIMEOUT, headers={"Authorization": f"Bearer {EMBEDDING_API_KEY}"}) as embed_client:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        while True:
            doc = await chunk_queue.get()
            if doc is None:
                break
            chunks = split_text(doc.content)
            try:
                embeddings: list[list[float]] = []
                for batch in batched(chunks, EMBEDDING_BATCH):
                    embeddings.extend(await embed_texts(embed_client, batch))
                if len(embeddings) != len(chunks):
                    raise RuntimeError("embedding count mismatch")
                ids = []
                metadatas = []
                documents = []
                async with db_lock:
                    await conn.execute(
                        "DELETE FROM chunks WHERE repo_type=? AND owner=? AND name=? AND path=?",
                        (doc.repo_type, doc.owner, doc.name, doc.path)
                    )
                    for idx, (chunk_text, embedding) in enumerate(zip(chunks, embeddings, strict=True)):
                        chunk_sha = sha256_text(chunk_text)
                        embedding_json = json.dumps(embedding)
                        await conn.execute(
                            "INSERT INTO chunks (repo_type, owner, name, path, chunk_index, chunk_sha256, content, length, embedding, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                doc.repo_type,
                                doc.owner,
                                doc.name,
                                doc.path,
                                idx,
                                chunk_sha,
                                chunk_text,
                                len(chunk_text),
                                embedding_json,
                                ModelScopeClient.now_utc(),
                            )
                        )
                        chunk_id = f"{doc.repo_type}:{doc.owner}:{doc.name}:{doc.path}:{idx}"
                        ids.append(chunk_id)
                        documents.append(chunk_text)
                        metadatas.append({
                            "repo_type": doc.repo_type,
                            "owner": doc.owner,
                            "name": doc.name,
                            "path": doc.path,
                            "chunk_index": idx,
                            "sha256": doc.sha256,
                            "fetched_at": ModelScopeClient.now_utc(),
                        })
                    await conn.execute(
                        "INSERT INTO sync_log (run_id, repo_type, owner, name, path, action, message, logged_at) "
                        "VALUES (?, ?, ?, ?, ?, 'chunked', '', ?)",
                        (run_id, doc.repo_type, doc.owner, doc.name, doc.path, ModelScopeClient.now_utc())
                    )
                    await conn.commit()
                collection.upsert(ids=ids, documents=documents, embeddings=embeddings, metadatas=metadatas)
                stats["chunked"] += 1
            except Exception as exc:  # noqa: BLE001
                async with db_lock:
                    await conn.execute(
                        "INSERT INTO sync_log (run_id, repo_type, owner, name, path, action, message, logged_at) "
                        "VALUES (?, ?, ?, ?, ?, 'chunk_failed', ?, ?)",
                        (run_id, doc.repo_type, doc.owner, doc.name, doc.path, str(exc), ModelScopeClient.now_utc())
                    )
                    await conn.commit()
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
        fetch_task = asyncio.create_task(fetch_documents(client, doc_queue, max_models, max_datasets))
        ingestor_task = asyncio.create_task(ingestor(db_path, doc_queue, chunk_queue, run_id, db_lock))
        chunk_task = asyncio.create_task(chunk_worker(db_path, chunk_queue, run_id, db_lock))
        stop_event = asyncio.Event()
        checkpoint_task = asyncio.create_task(periodic_checkpoint(db_path, checkpoint_interval, stop_event))

        await fetch_task
        await doc_queue.put(None)
        ingest_stats = await ingestor_task
        chunk_stats = await chunk_task
        stop_event.set()
        await checkpoint_task

    inserted = ingest_stats.get("insert", 0)
    updated = ingest_stats.get("update", 0)
    skipped = ingest_stats.get("skip", 0)
    errors = ingest_stats.get("error", 0)
    chunked = chunk_stats.get("chunked", 0)
    chunk_failed = chunk_stats.get("chunk_failed", 0)
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
