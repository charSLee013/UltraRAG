from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from contextlib import suppress
from typing import Optional

from dotenv import load_dotenv
from tqdm import tqdm

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion_pipeline.embed.adapters import get_default_embed_fn
from ingestion_pipeline.limits import PipelineRuntimeLimits
from ingestion_pipeline.runner import IngestionRunner
from ingestion_pipeline.sources.datasets_pipeline import ModelScopeDatasetsPipeline
from ingestion_pipeline.modelscope_client import ModelScopeClient
from ingestion_pipeline.stores.chroma import ChromaStore
from ingestion_pipeline.stores.ingestor import SqliteChromaIngestor
from ingestion_pipeline.stores.sqlite import SQLiteStore


load_dotenv()


def _ensure_embedding_env() -> None:
    required = (
        "EMBEDDING_API_URL",
        "EMBEDDING_API_KEY",
        "EMBEDDING_MODEL",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Missing embedding configuration. Set the following env keys: "
            + ", ".join(missing)
        )


async def _run() -> None:
    _ensure_embedding_env()

    target_raw = os.environ.get("MODELSCOPE_DATASETS_TARGET")
    target_count: Optional[int] = None
    if target_raw:
        with suppress(ValueError):
            parsed = int(target_raw)
            if parsed > 0:
                target_count = parsed

    sqlite_store = SQLiteStore()
    chroma_store = ChromaStore()
    ingestor = SqliteChromaIngestor(sqlite_store, chroma_store)

    # Pre-compute current counts for logging
    with sqlite_store.conn as conn:
        cur = conn.execute("SELECT COUNT(*) FROM repo WHERE repo_id LIKE 'datasets:%'")
        existing_total = cur.fetchone()[0]

    existing_content_hashes: set[str] = set()
    try:
        cur = sqlite_store.conn.execute("SELECT content_hash, repo_id FROM repo")
        for content_hash, repo_id in cur.fetchall():
            existing_content_hashes.add(content_hash or repo_id)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to enumerate existing repo ids: {exc}") from exc

    pipeline = ModelScopeDatasetsPipeline(
        target_repo_count=target_count,
        existing_content_hashes=existing_content_hashes,
    )
    embed_fn = get_default_embed_fn()
    limits = PipelineRuntimeLimits()

    progress = tqdm(desc="Datasets ingest", unit="repo", leave=True, total=target_count)
    totals = {"repos": 0, "chunks": 0}

    def ingest_with_progress(records, raw):
        try:
            ingestor.ingest(records, raw)
        except Exception as exc:  # noqa: BLE001
            logging.exception("[datasets.ingest] repo=%s failed", raw.repo_id)
            raise
        totals["repos"] += 1
        totals["chunks"] += len(records)
        progress.update(1)
        progress.set_postfix(repo=raw.locator.owner_repo, chunks=len(records))
        pipeline.register_ingested_hash(raw.content_hash)

    runner = IngestionRunner(
        pipeline,
        limits,
        embed_func=embed_fn,
        ingest_func=ingest_with_progress,
    )

    total_remote: Optional[int] = None
    async with ModelScopeClient(endpoint=os.environ.get("MODELSCOPE_ENDPOINT", "https://modelscope.cn")) as client:
        page = await client._datasets_page(1)
        data = page.get("Data") or []
        total_remote = page.get("TotalCount")
        if total_remote is None:
            total_remote = len(data)

    remaining = None
    if total_remote is not None:
        remaining = max(total_remote - len(existing_content_hashes), 0)

    logging.info("[datasets.main] target_count=%s", target_count)
    logging.info(
        "[datasets.main] existing datasets=%s (repo_id LIKE 'datasets:%%')",
        existing_total,
    )
    if total_remote is not None:
        logging.info("[datasets.main] remote_total=%s", total_remote)
    if remaining is not None:
        planned_total = remaining if target_count is None else min(remaining, target_count)
        logging.info("[datasets.main] dedupe_remaining=%s", planned_total)
        if planned_total and progress.total is None:
            progress.total = planned_total
            progress.refresh()
        if planned_total == 0:
            logging.info("[datasets.main] nothing new to ingest; still scanning for verification")
    start = time.perf_counter()
    try:
        metrics = await runner.run()
    finally:
        progress.close()
        sqlite_store.close()

    elapsed = max(time.perf_counter() - start, 1e-6)
    rate = totals["repos"] / elapsed if totals["repos"] else 0.0

    print("\nIngestion summary:")
    print(f"  repos_ingested   : {totals['repos']}")
    print(f"  chunks_created   : {totals['chunks']}")
    print(f"  elapsed_seconds  : {elapsed:.2f}")
    print(f"  repos_per_second : {rate:.2f}")
    print(f"  sqlite_path      : {sqlite_store.db_path}")
    print(f"  chroma_path      : {chroma_store.path}")
    print(f"  chroma_collection: {chroma_store.collection_name}")

    print("\nStageMetrics:")
    print(json.dumps(metrics.__dict__, indent=2, default=str))


def main() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.default_int_handler)
        
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    asyncio.run(_run())


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:  # noqa: BLE001
        print(f"[fatal] {exc}", file=sys.stderr)
        sys.exit(1)
