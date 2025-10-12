from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from contextlib import suppress

from dotenv import load_dotenv
from tqdm import tqdm

from ingestion_pipeline.embed.adapters import get_default_embed_fn
from ingestion_pipeline.limits import PipelineRuntimeLimits
from ingestion_pipeline.runner import IngestionRunner
from ingestion_pipeline.sources.modelscope_models import ModelScopeModelsPipeline
from ingestion_pipeline.stores.chroma import ChromaStore
from ingestion_pipeline.stores.ingestor import SqliteChromaIngestor
from ingestion_pipeline.stores.sqlite import SQLiteStore


load_dotenv()


MANDATORY_ENV_SETS: tuple[tuple[str, ...], ...] = (
    ("EMBEDDING_API_URL", "OPENAI_BASE_URL", "LLM_BASE_URL"),
    ("EMBEDDING_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY"),
    ("EMBEDDING_MODEL",),
)


def _resolve_env_group(group: tuple[str, ...]) -> str | None:
    for key in group:
        value = os.environ.get(key)
        if value:
            return value
    return None


def _ensure_embedding_env() -> None:
    missing: list[str] = []
    for group in MANDATORY_ENV_SETS:
        if not _resolve_env_group(group):
            missing.append("/".join(group))
    if missing:
        raise RuntimeError(
            "Missing embedding configuration. Set the following env keys: " + ", ".join(missing)
        )


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    with suppress(ValueError):
        return max(1, int(raw))
    return default


async def _run_ingestion() -> None:
    _ensure_embedding_env()

    page_size = _int_env("MODELSCOPE_PAGE_SIZE", 5)

    sqlite_store = SQLiteStore()
    chroma_store = ChromaStore()
    ingestor = SqliteChromaIngestor(sqlite_store, chroma_store)

    pipeline = ModelScopeModelsPipeline(page_size=page_size)
    embed_fn = get_default_embed_fn()

    limits = PipelineRuntimeLimits()

    progress = tqdm(desc="ModelScope ingest", unit="repo", leave=True)
    totals = {"repos": 0, "chunks": 0}

    def ingest_with_progress(records, raw):
        ingestor.ingest(records, raw)
        totals["repos"] += 1
        totals["chunks"] += len(records)
        progress.update(1)
        progress.set_postfix(
            repo=raw.locator.owner_repo,
            chunks=len(records),
        )

    runner = IngestionRunner(
        pipeline,
        limits,
        embed_func=embed_fn,
        ingest_func=ingest_with_progress,
    )

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
    asyncio.run(_run_ingestion())


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print(f"[fatal] {exc}", file=sys.stderr)
        sys.exit(1)
