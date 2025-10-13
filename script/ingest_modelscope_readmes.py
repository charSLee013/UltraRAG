from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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

    target_models = _int_env("MODELSCOPE_MODEL_SIZE", 0)
    if target_models <= 0:
        target_models = None

    sqlite_store = SQLiteStore()
    chroma_store = ChromaStore()
    ingestor = SqliteChromaIngestor(sqlite_store, chroma_store)

    existing_content_hashes: set[str] = set()
    try:
        cur = sqlite_store.conn.execute("SELECT content_hash, repo_id FROM repo")
        for content_hash, repo_id in cur.fetchall():
            existing_content_hashes.add(content_hash or repo_id)
    except Exception as exc:
        raise RuntimeError(f"Failed to enumerate existing repo ids: {exc}") from exc

    pipeline = ModelScopeModelsPipeline(
        page_size=target_models,
        existing_content_hashes=existing_content_hashes,
        prefetch_planning=True,  # preselect (ListModels - SQLite) so tqdm total is the remaining count
    )
    embed_fn = get_default_embed_fn()

    limits = PipelineRuntimeLimits()

    total_hint = pipeline.estimate_total_models()
    total_for_run: Optional[int]
    if target_models is not None and target_models > 0:
        total_for_run = target_models
        if total_hint:
            total_for_run = min(total_hint, target_models)
    else:
        total_for_run = total_hint

    progress = tqdm(desc="ModelScope ingest", unit="repo", leave=True, total=total_for_run)
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
        pipeline.register_ingested_hash(raw.content_hash)

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
