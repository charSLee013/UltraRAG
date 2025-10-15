from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion_pipeline.embed.adapters import get_default_embed_fn  # noqa: E402
from ingestion_pipeline.limits import PipelineRuntimeLimits  # noqa: E402
from ingestion_pipeline.runner import IngestionRunner  # noqa: E402
from ingestion_pipeline.sources.datasets_pipeline import ModelScopeDatasetsPipeline  # noqa: E402
from ingestion_pipeline.stores.chroma import ChromaStore  # noqa: E402
from ingestion_pipeline.stores.ingestor import SqliteChromaIngestor  # noqa: E402
from ingestion_pipeline.stores.sqlite import SQLiteStore  # noqa: E402


load_dotenv()


def _ensure_embedding_env() -> None:
    required = ("EMBEDDING_API_URL", "EMBEDDING_API_KEY", "EMBEDDING_MODEL")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError("Missing embedding configuration: " + ", ".join(missing))


async def _run() -> None:
    _ensure_embedding_env()

    sqlite_store = SQLiteStore()
    chroma_store = ChromaStore()
    ingestor = SqliteChromaIngestor(sqlite_store, chroma_store)

    # Optional cap via env: MODELSCOPE_DATASETS_TARGET
    target_raw = os.environ.get("MODELSCOPE_DATASETS_TARGET")
    target_count = None
    if target_raw:
        try:
            v = int(target_raw)
            if v >= 1:
                target_count = v
        except Exception:
            target_count = None

    pipeline = ModelScopeDatasetsPipeline(
        dataset_page_size=100,
        timeout=60.0,
        target_repo_count=target_count,
    )
    limits = PipelineRuntimeLimits(max_workers=8, max_embed_concurrency=8, ingest_batch_size=8, chunk_max_size=32768)
    runner = IngestionRunner(
        pipeline=pipeline,
        limits=limits,
        embed_func=get_default_embed_fn(),
        fetch_force=False,
        ingest_func=ingestor.ingest,
        ingest_many_func=ingestor.ingest_many,
    )

    metrics = await runner.run()
    print("Ingestion summary:")
    print(f"  repos_ingested   : {metrics.total_in}")
    print(f"  chunks_created   : {metrics.total_out}")


def main() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.default_int_handler)

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    asyncio.run(_run())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[fatal] {exc}", file=sys.stderr)
        sys.exit(1)
