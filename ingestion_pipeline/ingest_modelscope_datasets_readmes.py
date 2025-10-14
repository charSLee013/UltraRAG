from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from contextlib import suppress
from typing import Optional, List, Tuple
import random

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
    logging.info("[datasets.plan] existing_total=%s", existing_total)

    existing_content_hashes: set[str] = set()
    try:
        cur = sqlite_store.conn.execute("SELECT content_hash, repo_id FROM repo")
        for content_hash, repo_id in cur.fetchall():
            existing_content_hashes.add(content_hash or repo_id)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to enumerate existing repo ids: {exc}") from exc

    # Planning: enumerate all remote datasets by TotalCount, build diff set, then ingest planned only
    embed_fn = get_default_embed_fn()
    limits = PipelineRuntimeLimits()

    all_repo_ids: List[Tuple[str, str]] = []
    total_remote: Optional[int] = None
    page_size = 100
    async with ModelScopeClient(endpoint=os.environ.get("MODELSCOPE_ENDPOINT", "https://modelscope.cn"), dataset_page_size=page_size) as client:
        page1 = await client._datasets_page(1)
        total_remote = page1.get("TotalCount") or 0
        max_pages = (int(total_remote) + page_size - 1) // page_size if total_remote else 0
        plan_bar = tqdm(desc="Planning datasets", unit="page", total=max_pages, leave=False)
        plan_bar.set_postfix(total=total_remote, page_size=page_size, accumulated_all=0)
        for i in range(1, max_pages + 1):
            if i == 1:
                data = page1
            else:
                # Robust retry on the same official endpoint (no fallback paths)
                attempts, backoff = 0, 1.0
                while True:
                    try:
                        data = await client._datasets_page(i)
                        break
                    except Exception as e:  # noqa: BLE001
                        attempts += 1
                        if attempts >= 6:
                            plan_bar.close()
                            raise RuntimeError(f"Failed to fetch datasets page {i}/{max_pages}: {e}") from e
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 8.0)
            lst = data.get("Data") or []
            for item in lst:
                owner = (item.get("Namespace") or item.get("Owner") or item.get("CreatedBy") or "").strip()
                name = (item.get("Name") or "").strip()
                if not owner or not name:
                    continue
                all_repo_ids.append((owner, name))
            plan_bar.update(1)
            plan_bar.set_postfix(total=total_remote, page_size=page_size, accumulated_all=len(all_repo_ids))
        plan_bar.close()

    existing_set = set(existing_content_hashes)
    planned_pairs: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for owner, name in all_repo_ids:
        repo_id = f"datasets:{owner}/{name}"
        if repo_id in seen:
            continue
        seen.add(repo_id)
        if repo_id in existing_set:
            continue
        planned_pairs.append((owner, name))
        if target_count and len(planned_pairs) >= target_count:
            break

    logging.info("[datasets.plan] planned=%s existing=%s quota=%s", len(planned_pairs), len(existing_set), target_count)

    # Planned-only pipeline mirroring models flow
    class PlannedDatasetsPipeline(ModelScopeDatasetsPipeline):
        def __init__(self, planned: List[Tuple[str, str]]):
            super().__init__(target_repo_count=len(planned) or None, existing_content_hashes=None)
            self._planned = planned

        async def fetch(self, *, force: bool = False, max_docs: Optional[int] = None, **kwargs):
            from ingestion_pipeline.modelscope_client import ModelScopeClient  # local import
            delay_min, delay_max = self.delay_range
            async with ModelScopeClient(endpoint=os.environ.get("MODELSCOPE_ENDPOINT", "https://modelscope.cn"), dataset_page_size=100, timeout=int(self.timeout)) as client:
                tasks: List[Tuple[int, str, asyncio.Task[Optional[object]]]] = []
                for idx, (owner, name) in enumerate(self._planned):
                    repo_id = f"datasets:{owner}/{name}"
                    async def _job(o=owner, n=name, rid=repo_id):
                        await asyncio.sleep(random.uniform(delay_min, delay_max))
                        text, _url = await self._fetch_readme_text(client, o, n)
                        if text is None:
                            return None
                        payload = self._build_payload_dict(o, n, None, text)
                        raw_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                        return Raw(
                            owner=o,
                            name=n,
                            repo_id=f"datasets:{o}/{n}",
                            payload=raw_payload,
                        )

                    tasks.append((idx, repo_id, asyncio.create_task(_job())))

                for idx, repo_id, task in sorted(tasks, key=lambda t: t[0]):
                    try:
                        item = await task
                    except Exception:  # noqa: BLE001
                        continue
                    if item is None:
                        continue
                    yield self._raw_from_payload(item.owner, item.name, item.repo_id, item.payload)

        def _raw_from_payload(self, owner: str, name: str, repo_id: str, raw_payload: str):
            from datetime import datetime, timezone
            from ingestion_pipeline.types import SourceLocator, SourceType, RawDocument
            return RawDocument(
                locator=SourceLocator(
                    source_type=SourceType.DATASETS,
                    owner_repo=f"{owner}/{name}",
                    source_url=f"https://modelscope.cn/datasets/{owner}/{name}",
                ),
                repo_id=repo_id,
                payload=raw_payload,
                fetched_at=datetime.now(timezone.utc),
                content_hash=repo_id,
            )

    # shim record for planned fetch
    from dataclasses import dataclass
    @dataclass
    class Raw:
        owner: str
        name: str
        repo_id: str
        payload: str

    pipeline = PlannedDatasetsPipeline(planned_pairs)

    progress = tqdm(desc="Datasets ingest", unit="repo", leave=True, total=len(planned_pairs) or target_count)
    totals = {"repos": 0, "chunks": 0}

    def ingest_with_progress(records, raw):
        try:
            ingestor.ingest(records, raw)
        except Exception:  # noqa: BLE001
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
