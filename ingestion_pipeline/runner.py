from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, List, Optional
import logging, time

from dotenv import load_dotenv

from .base import BaseIngestionPipeline, EmbedFn
from .limits import PipelineRuntimeLimits
from .types import RawDocument, ChunkDraft, EmbeddedChunk, ChunkRecord, StageMetrics


load_dotenv()
_logger = logging.getLogger("ingestion.runner")


class IngestionRunner:
    def __init__(
        self,
        pipeline: BaseIngestionPipeline,
        limits: PipelineRuntimeLimits,
        *,
        embed_func: EmbedFn,
        fetch_force: bool = False,
        fetch_kwargs: Optional[dict] = None,
        ingest_func: Optional[Callable[[List[ChunkRecord], RawDocument], Optional[Awaitable[None]]]] = None,
    ) -> None:
        self.pipeline = pipeline
        self.limits = limits
        self.fetch_force = fetch_force
        self.fetch_kwargs = fetch_kwargs or {}
        self.q_docs: asyncio.Queue[Optional[RawDocument]] = asyncio.Queue(
            maxsize=max(2 * limits.max_workers, 1)
        )
        self.embed_budget = max(1, limits.max_embed_concurrency)
        self.embed_sem = asyncio.Semaphore(self.embed_budget)
        self._embed_func = embed_func
        self._ingest_func = ingest_func
        self._docs_produced = 0
        self._records_ingested = 0
        self._embed_trace: list[int] = [self.embed_budget]

    async def run(self) -> StageMetrics:
        producer = asyncio.create_task(self._produce_docs())
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.limits.max_workers)]
        await producer
        await self.q_docs.join()
        for _ in workers:
            await self.q_docs.put(None)
        await asyncio.gather(*workers)
        return StageMetrics(
            stage="run",
            total_in=self._docs_produced,
            total_out=self._records_ingested,
            embed_budget_trace=self._embed_trace,
        )

    async def _produce_docs(self) -> None:
        async for raw in self.pipeline.fetch(force=self.fetch_force, **self.fetch_kwargs):
            _logger.info(
                "[fetch] queued repo=%s source=%s",
                raw.repo_id,
                raw.locator.source_url,
            )
            await self.q_docs.put(raw)
            self._docs_produced += 1

    async def _worker(self, worker_id: int) -> None:
        while True:
            raw = await self.q_docs.get()
            try:
                if raw is None:
                    return
                t0 = time.perf_counter()
                records = await self.pipeline.process(
                    raw,
                    chunk_max_size=self.limits.chunk_max_size,
                    embed=self._embed_with_limits,
                )
                _logger.info(
                    "[process] repo=%s records=%s elapsed=%.2fs",
                    raw.repo_id,
                    len(records),
                    time.perf_counter() - t0,
                )
                t1 = time.perf_counter()
                await self._call_ingest(records, raw)
                _logger.info(
                    "[ingest] repo=%s records=%s elapsed=%.2fs",
                    raw.repo_id,
                    len(records),
                    time.perf_counter() - t1,
                )
                self._records_ingested += len(records)
            finally:
                self.q_docs.task_done()

    async def _embed_with_limits(self, drafts: List[ChunkDraft]) -> List[EmbeddedChunk]:
        attempt = 0
        while True:
            async with self.embed_sem:
                try:
                    result = await self._embed_func(drafts)
                except Exception:
                    self._embed_failure()
                    await asyncio.sleep(min(8, 2 ** attempt))
                    attempt += 1
                    continue
                else:
                    self._embed_success()
                    return result

    def _embed_success(self) -> None:
        if self.embed_budget < self.limits.max_embed_concurrency:
            self.embed_budget += 1
            self.embed_sem = asyncio.Semaphore(self.embed_budget)
        self._embed_trace.append(self.embed_budget)

    def _embed_failure(self) -> None:
        self.embed_budget = max(1, self.embed_budget // 2)
        self.embed_sem = asyncio.Semaphore(self.embed_budget)
        self._embed_trace.append(self.embed_budget)

    async def _call_ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:
        func = self._ingest_func or self.pipeline.ingest
        result = func(records, raw)
        if asyncio.iscoroutine(result):
            await result
