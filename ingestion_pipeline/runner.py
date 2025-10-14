from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, List, Optional, Tuple
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
        ingest_many_func: Optional[
            Callable[[List[Tuple[List[ChunkRecord], RawDocument]]], Optional[Awaitable[None]]]
        ] = None,
    ) -> None:
        self.pipeline = pipeline
        self.limits = limits
        self.fetch_force = fetch_force
        self.fetch_kwargs = fetch_kwargs or {}
        queue_size = max(2 * limits.max_workers, 1)
        self.q_docs: asyncio.Queue[Optional[RawDocument]] = asyncio.Queue(maxsize=queue_size)
        self.q_ingest: asyncio.Queue[Optional[Tuple[List[ChunkRecord], RawDocument]]] = asyncio.Queue(
            maxsize=queue_size
        )
        self.embed_budget = max(1, limits.max_embed_concurrency)
        self.embed_sem = asyncio.Semaphore(self.embed_budget)
        self._embed_func = embed_func
        self._ingest_func = ingest_func
        self._ingest_many_func = ingest_many_func
        self._docs_produced = 0
        self._records_ingested = 0
        self._embed_trace: list[int] = [self.embed_budget]
        self._writer_items = 0
        self._max_ingest_queue = 0

    async def run(self) -> StageMetrics:
        producer = asyncio.create_task(self._produce_docs())
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.limits.max_workers)]
        writer = asyncio.create_task(self._writer())

        await producer
        await self.q_docs.join()
        for _ in workers:
            await self.q_docs.put(None)
        await asyncio.gather(*workers)

        await self.q_ingest.join()
        await self.q_ingest.put(None)
        await writer

        return StageMetrics(
            stage="run",
            total_in=self._docs_produced,
            total_out=self._records_ingested,
            embed_budget_trace=self._embed_trace,
            writer_items=self._writer_items,
            max_queue_depth=self._max_ingest_queue,
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
                await self._enqueue_ingest(records, raw)
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

    async def _enqueue_ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:
        await self.q_ingest.put((records, raw))
        size = self.q_ingest.qsize()
        if size > self._max_ingest_queue:
            self._max_ingest_queue = size

    async def _writer(self) -> None:
        batch: List[Tuple[List[ChunkRecord], RawDocument]] = []
        while True:
            item = await self.q_ingest.get()
            if item is None:
                self.q_ingest.task_done()
                await self._flush_ingest_batch(batch)
                return
            batch.append(item)
            self.q_ingest.task_done()
            if len(batch) >= self._ingest_batch_size:
                await self._flush_ingest_batch(batch)

    async def _flush_ingest_batch(self, batch: List[Tuple[List[ChunkRecord], RawDocument]]) -> None:
        if not batch:
            return
        if self._ingest_many_func is None:
            if self._ingest_func is None:
                raise RuntimeError("IngestionRunner requires an ingest function")

            async def _default_ingest_many(items: List[Tuple[List[ChunkRecord], RawDocument]]) -> None:
                for records, raw in items:
                    result = self._ingest_func(records, raw)
                    if asyncio.iscoroutine(result):
                        await result

            self._ingest_many_func = _default_ingest_many

        t0 = time.perf_counter()
        result = self._ingest_many_func(batch)
        if asyncio.iscoroutine(result):
            await result
        elapsed = time.perf_counter() - t0
        _logger.info("[ingest] batch repos=%s elapsed=%.2fs", len(batch), elapsed)
        for records, _raw in batch:
            self._records_ingested += len(records)
            self._writer_items += 1
        batch.clear()
        self._ingest_batch_size = max(1, limits.ingest_batch_size)
