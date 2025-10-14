from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator, List

from ingestion_pipeline.base import BaseIngestionPipeline, EmbedFn
from ingestion_pipeline.runner import IngestionRunner
from ingestion_pipeline.limits import PipelineRuntimeLimits
from ingestion_pipeline.types import (
    ChunkDraft,
    ChunkRecord,
    EmbeddedChunk,
    RawDocument,
    SourceLocator,
    SourceType,
    StageMetrics,
)


class _StubPipeline(BaseIngestionPipeline):
    def __init__(self, docs: List[RawDocument]) -> None:
        self._docs = docs

    async def fetch(self, *, force: bool = False, **kwargs) -> AsyncIterator[RawDocument]:
        for doc in self._docs:
            yield doc

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        # Produce a single deterministic chunk per document; embeddings are mocked in ingest tests.
        draft = ChunkDraft(
            locator=raw.locator,
            repo_id=raw.repo_id,
            content_hash=raw.content_hash,
            index=0,
            text=f"text:{raw.repo_id}",
        )
        embedded = await embed([draft])
        return [
            ChunkRecord(
                chunk_uuid=f"{raw.repo_id}:{draft.index}",
                repo_id=raw.repo_id,
                content_hash=raw.content_hash,
                chunk_index=draft.index,
                text=draft.text,
                locator=raw.locator,
                embedding=embedded[0].embedding,
                fetched_at=raw.fetched_at,
            )
        ]

    async def ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:
        raise AssertionError("pipeline.ingest should not be called directly")


async def _fake_embed(drafts: List[ChunkDraft]) -> List[EmbeddedChunk]:
    return [EmbeddedChunk(chunk=d, embedding=[float(d.index)]) for d in drafts]


def test_ingestion_runner_uses_single_writer() -> None:
    docs = [
        RawDocument(
            locator=SourceLocator(
                source_type=SourceType.DATASETS,
                owner_repo=f"owner/{i}",
                source_url=f"https://example.com/{i}",
            ),
            repo_id=f"repo-{i}",
            payload="{}",
            fetched_at=datetime.now(timezone.utc),
            content_hash=f"hash-{i}",
        )
        for i in range(6)
    ]
    pipeline = _StubPipeline(docs)
    limits = PipelineRuntimeLimits(max_workers=3, max_embed_concurrency=3, chunk_max_size=1024)

    state = {"in_flight": 0, "max_parallel": 0}
    order: list[str] = []

    async def ingest_func(records: List[ChunkRecord], raw: RawDocument) -> None:
        state["in_flight"] += 1
        state["max_parallel"] = max(state["max_parallel"], state["in_flight"])
        await asyncio.sleep(0.01)
        order.append(raw.repo_id)
        state["in_flight"] -= 1

    async def run() -> StageMetrics:
        runner = IngestionRunner(
            pipeline,
            limits,
            embed_func=_fake_embed,
            ingest_func=ingest_func,
        )
        return await runner.run()

    metrics = asyncio.run(run())

    assert state["max_parallel"] == 1, "writer must process exactly one repo at a time"
    assert order == [doc.repo_id for doc in docs], "ingest order must preserve fetch order"
    assert metrics.total_in == len(docs)
    assert metrics.writer_items == len(docs)
    assert metrics.total_out == len(docs)
    assert metrics.max_queue_depth >= 1


def test_ingestion_runner_batches_use_ingest_many() -> None:
    docs = [
        RawDocument(
            locator=SourceLocator(
                source_type=SourceType.MODELS,
                owner_repo=f"owner/{i}",
                source_url=f"https://example.com/{i}",
            ),
            repo_id=f"repo-{i}",
            payload="{}",
            fetched_at=datetime.now(timezone.utc),
            content_hash=f"hash-{i}",
        )
        for i in range(5)
    ]
    pipeline = _StubPipeline(docs)
    limits = PipelineRuntimeLimits(max_workers=2, max_embed_concurrency=2, chunk_max_size=1024, ingest_batch_size=2)

    batch_log: list[list[str]] = []

    def ingest_many(items):
        batch_log.append([raw.repo_id for _, raw in items])

    async def run() -> StageMetrics:
        runner = IngestionRunner(
            pipeline,
            limits,
            embed_func=_fake_embed,
            ingest_many_func=ingest_many,
        )
        return await runner.run()

    metrics = asyncio.run(run())

    assert batch_log == [["repo-0", "repo-1"], ["repo-2", "repo-3"], ["repo-4"]]
    assert metrics.total_in == len(docs)
    assert metrics.total_out == len(docs)
    assert metrics.writer_items == len(docs)
