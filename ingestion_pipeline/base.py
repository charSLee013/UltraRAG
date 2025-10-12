from __future__ import annotations

import abc
from typing import AsyncIterable, Awaitable, Callable, List

from .types import RawDocument, ChunkDraft, EmbeddedChunk, ChunkRecord


# Embed function signature used by pipelines and runner.
EmbedFn = Callable[[List[ChunkDraft]], Awaitable[List[EmbeddedChunk]]]


class BaseIngestionPipeline(abc.ABC):
    @abc.abstractmethod
    async def fetch(self, *, force: bool = False, **kwargs) -> AsyncIterable[RawDocument]:
        ...

    @abc.abstractmethod
    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        ...

    @abc.abstractmethod
    async def ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:
        ...
