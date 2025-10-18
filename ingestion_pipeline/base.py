from __future__ import annotations

import abc
from typing import AsyncIterable, Awaitable, Callable, List, Set, Optional

from .types import RawDocument, ChunkDraft, EmbeddedChunk, ChunkRecord, SourceType


# Embed function signature used by pipelines and runner.
EmbedFn = Callable[[List[ChunkDraft]], Awaitable[List[EmbeddedChunk]]]


class BaseIngestionPipeline(abc.ABC):
    # 每个具体管线必须声明所属来源层（datasets/models/...）
    source_type: SourceType

    @abc.abstractmethod
    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[Set[str]] = None,
        **kwargs,
    ) -> AsyncIterable[List[RawDocument]]:
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
