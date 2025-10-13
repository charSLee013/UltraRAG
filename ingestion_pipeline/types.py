from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class SourceType(str, Enum):
    MODELS = "models"
    DATASETS = "datasets"
    DOCS = "docs_overview"
    LEARN = "learn"
    STUDIOS = "studios"
    MCP = "mcp"
    AIGC = "aigc"
    GITHUB = "github"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SourceLocator:
    source_type: SourceType
    owner_repo: str
    source_url: str


@dataclass(frozen=True)
class RawDocument:
    locator: SourceLocator
    repo_id: str
    payload: str
    fetched_at: datetime
    content_hash: str


@dataclass(frozen=True)
class CleanDocument:
    locator: SourceLocator
    repo_id: str
    content_hash: str
    text: str


@dataclass(frozen=True)
class ChunkDraft:
    locator: SourceLocator
    repo_id: str
    content_hash: str
    index: int
    text: str


@dataclass(frozen=True)
class EmbeddedChunk:
    chunk: ChunkDraft
    embedding: List[float]


@dataclass
class ChunkRecord:
    chunk_uuid: str
    repo_id: str
    content_hash: str
    chunk_index: int
    text: str
    locator: SourceLocator
    embedding: List[float]
    fetched_at: Optional[datetime] = None


@dataclass
class StageMetrics:
    stage: str
    total_in: int
    total_out: int
    dropped: int = 0
    warnings: List[str] = field(default_factory=list)
    sample_locators: List[SourceLocator] = field(default_factory=list)
    embed_budget_trace: List[int] = field(default_factory=list)
    writer_items: int = 0
    max_queue_depth: int = 0
