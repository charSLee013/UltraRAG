"""Unified ingestion pipeline package.

Exports core types, limits, and store/adapters for use by pipelines.
"""

from .types import (
    SourceType,
    SourceLocator,
    RawDocument,
    CleanDocument,
    ChunkDraft,
    EmbeddedChunk,
    ChunkRecord,
    StageMetrics,
)
from .limits import PipelineRuntimeLimits

__all__ = [
    "SourceType",
    "SourceLocator",
    "RawDocument",
    "CleanDocument",
    "ChunkDraft",
    "EmbeddedChunk",
    "ChunkRecord",
    "StageMetrics",
    "PipelineRuntimeLimits",
]

