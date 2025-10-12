from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineRuntimeLimits:
    max_workers: int = 32
    max_embed_concurrency: int = 512
    chunk_max_size: int = 32768
