from __future__ import annotations

import hashlib
import os
from typing import List

from ..types import ChunkDraft, EmbeddedChunk


def _hash_to_vector(text: str, dim: int = 384) -> list[float]:
    data = text.encode("utf-8")
    buf = bytearray()
    h = hashlib.sha256(data).digest()
    while len(buf) < dim * 4:
        buf.extend(h)
        h = hashlib.sha256(h + data).digest()
    vec: list[float] = []
    for i in range(dim):
        # 4 bytes → int → scale to [-1, 1]
        j = int.from_bytes(buf[4 * i : 4 * i + 4], "little", signed=False)
        vec.append(((j % 1000000) / 500000.0) - 1.0)
    return vec


_fail_budget_key = "LOCAL_EMBED_FAILS"


def _should_fail() -> bool:
    v = os.environ.get(_fail_budget_key)
    if not v:
        return False
    try:
        n = int(v)
    except ValueError:
        return False
    if n <= 0:
        return False
    os.environ[_fail_budget_key] = str(n - 1)
    return True


async def embed_local(drafts: List[ChunkDraft]) -> List[EmbeddedChunk]:
    if _should_fail():
        raise RuntimeError("local embed induced failure")
    return [EmbeddedChunk(chunk=d, embedding=_hash_to_vector(d.text)) for d in drafts]

