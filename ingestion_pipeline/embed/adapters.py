from __future__ import annotations

from __future__ import annotations

import logging
import os
import time
from typing import List, Sequence

import httpx
from dotenv import load_dotenv

from ..types import ChunkDraft, EmbeddedChunk

load_dotenv()
_logger = logging.getLogger("ingestion.embed")


def _debug_enabled() -> bool:
    v = os.getenv("INGESTION_DEBUG")
    return v not in (None, "0", "false", "False")


def _env(name: str, fallback: Sequence[str] | None = None) -> str | None:
    value = os.environ.get(name)
    if value:
        return value
    if fallback:
        for alt in fallback:
            val = os.environ.get(alt)
            if val:
                return val
    return None


async def embed_httpx(drafts: List[ChunkDraft]) -> List[EmbeddedChunk]:
    """Embed texts via an HTTP endpoint described in ingestion_pipeline_sop.md."""

    # [块] 端点解析：env-first，多别名兼容；缺失直接失败
    base_url = _env(
        "EMBEDDING_API_URL",
        ("OPENAI_BASE_URL", "LLM_BASE_URL"),
    )
    api_key = _env(
        "EMBEDDING_API_KEY",
        ("OPENAI_API_KEY", "LLM_API_KEY"),
    )
    model = os.environ.get("EMBEDDING_MODEL")

    if not base_url or not api_key or not model:
        raise RuntimeError(
            "Embedding settings missing: set EMBEDDING_API_URL/KEY and EMBEDDING_MODEL"
        )

    base_url = base_url.rstrip("/")
    if base_url.endswith("/embeddings"):
        endpoint = base_url
    else:
        endpoint = f"{base_url}/embeddings"

    try:
        timeout_s = float(os.environ.get("EMBEDDING_TIMEOUT", "30"))
    except Exception:
        timeout_s = 30.0

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # [块] 请求载荷：仅发送必要字段，按需追加扩展项
    payload: dict[str, object] = {
        "model": model,
        "input": [d.text for d in drafts],
    }

    encoding_format = os.environ.get("EMBEDDING_ENCODING_FORMAT")
    if encoding_format:
        payload["encoding_format"] = encoding_format
    dimensions = os.environ.get("EMBEDDING_DIMENSIONS")
    if dimensions:
        try:
            payload["dimensions"] = int(dimensions)
        except ValueError:
            _logger.warning("[embed] invalid EMBEDDING_DIMENSIONS=%s ignored", dimensions)

    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        if _debug_enabled():
            _logger.info(
                "[embed] sending batch size=%s timeout=%s url=%s",
                len(drafts),
                timeout_s,
                endpoint,
            )
        response = await client.post(endpoint, headers=headers, json=payload)
    elapsed = time.perf_counter() - t0

    if _debug_enabled():
        _logger.info("[embed] response status=%s elapsed=%.2fs", response.status_code, elapsed)

    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or "data" not in data:
        raise RuntimeError("Embedding response missing 'data' field")
    embeddings = data["data"]
    if not isinstance(embeddings, list):
        raise RuntimeError("Embedding response 'data' is not a list")

    if len(embeddings) != len(drafts):
        raise RuntimeError(
            f"Embedding count mismatch: drafts={len(drafts)}, embeddings={len(embeddings)}"
        )

    vectors: List[List[float]] = []
    for item in embeddings:
        if not isinstance(item, dict) or "embedding" not in item:
            raise RuntimeError("Embedding item missing 'embedding'")
        vector = item["embedding"]
        if not isinstance(vector, list):
            raise RuntimeError("Embedding vector is not a list")
        vectors.append(vector)

    return [EmbeddedChunk(chunk=d, embedding=vectors[i]) for i, d in enumerate(drafts)]


def get_default_embed_fn():
    return embed_httpx
