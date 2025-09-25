from __future__ import annotations

import asyncio
import os
from typing import Iterable, List, Tuple

import httpx
from dotenv import load_dotenv

load_dotenv()

RERANK_API_URL = os.environ["RERANK_API_URL"]
RERANK_API_KEY = os.environ["RERANK_API_KEY"]
RERANK_MODEL = os.environ.get("RERANK_MODEL", "BAAI/bge-reranker-v2-m3")
RERANK_TIMEOUT = int(os.environ.get("RERANK_TIMEOUT", "60"))


async def rerank_async(query: str, documents: Iterable[str]) -> List[Tuple[str, float]]:
    docs = list(documents)
    if not docs:
        return []
    payload = {
        "model": RERANK_MODEL,
        "query": query,
        "documents": docs,
    }
    headers = {"Authorization": f"Bearer {RERANK_API_KEY}"}
    backoff = 1.0
    for attempt in range(5):
        try:
            async with httpx.AsyncClient(timeout=RERANK_TIMEOUT) as client:
                resp = await client.post(RERANK_API_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("data") or []
            scored = []
            for item in results:
                idx = item.get("index")
                score = item.get("score")
                if idx is None or score is None:
                    continue
                scored.append((docs[idx], float(score)))
            scored.sort(key=lambda x: x[1], reverse=True)
            return scored
        except Exception:  # noqa: BLE001
            if attempt == 4:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2
    raise RuntimeError("unreachable")


def rerank(query: str, documents: Iterable[str]) -> List[Tuple[str, float]]:
    return asyncio.run(rerank_async(query, documents))


__all__ = ["rerank", "rerank_async"]
