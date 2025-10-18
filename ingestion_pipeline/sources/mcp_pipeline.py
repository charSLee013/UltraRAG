from __future__ import annotations

"""
ModelScope MCP Servers ingestion pipeline.

Implements the approved SOP for MCP source:
1) SourceType=MCP; owner_repo from id (strip leading '@'), repo_id=f"mcp:{owner_repo}"
2) Description selection: top-level `description` > `locales.zh.description` > `locales.en.description`;
   skip if empty or length < 7
3) Filter: keep only items with view_count > 100; log skipped reasons, continue
4) Fetch via PUT /openapi/v1/mcp/servers with pagination (page_size<=100), no per-item concurrency,
   add slight jitter between pages
5) Dedupe using Runner-provided existing_hashes ∪ per-page seen_hashes; content_hash is sha256(description)
6) Process: structure-aware split (usually 1 chunk) → embed → assemble ChunkRecord (uuid5)
7) Ingest delegated to SqliteChromaIngestor via IngestionRunner
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import (
    ChunkDraft,
    ChunkRecord,
    RawDocument,
    SourceLocator,
    SourceType,
)
from ..limits import PipelineRuntimeLimits
from ..modelscope_headers import build_user_agent


load_dotenv()
logger = logging.getLogger("ingestion.sources.modelscope_mcp")


class ModelScopeMCPPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.MCP

    def __init__(
        self,
        *,
        page_size: int = 100,
        timeout: float | None = 60.0,
        target_repo_count: Optional[int] = None,
    ) -> None:
        self.page_size = max(1, min(int(page_size or 100), 100))
        self.timeout = float(timeout or 60.0)
        self.jitter_range = (0.2, 1.5)
        self.target_repo_count = int(target_repo_count) if target_repo_count else None

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or "https://modelscope.cn"
        self._endpoint = endpoint.rstrip("/")

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        page_number = 1
        total_pages: Optional[int] = None
        produced = 0
        url = f"{self._endpoint}/openapi/v1/mcp/servers"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while True:
                await asyncio.sleep(random.uniform(*self.jitter_range))
                body = {
                    "filter": {},
                    "page_number": page_number,
                    "page_size": self.page_size,
                    "search": "",
                }
                # Per-request rotating headers per SOP (UA + X-Request-ID)
                attempt = 0
                max_retries = 4
                last_exc: Optional[Exception] = None
                payload: Optional[Dict[str, object]] = None
                while True:
                    headers = {
                        "User-Agent": build_user_agent(),
                        "X-Request-ID": uuid.uuid4().hex,
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    }
                    try:
                        resp = await client.put(url, headers=headers, json=body)
                        # retry on 403/429/5xx
                        if resp.status_code in {403, 429, 500, 502, 503, 504}:
                            raise httpx.HTTPStatusError("server busy", request=resp.request, response=resp)
                        resp.raise_for_status()
                        payload = resp.json()
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc
                        if attempt >= max_retries - 1:
                            logger.warning(
                                "[mcp.fetch] request failed page=%s err=%s", page_number, exc
                            )
                            # stop fetching further pages but keep previously yielded pages
                            break
                        await asyncio.sleep(min(8, 2 ** attempt))
                        attempt += 1
                if payload is None:
                    break

                data = payload.get("data") or {}
                servers = data.get("mcp_server_list") or []
                if total_pages is None:
                    total_count = int(data.get("total_count") or 0)
                    total_pages = (total_count + self.page_size - 1) // self.page_size if total_count else None

                page_docs: List[RawDocument] = []
                skipped_view = skipped_desc = 0

                for item in servers:
                    # Extract fields
                    sid = item.get("id") or ""
                    if not isinstance(sid, str) or not sid.startswith("@"):
                        continue
                    owner_repo = sid[1:].strip()
                    if not owner_repo or "/" not in owner_repo:
                        continue

                    view_count = item.get("view_count")
                    try:
                        view_count = int(view_count)
                    except Exception:
                        view_count = 0
                    if view_count <= 100:
                        skipped_view += 1
                        continue

                    desc = self._select_description(item)
                    if not desc or len(desc.strip()) < 7:
                        skipped_desc += 1
                        continue

                    repo_id = f"mcp:{owner_repo}"
                    content_hash = hashlib.sha256(desc.strip().encode("utf-8")).hexdigest()
                    if content_hash in existing_set or content_hash in seen_hashes:
                        continue

                    source_url = f"https://modelscope.cn/mcp/servers/@{owner_repo}"
                    payload_dict = self._build_payload_dict(owner_repo, source_url, desc)
                    raw_payload = json.dumps(payload_dict, ensure_ascii=False, sort_keys=True)

                    page_docs.append(
                        RawDocument(
                            locator=SourceLocator(
                                source_type=SourceType.MCP,
                                owner_repo=owner_repo,
                                source_url=source_url,
                            ),
                            repo_id=repo_id,
                            payload=raw_payload,
                            fetched_at=datetime.now(timezone.utc),
                            content_hash=content_hash,
                        )
                    )
                    seen_hashes.add(content_hash)
                    produced += 1
                    if self.target_repo_count is not None and produced >= self.target_repo_count:
                        # reached cap for this run; stop collecting further items in this page
                        break

                logger.info(
                    "[mcp.fetch] page=%s items=%s succeeded=%s skipped_view<=100=%s skipped_desc<7=%s",
                    page_number,
                    len(servers),
                    len(page_docs),
                    skipped_view,
                    skipped_desc,
                )
                if page_docs:
                    yield page_docs
                    if self.target_repo_count is not None and produced >= self.target_repo_count:
                        return

                page_number += 1
                if total_pages is not None and page_number > total_pages:
                    break
                if not servers:  # safety: stop when empty page
                    break

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        readme_info = payload.get("readme") if isinstance(payload, dict) else None
        if not isinstance(readme_info, dict):
            return []
        clean_text = readme_info.get("text_clean") or ""
        if not clean_text.strip():
            return []

        effective_chunk_size = int(chunk_max_size or 32768)
        if effective_chunk_size <= 0:
            effective_chunk_size = 32768

        segments = self._structure_aware_chunks(clean_text, effective_chunk_size)
        if not segments:
            return []

        drafts: List[ChunkDraft] = []
        for idx, segment in enumerate(segments):
            drafts.append(
                ChunkDraft(
                    locator=raw.locator,
                    repo_id=raw.repo_id,
                    content_hash=raw.content_hash,
                    index=idx,
                    text=segment,
                )
            )

        embedded = await embed(drafts)
        if len(embedded) != len(drafts):
            raise RuntimeError(
                f"Embedding length mismatch for {raw.repo_id}: drafts={len(drafts)} embeddings={len(embedded)}"
            )

        records: List[ChunkRecord] = []
        for item in embedded:
            d = item.chunk
            chunk_uuid = self._make_chunk_uuid(raw.repo_id, raw.content_hash, d.index)
            records.append(
                ChunkRecord(
                    chunk_uuid=chunk_uuid,
                    repo_id=raw.repo_id,
                    content_hash=raw.content_hash,
                    chunk_index=d.index,
                    text=d.text,
                    locator=d.locator,
                    embedding=item.embedding,
                    fetched_at=raw.fetched_at,
                )
            )
        return records

    async def ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:  # pragma: no cover
        raise NotImplementedError(
            "Ingest stage delegated to SqliteChromaIngestor via IngestionRunner."
        )

    def _select_description(self, item: Dict[str, object]) -> str:
        # Order: top-level description → locales.zh.description → locales.en.description
        desc = item.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc
        locales = item.get("locales") if isinstance(item.get("locales"), dict) else None
        if isinstance(locales, dict):
            zh = locales.get("zh") if isinstance(locales.get("zh"), dict) else None
            if zh and isinstance(zh.get("description"), str) and zh["description"].strip():
                return zh["description"]
            en = locales.get("en") if isinstance(locales.get("en"), dict) else None
            if en and isinstance(en.get("description"), str) and en["description"].strip():
                return en["description"]
        return ""

    def _build_payload_dict(self, owner_repo: str, source_url: str, readme_text: str) -> Dict[str, object]:
        clean_text = self._clean_text(readme_text)
        return {
            "owner_repo": owner_repo,
            "source_url": source_url,
            "readme": {
                "url": source_url,
                "text_raw": readme_text,
                "text_clean": clean_text,
                "clean_state": "blanklines_stripped",
            },
        }

    def _make_chunk_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        seed = f"{repo_id}:{content_hash}:{index}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    def _clean_text(self, text: str) -> str:
        lines = []
        for line in (text or "").splitlines():
            s = line.strip()
            if s:
                lines.append(s)
        return "\n".join(lines)

    def _structure_aware_chunks(self, text: str, max_size: int) -> List[str]:
        text = (text or "").strip()
        if not text:
            return []
        if len(text) <= max_size:
            return [text]

        # For short descriptions, treat as single section; fallback to size-based split
        lines = text.splitlines()
        chunks: List[str] = []
        buffer: List[str] = []
        buffer_len = 0

        def flush_buffer() -> None:
            nonlocal buffer, buffer_len
            if buffer:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0

        for line in lines:
            line_len = len(line)
            add_len = line_len if not buffer else line_len + 1
            if buffer_len + add_len <= max_size:
                buffer.append(line)
                buffer_len += add_len
            else:
                flush_buffer()
                if line_len <= max_size:
                    buffer.append(line)
                    buffer_len = line_len
                else:
                    # Cut long line into fixed-size chunks
                    for start in range(0, line_len, max_size):
                        chunks.append(line[start : start + max_size])
        flush_buffer()
        return chunks
