from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType


load_dotenv()


logger = logging.getLogger("ingestion.sources.modelscope_learn")


@dataclass(frozen=True)
class _ArticleBrief:
    id: int
    title: Optional[str]


class ModelScopeLearnPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.LEARN

    def __init__(
        self,
        *,
        page_size: int = 18,
        timeout: float | None = 60.0,
        target_repo_count: Optional[int] = None,
    ) -> None:
        self.page_size = max(1, int(page_size or 18))
        self.timeout = float(timeout or 60.0)
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.jitter_range = (1.0, 8.0)  # per approved spec

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        produced = 0

        async with httpx.AsyncClient(timeout=self.timeout) as http:
            # First page to infer total
            first = await self._list_page(http, 1)
            total = int((first.get("Data") or {}).get("TotalCount") or 0)
            pages = math.ceil(total / self.page_size) if total else 0

            for page in range(1, (pages or 1) + 1):
                await asyncio.sleep(random.uniform(*self.jitter_range))
                data = first if page == 1 else await self._list_page(http, page)
                items = (data.get("Data") or {}).get("Articles") or []

                briefs: List[_ArticleBrief] = []
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    art_id = it.get("Id")
                    try:
                        art_id = int(art_id)
                    except Exception:
                        continue
                    briefs.append(_ArticleBrief(id=art_id, title=(it.get("Title") or None)))

                # Page-level concurrency for details
                sem = asyncio.Semaphore(16)

                async def build(brief: _ArticleBrief) -> Optional[RawDocument]:
                    async with sem:
                        detail = await self._detail(http, brief.id)
                        if not detail:
                            return None
                        article = (detail.get("Data") or {}).get("Articles") or []
                        if not article or not isinstance(article[0], dict):
                            return None
                        a = article[0]
                        title = (a.get("Title") or brief.title or "").strip()
                        desc = a.get("Desc") or ""
                        plain = a.get("PlainContent") or ""
                        text_source = "PlainContent"
                        text = str(plain or "").strip()
                        if len(text) < 7:
                            content_raw = a.get("Content")
                            if isinstance(content_raw, str) and content_raw.strip():
                                text = self._parse_content_to_text(content_raw)
                                text_source = "Content"
                        text = self._clean_text_basic(text)
                        if not text or len(text) < 7:
                            return None

                        repo_id = f"learn:{brief.id}"
                        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                        if content_hash in existing_set or content_hash in seen_hashes:
                            return None

                        payload = {
                            "article": {
                                "id": brief.id,
                                "title": title,
                                "desc": desc,
                                "source": text_source,
                                "text_raw": text,
                            }
                        }
                        raw_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                        seen_hashes.add(content_hash)
                        return RawDocument(
                            locator=SourceLocator(
                                source_type=self.source_type,
                                owner_repo=f"default/{brief.id}",
                                source_url=f"https://modelscope.cn/learn/{brief.id}",
                            ),
                            repo_id=repo_id,
                            payload=raw_payload,
                            fetched_at=datetime.now(timezone.utc),
                            content_hash=content_hash,
                        )

                tasks = [asyncio.create_task(build(b)) for b in briefs]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                batch: List[RawDocument] = []
                for res in results:
                    if isinstance(res, Exception) or res is None:
                        continue
                    batch.append(res)
                    produced += 1
                    if self.target_repo_count is not None and produced >= self.target_repo_count:
                        break

                logger.info(
                    "[learn.fetch] page=%s items=%s succeeded=%s",
                    page,
                    len(items),
                    len(batch),
                )
                if batch:
                    yield batch
                if self.target_repo_count is not None and produced >= self.target_repo_count:
                    return

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        art = payload.get("article") if isinstance(payload, dict) else None
        if not isinstance(art, dict):
            return []
        text = str(art.get("text_raw") or "").strip()
        if not text:
            return []

        # Normal structure-aware chunking (as in docs/models style)
        segments = self._structure_chunks(text, int(chunk_max_size or 32768))
        if not segments:
            return []

        drafts: List[ChunkDraft] = []
        for idx, seg in enumerate(segments):
            drafts.append(
                ChunkDraft(
                    locator=raw.locator,
                    repo_id=raw.repo_id,
                    content_hash=raw.content_hash,
                    index=idx,
                    text=seg,
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
            chunk_uuid = self._make_uuid(raw.repo_id, raw.content_hash, d.index)
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

    async def _list_page(self, http: httpx.AsyncClient, page_number: int) -> Dict[str, object]:
        params = {
            "PageNumber": page_number,
            "PageSize": self.page_size,
            "Sort": "gmt_modified",
            "Type": 2,
            "IsCourse": "0,1",
        }
        resp = await http.get("https://modelscope.cn/api/v1/articles", params=params)
        # Retries handled by runner; here we raise fast
        resp.raise_for_status()
        return resp.json()

    async def _detail(self, http: httpx.AsyncClient, article_id: int) -> Dict[str, object]:
        resp = await http.get(f"https://modelscope.cn/api/v1/articles/{article_id}")
        resp.raise_for_status()
        return resp.json()

    # Utilities
    def _parse_content_to_text(self, content: str) -> str:
        # Content is usually a JSON string of a rich-text document
        try:
            obj = json.loads(content)
        except Exception:
            return content.strip()
        parts: List[str] = []

        def walk(node) -> None:
            if node is None:
                return
            if isinstance(node, str):
                s = node.strip()
                if s:
                    parts.append(s)
                return
            if isinstance(node, list):
                for x in node:
                    walk(x)
                parts.append("\n")
                return
            if isinstance(node, dict):
                for k, v in node.items():
                    # Skip obvious non-text binary blobs
                    if k in {"src", "name", "id", "width", "height", "size"}:
                        continue
                    walk(v)
                return

        walk(obj)
        text = " ".join([p for p in parts if p is not None]).replace("  ", " ")
        return self._clean_text_basic(text)

    def _clean_text_basic(self, text: str) -> str:
        out: List[str] = []
        for line in (text or "").splitlines():
            s = line.rstrip()
            if s or (out and out[-1] != ""):
                out.append(s)
        return "\n".join(out).strip()

    def _structure_chunks(self, text: str, max_size: int) -> List[str]:
        text = (text or "").strip()
        if not text:
            return []
        if len(text) <= max_size:
            return [text]
        # simple header-aware split
        sections: List[str] = []
        current: List[str] = []
        for line in text.splitlines():
            if line.startswith("#") or line.startswith("【") or line.startswith("=="):
                if current:
                    sections.append("\n".join(current))
                    current = [line]
                else:
                    current.append(line)
            else:
                current.append(line)
        if current:
            sections.append("\n".join(current))
        # merge sections to fit max_size
        chunks: List[str] = []
        buf: List[str] = []
        size = 0
        for sec in sections:
            if size + len(sec) + 1 > max_size and buf:
                chunks.append("\n".join(buf).strip())
                buf = [sec]
                size = len(sec)
            else:
                buf.append(sec)
                size += len(sec) + 1
        if buf:
            chunks.append("\n".join(buf).strip())
        return [c for c in chunks if c]

    def _make_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        import uuid as _uuid

        seed = f"{repo_id}:{content_hash}:{index}"
        return str(_uuid.uuid5(_uuid.NAMESPACE_URL, seed))

