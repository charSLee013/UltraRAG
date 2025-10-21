from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType


load_dotenv()
logger = logging.getLogger("ingestion.sources.modelscope_aigc")


class ModelScopeAIGCPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.AIGC

    def __init__(self, *, timeout: float | None = 60.0, page_size: int = 100) -> None:
        self.timeout = float(timeout or 60.0)
        self.page_size = max(1, min(int(page_size or 100), 100))

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        page = 1
        produced = 0
        url = "https://modelscope.cn/api/v1/dolphin/models"

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            # First page for total count
            base_payload = {
                "PageSize": self.page_size,
                "PageNumber": 1,
                "SortBy": "AigcDefault",
                "Target": "",
                "IsAigc": True,
                "Name": "",
                "ImgUrl": "",
                "SingleCriterion": [
                    {"category": "aigc_type", "DateType": "string", "predicate": "equal", "StringValue": "all"},
                    {"category": "vision_foundation", "DateType": "string", "predicate": "equal", "StringValue": "all"},
                ],
                "Criterion": [],
            }
            headers = {
                "User-Agent": "modelscope/1.30.0; python/3.11.9; env/custom; user/unknown",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Content-Type": "application/json",
            }

            resp = await client.put(url, headers=headers, json=base_payload)
            resp.raise_for_status()
            data = resp.json().get("Data") or {}
            model = data.get("Model") or {}
            total = int(model.get("TotalCount") or 0)
            if total <= 0:
                return
            max_pages = (total + self.page_size - 1) // self.page_size

            while page <= max_pages:
                payload = dict(base_payload)
                payload["PageNumber"] = page
                resp = await client.put(url, headers=headers, json=payload)
                resp.raise_for_status()
                content = resp.json()
                md = content.get("Data") or {}
                block = md.get("Model") or {}
                items = block.get("Models") or []

                page_docs: List[RawDocument] = []
                for item in items:
                    owner = (item.get("Organization") or {}).get("Name") or item.get("CreatedBy")
                    name = item.get("Name")
                    if not owner or not name:
                        continue

                    owner_repo = f"{owner}/{name}"
                    text = self._compose_text(item)
                    if not text.strip():
                        continue
                    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                    if content_hash in existing_set and not force:
                        continue

                    payload_dict = {
                        "aigc": {
                            "owner": owner,
                            "name": name,
                            "model_id": item.get("ModelId") or item.get("Id"),
                            "revision": item.get("Revision"),
                            "fields": {
                                "Tasks": item.get("Tasks"),
                                "AigcType": item.get("AigcType"),
                                "VisionFoundation": item.get("VisionFoundation"),
                                "TriggerWords": item.get("TriggerWords"),
                                "Description": item.get("Description"),
                            },
                            "text_raw": text,
                        }
                    }
                    raw_payload = json.dumps(payload_dict, ensure_ascii=False, sort_keys=True)
                    page_docs.append(
                        RawDocument(
                            locator=SourceLocator(
                                source_type=SourceType.AIGC,
                                owner_repo=owner_repo,
                                source_url=f"https://modelscope.cn/models/{owner}/{name}",
                            ),
                            repo_id=owner_repo,
                            payload=raw_payload,
                            fetched_at=datetime.now(timezone.utc),
                            content_hash=content_hash,
                        )
                    )
                    produced += 1

                if page_docs:
                    yield page_docs
                page += 1

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        aigc = payload.get("aigc") if isinstance(payload, dict) else None
        if not isinstance(aigc, dict):
            return []
        text_raw = aigc.get("text_raw") or ""
        clean_text = self._clean_text(text_raw)

        segments = self._structure_chunks(clean_text, int(chunk_max_size or 32768))
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

    def _compose_text(self, item: dict) -> str:
        lines: List[str] = []
        tasks = item.get("Tasks") or []
        task_cn = None
        if isinstance(tasks, list) and tasks:
            t0 = tasks[0] or {}
            if isinstance(t0, dict):
                task_cn = t0.get("ChineseName") or t0.get("Name")
        if task_cn:
            lines.append(f"Task: {task_cn}")
        if item.get("AigcType"):
            lines.append(f"AigcType: {item.get('AigcType')}")
        if item.get("VisionFoundation"):
            lines.append(f"VisionFoundation: {item.get('VisionFoundation')}")
        tw = item.get("TriggerWords") or []
        if isinstance(tw, list) and tw:
            lines.append("TriggerWords: " + ", ".join(str(x) for x in tw))
        desc = item.get("Description") or ""
        if isinstance(desc, str) and desc.strip():
            lines.append("Description: " + desc.strip())
        return "\n".join(lines).strip()

    def _clean_text(self, text: str) -> str:
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
        # simple line packing
        chunks: List[str] = []
        buf: List[str] = []
        buf_len = 0
        def flush():
            nonlocal buf, buf_len
            if buf:
                chunks.append("\n".join(buf))
                buf = []
                buf_len = 0
        for line in text.split("\n"):
            L = len(line)
            add = L if not buf else L + 1
            if buf_len + add <= max_size:
                buf.append(line)
                buf_len += add
            else:
                flush()
                buf.append(line)
                buf_len = L
        flush()
        return chunks

    def _make_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        import uuid
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{repo_id}:{content_hash}:{index}"))

