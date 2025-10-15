from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from modelscope.hub.api import ModelScopeConfig
import httpx

from ingestion_pipeline.base import BaseIngestionPipeline, EmbedFn
from ingestion_pipeline.types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType

load_dotenv()


class ModelScopeModelsPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.MODELS
    """Model ingestion via official ModelScope Hub API.

    - fetch: page loop (≤100), in-page concurrency + timeout, per-page yield
    - process: clean → split → embed → assemble ChunkRecord
    - ingest: delegated to Runner/SqliteChromaIngestor
    """
    def __init__(
        self,
        *,
        target_repo_count: Optional[int] = None,
        model_page_size: int = 100,
        timeout: float | None = 60.0,
    ) -> None:
        # Hard threshold: only ingest models with Stars >= 2
        self._min_stars = 2
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.timeout = float(timeout or 60.0)
        self.page_size = max(1, min(int(model_page_size or 100), 100))
        self.delay_range = (0.1, 1.5)
        self.logger = logging.getLogger("ingestion.sources.modelscope_models")

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or "https://modelscope.cn"
        self._endpoint = endpoint.rstrip("/")

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        target_successes: Optional[int] = None
        if self.target_repo_count is not None and self.target_repo_count > 0:
            target_successes = self.target_repo_count

        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        # Page 1 to derive total count
        first = await self._list_models_page_http(1)
        total = int(first.get("TotalCount") or 0)
        max_pages = (total + self.page_size - 1) // self.page_size if total else 0

        produced = 0
        page_concurrency = 16

        async def build(owner: str, name: str, raw_item: Dict[str, object]) -> Optional[RawDocument]:
            await asyncio.sleep(random.uniform(*self.delay_range))
            try:
                text, url = await asyncio.wait_for(
                    self._fetch_readme_text_http(owner, name),
                    timeout=self.timeout,
                )
            except asyncio.TimeoutError:
                return None
            except Exception:
                return None
            if not text:
                return None
            repo_canon = f"models:{owner}/{name}"
            payload = self._build_payload_dict(owner, name, raw_item, url, text)
            return RawDocument(
                locator=SourceLocator(
                    source_type=SourceType.MODELS,
                    owner_repo=f"{owner}/{name}",
                    source_url=self._build_source_url(owner, name),
                ),
                repo_id=repo_canon,
                payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                fetched_at=datetime.now(timezone.utc),
                content_hash=repo_canon,
            )

        for page in range(1, (max_pages or 1) + 1):
            data = first if page == 1 else await self._list_models_page_http(page)
            models = data.get("Models") or []
            total_entries = len(models)
            candidates: List[Tuple[str, str, Dict[str, object]]] = []
            skipped = 0
            for item in models:
                if not isinstance(item, dict):
                    continue
                owner = self._extract_owner(item)
                name = self._extract_name(item)
                if not owner or not name:
                    continue
                content_hash = f"models:{owner}/{name}"
                if content_hash in existing_set or content_hash in seen_hashes:
                    skipped += 1
                    continue
                candidates.append((owner, name, item))

            self.logger.info(
                "[models.fetch] page=%s entries=%s candidates=%s skipped=%s",
                page,
                total_entries,
                len(candidates),
                skipped,
            )

            if not candidates:
                if max_pages and page >= max_pages:
                    break
                continue

            sem = asyncio.Semaphore(page_concurrency)

            async def run_task(owner: str, name: str, raw_item: Dict[str, object]) -> Optional[RawDocument]:
                async with sem:
                    return await build(owner, name, raw_item)

            tasks = [asyncio.create_task(run_task(o, n, it)) for o, n, it in candidates]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            page_docs: List[RawDocument] = []
            for res, (o, n, _it) in zip(results, candidates):
                if isinstance(res, Exception) or res is None:
                    continue
                page_docs.append(res)
                produced += 1
                seen_hashes.add(res.content_hash)
                if target_successes is not None and produced >= target_successes:
                    break

            self.logger.info("[models.fetch] page=%s succeeded=%s", page, len(page_docs))
            if page_docs:
                yield page_docs
                if target_successes is not None and produced >= target_successes:
                    return
            if max_pages and page >= max_pages:
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
            self.logger.debug(
                "[modelscope.process] repo=%s has no readme data; skipping",
                raw.repo_id,
            )
            return []

        clean_text = readme_info.get("text_clean") or ""
        if not clean_text.strip():
            self.logger.debug(
                "[modelscope.process] repo=%s readme empty after cleaning; skipping",
                raw.repo_id,
            )
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

        embedded_chunks = await embed(drafts)
        if len(embedded_chunks) != len(drafts):
            raise RuntimeError(
                f"Embedding length mismatch for {raw.repo_id}: drafts={len(drafts)} embeddings={len(embedded_chunks)}"
            )

        records: List[ChunkRecord] = []
        for embedded in embedded_chunks:
            draft = embedded.chunk
            chunk_uuid = self._make_chunk_uuid(raw.repo_id, raw.content_hash, draft.index)
            record = ChunkRecord(
                chunk_uuid=chunk_uuid,
                repo_id=raw.repo_id,
                content_hash=raw.content_hash,
                chunk_index=draft.index,
                text=draft.text,
                locator=draft.locator,
                embedding=embedded.embedding,
                fetched_at=raw.fetched_at,
            )
            records.append(record)

        return records

    async def ingest(  # pragma: no cover - intentionally unimplemented
        self,
        records: List[ChunkRecord],
        raw: RawDocument,
    ) -> None:
        raise NotImplementedError(
            "Ingest stage delegated to SqliteChromaIngestor via IngestionRunner."
        )

    async def _fetch_readme_text_http(self, owner: str, name: str) -> Tuple[Optional[str], Optional[str]]:
        # One-off connection per call
        cookies = ModelScopeConfig.get_cookies()
        headers = {
            "User-Agent": "UltraRAG-Community-Agent/0.1",
            "X-Request-ID": uuid.uuid4().hex,
            "Accept": "application/json",
            "Connection": "close",
        }
        files_url = f"{self._endpoint}/api/v1/models/{owner}/{name}/repo/files"
        params = {"Recursive": "true"}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r1 = await client.get(files_url, headers=headers, cookies=cookies, params=params)
            r1.raise_for_status()
            payload = r1.json()
        files = (payload.get("Data", {}) or {}).get("Files", [])
        readme_path = None
        for item in files:
            if not isinstance(item, dict):
                continue
            p = str(item.get("Path") or "").strip().lower()
            if p in {"readme.md", "readme.markdown", "readme.rst", "readme.txt"}:
                readme_path = item.get("Path")
                break
        if not readme_path:
            return None, None
        headers_text = {
            "User-Agent": "UltraRAG-Community-Agent/0.1",
            "X-Request-ID": uuid.uuid4().hex,
            "Accept": "text/plain, */*",
            "Connection": "close",
        }
        content_url = f"{self._endpoint}/api/v1/models/{owner}/{name}/repo"
        params2 = {"FilePath": readme_path}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r2 = await client.get(content_url, headers=headers_text, cookies=cookies, params=params2)
            r2.raise_for_status()
            text = (r2.text or "").strip()
            if not text:
                return None, None
            if "当前模型的贡献者未提供更加详细" in text:
                return None, None
            return text, str(r2.request.url)

    def _extract_owner(self, raw_item: Dict[str, object]) -> str:
        owner = (
            raw_item.get("Path")
            or raw_item.get("Owner")
            or raw_item.get("OwnerName")
            or raw_item.get("UserName")
            or raw_item.get("Publisher")
            or raw_item.get("OrganizationName")
            or raw_item.get("User")
        )
        if not owner and isinstance(raw_item.get("Organization"), dict):
            org = raw_item["Organization"]
            owner = org.get("Name") or org.get("FullName") or org.get("DisplayName")
        if not owner:
            owner = raw_item.get("CreatedBy")
        return str(owner).strip() if isinstance(owner, str) else (owner or "")

    def _extract_name(self, raw_item: Dict[str, object]) -> str:
        name = raw_item.get("ModelName") or raw_item.get("Name")
        return str(name).strip() if isinstance(name, str) else (name or "")

    def _build_source_url(self, owner: str, name: str) -> str:
        return f"https://modelscope.cn/models/{owner}/{name}"

    async def _list_models_page_http(self, page_number: int) -> Dict[str, object]:
        url = f"{self._endpoint}/api/v1/models"
        cookies = ModelScopeConfig.get_cookies()
        headers = {
            "User-Agent": "UltraRAG-Community-Agent/0.1",
            "X-Request-ID": uuid.uuid4().hex,
            "Accept": "application/json",
            "Connection": "close",
        }
        body = {"Path": "", "PageNumber": page_number, "PageSize": self.page_size}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.put(url, headers=headers, cookies=cookies, json=body)
            resp.raise_for_status()
            data = resp.json().get("Data") or {}
        return {"Models": data.get("Models") or [], "TotalCount": int(data.get("TotalCount") or 0)}

    # Legacy sync README helper removed; single-path httpx is used

    def _build_payload_dict(
        self,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
        readme_url: Optional[str],
        readme_text: str,
    ) -> Dict[str, object]:
        clean_text = self._clean_readme_text(readme_text)
        clean_state = "blanklines_stripped"
        payload = {
            "owner": owner,
            "name": name,
            "repo_id": f"models:{owner}/{name}",
            "source_url": self._build_source_url(owner, name),
            "readme": {
                "url": readme_url,
                "text_raw": readme_text,
                "text_clean": clean_text,
                "clean_state": clean_state,
            },
            "raw": raw_item,
        }
        return payload

    # content_hash == repo_id for models; no separate helper needed

    def _make_chunk_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        seed = f"{repo_id}:{content_hash}:{index}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    def _clean_readme_text(self, text: str) -> str:
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return "\n".join(lines)

    def _structure_aware_chunks(self, text: str, max_size: int) -> List[str]:
        text = (text or "").strip()
        if not text:
            return []
        if len(text) <= max_size:
            return [text]

        sections: List[str] = []
        current: List[str] = []
        for line in text.splitlines():
            if line.startswith("#") and current:
                sections.append("\n".join(current))
                current = [line]
            else:
                current.append(line)
        if current:
            sections.append("\n".join(current))

        chunks: List[str] = []
        buffer: List[str] = []
        buffer_len = 0

        def flush_buffer():
            nonlocal buffer, buffer_len
            if buffer:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0

        for section in sections:
            section_len = len(section)
            if section_len > max_size:
                flush_buffer()
                chunks.extend(self._split_large_section(section, max_size))
                continue

            additional_len = section_len if not buffer else section_len + 1
            if buffer_len + additional_len <= max_size:
                buffer.append(section)
                buffer_len += additional_len
            else:
                flush_buffer()
                if section_len <= max_size:
                    buffer.append(section)
                    buffer_len = section_len
                else:
                    chunks.extend(self._split_large_section(section, max_size))
        flush_buffer()
        return chunks

    def _split_large_section(self, section: str, max_size: int) -> List[str]:
        lines = section.split("\n")
        chunks: List[str] = []
        buffer: List[str] = []
        buffer_len = 0

        def flush_buffer():
            nonlocal buffer, buffer_len
            if buffer:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0

        for line in lines:
            line_len = len(line)
            if line_len > max_size:
                flush_buffer()
                chunks.extend(self._split_long_line(line, max_size))
                continue

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
                    chunks.extend(self._split_long_line(line, max_size))
        flush_buffer()
        return chunks

    def _split_long_line(self, line: str, max_size: int) -> List[str]:
        if len(line) <= max_size:
            return [line]
        fragments = []
        start = 0
        while start < len(line):
            end = min(len(line), start + max_size)
            fragments.append(line[start:end])
            start = end
        return fragments
