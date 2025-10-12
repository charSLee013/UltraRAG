from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Iterable, List, Optional, Tuple

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType

load_dotenv()


class ModelScopeModelsPipeline(BaseIngestionPipeline):
    """Ingestion pipeline for models listed on ModelScope (modelscope.cn/models).

    The fetch stage queries the public `dolphin/models` endpoint with PUT
    requests, applies exponential backoff for 429/5xx responses, normalizes README
    metadata (case-insensitive resolution), and emits `RawDocument` entries.
    The process stage cleans README text, chunks it according to `chunk_max_size`,
    and invokes the provided `EmbedFn`, returning `ChunkRecord` objects without
    touching downstream stores. Ingest is intentionally left to the runner's
    `SqliteChromaIngestor`.
    """

    DEFAULT_ENDPOINT = "https://modelscope.cn/api/v1/dolphin/models"
    RETRY_STATUS = {429, 500, 502, 503, 504}
    README_CANDIDATES = (
        "README.md",
        "Readme.md",
        "readme.md",
        "README.MD",
        "README.markdown",
        "ReadMe.md",
        "Readme.MD",
    )

    def __init__(
        self,
        *,
        page_size: Optional[int] = 20,
        sort_by: str = "Default",
        target: str = "",
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        backoff_factor: float = 2.0,
        timeout: float | None = 60.0,
        existing_repo_ids: Optional[Iterable[str]] = None,
        session_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.endpoint = os.environ.get("MODELSCOPE_MODELS_API_URL", self.DEFAULT_ENDPOINT)
        if page_size is not None and page_size < 1:
            raise ValueError("page_size must be None or an integer >= 1")
        self.page_size = page_size
        self.sort_by = sort_by
        self.target = target
        self.max_retries = max(1, max_retries)
        self.initial_backoff = max(0.1, initial_backoff)
        self.backoff_factor = max(1.0, backoff_factor)
        self.timeout = timeout
        self._existing_repo_ids = set(existing_repo_ids or [])
        self._base_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": self._build_user_agent(),
            "Referer": "https://modelscope.cn/",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "Origin": "https://modelscope.cn",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "X-Requested-With": "XMLHttpRequest",
        }
        if session_headers:
            self._base_headers.update(session_headers)
        self.logger = logging.getLogger("ingestion.sources.modelscope_models")

    async def fetch(
        self,
        *,
        force: bool = False,
        page_size: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[RawDocument]:
        """Yield RawDocument entries for ModelScope models.

        Args:
            force: When False, entries whose repo_id is already present in
                `existing_repo_ids` or `skip_repo_ids` (from kwargs) are skipped.
            page_size: Optional override for the page size.
            **kwargs: Supports `skip_repo_ids` (Iterable[str]) to skip repos in
                addition to the constructor-level cache.
        """

        skip_repo_ids = set(self._existing_repo_ids)
        skip_repo_ids.update(kwargs.get("skip_repo_ids", []))

        max_docs_raw = kwargs.get("max_docs")
        max_docs: Optional[int] = None
        if max_docs_raw is not None:
            try:
                candidate = int(max_docs_raw)
            except (TypeError, ValueError):
                candidate = None
            if candidate is not None and candidate > 0:
                max_docs = candidate

        seen_repo_ids: set[str] = set()
        current_page = 1
        effective_page_size = page_size if page_size is not None else self.page_size
        yielded_total = 0

        headers = dict(self._base_headers)
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True, headers=headers) as client:
            await self._warmup_session(client)
            csrf_token = client.cookies.get("csrf_token")
            if csrf_token:
                self._base_headers["X-CSRF-TOKEN"] = csrf_token
            while True:
                payload = self._make_payload(page_number=current_page, page_size=effective_page_size)
                response_json = await self._request_page(client, payload, current_page)
                items = self._extract_model_entries(response_json)
                if not items:
                    self.logger.info(
                        "[modelscope.fetch] stopping at page=%s (no items returned)", current_page
                    )
                    break

                yielded_on_page = 0
                download_tasks: List[Tuple[int, str, asyncio.Task[Optional[RawDocument]]]] = []

                for idx, (owner, name, raw_item) in enumerate(items):
                    repo_id = f"models:{owner}/{name}"
                    if repo_id in seen_repo_ids:
                        continue
                    seen_repo_ids.add(repo_id)

                    if not force and repo_id in skip_repo_ids:
                        continue

                    locator = SourceLocator(
                        source_type=SourceType.MODELS,
                        owner_repo=f"{owner}/{name}",
                        source_url=self._build_source_url(owner, name),
                    )
                    task = asyncio.create_task(
                        self._build_raw_document(
                            client=client,
                            owner=owner,
                            name=name,
                            raw_item=raw_item,
                            locator=locator,
                            repo_id=repo_id,
                        )
                    )
                    download_tasks.append((idx, repo_id, task))

                for idx, repo_id, task in sorted(download_tasks, key=lambda entry: entry[0]):
                    try:
                        raw_document = await task
                    except Exception as exc:  # pragma: no cover - IO failure traced via logging
                        self.logger.debug(
                            "[modelscope.fetch] repo=%s readme task raised err=%s",
                            repo_id,
                            exc,
                        )
                        continue

                    if raw_document is None:
                        continue

                    yield raw_document
                    yielded_on_page += 1
                    yielded_total += 1

                    if max_docs is not None and yielded_total >= max_docs:
                        self.logger.info(
                            "[modelscope.fetch] reached max_docs=%s, stopping",
                            max_docs,
                        )
                        return

                self.logger.info(
                    "[modelscope.fetch] page=%s yielded=%s (total seen=%s)",
                    current_page,
                    yielded_on_page,
                    len(seen_repo_ids),
                )
                current_page += 1

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

    def _make_payload(self, *, page_number: int, page_size: Optional[int]) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "PageNumber": page_number,
            "SortBy": self.sort_by,
            "Target": self.target,
            "SingleCriterion": [],
            "Criterion": [],
        }
        if page_size is not None:
            payload["PageSize"] = page_size
        return payload

    def _build_user_agent(self) -> str:
        return (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )

    async def _warmup_session(self, client: httpx.AsyncClient) -> None:
        try:
            await client.get("https://modelscope.cn/", timeout=self.timeout)
        except httpx.HTTPError:
            self.logger.debug("[modelscope.fetch] warmup request failed", exc_info=True)
        try:
            await client.get("https://modelscope.cn/models", timeout=self.timeout)
        except httpx.HTTPError:
            self.logger.debug("[modelscope.fetch] warmup models request failed", exc_info=True)

    async def _build_raw_document(
        self,
        *,
        client: httpx.AsyncClient,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
        locator: SourceLocator,
        repo_id: str,
    ) -> Optional[RawDocument]:
        readme_text, readme_url = await self._fetch_readme_text(client, owner, name)
        if readme_text is None:
            self.logger.debug(
                "[modelscope.fetch] skip repo=%s (no README found)",
                repo_id,
            )
            return None

        payload_dict = self._build_payload_dict(owner, name, raw_item, readme_url, readme_text)
        raw_payload = json.dumps(payload_dict, ensure_ascii=False, sort_keys=True)
        content_hash = self._compute_content_hash(repo_id)
        return RawDocument(
            locator=locator,
            repo_id=repo_id,
            payload=raw_payload,
            fetched_at=datetime.now(timezone.utc),
            content_hash=content_hash,
        )

    async def _request_page(
        self,
        client: httpx.AsyncClient,
        payload: Dict[str, object],
        page_number: int,
    ) -> Dict[str, object]:
        backoff = self.initial_backoff
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.put(self.endpoint, headers=self._base_headers, json=payload)
            except httpx.HTTPError as exc:
                if attempt == self.max_retries:
                    raise
                wait_time = backoff * (1.0 + random.random())
                self.logger.warning(
                    "[modelscope.fetch] HTTP error on page=%s attempt=%s err=%s; retrying in %.2fs",
                    page_number,
                    attempt,
                    exc,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
                backoff *= self.backoff_factor
                continue

            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                    raise RuntimeError(
                        f"Unable to decode JSON for page {page_number}: {exc}"
                    ) from exc

            if response.status_code in self.RETRY_STATUS:
                if attempt == self.max_retries:
                    response.raise_for_status()
                wait_time = backoff * (1.0 + random.random())
                self.logger.warning(
                    "[modelscope.fetch] %s on page=%s attempt=%s; retrying in %.2fs",
                    response.status_code,
                    page_number,
                    attempt,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
                backoff *= self.backoff_factor
                continue

            response.raise_for_status()

        raise RuntimeError(f"Exhausted retries for page {page_number}")

    def _extract_model_entries(
        self,
        payload: Dict[str, object],
    ) -> List[Tuple[str, str, Dict[str, object]]]:
        container = payload
        if isinstance(container, dict):
            container = container.get("Data") or container.get("data") or container
            if isinstance(container, dict):
                container = container.get("Model") or container.get("model") or container

        items: Optional[Iterable[object]] = None
        if isinstance(container, dict):
            for key in ("Models", "List", "Items", "models", "list", "items"):
                value = container.get(key)
                if isinstance(value, list):
                    items = value
                    break
        elif isinstance(container, list):
            items = container

        result: List[Tuple[str, str, Dict[str, object]]] = []
        if not items:
            return result

        for raw_item in items:
            if not isinstance(raw_item, dict):
                continue
            owner = (
                raw_item.get("Owner")
                or raw_item.get("OwnerName")
                or raw_item.get("UserName")
                or raw_item.get("Publisher")
                or raw_item.get("OrganizationName")
                or raw_item.get("User")
                or raw_item.get("UserNickName")
            )
            if not owner:
                organization = raw_item.get("Organization")
                if isinstance(organization, dict):
                    owner = (
                        organization.get("Name")
                        or organization.get("FullName")
                        or organization.get("DisplayName")
                    )
            name = raw_item.get("ModelName") or raw_item.get("Name")
            if not owner or not name:
                continue
            owner = str(owner).strip()
            name = str(name).strip()
            if not owner or not name:
                continue
            result.append((owner, name, raw_item))
        return result

    def _build_source_url(self, owner: str, name: str) -> str:
        return f"https://modelscope.cn/models/{owner}/{name}"

    def _build_readme_url(self, owner: str, name: str) -> str:
        return f"https://modelscope.cn/models/{owner}/{name}/resolve/master/README.md"

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

    def _compute_content_hash(self, repo_id: str) -> str:
        """ModelScope 模型库使用 repo_id 作为稳定的 content_hash."""
        return repo_id

    async def _fetch_readme_text(
        self,
        client: httpx.AsyncClient,
        owner: str,
        name: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        base_url = f"https://modelscope.cn/models/{owner}/{name}/resolve/master"
        candidates = list(self.README_CANDIDATES)
        seen_urls = set()
        for candidate in candidates:
            url = f"{base_url}/{candidate}"
            if url in seen_urls:
                continue
            seen_urls.add(url)
            try:
                headers = dict(self._base_headers)
                headers["Accept"] = "text/plain, */*"
                resp = await client.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    follow_redirects=True,
                )
            except httpx.HTTPError as exc:
                self.logger.debug(
                    "[modelscope.fetch] error fetching README url=%s err=%s",
                    url,
                    exc,
                )
                continue

            if resp.status_code == 404:
                continue

            if resp.status_code >= 400:
                self.logger.debug(
                    "[modelscope.fetch] non-success README status url=%s status=%s",
                    url,
                    resp.status_code,
                )
                continue

            text = resp.text.strip()
            if not text:
                continue
            return text, str(resp.url)
        return None, None

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
