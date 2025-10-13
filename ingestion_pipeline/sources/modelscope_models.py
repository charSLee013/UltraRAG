from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from modelscope.hub.api import HubApi, ModelScopeConfig
from modelscope.hub.file_download import get_file_download_url
from requests import exceptions as requests_exc

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType

load_dotenv()


class ModelScopeModelsPipeline(BaseIngestionPipeline):
    """Ingestion pipeline for models listed on ModelScope (modelscope.cn/models).

    The fetch stage paginates via the official Hub API (`HubApi.list_models`),
    resolves each repository's README through `HubApi.get_model_files` and
    `get_file_download_url`, and emits `RawDocument` entries once README content
    is available. The constructor `page_size` argument (and its corresponding
    `MODELSCOPE_MODEL_SIZE` env overrides) defines the
    minimum number of successful repositories this pipeline should yield; the
    Hub API pagination size is capped at 100 and derived from that target. The
    process stage cleans README text, chunks it according to `chunk_max_size`,
    and invokes the provided `EmbedFn`, returning `ChunkRecord` objects without
    touching downstream stores. Ingest is delegated to the runner's
    `SqliteChromaIngestor`.
    """
    def __init__(
        self,
        *,
        page_size: Optional[int] = 100,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        backoff_factor: float = 2.0,
        timeout: float | None = 60.0,
        existing_content_hashes: Optional[Iterable[str]] = None,
    ) -> None:
        if page_size is not None and page_size < 1:
            raise ValueError("page_size must be >= 1 or None")

        if page_size is None:
            self.target_repo_count: Optional[int] = None
            self.api_page_size = 100
        else:
            self.target_repo_count = int(page_size)
            self.api_page_size = min(self.target_repo_count, 100)

        self.max_retries = max(1, max_retries)
        self.initial_backoff = max(0.1, initial_backoff)
        self.backoff_factor = max(1.0, backoff_factor)
        self.list_timeout = float(timeout or 60.0)
        self.readme_timeout = 60.0
        self.delay_range = (0.1, 3.5)
        self._existing_content_hashes = set(filter(None, existing_content_hashes or []))
        self.logger = logging.getLogger("ingestion.sources.modelscope_models")

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or None
        self._hub_api = HubApi(endpoint=endpoint, timeout=self.list_timeout, max_retries=self.max_retries)
        token = os.environ.get("MODELSCOPE_API_TOKEN")
        if token:
            try:
                self._hub_api.login(token, endpoint=self._hub_api.endpoint)
            except Exception as exc:
                self.logger.warning("[modelscope.fetch] login failed: %s", exc)

    def register_ingested_hash(self, content_hash: str) -> None:
        """Record a content_hash after a successful ingest to keep skip lists in sync."""
        if content_hash:
            self._existing_content_hashes.add(content_hash)

    def estimate_total_models(self) -> Optional[int]:
        """Return total models reported by the Hub API, or None on failure."""
        try:
            data = self._hub_api.list_models(owner_or_group="", page_number=1, page_size=1)
        except Exception as exc:
            self.logger.warning("[modelscope.fetch] failed to fetch total count: %s", exc)
            return None
        total = data.get("TotalCount")
        try:
            return int(total)
        except (TypeError, ValueError):
            return None

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
            page_size: Optional override for the Hub API page size (1-100).
            **kwargs: Supports `skip_repo_ids` (Iterable[str]) to skip repos in
                addition to the constructor-level cache.
        """

        skip_content_hashes = set(self._existing_content_hashes)
        skip_content_hashes.update(filter(None, kwargs.get("skip_content_hashes", [])))

        max_docs_raw = kwargs.get("max_docs")
        target_successes: Optional[int] = None
        if max_docs_raw is not None:
            try:
                candidate = int(max_docs_raw)
            except (TypeError, ValueError):
                candidate = None
            if candidate is not None and candidate > 0:
                target_successes = candidate
        elif self.target_repo_count is not None and self.target_repo_count > 0:
            target_successes = self.target_repo_count

        seen_repo_ids: set[str] = set()
        current_page = 1
        effective_page_size = page_size if page_size is not None else self.api_page_size
        if effective_page_size is None:
            effective_page_size = 100
        effective_page_size = max(1, min(int(effective_page_size), 100))
        yielded_total = 0  # successful README fetches in this run
        total_count: Optional[int] = None

        while True:
            entries, page_total_count = await asyncio.to_thread(
                self._list_models_page_with_retry,
                current_page,
                effective_page_size,
            )
            if total_count is None:
                total_count = page_total_count
            if not entries:
                self.logger.info(
                    "[modelscope.fetch] stopping at page=%s (no items returned)",
                    current_page,
                )
                break

            yielded_on_page = 0
            download_tasks: List[Tuple[int, str, asyncio.Task[Optional[RawDocument]]]] = []

            for idx, (owner, name, raw_item) in enumerate(entries):
                repo_id = f"models:{owner}/{name}"
                if repo_id in seen_repo_ids:
                    continue
                seen_repo_ids.add(repo_id)

                content_hash = self._compute_content_hash(repo_id)
                if not force and content_hash in skip_content_hashes:
                    continue

                locator = SourceLocator(
                    source_type=SourceType.MODELS,
                    owner_repo=f"{owner}/{name}",
                    source_url=self._build_source_url(owner, name),
                )
                task = asyncio.create_task(
                    self._build_raw_document_async(
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
                except Exception as exc:
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
                skip_content_hashes.add(raw_document.content_hash)

                if target_successes is not None and yielded_total >= target_successes:
                    self.logger.info(
                        "[modelscope.fetch] reached per-run quota=%s, stopping",
                        target_successes,
                    )
                    return

            # Do NOT stop on failures; continue paging until we reach the
            # requested number of successful README fetches or exhaust pages.

            self.logger.info(
                "[modelscope.fetch] page=%s yielded=%s (total seen=%s)",
                current_page,
                yielded_on_page,
                len(seen_repo_ids),
            )
            current_page += 1
            # If API reports a finite total, stop once we've inspected that many entries
            # without reaching the target successes. This still honours the success-based
            # quota by only breaking after all pages are exhausted.
            if total_count is not None and (current_page - 1) * effective_page_size >= total_count:
                break

        if target_successes is not None and yielded_total < target_successes:
            self.logger.warning(
                "[modelscope.fetch] exhausted available pages before meeting quota: yielded=%s target=%s",
                yielded_total,
                target_successes,
            )

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

    async def _build_raw_document_async(
        self,
        *,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
        locator: SourceLocator,
        repo_id: str,
    ) -> Optional[RawDocument]:
        await asyncio.sleep(random.uniform(*self.delay_range))
        return await asyncio.to_thread(
            self._build_raw_document_sync,
            owner,
            name,
            raw_item,
            locator,
            repo_id,
        )

    def _build_raw_document_sync(
        self,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
        locator: SourceLocator,
        repo_id: str,
    ) -> Optional[RawDocument]:
        readme_text, readme_url = self._fetch_readme_text_sync(owner, name, raw_item)
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
                raw_item.get("Path")
                or raw_item.get("Owner")
                or raw_item.get("OwnerName")
                or raw_item.get("UserName")
                or raw_item.get("Publisher")
                or raw_item.get("OrganizationName")
                or raw_item.get("User")
            )
            if not owner:
                organization = raw_item.get("Organization")
                if isinstance(organization, dict):
                    owner = (
                        organization.get("Name")
                        or organization.get("FullName")
                        or organization.get("DisplayName")
                    )
            if not owner:
                owner = raw_item.get("CreatedBy")
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

    def _list_models_page_with_retry(
        self,
        page_number: int,
        page_size: int,
    ) -> Tuple[List[Tuple[str, str, Dict[str, object]]], int]:
        backoff = self.initial_backoff
        for attempt in range(1, self.max_retries + 1):
            try:
                data = self._hub_api.list_models(
                    owner_or_group="",
                    page_number=page_number,
                    page_size=page_size,
                )
                total = int(data.get("TotalCount") or 0)
                container = {"Data": {"Models": data.get("Models") or []}}
                entries = self._extract_model_entries(container)
                return entries, total
            except Exception as exc:
                if attempt == self.max_retries:
                    raise
                wait_time = backoff * (1.0 + random.random())
                self.logger.warning(
                    "[modelscope.fetch] list_models page=%s attempt=%s err=%s; retrying in %.2fs",
                    page_number,
                    attempt,
                    exc,
                    wait_time,
                )
                time.sleep(wait_time)
                backoff *= self.backoff_factor
        raise RuntimeError(f"Unable to list models for page {page_number}")

    def _fetch_readme_text_sync(
        self,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
    ) -> Tuple[Optional[str], Optional[str]]:
        repo_id = f"{owner}/{name}"

        api_readme = raw_item.get("ReadMeContent") if isinstance(raw_item, dict) else None
        if isinstance(api_readme, str) and api_readme.strip():
            text = api_readme.strip()
            return text, None

        try:
            revision = self._hub_api.get_valid_revision(
                repo_id,
                endpoint=self._hub_api.endpoint,
            )
        except Exception as exc:
            self.logger.debug(
                "[modelscope.fetch] repo=%s failed to resolve revision: %s",
                repo_id,
                exc,
            )
            return None, None

        try:
            files = self._hub_api.get_model_files(
                model_id=repo_id,
                revision=revision,
                recursive=True,
                endpoint=self._hub_api.endpoint,
            )
        except Exception as exc:
            self.logger.debug(
                "[modelscope.fetch] repo=%s failed to list files: %s",
                repo_id,
                exc,
            )
            return None, None

        readme_entry = None
        for file_meta in files:
            if not isinstance(file_meta, dict):
                continue
            path = str(file_meta.get("Path") or "").strip()
            if path.lower() == "readme.md":
                readme_entry = file_meta
                break
        if readme_entry is None:
            return None, None

        download_url = get_file_download_url(
            model_id=repo_id,
            file_path=readme_entry["Path"],
            revision=revision,
            endpoint=self._hub_api.endpoint,
        )
        headers = self._hub_api.builder_headers(dict(self._hub_api.headers))
        headers["Accept"] = "text/plain, */*"
        try:
            resp = self._hub_api.session.get(
                download_url,
                headers=headers,
                cookies=ModelScopeConfig.get_cookies(),
                timeout=self.readme_timeout,
            )
            resp.raise_for_status()
        except requests_exc.RequestException as exc:
            self.logger.debug(
                "[modelscope.fetch] repo=%s failed to download README: %s",
                repo_id,
                exc,
            )
            return None, None

        text = resp.text.strip()
        if not text:
            return None, None
        return text, download_url

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
