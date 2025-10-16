from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional, Tuple, Dict

from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..modelscope_client import ModelScopeClient
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType


load_dotenv()


class ModelScopeDatasetsPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.DATASETS
    """Dataset ingestion pipeline using official ModelScope APIs.

    Fetch enumerates datasets via `/api/v1/dolphin/datasets` and retrieves README
    text from `/api/v1/datasets/{owner}/{name}` (ReadmeContent field). The
    remainder mirrors the generic ingestion SOP (clean → split → embed → ingest).
    """

    def __init__(
        self,
        *,
        target_repo_count: Optional[int] = None,
        dataset_page_size: int = 100,
        timeout: float | None = 60.0,
    ) -> None:
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.timeout = float(timeout or 60.0)
        self.page_size = max(1, dataset_page_size)
        self.delay_range = (0.1, 3.5)
        self.logger = logging.getLogger("ingestion.sources.modelscope_datasets")

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or "https://modelscope.cn"
        self._endpoint = endpoint.rstrip("/")

    def register_ingested_hash(self, content_hash: str) -> None:
        # Kept for backward compatibility — no-op under new Runner-provided dedupe
        return None

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        max_docs: Optional[int] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        """Yield per-page lists of RawDocument with bounded in-page concurrency."""
        target_successes: Optional[int] = None
        if max_docs is not None:
            try:
                target_successes = max(1, int(max_docs))
            except (TypeError, ValueError):
                target_successes = None
        elif self.target_repo_count is not None and self.target_repo_count > 0:
            target_successes = self.target_repo_count

        # Step 1: build dedupe sets (provided by Runner)
        # [块] 去重集：运行期 existing_hashes（历史）+ seen_hashes（当页）
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        produced = 0
        delay_min, delay_max = (0.1, 1.5)  # gentle jitter per task within a page
        page_concurrency = 16  # fixed page-level concurrency (no env toggles)

        async with ModelScopeClient(
            endpoint=self._endpoint,
            dataset_page_size=self.page_size,
            timeout=int(self.timeout),
        ) as client:
            # Compute total pages up front using the first page
            # [块] 首页推导总页数（避免 O(N) 预扫描）
            first = await client._datasets_page(1)
            total_remote = int(first.get("TotalCount") or 0)
            max_pages = (total_remote + self.page_size - 1) // self.page_size if total_remote else 0

            timeouts = 0

            # [块] 单仓构建：读取 README 文本 → 构建 RawDocument（失败返回 None）
            async def build(owner: str, name: str) -> Optional[RawDocument]:
                # per-task jitter and timeout wrapping around README fetch helper
                await asyncio.sleep(random.uniform(delay_min, delay_max))
                try:
                    text, source_url = await asyncio.wait_for(
                        self._fetch_readme_text(client, owner, name),
                        timeout=int(self.timeout),
                    )
                except asyncio.TimeoutError:
                    nonlocal timeouts
                    timeouts += 1
                    self.logger.info("[datasets.fetch] timeout owner=%s name=%s", owner, name)
                    return None
                except Exception:
                    self.logger.info("[datasets.fetch] failed owner=%s name=%s", owner, name)
                    return None

                if not text:
                    return None

                payload = self._build_payload_dict(owner, name, source_url, text)
                raw_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                repo_canon = f"datasets:{owner}/{name}"
                return RawDocument(
                    locator=SourceLocator(
                        source_type=SourceType.DATASETS,
                        owner_repo=f"{owner}/{name}",
                        source_url=self._build_source_url(owner, name),
                    ),
                    repo_id=repo_canon,
                    payload=raw_payload,
                    fetched_at=datetime.now(timezone.utc),
                    content_hash=repo_canon,
                )

            # Page loop
            for page in range(1, (max_pages or 1) + 1):
                data = first if page == 1 else await client._datasets_page(page)
                entries = data.get("Data") or []
                # [块] 列表页轻筛：owner/name 完整性 → 增量去重
                candidates: list[tuple[str, str, str]] = []  # (repo_id, owner, name)
                total_entries = len(entries)
                skipped = 0

                for item in entries:
                    owner_raw = item.get("Namespace") or item.get("Owner") or item.get("CreatedBy")
                    name_raw = item.get("Name")
                    if not owner_raw or not name_raw:
                        continue
                    owner = str(owner_raw).strip()
                    name = str(name_raw).strip()
                    if not owner or not name:
                        continue
                    content_hash = self.compute_content_hash(owner, name)
                    if not content_hash:
                        continue
                    if content_hash in existing_set or content_hash in seen_hashes:
                        skipped += 1
                        continue
                    repo_id = content_hash  # identity hash for datasets
                    candidates.append((repo_id, owner, name))

                self.logger.info(
                    "[datasets.fetch] page=%s entries=%s candidates=%s skipped=%s",
                    page,
                    total_entries,
                    len(candidates),
                    skipped,
                )

                # No candidates in this page
                if not candidates:
                    if max_pages and page >= max_pages:
                        break
                    continue

                # [块] 页内并发：固定令牌，单页上限
                sem = asyncio.Semaphore(page_concurrency)
                timeouts = 0

                async def run_task(owner: str, name: str) -> Optional[RawDocument]:
                    async with sem:
                        doc = await build(owner, name)
                        if doc is None:
                            # best-effort detection already logged in build; cannot distinguish error vs timeout here
                            return None
                        return doc

                # [块] 并发收集：单个失败不影响整页聚合
                tasks = [asyncio.create_task(run_task(o, n)) for _rid, o, n in candidates]
                # Gather page results; we purposely do not raise on individual failures
                results = await asyncio.gather(*tasks, return_exceptions=True)

                succeeded: int = 0
                page_docs: List[RawDocument] = []
                for res, (_rid, o, n) in zip(results, candidates):
                    if isinstance(res, Exception):
                        # already logged in build; skip
                        continue
                    if res is None:
                        continue
                    page_docs.append(res)
                    succeeded += 1
                    produced += 1
                    seen_hashes.add(res.content_hash)
                    if target_successes is not None and produced >= target_successes:
                        # yield remaining of this page up to cap
                        break

                self.logger.info(
                    "[datasets.fetch] page=%s succeeded=%s timeouts=%s", page, succeeded, timeouts
                )

                if page_docs:
                    # [块] 产出：按页 yield，Runner 负责后续 process/ingest
                    yield page_docs
                    # If we've reached the target success count, stop fetching further pages
                    if target_successes is not None and produced >= target_successes:
                        return

                # Move on to next page
                if max_pages and page >= max_pages:
                    break

    # plan_total_pages removed per SOP update to avoid pre-pass latency

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
            self.logger.debug("[datasets.process] repo=%s has no readme data; skipping", raw.repo_id)
            return []

        clean_text = readme_info.get("text_clean") or ""
        if not clean_text.strip():
            self.logger.debug("[datasets.process] repo=%s readme empty after cleaning; skipping", raw.repo_id)
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
            "Ingest stage delegated to SqliteChromaIngestor via the IngestionRunner."
        )

    async def _fetch_readme_text(self, client: ModelScopeClient, owner: str, name: str) -> Tuple[Optional[str], Optional[str]]:
        detail = await client.fetch_dataset_detail(owner, name)
        if detail and isinstance(detail.get("ReadmeContent"), str):
            text = detail["ReadmeContent"].strip()
            if text:
                return text, None
        self.logger.debug("[datasets.fetch] repo=%s/%s missing README content", owner, name)
        return None, None

    def _build_payload_dict(self, owner: str, name: str, readme_url: Optional[str], readme_text: str) -> Dict[str, object]:
        clean_text = self._clean_readme_text(readme_text)
        clean_state = "blanklines_stripped"
        return {
            "owner": owner,
            "name": name,
            "repo_id": f"datasets:{owner}/{name}",
            "source_url": self._build_source_url(owner, name),
            "readme": {
                "url": readme_url,
                "text_raw": readme_text,
                "text_clean": clean_text,
                "clean_state": clean_state,
            },
        }

    def compute_content_hash(self, owner: str, name: str) -> str:
        owner_norm = (owner or "").strip()
        name_norm = (name or "").strip()
        if not owner_norm or not name_norm:
            return ""
        return f"datasets:{owner_norm}/{name_norm}"

    def _make_chunk_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        seed = f"{repo_id}:{content_hash}:{index}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    def _build_source_url(self, owner: str, name: str) -> str:
        return f"https://modelscope.cn/datasets/{owner}/{name}"

    def _clean_readme_text(self, text: str) -> str:
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

        def flush_buffer() -> None:
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
            add_len = section_len if not buffer else section_len + 1
            if buffer_len + add_len <= max_size:
                buffer.append(section)
                buffer_len += add_len
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

        def flush_buffer() -> None:
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
        out: List[str] = []
        start = 0
        while start < len(line):
            end = min(len(line), start + max_size)
            out.append(line[start:end])
            start = end
        return out
