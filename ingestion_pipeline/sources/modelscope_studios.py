from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..modelscope_client import ModelScopeClient
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType


load_dotenv()


class ModelScopeStudiosPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.STUDIOS

    def __init__(
        self,
        *,
        target_repo_count: Optional[int] = None,
        studios_page_size: int = 24,
        timeout: float | None = 60.0,
        criterion: Optional[List[Dict[str, object]]] = None,
    ) -> None:
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.timeout = float(timeout or 60.0)
        self.page_size = max(1, studios_page_size)
        self.page_concurrency = 8
        self.logger = logging.getLogger("ingestion.sources.modelscope_studios")

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or "https://modelscope.cn"
        self._endpoint = endpoint.rstrip("/")
        self._criterion = criterion or []

        base_url = os.environ.get("LLM_BASE_URL")
        api_key = os.environ.get("LLM_API_KEY")
        model = os.environ.get("LLM_MODEL")
        if not base_url or not api_key or not model:
            raise RuntimeError("Studios pipeline requires LLM_BASE_URL/LLM_API_KEY/LLM_MODEL")
        self._llm_base_url = base_url.rstrip("/")
        self._llm_api_key = api_key
        self._llm_model = model

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        max_docs: Optional[int] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        produced = 0
        target = None
        if max_docs is not None:
            try:
                target = max(1, int(max_docs))
            except Exception:
                target = None
        elif self.target_repo_count is not None and self.target_repo_count > 0:
            target = self.target_repo_count

        async with ModelScopeClient(
            endpoint=self._endpoint,
            timeout=int(self.timeout),
            model_page_size=self.page_size,
            dataset_page_size=self.page_size,
        ) as client:
            page = 1
            total_pages: Optional[int] = None

            while True:
                page_payload = await client._studios_page(
                    page,
                    page_size=self.page_size,
                    criterion=self._criterion,
                )
                data = page_payload.get("Data") or {}
                studios = data.get("Studios") or []
                total_count = int(data.get("TotalCount") or 0)
                if total_pages is None and total_count:
                    total_pages = (total_count + self.page_size - 1) // self.page_size

                if not studios:
                    break

                sem = asyncio.Semaphore(self.page_concurrency)

                async def build(studio: Dict[str, object]) -> Optional[RawDocument]:
                    async with sem:
                        owner = self._extract_owner(studio)
                        name = self._extract_name(studio)
                        if not owner or not name:
                            return None
                        repo_id = f"studios:{owner}/{name}"
                        update_marker = studio.get("LastUpdatedTime") or studio.get("Revision") or studio.get("Id")
                        content_hash = f"{repo_id}:{update_marker}" if update_marker else repo_id
                        if content_hash in existing_set or content_hash in seen_hashes:
                            return None

                        try:
                            app_text = await asyncio.wait_for(
                                client.fetch_studio_app(owner, name),
                                timeout=self.timeout,
                            )
                        except asyncio.TimeoutError:
                            self.logger.info("[studios.fetch] timeout owner=%s name=%s", owner, name)
                            return None
                        except Exception:
                            self.logger.warning("[studios.fetch] failed owner=%s name=%s", owner, name)
                            return None

                        if not app_text or not app_text.strip():
                            return None

                        studio_meta = self._shrink_studio_metadata(studio)
                        payload = {
                            "repo_id": repo_id,
                            "studio": studio_meta,
                            "app": {
                                "path": "app.py",
                                "text_raw": app_text,
                            },
                        }

                        raw_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                        return RawDocument(
                            locator=SourceLocator(
                                source_type=SourceType.STUDIOS,
                                owner_repo=f"{owner}/{name}",
                                source_url=f"https://modelscope.cn/studios/{owner}/{name}",
                            ),
                            repo_id=repo_id,
                            payload=raw_payload,
                            fetched_at=datetime.now(timezone.utc),
                            content_hash=content_hash,
                        )

                tasks = [asyncio.create_task(build(item)) for item in studios]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                batch: List[RawDocument] = []
                for result in results:
                    if isinstance(result, Exception) or result is None:
                        continue
                    if target is not None and produced >= target:
                        break
                    batch.append(result)
                    seen_hashes.add(result.content_hash)
                    produced += 1
                if batch:
                    yield batch
                if target is not None and produced >= target:
                    return
                page += 1
                if total_pages is not None and page > total_pages:
                    break

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        app = payload.get("app") or {}
        studio_meta = payload.get("studio") or {}
        app_text = app.get("text_raw") or ""
        app_text = str(app_text)
        if not app_text.strip():
            return []

        summary = await self._summarize_app(app_text, studio_meta, raw.repo_id)
        if summary is None:
            return []
        drafts: List[ChunkDraft] = []

        summary_text = self._format_summary(summary, studio_meta, raw.repo_id)
        drafts.append(
            ChunkDraft(
                locator=raw.locator,
                repo_id=raw.repo_id,
                content_hash=raw.content_hash,
                index=len(drafts),
                text=summary_text,
            )
        )

        for segment in self._split_code(app_text, chunk_max_size):
            formatted = f"```python\n{segment}\n```"
            drafts.append(
                ChunkDraft(
                    locator=raw.locator,
                    repo_id=raw.repo_id,
                    content_hash=raw.content_hash,
                    index=len(drafts),
                    text=formatted,
                )
            )

        embedded = await embed(drafts)
        if len(embedded) != len(drafts):
            raise RuntimeError(
                f"Embedding length mismatch for {raw.repo_id}: drafts={len(drafts)} embeddings={len(embedded)}"
            )

        records: List[ChunkRecord] = []
        for item in embedded:
            draft = item.chunk
            chunk_uuid = self._make_chunk_uuid(raw.repo_id, raw.content_hash, draft.index)
            records.append(
                ChunkRecord(
                    chunk_uuid=chunk_uuid,
                    repo_id=raw.repo_id,
                    content_hash=raw.content_hash,
                    chunk_index=draft.index,
                    text=draft.text,
                    locator=draft.locator,
                    embedding=item.embedding,
                    fetched_at=raw.fetched_at,
                )
            )
        return records

    async def ingest(self, records: List[ChunkRecord], raw: RawDocument) -> None:  # pragma: no cover
        raise NotImplementedError("Ingest handled by shared SqliteChromaIngestor via IngestionRunner")

    async def _summarize_app(self, code: str, studio_meta: Dict[str, object], repo_id: str) -> str:
        system_prompt = (
            "You are auditing a ModelScope Studio project. "
            "You will receive the full contents of app.py. "
            "Summarize the app strictly based on the code. "
            "Report UI frameworks, primary functions, external services, and interaction flow. "
            "If the purpose cannot be determined, respond with 'Unknown'."
        )
        user_prompt = (
            "Studio metadata:\n"
            f"{json.dumps(studio_meta, ensure_ascii=False)}\n\n"
            "app.py source:\n" + code
        )
        for attempt in range(10):
            try:
                text = await self._post_llm(system_prompt, user_prompt)
                break
            except Exception as exc:  # noqa: PERF203 - explicit retry loop
                status = getattr(exc, "status_code", None)
                if status == 429 and attempt < 9:
                    await asyncio.sleep(30)
                    continue
                if status is not None and 400 <= status < 500:
                    self.logger.warning("[studios.summary] llm failed repo=%s err=%s", repo_id, exc)
                    return None
                raise
        if not text:
            return None
        normalized = text.strip()
        if normalized.lower().startswith("unknown"):
            return None
        return normalized

    async def _post_llm(self, system_prompt: str, user_prompt: str) -> str:
        url = f"{self._llm_base_url}/chat/completions"
        payload = {
            "model": self._llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "max_tokens": 512,
            "temperature": 0.2,
            "top_p": 0.8,
        }
        headers = {
            "Authorization": f"Bearer {self._llm_api_key}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code == 404:
            err = RuntimeError("404 page not found")
            err.status_code = resp.status_code
            raise err
        if resp.status_code == 429:
            err = RuntimeError("rate limited")
            err.status_code = resp.status_code
            raise err
        if resp.status_code >= 400:
            resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            return ""
        message = choices[0].get("message") or {}
        content = message.get("content") or ""
        return str(content)

    def _format_summary(self, summary: str, studio_meta: Dict[str, object], repo_id: str) -> str:
        parts: List[str] = []
        name = studio_meta.get("name") or repo_id
        parts.append(f"# Studio Summary: {name}")
        if studio_meta.get("type"):
            parts.append(f"Type: {studio_meta['type']}")
        if studio_meta.get("predicts") is not None:
            parts.append(f"Total predictions: {studio_meta['predicts']}")
        if studio_meta.get("stars") is not None:
            parts.append(f"Stars: {studio_meta['stars']}")

        if summary == "Unknown":
            parts.append("Summary unavailable: Unknown")
        else:
            parts.append("")
            parts.append(summary)
        return "\n".join(parts).strip()

    def _split_code(self, text: str, max_size: int) -> List[str]:
        text = text.strip()
        if not text:
            return []
        effective = max(1, max_size)
        if len(text) <= effective:
            return [text]
        chunks: List[str] = []
        buffer: List[str] = []
        buffer_len = 0
        for line in text.splitlines():
            line_len = len(line)
            add_len = line_len + (1 if buffer else 0)
            if line_len > effective:
                if buffer:
                    chunks.append("\n".join(buffer))
                    buffer = []
                    buffer_len = 0
                for start in range(0, line_len, effective):
                    chunks.append(line[start : start + effective])
                continue
            if buffer_len + add_len > effective and buffer:
                chunks.append("\n".join(buffer))
                buffer = [line]
                buffer_len = line_len
            else:
                if buffer:
                    buffer.append(line)
                    buffer_len += add_len
                else:
                    buffer = [line]
                    buffer_len = line_len
            if buffer_len >= effective:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0
        if buffer:
            chunks.append("\n".join(buffer))
        return chunks

    def _shrink_studio_metadata(self, studio: Dict[str, object]) -> Dict[str, object]:
        return {
            "id": studio.get("Id"),
            "name": studio.get("Name"),
            "path": studio.get("Path"),
            "type": studio.get("Type"),
            "stars": studio.get("Stars"),
            "predicts": studio.get("Predicts"),
            "visits": studio.get("Visits"),
            "owner": studio.get("CreatedBy"),
            "license": studio.get("License"),
        }

    def _extract_owner(self, studio: Dict[str, object]) -> str:
        owner = studio.get("Path") or studio.get("CreatedBy")
        return str(owner).strip() if isinstance(owner, str) else (owner or "")

    def _extract_name(self, studio: Dict[str, object]) -> str:
        name = studio.get("Name")
        return str(name).strip() if isinstance(name, str) else (name or "")

    def _make_chunk_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        seed = f"{repo_id}:{content_hash}:{index}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
