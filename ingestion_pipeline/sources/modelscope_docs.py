from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional

from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import (
    ChunkDraft,
    ChunkRecord,
    RawDocument,
    SourceLocator,
    SourceType,
)


load_dotenv()


class ModelScopeDocsPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.DOCS

    def __init__(self, *, timeout: float | None = 60.0) -> None:
        self.timeout = float(timeout or 60.0)
        self.logger = logging.getLogger("ingestion.sources.modelscope_docs")

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        existing_set: set[str] = set() if force else set(existing_hashes or set())
        produced = 0
        page_docs: List[RawDocument] = []
        sem = asyncio.Semaphore(16)

        import httpx

        DEFAULT_MAIN_DOC = "https://modelscope.cn/api/v1/document/main_doc"

        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as http:
            # main_doc
            r = await http.get(DEFAULT_MAIN_DOC, headers={"Accept": "application/json, text/plain, */*"})
            r.raise_for_status()
            data = r.json().get("Data") or {}
            target_prefix = str(data.get("TargetPrefix") or "").rstrip("/")
            version = str(data.get("Version") or "")
            if not target_prefix or not version:
                raise RuntimeError("main_doc missing TargetPrefix/Version")

            # index.json
            r = await http.get(f"{target_prefix}/dist/index.json", headers={"Accept": "application/json, text/plain, */*"})
            r.raise_for_status()
            index_json = r.json()

            # iterate md nodes (skip sphinx)
            nodes: list[tuple[str, str, str]] = []  # (title, url, path)
            stack = [index_json]
            while stack:
                n = stack.pop()
                for c in (n.get("children") or []):
                    if not isinstance(c, dict):
                        continue
                    if c.get("sphinx") is True:
                        # skip sphinx-only nodes
                        stack.append(c)
                        continue
                    if c.get("md") is True:
                        title = str(c.get("title") or "").strip()
                        url = str(c.get("url") or "").strip()
                        raw_path = str(c.get("path") or "").rstrip("/")
                        path = f"{raw_path}/index.md" if c.get("dir") is True else (raw_path if raw_path.endswith(".md") else f"{raw_path}.md")
                        nodes.append((title, url, path))
                    stack.append(c)

            async def build_one(title: str, url: str, path: str) -> Optional[RawDocument]:
                async with sem:
                    from urllib.parse import quote
                    r = await http.get(f"{target_prefix}/dist/{quote(path)}", headers={"Accept": "text/markdown, text/plain, */*"})
                    r.raise_for_status()
                    text = r.text
                    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                    if content_hash in existing_set and not force:
                        return None
                    payload = {
                        "version": version,
                        "md": {
                            "path": path,
                            "url": url,
                            "title": title,
                            "text_raw": text,
                        },
                    }
                    # repo_id uses the resolved markdown path
                    repo_id = f"docs_overview:{path}"
                    return RawDocument(
                        locator=SourceLocator(
                            source_type=SourceType.DOCS,
                            owner_repo=f"default/{url or path}",
                            source_url=(f"https://modelscope.cn/docs/{url}" if url else "https://modelscope.cn/docs/overview"),
                        ),
                        repo_id=repo_id,
                        payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        fetched_at=datetime.now(timezone.utc),
                        content_hash=content_hash,
                    )

            tasks = [asyncio.create_task(build_one(t, u, p)) for (t, u, p) in nodes]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception) or res is None:
                    continue
                page_docs.append(res)
                produced += 1

            if page_docs:
                yield page_docs

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        md = payload.get("md") if isinstance(payload, dict) else None
        if not isinstance(md, dict):
            return []
        text_raw = md.get("text_raw") or ""
        clean_text = self._clean_markdown_text(text_raw)

        segments = self._structure_aware_chunks(clean_text, int(chunk_max_size or 32768))
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

    def _make_chunk_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        import uuid

        seed = f"{repo_id}:{content_hash}:{index}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))

    def _clean_markdown_text(self, text: str) -> str:
        # Basic badge removal and blankline normalization, keep content otherwise intact
        out: List[str] = []
        for line in (text or "").splitlines():
            s = line.strip()
            if not s:
                if out and out[-1] != "":
                    out.append("")
                continue
            # Drop obvious badges/images at the very beginning of a line
            if s.startswith("![") and ("](" in s):
                continue
            out.append(s)
        return "\n".join(out).strip()

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

        def flush() -> None:
            nonlocal buffer, buffer_len
            if buffer:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0

        for section in sections:
            slen = len(section)
            if slen > max_size:
                flush()
                chunks.extend(self._split_large_section(section, max_size))
                continue
            add_len = slen if not buffer else slen + 1
            if buffer_len + add_len <= max_size:
                buffer.append(section)
                buffer_len += add_len
            else:
                flush()
                buffer.append(section)
                buffer_len = slen
        flush()
        return chunks

    def _split_large_section(self, section: str, max_size: int) -> List[str]:
        lines = section.split("\n")
        chunks: List[str] = []
        buffer: List[str] = []
        buffer_len = 0

        def flush() -> None:
            nonlocal buffer, buffer_len
            if buffer:
                chunks.append("\n".join(buffer))
                buffer = []
                buffer_len = 0

        for line in lines:
            l = len(line)
            if l > max_size:
                flush()
                for start in range(0, l, max_size):
                    chunks.append(line[start : start + max_size])
                continue
            add_len = l if not buffer else l + 1
            if buffer_len + add_len <= max_size:
                buffer.append(line)
                buffer_len += add_len
            else:
                flush()
                buffer.append(line)
                buffer_len = l
        flush()
        return chunks
