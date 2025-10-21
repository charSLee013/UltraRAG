from __future__ import annotations

"""
GitHub (modelscope org) README ingestion pipeline.

Single path per SOP and user spec:
- Org fixed to `modelscope`.
- List via GET /orgs/modelscope/repos?per_page=100&page=N until empty.
- README via GET /repos/modelscope/{repo}/readme (JSON, base64 content). 404 → skip.
- Keys: owner_repo = repo_id = "modelscope/{repo}" (exact string, no prefix).
- content_hash: use `sha` returned by the readme API; as fallback sha256(decoded_text).
- No heuristics: do not filter forks/archived; only skip when README missing or text length < 7.
- Jitter: 1–8s per page; README concurrency 16.
- Rate limit: if 403/429 and X-RateLimit-Remaining == 0, sleep 10 minutes and retry; max 6 sleeps/page then fail.

Ingest is delegated to SqliteChromaIngestor via IngestionRunner.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv

from ..base import BaseIngestionPipeline, EmbedFn
from ..types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType


load_dotenv()
logger = logging.getLogger("ingestion.sources.github_modelscope")


ORG = "modelscope"
API_BASE = "https://api.github.com"


@dataclass(frozen=True)
class _Repo:
    name: str


class GitHubModelScopePipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.GITHUB

    def __init__(self, *, timeout: float | None = 60.0, target_repo_count: Optional[int] = None) -> None:
        self.timeout = float(timeout or 60.0)
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.jitter_range = (1.0, 8.0)
        self.page_concurrency = 16

        token = os.environ.get("GITHUB_TOKEN")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "UltraRAG-Ingestion/1.0 (+github.com/OpenBMB/UltraRAG)",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._headers = headers

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

        async with httpx.AsyncClient(base_url=API_BASE, headers=self._headers, timeout=self.timeout) as http:
            page = 1
            while True:
                await asyncio.sleep(random.uniform(*self.jitter_range))
                repos = await self._list_repos(http, page)
                if not repos:
                    break

                # Build per-repo tasks with concurrency limit; cap work to remaining target
                sem = asyncio.Semaphore(self.page_concurrency)
                repos_slice = repos
                if self.target_repo_count is not None and self.target_repo_count > produced:
                    remain = self.target_repo_count - produced
                    if remain < len(repos_slice):
                        repos_slice = repos_slice[:remain]

                async def build(repo: _Repo) -> Optional[RawDocument]:
                    async with sem:
                        text, sha = await self._get_readme_text(http, repo.name)
                        if not text or len(text.strip()) < 7:
                            return None
                        clean_text = self._clean_text(text)
                        if len(clean_text) < 7:
                            return None
                        owner_repo = f"{ORG}/{repo.name}"
                        repo_id = f"github:{owner_repo}"
                        # Canonical hash ties repo and README version together
                        if sha:
                            content_hash = f"{repo_id}@{sha}"
                        else:
                            content_hash = f"{repo_id}@{hashlib.sha256(clean_text.encode('utf-8')).hexdigest()}"
                        if content_hash in existing_set or content_hash in seen_hashes:
                            return None
                        payload = {
                            "owner_repo": owner_repo,
                            "source_url": f"https://github.com/{owner_repo}",
                            "readme": {
                                "text_raw": text,
                                "text_clean": clean_text,
                                "clean_state": "blanklines_stripped",
                                "sha": sha,
                            },
                        }
                        raw = RawDocument(
                            locator=SourceLocator(
                                source_type=self.source_type,
                                owner_repo=owner_repo,
                                source_url=f"https://github.com/{owner_repo}",
                            ),
                            repo_id=repo_id,
                            payload=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                            fetched_at=datetime.now(timezone.utc),
                            content_hash=content_hash,
                        )
                        seen_hashes.add(content_hash)
                        return raw

                tasks = [asyncio.create_task(build(r)) for r in repos_slice]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                page_docs: List[RawDocument] = []
                for res in results:
                    if isinstance(res, Exception) or res is None:
                        continue
                    page_docs.append(res)
                    produced += 1
                    if self.target_repo_count is not None and produced >= self.target_repo_count:
                        break

                logger.info("[github.fetch] page=%s repos=%s succeeded=%s", page, len(repos_slice), len(page_docs))
                if page_docs:
                    yield page_docs
                    if self.target_repo_count is not None and produced >= self.target_repo_count:
                        return

                page += 1

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,
    ) -> List[ChunkRecord]:
        payload = json.loads(raw.payload)
        readme = payload.get("readme") if isinstance(payload, dict) else None
        if not isinstance(readme, dict):
            return []
        text = str(readme.get("text_clean") or readme.get("text_raw") or "").strip()
        if not text:
            return []
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
        raise NotImplementedError("Ingest handled by SqliteChromaIngestor via IngestionRunner")

    # --- GitHub API helpers ---
    async def _list_repos(self, http: httpx.AsyncClient, page: int) -> List[_Repo]:
        # Handle rate limit with 10-min sleeps, up to 6 times
        sleeps = 0
        while True:
            r = await http.get(f"/orgs/{ORG}/repos", params={"per_page": 100, "page": page})
            if r.status_code in (403, 429):
                if (r.headers.get("X-RateLimit-Remaining") == "0") or (r.status_code == 429):
                    if sleeps >= 6:
                        r.raise_for_status()
                    logger.warning("[github.fetch] rate-limited on list page=%s; sleeping 10min (try %s/6)", page, sleeps + 1)
                    await asyncio.sleep(600)
                    sleeps += 1
                    continue
            r.raise_for_status()
            data = r.json()
            out: List[_Repo] = []
            if isinstance(data, list):
                for it in data:
                    name = it.get("name") if isinstance(it, dict) else None
                    if isinstance(name, str) and name.strip():
                        out.append(_Repo(name=name.strip()))
            return out

    async def _get_readme_text(self, http: httpx.AsyncClient, repo: str) -> Tuple[str, str]:
        sleeps = 0
        while True:
            r = await http.get(f"/repos/{ORG}/{repo}/readme")
            if r.status_code == 404:
                return "", ""
            if r.status_code in (403, 429):
                if (r.headers.get("X-RateLimit-Remaining") == "0") or (r.status_code == 429):
                    if sleeps >= 6:
                        r.raise_for_status()
                    logger.warning("[github.fetch] rate-limited on readme %s/%s; sleeping 10min (try %s/6)", ORG, repo, sleeps + 1)
                    await asyncio.sleep(600)
                    sleeps += 1
                    continue
            r.raise_for_status()
            data = r.json()
            sha = data.get("sha") or ""
            enc = data.get("encoding")
            content = data.get("content") or ""
            if enc == "base64" and content:
                try:
                    raw = base64.b64decode(content, validate=True)
                except Exception:
                    raw = base64.b64decode(content)
                text = raw.decode("utf-8", errors="replace")
                return text, sha
            # fallback to download_url
            dl = data.get("download_url")
            if dl:
                r2 = await http.get(dl)
                if r2.status_code in (403, 429) and sleeps < 6:
                    await asyncio.sleep(600)
                    sleeps += 1
                    continue
                r2.raise_for_status()
                return r2.text, sha
            return "", sha

    # --- text utils ---
    def _clean_text(self, text: str) -> str:
        lines = []
        for line in (text or "").splitlines():
            s = line.strip()
            if s:
                lines.append(s)
        return "\n".join(lines)

    def _structure_chunks(self, text: str, max_size: int) -> List[str]:
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
        # merge
        chunks: List[str] = []
        buf: List[str] = []
        buf_len = 0
        def flush() -> None:
            nonlocal buf, buf_len
            if buf:
                chunks.append("\n".join(buf))
                buf = []
                buf_len = 0
        for sec in sections:
            L = len(sec)
            add = L if not buf else L + 1
            if buf_len + add <= max_size:
                buf.append(sec)
                buf_len += add
            else:
                flush()
                if L <= max_size:
                    buf.append(sec)
                    buf_len = L
                else:
                    for i in range(0, L, max_size):
                        chunks.append(sec[i:i+max_size])
        flush()
        return chunks

    def _make_uuid(self, repo_id: str, content_hash: str, index: int) -> str:
        import uuid as _uuid
        seed = f"{repo_id}:{content_hash}:{index}"
        return str(_uuid.uuid5(_uuid.NAMESPACE_URL, seed))
