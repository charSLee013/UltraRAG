"""
ModelScope 模型 README 摄取流水线（严格对齐 ingestion_pipeline_sop.md）。

设计要点（原则 → 实现落点）：
- Official API First：仅使用 ModelScope 官方 REST 接口（列表/详情/文件/内容），不做抓取/镜像兜底。
- Single Path Only：单一路径实现；没有“兼容模式/备用分支”。缺失或异常时直接返回 None，由上层 Runner 记录并继续。
- 去重契约：content_hash 恒等于 repo_id（models:{owner}/{name}），由 Runner 统一传入 existing_hashes 控制增量。
- 性能与可观测：页内并发受限（固定 16），每步骤记录数量与跳过原因；不预扫描全集，仅用首页推导总页数。
- README 过滤：只在 README 以占位提示开头时跳过（精确 startswith），避免误伤包含该短语的正常文本。

本模块不做环境读取（dotenv 在 Runner 侧统一加载），也不写 SQLite/Chroma（由 ingestor 负责）。
"""

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

from ingestion_pipeline.base import BaseIngestionPipeline, EmbedFn
from ingestion_pipeline.types import ChunkDraft, ChunkRecord, RawDocument, SourceLocator, SourceType
from ingestion_pipeline.modelscope_client import ModelScopeClient, ModelFile

load_dotenv()


class ModelScopeModelsPipeline(BaseIngestionPipeline):
    source_type: SourceType = SourceType.MODELS
    """使用官方 ModelScope Hub API 的模型 README 摄取管线。

    流水线职责（与 SOP 对齐）：
    - fetch：页循环（≤100），页内受限并发，按页产出 RawDocument；仅保留 Stars≥阈值的候选以减少无谓请求。
    - process：清洗→结构化切分→嵌入→组装 ChunkRecord（仅内存，不直接写库）。
    - ingest：由 Runner/SqliteChromaIngestor 负责写 SQLite/Chroma，本类不承担。
    """
    def __init__(
        self,
        *,
        target_repo_count: Optional[int] = None,
        model_page_size: int = 100,
        timeout: float | None = 60.0,
    ) -> None:
        # Hard threshold: only ingest models with Stars >= 2
        self._min_stars = 10
        self.target_repo_count = int(target_repo_count) if target_repo_count else None
        self.timeout = float(timeout or 60.0)
        self.page_size = max(1, min(int(model_page_size or 100), 100))
        self.delay_range = (0.1, 1.5)
        self.logger = logging.getLogger("ingestion.sources.modelscope_models")

        endpoint = os.environ.get("MODELSCOPE_ENDPOINT") or "https://modelscope.cn"
        self._endpoint = endpoint.rstrip("/")

    # 旧钩子与预扫描逻辑已移除

    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: Optional[set[str]] = None,
        **_: object,
    ) -> AsyncIterator[List[RawDocument]]:
        """列举模型并逐页产出 RawDocument。

        关键行为：
        - 增量：content_hash=repo_id，使用 Runner 提供的 existing_hashes 做去重；同一页内用 seen_hashes 防重。
        - 限流：固定 `page_concurrency=16`，每个候选独立任务，网络调用包裹超时；失败/超时不抛出，全局继续。
        - Stars 预过滤：用列表页/详情 Stars 降低后续 README 请求量（不影响一致性）。
        - README 获取：优先详情 ReadMeContent，其次 repo/files→repo?FilePath=...；仅当 README 以占位提示开头时跳过。
        """
        target_successes: Optional[int] = None
        if self.target_repo_count is not None and self.target_repo_count > 0:
            target_successes = self.target_repo_count

        existing_set: set[str] = set() if force else set(existing_hashes or set())
        seen_hashes: set[str] = set()

        produced = 0
        # 页内并发固定为 16（不暴露为环境/参数，防止“多路径配置”）
        page_concurrency = 16

        async with ModelScopeClient(
            endpoint=self._endpoint,
            model_page_size=self.page_size,
            timeout=int(self.timeout),
        ) as client:
            first = await client._models_page(1)
            total = int(first.get("TotalCount") or 0)
            max_pages = (total + self.page_size - 1) // self.page_size if total else 0

            async def build(owner: str, name: str, raw_item: Dict[str, object]) -> Optional[RawDocument]:
                """单个仓库的 README 拉取与 RawDocument 构建（失败返回 None）。"""
                await asyncio.sleep(random.uniform(*self.delay_range))
                try:
                    text, url = await asyncio.wait_for(
                        self._fetch_readme_text(client, owner, name, raw_item),
                        timeout=int(self.timeout),
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
                data = first if page == 1 else await client._models_page(page)
                models = data.get("Models") or []
                total_entries = len(models)
                candidates: List[Tuple[str, str, Dict[str, object]]] = []
                skipped = 0
                # [块] 列表页轻量筛选：owner/name 完整性 → Stars 预过滤 → 增量去重
                for item in models:
                    if not isinstance(item, dict):
                        continue
                    owner = self._extract_owner(item)
                    name = self._extract_name(item)
                    if not owner or not name:
                        continue
                    # [块] Stars 预过滤：低于阈值则在本地跳过，减少后续 README 请求
                    try:
                        stars = int(item.get("Stars") or 0)
                    except Exception:
                        stars = 0
                    if stars < self._min_stars:
                        skipped += 1
                        continue
                    content_hash = f"models:{owner}/{name}"
                    # [块] 增量去重：历史 existing_hashes ∪ 当页 seen_hashes
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

                # [块] 页内并发：固定令牌，防止无限并发
                sem = asyncio.Semaphore(page_concurrency)

                async def run_task(owner: str, name: str, raw_item: Dict[str, object]) -> Optional[RawDocument]:
                    async with sem:
                        return await build(owner, name, raw_item)

                # [块] 构建与收集：失败不抛出，逐个累积成功项
                tasks = [asyncio.create_task(run_task(o, n, it)) for o, n, it in candidates]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                page_docs: List[RawDocument] = []
                # [块] 聚合页结果：统计成功、更新 seen，达到目标条数时收束
                for res, (_o, _n, _it) in zip(results, candidates):
                    if isinstance(res, Exception) or res is None:
                        continue
                    page_docs.append(res)
                    produced += 1
                    seen_hashes.add(res.content_hash)
                    if target_successes is not None and produced >= target_successes:
                        break

                self.logger.info("[models.fetch] page=%s succeeded=%s", page, len(page_docs))
                if page_docs:
                    # [块] 产出本页结果；外层 Runner 继续处理/入库
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
        """将 RawDocument 转换为一组 ChunkRecord。

        - 输入：RawDocument.payload 内含 `readme.text_clean`（fetch 阶段已清洗/抽取）。
        - 切分：结构感知（按标题优先拼接），限制单块最大长度 `chunk_max_size`。
        - 嵌入：使用 Runner 注入的 `embed` 函数（已做并发/退避），保持单一入口。
        - 输出：仅返回记录对象；落库由上层 ingestor 统一执行。
        """
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

    async def _fetch_readme_text(
        self,
        client: ModelScopeClient,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
    ) -> Tuple[Optional[str], Optional[str]]:
        """按“官方 API → 文件列表 → 文件内容”的顺序获取 README 文本。

        规则：
        - 详情优先：若 `ReadMeContent/ReadmeContent` 非空，直接使用；
          仅当文本不以“### 当前模型的贡献者未提供更加详细的模型介绍。”开头时视为有效。
        - 否则：定位 README 文件（若存在），再取其内容并应用相同的占位检测。
        - 任何异常/占位开头/空串 → 返回 (None, None)，上层据此跳过该仓库。
        """
        # [块] 优先从模型详情读取 ReadMeContent（更少请求、无需定位文件）
        try:
            detail = await client.fetch_model_detail(owner, name)
            if isinstance(detail, dict):
                try:
                    stars = int(detail.get("Stars") or 0)
                except Exception:
                    stars = 0
                if stars < self._min_stars:
                    return None, None
                api_readme = detail.get("ReadMeContent") or detail.get("ReadmeContent")
                if isinstance(api_readme, str):
                    text0 = api_readme.strip()
                    # 仅当 README 不是以占位提示开头时才接受
                    if text0 and not text0.startswith("### 当前模型的贡献者未提供更加详细的模型介绍。"):
                        return text0, None
                    else:
                        return None,None
        except Exception:
            pass

        # [块] 回退：列出仓库文件，定位 README，再取文件内容
        try:
            files = await client.fetch_model_files(owner, name)
        except Exception:
            return None, None

        readme_entry: Optional[ModelFile] = None
        for f in files:
            pl = (f.path or "").strip().lower()
            if pl in {"readme.md", "readme.markdown", "readme.rst", "readme.txt"}:
                readme_entry = f
                break
        if readme_entry is None:
            return None, None

        try:
            content = await client.fetch_model_file_content(readme_entry)
        except Exception:
            return None, None
        if not content:
            return None, None
        text, url = content
        text = (text or "").strip()
        # 跳过以占位提示开头的 README（精确 startswith，避免误伤包含该句的正常内容）
        if not text or text.startswith("### 当前模型的贡献者未提供更加详细的模型介绍。"):
            return None, None
        return text, url

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

    def _list_models_page(self, page_number: int) -> Dict[str, object]:
        # Legacy SDK path removed; kept to avoid breaking stale references.
        return {"Models": [], "TotalCount": 0}

    def _fetch_readme_text_sync(
        self,
        owner: str,
        name: str,
        raw_item: Dict[str, object],
    ) -> Tuple[Optional[str], Optional[str]]:
        # Legacy SDK path removed; this method is no longer used.
        return None, None

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
        """基于 (repo_id, content_hash, index) 的稳定 UUIDv5，保证幂等写入。"""
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
