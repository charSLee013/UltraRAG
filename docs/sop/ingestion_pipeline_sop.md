# SOP: README / ModelScope Ingestion Pipeline Paradigm

**版本**：2025-10-10  
**负责人**：<待指定>

## 1. 目的
- 为 UltraRAG 的所有知识来源建立统一的“获取 → 去重 → 清洗 → 切割 → 嵌入 → 标签 → 入库”范式。 
- 以 `SourceLocator` 三元组为唯一定位键，贯穿 SQLite 与 Chroma，支撑可追溯、可审计的知识图谱。 
- 通过抽象基类与异步编排，确保不同来源在不复制业务逻辑的情况下共享相同的质量闸与事务保证。 
- 对齐《README Retriever SOP》《chroma_retriever_sop》和 Search-o1 中立原则，避免引入来源偏置或输出歧义。

## 2. 核心数据结构

```python
class SourceType(str, Enum):
    MODELS = "models"
    DATASETS = "datasets"
    DOCS = "docs_overview"
    LEARN = "learn"
    STUDIOS = "studios"
    MCP = "mcp"
    AIGC = "aigc"
    GITHUB = "github"
    UNKNOWN = "unknown"

@dataclass(frozen=True)
class SourceLocator:
    source_type: SourceType
    owner_repo: str         # “owner/name”，无作者用 "default/<slug>"
    source_url: str         # 原始页面/API URL

@dataclass(frozen=True)
class RawDocument:
    locator: SourceLocator
    repo_id: str            # 归一化主键，推荐 f"{locator.source_type}:{locator.owner_repo}"
    payload: str            # 原始响应文本；GET 阶段必须完成解码
    fetched_at: datetime
    content_hash: str       # 可插拔哈希（不限定为 sha256）用于增量判定

@dataclass(frozen=True)
class CleanDocument:
    locator: SourceLocator
    repo_id: str
    content_hash: str       # 与 RawDocument.content_hash 保持一致
    text: str               # 清洗后的纯文本

@dataclass(frozen=True)
class ChunkDraft:
    locator: SourceLocator
    repo_id: str
    content_hash: str
    index: int              # 基于语义边界的稳定序号（0 起）
    text: str

@dataclass(frozen=True)
class EmbeddedChunk:
    chunk: ChunkDraft
    embedding: list[float]

@dataclass(frozen=True)
class ChunkRecord:
    chunk_uuid: uuid.UUID   # 与 Chroma 向量 id 同步
    repo_id: str
    content_hash: str
    index: int
    text: str
    locator: SourceLocator
    embedding: list[float]

@dataclass
class StageMetrics:
    stage: str
    total_in: int
    total_out: int
    dropped: int = 0
    warnings: list[str] = field(default_factory=list)
    sample_locators: list[SourceLocator] = field(default_factory=list)

@dataclass(frozen=True)
class PipelineRuntimeLimits:
    max_workers: int = 8                  # 并行“按文档”工人数量
    max_embed_concurrency: int = 32       # 嵌入 API 同时在飞请求数
    chunk_max_size: int = 32_768          # Split 生成单 chunk 的最大字符数
```

- `SourceLocator`：唯一的来源定位键，贯穿所有阶段与向量元数据。 
- `repo_id`：来源级主键，可按需重写（例如模型分支差异化）；必须与 SQLite `repo.repo_id` 一致。 
- `content_hash`：由获取或专用策略生成，用于识别文档内容变化（可使用摘要、版本号等函数）。 
- `ChunkDraft.index`：切割阶段生成的稳定序号，用于 deterministic 重放与批量删除。 
- `ChunkRecord.chunk_uuid`：由标签阶段统一生成，作为 SQLite `chunks` 主键与 Chroma `id`。 
- `StageMetrics`：统计各阶段的吞吐与异常，便于监控与验收。
- `PipelineRuntimeLimits`：运行时仅保留三个必须控制项——并行工人数、嵌入并发上限、chunk 最大长度；所有背压/限流逻辑围绕这三项展开。

## 3. 单一管线抽象基类（ABC）

```python
class BaseIngestionPipeline(abc.ABC):
    @abc.abstractmethod
    async def fetch(self, *, force: bool = False, **kwargs) -> AsyncIterable[RawDocument]:
        ...  # 产出 RawDocument（含 locator/repo_id/content_hash/payload）

    @abc.abstractmethod
    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,              # 由 Runner 注入，内部已做并发/令牌/退避
    ) -> list[ChunkRecord]:
        ...  # RawDocument In → List[ChunkRecord] Out（Clean→Split→embed(...)→Tag）


    @abc.abstractmethod
    async def ingest(
        self,
        records: list[ChunkRecord],
    ) -> None:
        ...  # SQLite 事务替换 + Chroma delete/upsert（带重试/补偿）
```

- 阶段顺序固定：**Fetch（含去重）→ Process（Clean→Split→Embed→Tag）→ Ingest**。 
- 抽象基类只定义业务契约，不承担运行参数或并发配置。

## 4. 运行器（Runner）与并发编排

运行器仅依赖三项限额：`max_workers`（并行工人）、`max_embed_concurrency`（嵌入信号量令牌数）与 `chunk_max_size`（Split 的硬上限）。
单个有界队列 `Q_docs` 承载去重后的 `RawDocument`，每名工人调用 `process`（内部完成 Clean→Split→Embed→Tag）再执行 `ingest`；嵌入阶段获取信号量令牌并按 AIMD 规则自适应调节并发。

```python
class IngestionRunner:
    def __init__(
        self,
        pipeline: BaseIngestionPipeline,
        limits: PipelineRuntimeLimits,
        *,
        embed_func: EmbedFn,
        fetch_force: bool = False,
        fetch_kwargs: dict[str, Any] | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.limits = limits
        self.fetch_force = fetch_force
        self.fetch_kwargs = fetch_kwargs or {}
        self.metrics = metrics
        self.q_docs: asyncio.Queue[RawDocument | None] = asyncio.Queue(maxsize=2 * limits.max_workers)
        self.embed_sem = asyncio.Semaphore(limits.max_embed_concurrency)
        self.embed_budget = limits.max_embed_concurrency
        self._embed_func = embed_func

    async def run(self) -> StageMetrics:
        producer = asyncio.create_task(self._produce_docs())
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.limits.max_workers)]
        await producer
        await self.q_docs.join()
        for _ in workers:
            await self.q_docs.put(None)
        await asyncio.gather(*workers)
        return StageMetrics(stage="run", total_in=0, total_out=0)

    async def _produce_docs(self) -> None:
        async for raw in self.pipeline.fetch(force=self.fetch_force, **self.fetch_kwargs):
            await self.q_docs.put(raw)

    async def _worker(self, worker_id: int) -> None:
        while True:
            raw = await self.q_docs.get()
            try:
                if raw is None:
                    return
                records = await self.pipeline.process(
                    raw,
                    chunk_max_size=self.limits.chunk_max_size,
                    embed=self._embed_with_limits,
                )
                await self.pipeline.ingest(records)
            finally:
                self.q_docs.task_done()

    async def _embed_with_limits(self, drafts: list[ChunkDraft]) -> list[EmbeddedChunk]:
        results: list[EmbeddedChunk] = []
        for batch in _group_for_api(drafts, self.limits.chunk_max_size):
            attempt = 0
            while True:
                async with self.embed_sem:
                    try:
                        embs = await self._embed_func(batch)
                    except TransientEmbedError:
                        self._embed_failure()
                        await asyncio.sleep(min(8, 2 ** attempt))
                        attempt += 1
                        continue
                    else:
                        self._embed_success()
                        results.extend(embs)
                        break
        return results

    def _embed_success(self) -> None:
        if self.embed_budget < self.limits.max_embed_concurrency:
            self.embed_budget += 1
            self.embed_sem = asyncio.Semaphore(self.embed_budget)

    def _embed_failure(self) -> None:
        self.embed_budget = max(1, self.embed_budget // 2)
        self.embed_sem = asyncio.Semaphore(self.embed_budget)


def _group_for_api(drafts: list[ChunkDraft], chunk_max_size: int) -> list[list[ChunkDraft]]:
    # 根据嵌入 API 限制拆批；示例保持单批，真实实现可按 token/字节拆分
    return [drafts]
```

- `Q_docs` 是唯一缓冲，容量约 `2 × max_workers`，当嵌入或入库变慢时自动堆满并向 Fetch/去重施加背压。
- 每名工人通过 `process` 串行完成 Clean→Split→Embed→Tag，再调用 `ingest`，保持“单文档原子”语义，便于审计与重放。
- 嵌入阶段通过信号量 + AIMD：成功逐步恢复并发，失败（429/5xx/Timeout）立即减半并指数退避。
- `chunk_max_size` 在 Split 阶段强制约束单 chunk 长度，避免 Embed 请求触发 413。
- Runner 汇总 `StageMetrics` 与失败率；Pipeline 子类只实现业务逻辑。

## 5. 实现指南

1. **来源接入（Fetch）**：
   - `fetch(force=False, **kwargs)` 必须内部完成 repo/doc 去重：`force=False` 时仅产出内容发生变化的文档，`force=True` 时可用于全量重建。
   - 支持 `**kwargs` 以适配不同来源（分页、筛选条件等）。
   - `payload` 必须为 `str`；无法解码时应抛出异常，禁止 bytes 进入流程。

2. **变换（Process）**：
   - `process(raw, chunk_max_size=..., embed=...)` 负责完成 Clean→Split→embed(...)→Tag 并产生 `ChunkRecord` 列表。
   - 实现需在 Split 阶段严格遵守 `chunk_max_size`（默认 32,768 字符），并保持索引从 0 连续增长。
   - 调用 `embed` 回调时必须原样传回 `ChunkDraft` 的顺序，确保向量与文本一一对应；嵌入失败应抛出 `TransientEmbedError` 让 Runner 处理重试与限速。
   - `ChunkRecord` 必须包含 `SourceLocator`、`repo_id`、`content_hash`、`chunk_index`、`text`、`embedding`，并生成稳定的 `chunk_uuid`（如基于 UUIDv5）。

3. **入库（Ingest）**：
   - `ingest(records, raw)` 按“删除阶段 → 写入阶段”执行，并在每个阶段内同步操作 Chroma 与 SQLite；任一环节失败时立即回滚当前事务并返回错误。
     1. 删除阶段：先调用 `Chroma.delete(old_chunk_ids)`（若失败直接退出），再开启 SQLite 事务删除旧 chunk 行并提交；这样对外始终不会出现“SQLite 指向已删向量”的瞬间。
     2. 写入阶段：开启新的 SQLite 事务插入全部新 chunk；随后调用 `Chroma.upsert(new_chunk_records)`。若写入或向量入库失败，回滚事务并在 Chroma 端删除已写入的向量，再返回错误。
   - 两个阶段都成功后即完成一次完整替换；额外的重试或告警策略由上层调度实现。

4. **度量**：
   - Runner 聚合 `StageMetrics`（文档数、入库数、失败数等）；Pipeline 可在关键路径输出附加日志。

5. **审计/重放**：
   - 保留 `RawDocument.payload` 与 `content_hash` 对应关系，便于复现。
   - 所有日志需包含 `repo_id`、`content_hash` 与 `chunk_uuid`，确保重放与排错。

**默认参数建议**（如来源无特殊要求，按下列配置）：
- `PipelineRuntimeLimits.max_workers = 8`（可根据 CPU/IO 调整，但需确保嵌入仍为瓶颈）。
- `PipelineRuntimeLimits.max_embed_concurrency = 32`，并使用信号量 + AIMD（失败减半、成功加 1）。
- `PipelineRuntimeLimits.chunk_max_size = 32_768` 字符，与目标嵌入 API 上限同步。
- 嵌入批量建议控制在 32 条以内，具体由 `process` 内部实现决定。
## 6. SQLite 与 Chroma 结构

```sql
CREATE TABLE repo (
    repo_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    owner_repo TEXT NOT NULL,
    source_url TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE chunks (
    chunk_uuid TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    FOREIGN KEY (repo_id) REFERENCES repo(repo_id)
);

CREATE INDEX idx_chunks_repo ON chunks(repo_id, chunk_index);
```

- 以上结构与用户提出的“最小三元定位 + repo/chunk 双表”一致，`chunk_uuid` 同步作为 Chroma `id`。 
- `repo.content_hash` 可由不同来源自定义生成方式（例如 README 正文哈希、API revision、手工校验值）；SOP 不限定具体算法，但必须稳定且可复现。 



## 7. 挂载流程示例

```python
class ModelScopeModelsPipeline(BaseIngestionPipeline):
    def __init__(
        self,
        client: ModelScopeClient,
        repo_store: RepoStateStore,
        doc_store: DocFingerprintStore,
        sqlite_db: SqliteConnection,
        chroma: ChromaClient, `
        limits: PipelineRuntimeLimits,
    ) -> None:
        self.client = client
        self.repo_store = repo_store
        self.doc_store = doc_store
        self.sqlite_db = sqlite_db
        self.chroma = chroma
        self.limits = limits

    async def fetch(
        self,
        *,
        force: bool = False,
        **kwargs: Any,
    ) -> AsyncIterable[RawDocument]:
        async for raw in ModelScopeModelsGetter(self.client, **kwargs):
            repo_changed = force or await self.repo_store.should_refresh(raw.repo_id, raw.content_hash)
            doc_changed = force or await self.doc_store.should_refresh(raw.locator, raw.content_hash)
            if repo_changed and doc_changed:
                yield raw

    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: Callable[[list[ChunkDraft]], Awaitable[list[EmbeddedChunk]]],
    ) -> list[ChunkRecord]:
        cleaner = MarkdownCleaner()
        clean = await cleaner.clean(raw)
        drafts = SemanticSplitter(max_chars=chunk_max_size).split(clean)
        embedded = await embed(drafts)
        assembler = ChunkAssembler(uuid_strategy="uuid5")
        return assembler.assemble(embedded, raw=raw, clean=clean)

    async def ingest(
        self,
        records: list[ChunkRecord],
    ) -> None:
        ingestor = SqliteChromaIngestor(self.sqlite_db, self.chroma)
        await ingestor.ingest(records)


limits = PipelineRuntimeLimits(max_workers=8, max_embed_concurrency=32, chunk_max_size=32_768)
pipeline = ModelScopeModelsPipeline(
    client=modelscope_client,
    repo_store=sqlite_repo_store,
    doc_store=content_hash_store,
    sqlite_db=sqlite_conn,
    chroma=chroma_client,
    limits=limits,
)
embed_client = AsyncEmbeddingClient(model="bge-m3", rate_limiter=limiter)

runner = IngestionRunner(
    pipeline,
    limits,
    embed_func=embed_client.embed,
    fetch_kwargs={"page_size": 100},
    metrics=PromMetricsCollector(...),
)
metrics = asyncio.run(runner.run())
```
```

- 子类通过实现 `fetch`/`process`/`ingest` 方法与共享的 `limits` 复用 Runner 的并发/背压策略。 
- `SqliteChromaIngestor` 在 `ingest` 中执行“删除→写入”两段式操作，并在外部阶段处理 Chroma 删除与 upsert。 
- 将 `PipelineRuntimeLimits` 同时传给 Pipeline 与 Runner，确保 Split 和 Embed 使用一致的 `chunk_max_size` 与嵌入限额。 
- Runner 的 `fetch_kwargs` 可用于分页、筛选等来源特定参数；`force=True` 时可用于全量重建。

## 8. 实现目录建议

保持结构简洁的同时，用文件夹归类可复用组件，子类集中在 `sources/` 并提供 `base.py` 统一抽象：

```
ingestion_pipeline/
├── __init__.py
├── limits.py               # PipelineRuntimeLimits、EmbedFn 等类型定义
├── base.py                 # BaseIngestionPipeline 抽象
├── runner.py               # IngestionRunner + 嵌入限流/AIMD
├── stores/
│   ├── __init__.py
│   └── sqlite.py           # “删除→写入”两阶段 ingest 实现（可复用 chroma SOP 逻辑）
├── embed/
│   ├── __init__.py
│   └── async_client.py     # 对接嵌入 API，暴露 EmbedFn
└── sources/
    ├── __init__.py
    ├── base.py             # 定义 fetch/process/ingest 子类合同
    └── modelscope_models.py# 示例来源实现
```

- 未来若新增来源，只需在 `sources/` 下增加对应文件并继承 `sources/base.py`。
- 存储/向量相关扩展可放在 `stores/`，无需动 Runner 或核心抽象。
- 该布局保持轻量，便于围绕本 SOP 独立实现并复用 chroma retriever 的存储细节。

## 9. 输出路径

为与 `docs/sop/chroma_retriever_sop.md` 保持一致，推荐沿用相同的输出目录结构，便于后续组件共享：

- SQLite：`output/modelscope_docs/sqlite/docs.sqlite`（包含 `repo`、`chunks` 两张表）。
- Chroma：`output/modelscope_docs/chroma`（集合名推荐 `modelscope_docs`，向量 metadata 必须带 `source_type`/`owner_repo`/`source_url`/`content_hash`/`chunk_index` 等字段）。
- 日志：可将每次运行的指标/告警写入 `output/modelscope_docs/logs/`，命名规则 `run_{timestamp}.json` 或等价格式，便于审计。

若未来扩展其他来源，可在相同目录下按来源类型追加子目录，但 SQLite + Chroma 路径保持不变，确保原有检索器无需修改即可读取。

## 10. 保障措施

1. **SOP 优先**：任何阶段的字段、顺序、质量阈值调整，必须先更新本文件再行编码。 
2. **自动化验证**：CI 运行最小示例（抓取单来源 → 全流程 → 断言噪声阈值、嵌入维度、事务一致性）。 
3. **监控**：MetricsCollector 输出 Prometheus/JSON；噪声 >10%、嵌入失败率 >5%、SQLite 回滚 >3 次需报警。 
4. **向量元数据合同**：Chroma metadata 仅包含 `source_type/owner_repo/source_url/repo_id/content_hash/chunk_index/fetched_at`，禁止扩展品牌或手工标签。 
5. **重放能力**：所有阶段日志带 run-id，保留 `repo_id + content_hash` 与 `chunk_uuid`，保证失败可复现。 
6. **最小字段原则**：禁止在 DTO 或表结构中新增 `notes`、`context` 等自由字段；若治理需要扩展，请在 Metrics 或外部审计表完成。

## 11. 审计追溯与知识图谱
- 任意 `chunk_uuid` 可通过 `chunks.repo_id` 关联到 `repo` 表，再结合 `SourceLocator` 溯源到原始来源 URL。 
- 知识图谱以 `SourceLocator` 作为节点（`source_type/owner_repo/source_url`），边连接 `chunk_uuid` 与向量 id，确保检索→回答链条可审计。 
- 保留 `RawDocument.payload` 与 `content_hash` 的映射，满足“重新抓取与验证”需求。

---

> 本 SOP 为数据入库范式的唯一事实源。所有来源接入、清洗策略、并发策略或事务约束的变更，必须先修订本文件再实施。
