# SOP: README / ModelScope Ingestion Pipeline Paradigm

**版本**：2025-10-10  
**负责人**：<待指定>

## 1. 目的
- 为 UltraRAG 的所有知识来源建立统一的“获取 → 去重 → 清洗 → 切割 → 标签 → 嵌入 → 入库”范式。 
- 以 `SourceLocator` 三元组为唯一定位键，贯穿 SQLite 与 Chroma，支撑可追溯、可审计的知识图谱。 
- 通过抽象基类与异步编排，确保不同来源在不复制业务逻辑的情况下共享相同的质量闸与事务保证。 
- 对齐《README Retriever SOP》与本 SOP（Ingestion Pipeline）以及 Search‑o1 中立原则，避免引入来源偏置或输出歧义。

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
    chunk_index: int
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
    writer_items: int = 0
    max_queue_depth: int = 0

@dataclass(frozen=True)
class PipelineRuntimeLimits:
    max_workers: int = 32                 # 并行“按文档”工人数量
    max_embed_concurrency: int = 512      # 嵌入 API 同时在飞请求数（实现默认）
    chunk_max_size: int = 32_768          # Split 生成单 chunk 的最大字符数
    ingest_batch_size: int = 16           # 写入协程微批 flush 大小
```

- `SourceLocator`：唯一的来源定位键，贯穿所有阶段与向量元数据。 
- `repo_id`：来源级主键，可按需重写（例如模型分支差异化）；必须与 SQLite `repo.repo_id` 一致。 
- `content_hash`：由获取或专用策略生成，用于识别文档内容变化（例如对正文取摘要或版本号），**尽可能保持稳定**。每个来源可以自定义策略：比如 ModelScope 模型库（https://modelscope.cn/models）就将 `content_hash` 显式设为 `repo_id`，以避免 README 下载 URL 的临时 `auth_key` 导致哈希抖动。 
- `source_type` 持久化规范（重要）：写入 SQLite 与 Chroma 元数据时，必须存放枚举的 `.value`（如 `"datasets"`），而不是 `str(Enum)`（如 `"SourceType.DATASETS"`）。Runner 在读取时同样使用 `.value` 过滤，避免因枚举字符串化差异导致去重集为空。
- `ChunkDraft.index`：切割阶段生成的稳定序号，用于 deterministic 重放与批量删除。 
- `ChunkRecord.chunk_uuid`：由标签阶段统一生成，作为 SQLite `chunks` 主键与 Chroma `id`。 
- `StageMetrics`：统计各阶段的吞吐与异常，便于监控与验收。
- `PipelineRuntimeLimits`：运行时仅保留三个必须控制项——并行工人数、嵌入并发上限、chunk 最大长度；所有背压/限流逻辑围绕这三项展开。

### 3.0 环境加载（.env）— 铁律
- 进程启动即调用 `dotenv.load_dotenv()` 读取仓库根目录 `.env`；配置仅从环境变量读取（加载 `.env` 之后）。
- 不允许并行的配置来源或兜底策略；缺少必需键时必须 fail-fast 并输出缺项提示。
- Runner 启动子进程时必须继承当前环境（`env=os.environ.copy()`），确保父子进程视图一致。

必需：`EMBEDDING_API_URL`、`EMBEDDING_API_KEY`、`EMBEDDING_MODEL`。可选：`MODELSCOPE_DATASETS_TARGET`（≥1 时限制本轮成功抓取数）。

## 3. 单一管线抽象基类（ABC）

为支持“按来源类型的去重”与“按页拉取”，抽象基类的契约统一如下：

```python
class BaseIngestionPipeline(abc.ABC):
    # 所属来源层，必须显式声明，用于 Runner 选择正确的去重集
    source_type: SourceType

    @abc.abstractmethod
    async def fetch(
        self,
        *,
        force: bool = False,
        existing_hashes: set[str] | None = None,
        **kwargs,
    ) -> AsyncIterable[list[RawDocument]]:
        """
        按页产出：每次 yield 为一页内成功构建的 RawDocument 列表。
        - force=True 时，忽略去重集（existing_hashes 视为 set()）。
        - existing_hashes：仅包含本 source_type 的 content_hash。
        - 不得返回空列表（当页全失败时跳过该页）。
        """

    @abc.abstractmethod
    async def process(
        self,
        raw: RawDocument,
        *,
        chunk_max_size: int,
        embed: EmbedFn,              # 由 Runner 注入，内部已做并发/令牌/退避
    ) -> list[ChunkRecord]:
        """RawDocument In → List[ChunkRecord] Out（Clean→Split→embed(...)→Tag）。"""

    @abc.abstractmethod
    async def ingest(
        self,
        records: list[ChunkRecord],
        raw: RawDocument,
    ) -> None:
        """SQLite 事务替换 + 向量库删除/写入（带重试/补偿）。"""
```

Runner 对应职责更新：
- 启动阶段读取 SQLite 中本 source_type 的 content_hash 集合（例如 datasets 使用 `datasets:{owner}/{name}` 作为身份哈希）；`force=True` 则传入空集。
- 以 `existing_hashes` 显式参数调用 `pipeline.fetch(...)`；不再通过构造函数隐式注入去重集。
- 逐页消费 `List[RawDocument]` 并扁平化进入处理队列；其它阶段（process/embed/ingest）不变。
- 读取去重集的 SQL 必须使用 `pipeline.source_type.value` 作为过滤条件（与持久化约定一致）。

- 阶段顺序固定：**Fetch（含去重）→ Process（Clean→Split→Embed→Tag）→ Ingest**。 
- 抽象基类只定义业务契约，不承担运行参数或并发配置。

### 3.1 Fetch 阶段的性能约束

- **吞吐优先但需可控**：Fetch 应在满足业务完整性的前提下尽可能高效。允许来源实现有限的并发抓取策略（例如 README 下载异步化），但必须在本 SOP 中明确：最大并发、超时、重试/退避、失败记录方式。
- **必须记录失败与警告**：即便采取“超时后放弃”策略，也要在日志与 StageMetrics（`warnings`、`dropped` 等字段）中留下信息，便于后续补抓或复现；严禁静默丢弃。
- **受 Runner 调度约束**：Fetch 内的任何并发实现都必须与 Runner 的 `max_workers` 配合，不得私下创建无限线程/后台池；推荐通过受控的 `asyncio.Semaphore` 或令牌桶实现。
- **新增或修改并发策略前须先更新本 SOP**：遵循 Specification-First，先在文档里说明目标、参数、边界，再进入编码阶段。

#### 3.1.1 ModelScope 官方模型库 API 规范

- **唯一通道（仅适用于“模型库：https://modelscope.cn/models”这一来源）**：获取模型列表与 README 元数据必须复用官方 Hub API 的行为（等价于 `HubApi.list_models`、`HubApi.get_model_files`、`HubApi.get_model` 的请求），即便在代码中不直接依赖 `HubApi` 类，也必须构造相同的 HTTP 调用（路径、Body、Header、Cookie、超时/重试策略）。禁止重新访问旧的 `/dolphin/models` 或手写 README URL。
- **必备 Header**：每次请求至少包含 `User-Agent`、`Content-Type: application/json`（或 `text/plain` 对 README 下载）、`X-Request-ID`（随机 UUID）。如需认证，沿用 ModelScopeConfig 提供的 cookie/token。
- **README 下载**：仅允许使用 API 返回的 `ReadMeContent` 字段或 `README.md` 文件元数据生成的下载链接；不得再维护多种候选文件名或兜底抓取逻辑。缺失时跳过并在日志记录。
- **节流策略**：在 API 返回的单页数据上应用受控并发（推荐 Semaphore 16~32），并为每个 README 请求引入 0.1~3.5 秒随机延迟以减轻服务器压力；单次请求超时仍保持 60 秒。
- **失败处理**：`list_models` 级别出现 429/5xx 时以指数退避整页重试；单个 README 下载失败则记录日志并跳过，不回滚整页；不会因为连续失败而提前停止。本轮成功的模型不得重复抓取。
- **去重（新版 “全集→差集” 策略）**：
  - 规划阶段必须一次性拉取 **全部** `repo_id` 清单（允许分页拉取但结果需汇总），然后与 SQLite 中已存在的 `repo_id` 集合做差集，生成“待抓取列表”。规划阶段不得在分页循环中即时跳过或停止，确保 UI 与日志能准确显示 “page X/Y”。
  - 差集列表确定后，再按列表顺序（可批量异步）请求 README；README 下载失败只影响当前条目，不重回规划阶段。
  - ingest 成功后立即把新增 `repo_id`（=`content_hash`）写回集合，下一批次继续沿用，避免重复抓取。
- **基线同步**：运行器在调度前会读取 SQLite `repo` 表中的历史 `content_hash`，作为基线集合注入来源；来源必须在完成 ingest 后把当次成功的 `content_hash` 追加回集合，确保下一批次继续沿用真实库存（避免重复抓取）。ModelScope source 当前令 `content_hash == repo_id`，但框架层契约仍以 `content_hash` 为准。
- **抓取配额（严格定义）**：`MODELSCOPE_MODEL_SIZE` 控制“本轮最多成功抓取多少个 README”（以本次新增的 `content_hash` 计），值 ≤0 表示不限量。来源在统计是否达标时只计算本轮新增的成功数，基线集合仅用于去重。失败/跳过不计入成功数；若枚举所有页面仍未达到配额，必须记录告警。有限配额时，Hub API 的 `page_size` 建议取 `min(配额差值, 100)`；不限量则维持 100。

> ⚠️ 其它官方渠道（Docs / Learn / GitHub / Datasets / Studios / MCP / AIGC 等）同样遵循“优先官方 API/页面”的原则，但各自接口与结构不同，必须在编码前先在本 SOP 中补充对应的策略与合规说明，严禁直接沿用模型库的 Hub API 调用方式，以免引入噪声或合规风险。

## 4. 运行器（Runner）与并发编排

运行器仅依赖四项限额：`max_workers`（并行工人）、`max_embed_concurrency`（嵌入信号量令牌数）、`chunk_max_size`（Split 的硬上限）与 `ingest_batch_size`（写入微批大小）。
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
        self.q_ingest: asyncio.Queue[tuple[list[ChunkRecord], RawDocument] | None] = asyncio.Queue(maxsize=limits.max_workers)
        self.embed_sem = asyncio.Semaphore(limits.max_embed_concurrency)
        self.embed_budget = limits.max_embed_concurrency
        self._embed_func = embed_func

    async def run(self) -> StageMetrics:
        producer = asyncio.create_task(self._produce_docs())
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.limits.max_workers)]
        writer = asyncio.create_task(self._writer())
        await producer
        await self.q_docs.join()
        for _ in workers:
            await self.q_docs.put(None)
        await asyncio.gather(*workers)
        await self.q_ingest.join()
        await self.q_ingest.put(None)
        await writer
        return StageMetrics(stage="run", total_in=0, total_out=0)

    # 注：上述返回示例仅演示结构；真实实现需累积并返回文档与记录计数
    # （例如 total_in/total_out），以及并发轨迹等观测指标。

    async def _produce_docs(self) -> None:
        async for page in self.pipeline.fetch(force=self.fetch_force, **self.fetch_kwargs):
            for raw in page:
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
                await self.q_ingest.put((records, raw))
            finally:
                self.q_docs.task_done()

    async def _writer(self) -> None:
        while True:
            item = await self.q_ingest.get()
            if item is None:
                return
            records, raw = item
            try:
                await self.pipeline.ingest(records, raw)
            finally:
                self.q_ingest.task_done()

    async def _embed_with_limits(self, drafts: list[ChunkDraft]) -> list[EmbeddedChunk]:
        results: list[EmbeddedChunk] = []
        for batch in _group_for_api(drafts, self.limits.chunk_max_size):
            attempt = 0
            while True:
                async with self.embed_sem:
                    try:
                        embs = await self._embed_func(batch)
                    except Exception:
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
- `PipelineRuntimeLimits.max_workers = 32`（可根据 CPU/IO 调整，但仍需确保嵌入或上游限额是瓶颈）。
- `PipelineRuntimeLimits.max_embed_concurrency = 512`，并使用信号量 + AIMD（失败减半、成功加 1）。
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
        chroma: ChromaClient,
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
        raw: RawDocument,
    ) -> None:
        ingestor = SqliteChromaIngestor(self.sqlite_db, self.chroma)
        await ingestor.ingest(records, raw)


limits = PipelineRuntimeLimits(max_workers=32, max_embed_concurrency=32, chunk_max_size=32_768)
pipeline = ModelScopeModelsPipeline(
    client=modelscope_client,
    repo_store=sqlite_repo_store,
    doc_store=content_hash_store,
    sqlite_db=sqlite_conn,
    chroma=chroma_client,
    limits=limits,
)

runner = IngestionRunner(
    pipeline,
    limits,
    embed_func=embed_httpx,             # 调用 ingestion_pipeline.embed.adapters.embed_httpx
    fetch_kwargs={"page_size": 100},
    metrics=PromMetricsCollector(...),
)
metrics = asyncio.run(runner.run())
```
```

- 子类通过实现 `fetch`/`process`/`ingest` 方法，与共享的 `limits` 复用 Runner 的并发/背压策略。
- Runner 仅保留一个集中写入协程：所有 worker 将 `(records, raw)` 放入新建的 `AsyncQueue`，写入器按照入队顺序依次调用 `SqliteChromaIngestor.ingest()`；完成后才取下一条。**禁止任何代码绕过队列直接写入存储。**
- `PipelineRuntimeLimits` 仍只维护并发和分片大小等核心参数；队列容量固定为 `2 * max_workers`，无额外开关或重试策略。
- Runner 的 `fetch_kwargs` 可用于分页、筛选等来源特定参数；`force=True` 时可用于全量重建。写入器为单协程串行写入，支持微批 flush，阈值由 `limits.ingest_batch_size` 控制。
- 嵌入适配器使用 `httpx.AsyncClient`，向 `{EMBEDDING_API_URL.rstrip('/')}/embeddings` 发送 `POST`，Body 包含 `model`、`input`（批量文本）以及可选的 `encoding_format`、`dimensions`；必须提供 `Authorization: Bearer {EMBEDDING_API_KEY}`。严格保持最小实现，避免额外封装或“自动拼路径”造成路径重复。

- 原子性：每个 repo 固定执行  
  `SQLite: begin → upsert_repo → delete_repo_chunks → insert_chunks` →  
  `Chroma: delete_repo → upsert_records` → `SQLite: commit`。如写入失败，立即抛出异常并回滚当前 repo；不得引入静默降级或备用路径。
- 监控：`StageMetrics` 新增 `writer_items`（成功写入的 repo 数）与 `max_queue_depth`（运行期间的队列峰值），用于确认串行写入是否健康。

### ModelScope 数据集来源实现(可以参考学习)

`ingestion_pipeline/sources/datasets_pipeline.py` 复用同样抽象；Fetch 关键点：

- 分页：调用官方 `/api/v1/dolphin/datasets` 获取页数据；页内以 `Semaphore(N)` 并发抓取详情，并用 `asyncio.wait_for(timeout)` 保护。
- 去重：`content_hash = repo_id = datasets:{owner}/{name}`；用 `existing_hashes` 与本页 `seen` 去重。
- README：`/api/v1/datasets/{owner}/{name}` 读取 `ReadmeContent`；为空/缺失则跳过。
- 产出：按页一次性 `yield list[RawDocument]`；达成 `MODELSCOPE_DATASETS_TARGET`（若设置）即停止。
- Header：请求统一复制当前环境实测 UA（例如 `modelscope/1.30.0; python/3.11.9; platform/macOS-13.7-arm64-arm-64bit; processor/arm; env/custom; user/unknown`），在发送前仅替换其中的 `session_id/<hex>` 为新的 32 位 UUID，同时附带随机 `X-Request-ID`。
- 其它阶段（Clean → Split → Embed → Ingest）遵循通用规则：32_768 chunk 上限、UUIDv5 chunk_uuid、SQLite/Chroma 两阶段写入与最小 metadata 合同。

配套脚本 `ingestion_pipeline/ingest_modelscope_datasets_readmes.py` 为生产入口，保留 `MODELSCOPE_DATASETS_TARGET` 作为可选限制参数，其余流程固定、不可切换。示例：

```
EMBEDDING_API_URL=... \
EMBEDDING_API_KEY=... \
EMBEDDING_MODEL=... \
.venv/bin/python ingestion_pipeline/ingest_modelscope_datasets_readmes.py
```

模型 README 入库脚本 `ingestion_pipeline/ingest_modelscope_readmes.py` 同样复用这一单一写入通路；仓库中不再保留任何并行写入实现或隐藏开关。

兼容性：旧钩子已失效。

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
│   └── sqlite.py           # “删除→写入”两阶段 ingest 实现（原子替换写入）
├── embed/
│   ├── __init__.py
│   └── adapters.py         # httpx 方式调用 {EMBEDDING_API_URL}/embeddings，暴露 EmbedFn
└── sources/
    ├── __init__.py
    ├── base.py             # 定义 fetch/process/ingest 子类合同
    └── modelscope_models.py# 示例来源实现
```

- 未来若新增来源，只需在 `sources/` 下增加对应文件并继承 `sources/base.py`。
- 存储/向量相关扩展可放在 `stores/`，无需动 Runner 或核心抽象。
- 该布局保持轻量，便于围绕本 SOP 独立实现并复用 chroma retriever 的存储细节。

## 9. 输出路径

本 SOP 统一规定输出目录结构如下，便于后续组件共享：

- SQLite：固定为 `output/ingestion/sqlite/docs.sqlite`（包含 `repo`、`chunks` 两张表）。**所有入库流水线必须复用这一数据库，不得为不同来源另起路径。**
- Chroma：固定为 `output/ingestion/chroma`（集合名为 `modelscope_docs`，向量 metadata 必须带 `source_type`/`owner_repo`/`source_url`/`repo_id`/`content_hash`/`chunk_index`/`fetched_at`）。**同样所有流水线共享该向量库，严禁分散存放。**
- 日志：可将每次运行的指标/告警写入 `output/ingestion/logs/`，命名规则 `run_{timestamp}.json` 或等价格式，便于审计。

若未来扩展其他来源，可在相同目录下按来源类型追加子目录，但 SQLite + Chroma 路径保持不变，确保原有检索器无需修改即可读取。

> **注意**：新增 ingestion pipeline 时不得创建新的数据库或向量目录，务必复用上述 SQLite/Chroma 配置（通过环境变量覆盖亦需指向同一套路径），并以 Runner + Store 组件为唯一通路。

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

## 入库来源（覆盖范围）

为保证知识覆盖并减少偏置，入库来源限定为 ModelScope 官方/社区面向开发者的文档与仓库页面（仅说明类文本）：

- 文档中心（魔搭平台功能介绍）：https://www.modelscope.cn/docs/overview
- 研习社（模型解读与最佳实践）：https://modelscope.cn/learn
- GitHub（魔搭开源项目技术类文档）：https://github.com/modelscope
- 模型库：https://modelscope.cn/models
- 数据集：https://modelscope.cn/datasets
- 创空间应用：https://modelscope.cn/studios
- MCP：https://www.modelscope.cn/mcp
- AIGC 生图和训练：https://www.modelscope.cn/aigc

约束：
- 仅抓取 README/CHANGELOG/docs/**/*.md/.rst/.txt 等说明类文本；不下载模型二进制与非说明文件。
- 单条文档大小上限与切片/嵌入速率受本 SOP 对应环境变量与限流策略约束。



> 本 SOP 为数据入库范式的唯一事实源。所有来源接入、清洗策略、并发策略或事务约束的变更，必须先修订本文件再实施。
