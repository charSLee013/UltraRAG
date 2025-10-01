# SOP: ModelScope 文档同步与检索集成

**版本**：2025-09-29  
**负责人**：<待指定>

## 背景与目标
- 目标：同步 ModelScope 仓库中的说明文档（README、CHANGELOG 等），生成向量索引供检索模块使用。
- 限定范围：仅说明类文档；无需下载模型或非说明文件。

## 规格（Spec）
1. **说明文档采集**
   - 模型：优先使用 ModelScope README 接口；必要时仅抓 `README*`, `CHANGELOG*`, `docs/**/*.md`。
   - 数据集：使用 `ReadmeContent` 或 `/summary` 页面。
   - 禁止遍历仓库所有文本文件。

2. **仓库级增量缓存**
   - 新增表 `repo_state(repo_type TEXT, owner TEXT, name TEXT, revision TEXT, PRIMARY KEY(...))`。
   - `fetch_documents` 启动时一次性加载 `repo_state`；若远端 revision 与缓存一致 → 直接跳过仓库，不请求 README。
   - 只有 revision 改变时才抓取 README 文本；成功后写回 `repo_state`，失败不写（保留重试）。

3. **数据存储结构**（目录 `output/modelscope_docs/`）
   - `docs.sqlite`：
     - `docs`: 仅说明文档文本及 SHA；主键 `(repo_type, owner, name, path)`。
     - `chunks`: 切片文本 + 嵌入（JSON）；
     - `sync_log`: 记录 `insert/update/skip/error/chunked`；
     - `repo_state`: 仓库 revision 缓存。
   - Chroma 集合 `modelscope_docs`（`CHROMA_PATH`）：documents、embeddings、metadata 与 `chunks` 对齐。

4. **嵌入速率限制**
   - 自适应令牌桶：并发上限 256，最小 1；失败减半，成功逐步恢复。
   - 嵌入批次默认 64，使用 `asyncio.gather` 并发请求；仅嵌入阶段受限。

5. **批量写入与并发**
   - 默认 `CHUNK_WORKERS = 64` 并发消费者。
   - `chunk_worker` 缓冲 20~50 条切片后一次事务写入 `chunks/sync_log`，提交成功再 upsert Chroma。
   - SQLite 使用 `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, `PRAGMA busy_timeout=3000`；失败回滚并抛错。

6. **参数与环境**
   - `servers/retriever/parameter.yaml`: 仅包含 `chroma_path`, `chroma_collection`, `embedding_api_url`, `embedding_api_key`, `embedding_model` 等必要项。
   - `.env`/`.env.example`: 提供 `EMBEDDING_API_URL`, `EMBEDDING_API_KEY`, `EMBEDDING_MODEL`, `EMBEDDING_TIMEOUT`, `CHROMA_PATH`, `CHROMA_COLLECTION`, `CHUNK_FLUSH_THRESHOLD`, `CHUNK_WORKERS`。

## 实施计划
1. **代码**：
   - `fetch_documents` 按说明类接口/文件获取 README，结合 `repo_state` 做跳过；
   - 嵌入阶段利用令牌桶并发；`chunk_worker` 批量写入并更新 `repo_state`。
2. **配置/文档**：更新 `.env.example`、`servers/retriever/parameter.yaml`、`AGENTS.md`、`docs/community_agent_design.md` 说明上述限制。
3. **验证**：
   - 运行同步脚本确保 `docs/chunks` 仅含说明文档；
   - 检索示例 `ultrarag run examples/rag.yaml` 应引用说明文本；
   - 重复运行时，未变化的仓库应在 fetch 前被跳过。
4. **回滚**：如要恢复旧逻辑，先修订 SOP，再调整代码；禁止私下偏离规格。

## 教训与后续
- 始终以说明文档为核心，避免仓库文件泛滥。
- 增量判断必须发生在发请求前，避免串行 fetch 成为短板。
- SOP 是唯一事实源：任何实现偏离需先更新 SOP。
- 在范围收敛、仓库级跳过落地前，进一步并发表现有限。
