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
     - `chunks`: 切片文本 + 嵌入（JSON）。
     - `repo_state`: 仓库 revision 缓存。
   - 不再维护 `sync_log`（排查靠 debug() 日志与最小化统计）。
   - Chroma 集合 `modelscope_docs`（`CHROMA_PATH`）：documents、embeddings、metadata 与 `chunks` 对齐。

4. **嵌入速率限制**
   - 自适应令牌桶：并发上限 256，最小 1；失败减半，成功逐步恢复。
   - 嵌入批次默认 64，使用 `asyncio.gather` 并发请求；仅嵌入阶段受限。

5. **批量写入与并发**
   - 默认 `CHUNK_WORKERS = 64` 并发消费者。
   - `chunk_worker` 缓冲 20~50 条切片后一次事务写入 `chunks`，提交成功再 upsert Chroma。
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
   - 运行同步脚本确保 `docs/chunks` 仅含说明文档；必要时打开 `INGEST_DEBUG=1` 使用 `debug()` 输出观察进度与故障点。
   - 检索示例 `ultrarag run examples/rag.yaml` 应引用说明文本；
   - 重复运行时，未变化的仓库应在 fetch 前被跳过。
4. **回滚**：如要恢复旧逻辑，先修订 SOP，再调整代码；禁止私下偏离规格。

## 教训与后续
- 始终以说明文档为核心，避免仓库文件泛滥。
- 增量判断必须发生在发请求前，避免串行 fetch 成为短板。
- SOP 是唯一事实源：任何实现偏离需先更新 SOP。
- 在范围收敛、仓库级跳过落地前，进一步并发表现有限。

## 数据卫生警告（必须阅读）

以下问题已在真实运行中观测到，均源自“原文直接入库 → 事后切片/清洗”的流程。为保证检索-推理质量与中立性，后续入库迭代必须按本节约束整改。

1) 脏源未移除（HTML/徽章/资源链接碎片）
- 现象：检索段落混入 `gle.com/assets/colab-badge.svg`、`Open In Colab`、`<img ...>` 等非正文残片。
- 真实示例：`gle.com/assets/colab-badge.svg`（出自运行快照 output/memory__run_20251009_214415.json）。
- 原因：README 原文（HTML/JSON）在 `script/modelscope_docs_sync.py` 中直接写入后切片入库。

2) 切片不按段/句（半标签/半属性）
- 现象：出现 `... alt=\"Open In Colab\"` 等被截断的属性尾段，清洗器难以完全剥离。
- 原因：`split_text()` 基于字符窗切分，对 HTML/JSON 结构不敏感，可能在标签/属性中间断裂。

3) 清洗不足（入库后清洗，鲁棒性有限）
- 现象：检索结果仍含 `\u003cdiv\u003e` 或 `Ã¥Â…`（mojibake）、以及 `modelscope://MusePublic/Qwen-image?revision=v1` 等资源 URL 噪音。
- 原因：`src/ultrarag/utils.py: normalize_readme_text()` 假设“完整 JSON/HTML”；对“半标签/混合转义/属性残片”覆盖不足。

整改建议（不改变中立检索边界）
- 入库前清洗：在 `chunk_worker` 将 `doc.content` 先经 `normalize_readme_text()`，以清洗后的纯文本进入切片与嵌入。
- 切片改进：`split_text()` 优先采用“段落/标题/列表项/句子”边界，避免在标签/属性中断裂。
- 清洗兜底：为 `normalize_readme_text()` 增加对“半标签/孤立属性/大量转义”的兜底删除规则（如 `assets/*.svg`、`Open In Colab` 行）。

验收标准（必须量化）
- 单条检索段落中非文字噪声（URL/标签/转义）可见占比 < 10%。
- 上述“真实示例”在新入库后不再出现在 `ret_psg`。
- `clean_state` 字段统计：`html_stripped/json_decoded` 占比提升、`raw` 占比下降（同等数据规模下）。

## 数据入库警告（待解决问题）

为保持检索中立、答案可信与可审计，README 入库阶段（ModelScope → Chroma）存在的已知风险必须在后续迭代中修复。当前实现优先保证跑通，尚未在“入库前”做强清洗，导致检索结果可能混入噪声。

- 脏源未移除（HTML/徽章/资源链接碎片）
  - 现象：检索段落混入非正文片段，如 `gle.com/assets/colab-badge.svg`、`Open In Colab`、`<img ...>` 残片。
  - 成因：`script/modelscope_docs_sync.py` 将 README 原文（可能为 HTML/JSON）直接入库后再切片与嵌入，非正文元素随之进入向量库。

- 切片不按段/句（易产生“半标签/半属性”）
  - 现象：出现被截断的属性或标签尾段（例：`... alt=\"Open In Colab\"`），清洗难以完全剥离。
  - 成因：`split_text()` 以字符窗为主，对 HTML/JSON 结构不敏感，可能在标签/属性中部断裂。

- 清洗不足（入库后清洗，鲁棒性有限）
  - 现象：检索结果仍含转义或乱码，如 `\u003cdiv\u003e`、`Ã¥Â…`（mojibake），以及 `modelscope://MusePublic/Qwen-image?revision=v1` 这类资源 URL 噪音。
  - 成因：`src/ultrarag/utils.py: normalize_readme_text()` 假设“完整 JSON/HTML”，对“半标签/混合转义/属性残片”鲁棒性不足。

真实案例（来自运行快照 `output/memory__run_20251009_214415.json`）
- `gle.com/assets/colab-badge.svg`（Colab 徽章链接尾段）
- `modelscope://MusePublic/Qwen-image?revision=v1`（资源 URL）
- `Ã¥Â…` 等编码残影（mojibake）

> 说明：上述问题的解决路径见 README Hardening SOP；本 SOP 在“实施计划/质量验收”中同步要求将清洗前移至入库前并设置质量闸（阈值与抽样策略）。

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
