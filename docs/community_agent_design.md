# Community Intelligent Agent Design

## Goals
- Tailor UltraRAG as a community-oriented assistant capable of answering model usage, technical debugging, platform features, and project guidance questions.
- Deliver accurate, traceable responses under strict latency budgets (< 3s avg, < 10s complex queries).
- Maintain the framework's "less is more" minimalism while enabling fast iteration for competition data.

## High-Level Flow
1. **Corpus Preparation**
   - Ingest community assets (FAQ, tutorials, code labs, forum threads).
   - Normalize into `corpus.meta.jsonl` + `corpus.texts.tar.gz` manifests to preserve order and compression.
   - Generate vectors via SiliconFlow or local encoder, persist to `corpus.vectors.npy` + `corpus.faiss.index`.
2. **Pipeline Execution**
   - `benchmark.get_data`: serve inline or dataset-driven multi-turn queries.
   - `conversation.context_manager` (new) stitches previous turns + retrieved evidence.
   - `remote_retriever.retrieve`: lazily loads FAISS index, fetches top-k documents.
   - `remote_reranker.rerank`: refine ranking with SiliconFlow reranker.
   - `prompt.community_template`: emits instruction-tuned prompts (with cite, follow-up suggestions).
   - `generation.generate`: call ModelScope chat model (temperature 0 for deterministic replies).
   - `custom.output_extract_from_boxed`: trim final response + citations.
   - `evaluation.evaluate`: compute ROUGE/F1 for regression tests.
   - `llm_evaluator.evaluate_with_llm`: semantic scoring during QA sign-off.
3. **Response Delivery**
   - Return final answer + citations + optional next-step guidance.
   - Persist logs to `output/memory_*` for traceability.

## Key Modules & Enhancements
- **Conversation Memory**: new server managing session state, storing prior QA pairs, handing context to prompt generation.
- **Inline vs Dataset Input**: `servers/benchmark` already supports JSONL and inline queries; competition datasets can override via `path + key_map`.
- **Retrieval**: FAISS cache ensures sub-second lookups; fallback to fresh embedding only when corpus changes.
- **Evaluation**: LLM judge (ModelScope) complements hard metrics to approximate human scoring.
- **Extensibility**: Additional servers (e.g., `code_executor`, `error_diagnoser`) can be slotted between retriever and prompt for specialized diagnostics.

## Latency Strategy
- Warm load FAISS index at service boot.
- Use lightweight prompt + deterministic decoding (temperature = 0.0, top_p = 0.1).
- Batch embedding/rerank requests and leverage retry logic (30s × 3) to absorb remote hiccups without manual restarts.

## Accuracy & UX Notes
- Prompts must include document excerpts + citation tags (`[doc:path#chunk]`).
- Encourage follow-up suggestions when confidence < threshold (LLM judge output).
- For multi-modal data, store metadata in manifest and fetch attachments on demand.

## Stability & Ops
- All generated artifacts live under `data/` (corpus) and `output/` (memory, metrics); no temporary files persist after successful builds.
- Continuous runs compare timestamps to detect stale caches.
- Memory logs double as audit trail for competition submissions.

## Next Steps
1. Implement `conversation.context_manager` server.
2. Import社区文档（FAQ、README、使用指南、论坛精华）并转换成压缩 manifest + FAISS 索引。
3. Craft community-specific prompt templates with citation scaffolding.
4. Add specialized diagnostics servers (code execution, configuration tips) as required by problem categories.
5. Wire automated regression suites leveraging LLM judge to monitor answer quality.

## ModelScope Data Ingestion Milestones

**Milestone A — Public Discovery & Snapshot**
- *Must*: 使用无凭证的 `HubApi`/`repo_info` 拉取指定模型与数据集的公开元数据（名称、下载量、更新时间、README 摘要），并将结果以 JSONL 形式落盘；补充页面爬取兜底方案（当 API 缺字段时，解析 `/summary` 页面得到同等信息）。
- *May*: 构建轻量脚本对比多次抓取结果，输出字段变化 diff，用于监控 ModelScope 平台改版。

**Milestone B — 文档抓取与内容筛选**
- *Must*: 针对模型/数据集仓库，仅同步 README、CHANGELOG、使用指南、FAQ 等文本/代码片段文件（含多语言版本），记录来源 URL、时间戳与哈希；过滤二进制/大文件，确保知识库聚焦解疑资料。
- *May*: 结合社区论坛、Issue、博客等公开页的爬取适配器，扩充高价值问答与教程，并建立重复内容检测规则。

**Milestone C — 结构化入库与知识切片**
- *Must*: 将抓取到的文本资料与 `repo_info` 元数据合并为统一 schema（如 `knowledge_chunks`, `source_meta`），完成基础清洗（Markdown 转纯文本、去噪、语种标注、上下文切片）。
- *May*: 引入语义标签（任务类型、模型类别、常见问题）与可信度评分，方便后续检索排序与答案置信度估计。

**Milestone D — 检索就绪与场景评测**
- *Must*: 基于 Milestone C 的知识库生成向量索引（`corpus.meta.jsonl`、`corpus.texts.tar.gz`、`corpus.vectors.npy`、`corpus.faiss.index`），并在代表性问答集合上验证 `remote_retriever` + `prompt.community_template` 组合的召回与回答质量。
- *May*: 为热点主题生成自动化 synopsis / QA 样例，纳入评测集以覆盖多轮问答、代码片段、平台功能等场景。

**Milestone E — 持续同步与健康监控**
- *Must*: 制定更新策略（cron/触发式），对文档/README/FAQ 等源进行变更检测后自动执行抓取、入库、索引刷新，并在 `output/memory_*` 或监控表中记录版本差异与失败日志。
- *May*: 集成告警（例如抓取失败、关键字段缺失、索引过期）与可视化面板，及时提示知识库健康状况。
## Community Feature Coverage
- 模型中心：浏览、在线体验、下载、Notebook；大量 SOTA 模型（LLM/多模态/CV/语音/AI for Science）
- 数据集中心：Dataset Hub、Notebook 加载、比赛数据
- Pipeline / Spaces：任务级 Demo、Spaces 应用、在线推理
- Notebook & 云环境：GPU/CPU 一键环境、实验教学
- 社区功能：收藏、排行榜、活动/竞赛、贡献积分
- 平台文档：部署指南、下载 API、多语言 README、教程
- 技术支持：Issue、论坛、训练/推理并行策略、MLOps 支持
- 案例与最佳实践：README 示例、Notebook、Spaces 案例
