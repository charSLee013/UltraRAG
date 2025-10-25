# Community Intelligent Agent Design

## 原始驱动力（竞赛要求）
> 主题：“构建最懂开发者的 AI 助手”。要求助手能精准解答模型使用、技术调试、平台功能、项目指导等问题，依托真实社区问答数据（多轮、多模态），并在严格延迟预算下输出可信答案。

## 已完成的工作与对应 SOP
| 领域 | 当前成果 | 对应 SOP / 文档 |
| --- | --- | --- |
| README 语料抓取 | 采用统一的 Ingestion Pipeline（见右列），从官方来源拉取 README 并入库。 | `docs/sop/ingestion_pipeline_sop.md` |
| 数据入库抽象 | 单一路径的 `fetch → process → ingest` 管线（Runner + SQLite + Chroma），无备用实现。 | `docs/sop/ingestion_pipeline_sop.md` |
| 检索-推理闭环 | 通过 Python API 以 Search‑o1 范式执行（init→loop→finalize），loop 次数由参数注入；不依赖 CLI/脚本。 | `docs/sop/search_o1_answering_sop.md` |
| 模板与停词固化 | 使用通用模板（reasoning/refinement/finalize）与最小停词；输出协议收敛为 Markdown，由 `custom.output_passthrough` 透传。 | `docs/sop/search_o1_answering_sop.md` |
| 自定义工具链 | `custom.search_o1_query_extract`（读 TokenContract）、`router.search_o1_check`（读 TokenContract）、`custom.output_passthrough`（去停词并透传 Markdown）。 | `docs/sop/search_o1_answering_sop.md` |
| 运行脚本 | 仅支持 Python API（`from ultrarag.api import SearchO1Pipeline`）。 | `docs/sop/search_o1_answering_sop.md` |
| 验证与日志 | 始终写入 memory 快照；`SEARCH_O1_DEBUG=1` 仅增强日志与可选溯源；记录 router 判定与循环耗时。 | `docs/sop/search_o1_answering_sop.md` |

## 待补齐的问题（尚无 SOP 或需扩展）
本阶段无新增 SOP 需求；仅聚焦“数据入库 → 工具兼容 → 检索结果”的最小交付。

## 当前流程快照（2025-10-07）
```
[数据同步]
  ingestion_pipeline（统一 Runner） ──> SQLite 与 Chroma（由 .env 的 INGESTION_SQLITE_PATH / CHROMA_PATH / CHROMA_COLLECTION 指定）
                                        │
[Search-o1 Pipeline]
  retriever.retriever_init_readme
       │
  prompt.search_o1_init → generation.generate
       │
  loop (max N via parameter)
    ├─ router.search_o1_check
    ├─ custom.search_o1_query_extract
    ├─ retriever.retriever_search_readme
    ├─ prompt.searcho1_reasoning_indocument → generation.generate
    └─ prompt.search_o1_insert → generation.generate
       │
  prompt.search_o1_finalize → generation.generate
       │
  custom.output_passthrough（去停词并返回 Markdown 文本）
```

当前交付范围（按本项要求）：数据入库 → 工具兼容 → 检索结果。
待开发节点：无（本阶段不包含引证结构、性能评测、诊断工具）。

## 迭代原则
- **Specification-First**：上述待办在实现前需补充或更新相应 SOP 文档。
- **Unix & Simple Code**：优先组合最小可用组件，保持脚本与服务器职责单一。
- **Make it work → make it right → make it fast**：先实现功能，再优化结构和性能；保留验证脚本与日志链路。

