# Community Intelligent Agent Design

## 原始驱动力（竞赛要求）
> 主题：“构建最懂开发者的 AI 助手”。要求助手能精准解答模型使用、技术调试、平台功能、项目指导等问题，依托真实社区问答数据（多轮、多模态），并在严格延迟预算下输出可信答案。

## 已完成的工作与对应 SOP
| 领域 | 当前成果 | 对应 SOP / 文档 |
| --- | --- | --- |
| README 语料抓取 | `script/modelscope_docs_sync.py` 定期同步 ModelScope README 至 `output/modelscope_docs/chroma`，支撑 Env-first 检索。 | `docs/sop/readme_retriever_hardening_sop.md` |
| 检索-推理闭环 | `examples/search_o1.yaml` 将 `retriever.retriever_init_readme → search_o1_init → router.search_o1_check` 组合成“检索→插入→再推理”循环，loop 上限 2。 | `docs/sop/search_o1_answering_sop.md` |
| 模板与停词固化 | `prompt/search_o1_reasoning/refinement/finalize.jinja` 统一约束输出 `\boxed{...}<|im_end|>`；`servers/generation/parameter.yaml` & `examples/parameter/search_o1_parameter.yaml` 固定停词、Env-first 参数。 | `docs/sop/search_o1_answering_sop.md` |
| 自定义工具链 | `custom.search_o1_query_extract`、`custom.search_o1_ensure_stop`、`custom.output_extract_from_boxed` 实现查询抽取、停词补写、boxed 抽取。 | `docs/sop/search_o1_answering_sop.md` |
| 运行脚本 | `script/run_search_o1.py` 支持 `.venv` 下的 `build/run`，无需可执行安装。 | `docs/sop/search_o1_answering_sop.md` |
| 验证与日志 | Search-o1 pipeline 已在 `output/memory_manual_qwen_models_search_o1_*.json` 产出自动化清单；日志记录 finalize → generate → ensure_stop → extract → evaluate；Env-first 单测覆盖 (`tests/servers/test_generation_env_first.py`)。 | `docs/sop/search_o1_answering_sop.md` |

## 待补齐的问题（尚无 SOP 或需扩展）
| 未完成项 | 原始需求缺口 | 下一步动作 |
| --- | --- | --- |
| 多轮对话与记忆 | 竞赛要求多轮问答、个性化上下文；目前仅单轮 `benchmark.get_data`。 | 设计并撰写 `docs/sop/conversation_context_manager.md`（新），实现 `conversation.context_manager` 服务器。 |
| 证据引用与后续建议 | 原始目标强调可信、可追溯；当前输出无 cite、无 follow-up。 | 扩展 Search-o1 SOP，定义 citation 结构、自定义工具 `custom.collect_evidence`。 |
| 多模态 & 论坛数据 | 要求涵盖代码、图片、社区问答；目前仅 README 文本。 | 编写 `docs/sop/community_corpus_ingestion.md`（新）规划 Issue/论坛/多模态采集、向量化。 |
| Reranker & 性能预算 | 未验证 3s/10s SLA，缺少 rerank & 缓存策略。 | 更新 generation/检索 SOP，添加性能测试、SiliconFlow reranker 接口。 |
| 结构化 API 输出 | 目标是“Query → Answer → 中间证据”结构；现依赖磁盘 JSON。 | 在 Search-o1 SOP 内新增 `RunTrace` 设计，返回内存结构并保留可选磁盘落盘。 |

## 当前流程快照（2025-10-05）
```
[数据同步]
  script/modelscope_docs_sync.py ──> output/modelscope_docs/chroma
                                       │
[Search-o1 Pipeline]
  benchmark.get_data
       │
  retriever.retriever_init_readme
       │
  prompt.search_o1_init → generation.generate
       │
  loop (max 2)
    ├─ router.search_o1_check
    ├─ custom.search_o1_query_extract
    ├─ retriever.retriever_search_readme
    ├─ prompt.searcho1_reasoning_indocument → generation.generate
    └─ prompt.search_o1_insert → generation.generate
       │
  prompt.search_o1_finalize → generation.generate
       │
  custom.search_o1_ensure_stop → custom.output_extract_from_boxed
       │
  evaluation.evaluate ──> output/evaluate_results_*.json

当前完成节点：数据同步、检索闭环、Qwen 清单输出。
待开发节点：对话记忆、证据引用、多模态采集、性能评测、诊断工具、结构化 API。
```

## 迭代原则
- **Specification-First**：上述待办在实现前需补充或更新相应 SOP 文档。
- **Unix & Simple Code**：优先组合最小可用组件，保持脚本与服务器职责单一。
- **Make it work → make it right → make it fast**：先实现功能，再优化结构和性能；保留验证脚本与日志链路。
