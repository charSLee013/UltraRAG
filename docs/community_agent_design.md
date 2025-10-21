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
| 验证与日志 | Search‑o1 pipeline 可在 `SEARCH_O1_DEBUG=1` 下写入 memory 快照；日志记录 router 判定与循环耗时；Env‑first 单测覆盖 (`tests/servers/test_generation_env_first.py`)。 | `docs/sop/search_o1_answering_sop.md` |

## 待补齐的问题（尚无 SOP 或需扩展）
| 未完成项 | 原始需求缺口 | 下一步动作 |
| --- | --- | --- |
| 多轮对话与记忆 | 竞赛要求多轮问答、个性化上下文；目前仅单轮 `benchmark.get_data`。 | 设计并撰写 `docs/sop/conversation_context_manager.md`（新），实现 `conversation.context_manager` 服务器。 |
| 证据引用与后续建议 | 原始目标强调可信、可追溯；当前输出无 cite、无 follow-up。 | 扩展 Search-o1 SOP，定义 citation 结构、自定义工具 `custom.collect_evidence`。 |
| 多模态 & 论坛数据 | 要求涵盖代码、图片、社区问答；目前仅 README 文本。 | 编写 `docs/sop/community_corpus_ingestion.md`（新）规划 Issue/论坛/多模态采集、向量化。 |
| Reranker & 性能预算 | 未验证 3s/10s SLA，缺少 rerank & 缓存策略。 | 更新 generation/检索 SOP，添加性能测试、SiliconFlow reranker 接口。 |
| 结构化 API 输出 | 目标是“Query → Answer → 中间证据”结构；现依赖磁盘 JSON。 | 在 Search-o1 SOP 内新增 `RunTrace` 设计，返回内存结构并保留可选磁盘落盘。 |
| README 语料清洗 | 当前向量库仍混入 HTML/JSON 残片，检索证据不可直接引用。 | 制定 `docs/sop/readme_ingestion_cleaning.md`（新），在 Ingestion Pipeline 中强化清洗、分段与验收指标。 |

## 当前流程快照（2025-10-07）
```
[数据同步]
  ingestion_pipeline（统一 Runner） ──> output/ingestion/sqlite + output/ingestion/chroma
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

当前完成节点：数据同步、检索闭环、通用模板与 Python API 调用路径。
待开发节点：对话记忆、引证结构、多模态采集、性能评测、诊断工具。
```

## 迭代原则
- **Specification-First**：上述待办在实现前需补充或更新相应 SOP 文档。
- **Unix & Simple Code**：优先组合最小可用组件，保持脚本与服务器职责单一。
- **Make it work → make it right → make it fast**：先实现功能，再优化结构和性能；保留验证脚本与日志链路。
# Community Intelligent Agent Design

## 原始驱动力（Why we built this）
- 可观测、可审计、可复现：每一步的输入/输出、路由与标记必须可回放与核验（memory_* 快照 + 日志），让复杂的 Search‑o1 流水线在竞赛/评测中可追踪、可对比。
- 最小契约与中立检索：召回只返回 `ret_psg` 与最小 `metadata`，不注入品牌/白名单；结构化与去重在生成侧完成。
- 组合式流程与低门槛：用 YAML 声明串行/循环/分支；通过 Python API 调用（单一入口）；模板明确“什么时候检索/什么时候终止/如何给出可评测的最终答案”。
- 端到端交付：既支持数据集批跑，也能“传入一个问题 → 自动产出结构化答案”，并暴露必要的中间状态用于验收。

## 已完成工作（What we have）与对应 SOP
- Env‑First 运行（生成端）
  - 说明：`LLM_BASE_URL/LLM_MODEL_NAME/LLM_API_KEY` 优先，缺失 fail‑fast；示例参数不再写死本地端口。
  - 代码：servers/generation/src/generation.py（env‑first precedence）。
  - SOP：docs/sop/search_o1_answering_sop.md（“环境变量优先”条目与连通性自检）。
- 最小检索契约（清洗待加强）
  - 说明：检索仅返回 `ret_psg` + `metadata{repo_author, repo_name, score, clean_state}`；当前 normalize 流程已上线，但仍需后续增强以移除 HTML/JSON 残片（见“README 语料清洗”待办）。
  - 代码：servers/retriever/src/retriever.py；src/ultrarag/utils.py。
  - SOP：docs/sop/README_Retriever_SOP.md、docs/sop/search_o1_answering_sop.md。
- Search‑o1 通用模板与 Markdown 收官
  - 说明：init/refinement/finalize 三模板；Finalize → generate → output_passthrough（剥离停词并透传 Markdown）。
  - 代码：prompt/search_o1_*.jinja；servers/prompt/src/prompt.py（search_o1_finalize）；servers/custom/src/custom.py（output_passthrough）。
  - SOP：docs/sop/search_o1_answering_sop.md（已记录执行与验收要点）。
- 观测与验收制品
  - 说明：运行日志与 memory_* 快照（含 `<<SRCH_Q_BEGIN>>` / `<<SRCH_R_BEGIN>>` / `<<FINAL_ANSWER_END>>`）；是否写盘由开关控制。
  - 代码：src/ultrarag/client.py（snapshots 与落盘）。
  - SOP：docs/sop/search_o1_answering_sop.md（“观测与排障”“质量验收清单”）。
- 检索中立策略（反模式约束）
  - 说明：禁止在 retriever/query_instruction 注入品牌或白名单；仅允许通用超参调整；基于证据内容一致性做去重。
  - 规范：AGENTS.md “Search‑o1 Retrieval Neutrality & Anti‑Patterns”。
- 测试与样例
  - 说明：Env‑First、路由判定、盒装抽取等关键用例通过；实际 run 样例已产生自动盒装清单与非空 pred_ls。
  - 代码/产物：tests/servers/*；output/memory_* 与 logs/*（时间戳新近的运行）。

## 待补齐问题（Gaps）与拟新增 SOP（Next）
- Programmatic Mode（程序内回传）
  - 目标：传入单个问题，程序内回传 `question/final_answer/middle` 的轻量结构体（无需 JSON 落盘）；保留写盘为可观测开关。
  - 拟定 SOP 补充：在 Search‑o1 SOP 增加“Programmatic Mode”条目，定义最小数据结构、验收与兼容策略（write_files 开关）。
- Inline 单问单答输入
  - 目标：无需 JSONL，允许从 CLI 或 API 直接注入一条 `q_ls`；与 Programmatic Mode 配套。
  - 拟定 SOP 补充：新增“Inline Input”说明与示例。
- 模板中立性进一步收敛
  - 目标：模板仅提示维度/格式，移除具体型号枚举示例，避免答案注入；以证据一致性驱动列表完整性。
  - 拟定 SOP 补充：在模板规范中加入“不得注入品牌/白名单示例”的限制与验收。
- Provider 超时与重试的默认建议
  - 目标：SOP 明确推荐超时阈值/重试次数的区间与验收门槛，降低因远端延迟导致的“无结题”概率。
  - 拟定 SOP 补充：在“生成参数建议”加入 timeout/重试建议与日志核对项。

## 当前流程 ASCII 图（Search‑o1 + 收官）

```
User Q ─┐
        │  benchmark.get_data  →  retriever.retriever_init_readme
        │          │                           │
        │          └── q_ls, gt_ls             └── ready
        │
        ├─ prompt.search_o1_init  → generation.generate (首轮)
        │
        ├─ loop (times 上限)
        │     router.search_o1_check
        │       └─ retrieve 分支:
        │            custom.search_o1_query_extract
        │            retriever.retriever_search_readme   →  ret_psg + metadata(min)
        │            prompt.searcho1_reasoning_indocument
        │            generation.generate
        │            prompt.search_o1_insert             →  <|begin_search_result|>…<|end_search_result|>
        │            generation.generate
        │
        └─ 收官 finalize:
              prompt.search_o1_finalize
              generation.generate
              custom.output_passthrough   →  Markdown 文本

Artifacts: logs/*, output/memory_*（可观测）
```

## 数据入库警告（待解决问题）

为保持检索中立、答案可信与可审计，README 入库阶段（ModelScope → Chroma）存在的已知风险必须在后续迭代中修复。当前实现优先保证跑通，尚未在“入库前”做强清洗，导致检索结果可能混入噪声。

- 脏源未移除（HTML/徽章/资源链接碎片）
  - 现象：检索段落混入非正文片段，如 `gle.com/assets/colab-badge.svg`、`Open In Colab`、`<img ...>` 残片。
  - 成因：早期入库流程将 README 原文（可能为 HTML/JSON）直接入库后再切片与嵌入，非正文元素随之进入向量库。

- 切片不按段/句（易产生“半标签/半属性”）
  - 现象：出现被截断的属性或标签尾段（例：`... alt=\"Open In Colab\"`），清洗难以完全剥离。
  - 成因：`split_text()` 以字符窗为主，对 HTML/JSON 结构不敏感，可能在标签/属性中部断裂。

- 清洗不足（入库后清洗，鲁棒性有限）
  - 现象：检索结果仍含转义或乱码，如 `\u003cdiv\u003e`、`Ã¥Â…`（mojibake），以及 `modelscope://MusePublic/Qwen-image?revision=v1` 这类资源 URL 噪音。
  - 成因：`src/ultrarag/utils.py: normalize_readme_text()` 假设“完整 JSON/HTML”，对“半标签/混合转义/属性残片”鲁棒性不足。

真实案例（来自运行快照 output/memory__run_20251009_214415.json）
- `gle.com/assets/colab-badge.svg`（Colab 徽章链接尾段）
- `modelscope://MusePublic/Qwen-image?revision=v1`（资源 URL）
- `Ã¥Â…` 等编码残影（mojibake）

改造建议（不改变中立检索边界）
- 入库前清洗：在 `chunk_worker` 对 `doc.content` 先调用 `normalize_readme_text()`，将清洗后的纯文本用于切片与嵌入，避免把 HTML/JSON 垃圾写入 Chroma。
- 切片改进：`split_text()` 优先按“段落/标题/列表项/句子”分段，避免在标签/属性中间断裂。
- 清洗兜底：提高 `normalize_readme_text()` 对“半标签/孤立属性/大量转义”的兜底（例如剔除 `assets/*.svg`、`Open In Colab` 等徽章/资源行）。
- 验收门槛：
  - 单条 `ret_psg` 中非文字噪声（URL/标签/转义）的可见占比 < 10%。
  - 上述“真实案例”在新入库后不再出现在检索段落中。

## 迭代原则（Principles）
- Unix 哲学：执行与采集解耦（runner 做执行，collector/快照做观测）；组件职责单一，可组合。
- Embrace simple code：优先最小数据结构与最短调用链，减少全局状态与隐式耦合。
- Why this approach：每项改动在文档先行（SOP/RFC），明确取舍与替代方案；接受审视与回滚。
- Make it work → right → fast：先跑通（Programmatic Mode 最小实现），再补规范化与测试，最后考虑优化与 streaming。
- Prefer plan mode：所有功能以“计划清单 + 验收项”推进；代码变更与 SOP/AGENTS 同步更新。

---
注：本文档聚焦社区问答/Search‑o1 能力的设计与现状；与之配套的规范请见 docs/sop/search_o1_answering_sop.md 与 AGENTS.md 的检索中立章节。

### Ingestion Pipeline SOP（单一路径）

- 以《docs/sop/ingestion_pipeline_sop.md》为唯一规范；统一 `fetch → process → ingest` 抽象、单队列 Runner 与两阶段写入。
- 禁止并存备用实现与历史兜底；发现遗留脚本或并行路径应直接删除（示例：`stores/chroma_*`、`runner_legacy.py` 等）。
