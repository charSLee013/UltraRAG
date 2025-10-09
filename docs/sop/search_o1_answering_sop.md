# SOP: Search‑o1 Answering Loop (Noise‑Free Spec)

版本：2025-10-05  
负责人：<待指定>

## 1. 目的与边界
- **唯一目标**：实现一条可复用的“检索‑推理迭代循环”，产出可被上游（问题输入）与下游（报告阶段）统一消费的结果。
- **明确非目标**：数据索引、评测评分、可视化、报告撰写、API 适配等在其它 SOP/文档定义；本文件不再描述这些细节，避免遗留噪声。
- **思路重置**：沿用成熟代码路径，但消除所有与具体任务绑定的硬编码或历史补丁，使 Search‑o1 能在不同业务场景快速复用。

## 2. 设计原则
- **Unix 哲学**：router、query extractor、retriever、reasoner、inserter、finalizer、runner 各自负责单一职责。
- **简单优先**：所有配置通过显式对象注入，禁止跨模块隐藏状态；最小依赖、直白数据结构（dict/list）。
- **Why-first**：每个设计点在 SOP 中解释取舍；编码前先更新文档，再进入实现。
- **先跑通，再规范，再优化**：遵循 Make it work → Make it right → Make it fast。
- **Prefer plan mode**：变更先写规划与验收清单，避免“先写代码再写文档”的反复补丁模式。

## 3. 可插拔契约
### 3.1 TokenContract
- 字段（最小集）：
  - `begin_query`
  - `end_query`
  - `end_answer`
  - 可选扩展：`begin_result`、`end_result` 等，便于未来插入更多状态。
- Search‑o1 默认标记（设计为不与任意模型内部停词冲突的哨兵串，可在配置层覆写）：
  - `begin_query` → `<<SRCH_Q_BEGIN>>`
  - `end_query` → `<<SRCH_Q_END>>`
  - `begin_result` → `<<SRCH_R_BEGIN>>`
  - `end_result` → `<<SRCH_R_END>>`
  - `end_answer` → `<<FINAL_ANSWER_END>>`
- 实现层**必须**从 TokenContract 读取，禁止硬编码常量；若业务需自定义标记，须通过配置覆写，而非散落在代码中。

### 3.2 TemplatePlan
- 模板路径：`init`, `reasoning_indoc`, `insert`, `finalize`。
- 模板变量最小化：至少接受 `goal_description`（任务描述），其它变量按需扩展。
- **禁止**在模板里写死任务名/枚举清单；所有语境由参数注入。
- 模板应生成通用 Markdown 推理文字，不包含与数据源绑定的语句。

### 3.3 OutputSpec（仅 Markdown，轻量结构）
- 定义：返回轻量数据结构 `{"format": "markdown", "text": <最终文本>}`。
- 规则：终止仍由 `end_answer` 触发，但输出不再要求 `\\boxed{...}`；由 `custom.output_passthrough` 去停词并透传 Markdown 文本。
- 说明：回答阶段仅产出 Markdown 文本；控制标记仅用于路由/截断，下游若需盒装/JSON 由其自身适配。

## 4. 固定中间件
- **Env-first**：Runner 启动时永远 `load_dotenv()`，并使用环境变量作为默认配置；缺失关键变量时立即 fail-fast。
- **观测链路**：
  - INFO：恒定输出（每轮 router 判定、TokenContract 名称、耗时摘要）。
  - DEBUG：由单一开关 `SEARCH_O1_DEBUG` 控制；开启时同时打印最新查询（截断 200 字符）并写入 memory 快照，关闭时两者均停用。
  - Memory 快照：默认随 DEBUG 开关启用，写入 `output/memory_*`，用于排查审计。

### 4.1 终止标记与 stop 策略（重要更正）
- 生成阶段【不要】在采样参数里配置控制标记为 `stop`（如 `"<<FINAL_ANSWER_END>>"`, `"<<SRCH_Q_END>>"`）。
  - 原因：多数 OpenAI 兼容服务在遇到 stop 时会直接截断输出；若模型首 token 即为 stop，将返回空串，导致最终答案为空。
  - 正确做法：允许模型显式输出这些标记；由 Router/Extractor/Passthrough 识别并处理（路由或剥离），而非在采样层截断。
- 采样建议：仅保留通用参数（`max_tokens/temperature/top_p` 等），`extra_body.stop` 留空或删除。

## 5. 循环语义
1. **Init**：模板 `init` → `generation.generate`，生成首轮推理，可包含查询标记。
2. **Loop（≤ loop.times）**：
   - Router 根据 TokenContract 判断：含 `end_answer` → `stop`；含 `end_query` → `retrieve`；否则 `retrieve`。
   - `retrieve` 分支：
     1. QueryExtractor 提取查询（允许空字符串表示“无需检索”），
     2. Retriever 返回段落与最小 metadata，
     3. 模板 `reasoning_indoc` 整合证据，
     4. Inserter 模板 `insert` 把证据写回上下文，
     5. Generation 再次产出推理。
3. **Finalize**：模板 `finalize` → Generation → `custom.output_passthrough`（剥离停词并返回 Markdown 文本）。
   为保证 README 检索可用，流水线在最前加入 `retriever.retriever_init_readme` 完成索引/嵌入端点初始化（env-first，缺失即 fail-fast）。

## 6. Runner 职责
1. 加载配置（`.env` → CLI/参数文件 → TokenContract/TemplatePlan）。
2. 初始化各组件（router/query extractor/retriever 等），将契约对象注入；若 README 检索尚未初始化，立即执行 `retriever_init_readme`。
3. 按“init → loop → finalize”顺序执行。
4. 返回最终文本；在 `SEARCH_O1_DEBUG` 打开时写入 memory 快照。

附：Programmatic Mode 允许以 `seed_vars={"q_ls":[question]}` 方式预置首轮输入；实现层需在 IO 提取前预置这些变量，避免“第一步找不到 q_ls”。

## 7. 历史工作盘点与取舍
| 文件/模块 | 所处目录 | 之前的作用 | 现状判断 |
| --- | --- | --- | --- |
| `servers/router/src/router.py: search_o1_check` | router | 根据 `<<FINAL_ANSWER_END>>` / `<<SRCH_Q_END>>` 判停或检索 | **保留**：需改为读取 TokenContract 后继续使用 |
| `servers/custom/src/custom.py: search_o1_query_extract` | custom | 从文本中抽取查询；遇不到标记时返回占位 | **保留**：读取 TokenContract，返回空字符串代表无需检索 |
| `servers/custom/src/custom.py: search_o1_output_passthrough` | custom | 透传 Markdown 并移除停词 | **保留**：新的唯一输出路径，需保证无盒装逻辑 |
| `servers/custom/src/custom.py: search_o1_ensure_stop` | custom | 末轮补写 `<<FINAL_ANSWER_END>>` | **已移除**：主线仅保留 Markdown 透传，禁用兜底停词 |
| `servers/prompt/src/prompt.py: search_o1_insert` | prompt server | 将检索结果插入上下文 | **保留**：需改为读取 TokenContract 标记并保持最小注入 |
| `servers/retriever/src/retriever.py: retriever_init_readme / retriever_search_readme` | retriever | 初始化 README 向量、检索文章 | **保留**：与契约解耦，继续作为主要检索源 |
| `src/ultrarag/utils.py: normalize_readme_text` | utils | 清洗 README 文本、标记 `clean_state` | **保留**：支撑检索质量 |
| `servers/generation/src/generation.py: generate` | generation | 调用远程模型、处理停词 | **保留**：继续 Env-first 入口 |
| `prompt/search_o1_reasoning.jinja` / `search_o1_refinement.jinja` / `search_o1_finalize.jinja` | prompt | 包含 Qwen 场景化指令 | **重写**：改为中性 Markdown 模板，保留变量注入 |
| `pipelines/search_o1/run.yaml` | pipelines | 旧 CLI 流程拷贝 | **保留并更新**：作为唯一入口配置，引用 TokenContract/TemplatePlan |
| `pipelines/search_o1/template_plan.yaml` | pipelines | 尚未独立存储模板路径 | **新增**：集中模板计划，供 API 加载 |
| `pipelines/search_o1/parameter/run_parameter.yaml` | pipelines | 旧参数快照 | **保留并更新**：迁移至 Markdown-only 配置 |
| `pipelines/search_o1/server/run_server.yaml` | pipelines | 旧服务器组合 | **保留并更新**：限定 Search-o1 所需服务 |
| `script/run_search_o1.py` | script | 旧项目便捷脚本 | **Deprecated（legacy demo）**：正式路径使用 Python API（见“执行方式”） |
| `output/memory_*search_o1*.json` | output | 旧跑批产生的记录 | **归档参考**：新流程仅在 DEBUG 时写入；旧文件保留供排查 |
| `tests/servers/test_router_search_o1_check.py` | tests | 覆盖 router 判定 | **保留**：需更新断言以读取 TokenContract |
| `tests/servers/test_custom_search_o1_tools.py` | tests | 覆盖 query 抽取与输出 | **保留**：扩展覆盖空查询与 Markdown 透传 |
| `tests/servers/test_generation_env_first.py` | tests | 验证 Env-first 行为与停词 | **保留**：继续作为回归基线 |

## 8. 目录约束
- Search‑o1 专属资产统一放置在 `pipelines/search_o1/`（含 `token_contract.yaml`、`template_plan.yaml`、`run.yaml` 等）。
- `examples/` 目录仅保留上游项目自带的 demo，不得再承载或修改 Search‑o1 代码；若需参考，只能复制到 `pipelines/search_o1/` 并在此维护。
- 模板仍位于 `prompt/`，但内容必须保持任务无关；如需新增模板，同步登记在本 SOP。

## 9. 验收清单
**Work（最小闭环）**
- 默认 TokenContract 下，循环 ≤ 设定 `loop.times` 并以 `end_answer` 停止。
- QueryExtractor 按契约抽取查询，日志显示标记读取来源。
- Runner 返回 `{"format": "markdown", "text": ...}`；在 `SEARCH_O1_DEBUG=1` 时 memory 快照记录同样文本。

**Right（规范化）**
- 单测覆盖：router 读取定制 TokenContract、query extractor 空查询路径、generation 停词行为、runner 缺 env 时 fail-fast。
- 静态检查：`rg '<<SRCH_Q_BEGIN>>' src` 仅出现在配置/模板中。
- 文档同步：模板/参数更改必须更新本 SOP 对应章节。

**Fast（优化）**
- 观测到 loop 中检索/生成耗时；可配置缓存、并发。本阶段另行立项，不在本 SOP 详述。

## 10. 迁移步骤
1. **契约注入**：重构 Router & QueryExtractor 接受 TokenContract；Runner 负责构造并传入。
2. **模板通用化**：重写 `prompt/search_o1_*.jinja` 为中性 Markdown；移除任务特定语句。
3. **输出协议**：统一为“仅 Markdown + `end_answer` 截断”；实现 `custom.output_passthrough` 透传文本。
4. **单测/验收**：新增契约注入测试；更新 `pytest`/命令示例，指向 `pipelines/search_o1/`。

## 11. 变更审计
- Old SOP（Qwen 场景化）已归档至 `docs/sop/archive/2025-10-04_search_o1_answering.md`（由历史提交追溯）。
- 2025-10-05：重写为 Noise-free 版本；引入单一 DEBUG 开关；整理历史工作取舍表。

---
> 参考文档：`docs/community_agent_design.md`、`docs/sop/readme_retriever_hardening_sop.md`、`AGENTS.md`。

## 11. 硬编码风险与回滚
- TokenContract 提供所有标记；运行时代码不保留默认哨兵，若缺失则返回空串/直接失败。任何新标记必须先在配置层定义，再更新本 SOP 的“禁止硬编码”守卫。
- CI 断言仍生效：若 future 代码中出现哨兵字面量（`<<SRCH_Q_BEGIN>>` 等），`rg …` 守卫会立刻触发，作为回滚策略可恢复到稳定版本并补充测试。

## 12. 与 AGENTS.md 对齐（Alignment）
- 单一路径：仅支持 Python API（`SearchO1Pipeline.query()`）；CLI/脚本均属 legacy demo，不得出现在验收指引。
- 中立检索：严格最小合同（`ret_psg`+最小 `metadata`），禁止品牌/白名单与任何源域启发式。
- 单一输出协议：Markdown 经过 `custom.output_passthrough` 透传；不得保留“兼容模式”（boxed/JSON 等）。
- 规范先行：任何对检索/循环/终止/输出的行为变更，必须先更新本 SOP 与 AGENTS.md，再实施代码修改。
- 可观测：保留 memory_* 快照与关键信息日志；诊断输出不得丢失来源最小信息。

## 13. CI 守卫（噪声防护）
在 CI 中添加文本扫描，禁止遗留路径与兼容实现重新进入主干：
```bash
# 禁止脚本/例子成为入口
rg -n "script/run_search_o1.py|examples/search_o1.yaml" -- docs src servers && exit 1 || true

# 禁止盒装与旧抽取逻辑
rg -n "\\\\boxed\{|output_extract_from_boxed|ensure_stop" -- src servers prompt && exit 1 || true

# 禁止硬编码标记（仅 TokenContract 中允许）
rg -n "<<SRCH_Q_BEGIN>>|<<SRCH_Q_END>>|<<FINAL_ANSWER_END>>" src servers | rg -v "TokenContract|parameter|template|jinja" && exit 1 || true
```

## 12. 执行计划（Execution Plan）
（2025-10-08 更正补充）
- 终止策略更正：移除采样层 stop（不再将 `<<FINAL_ANSWER_END>>`/`<<SRCH_Q_END>>` 配置到 stop），由 Router/Extractor/Passthrough 识别/剥离。
- 流水线补充 README 检索初始化：在 run.yaml 首步加入 `retriever.retriever_init_readme`。
- 参数作用域收敛：在 run.yaml 中使用每 server 的本地键（如 `$model_name/$base_url/$top_k`），避免 `$server.key` 交叉作用域导致解析失败。
- Programmatic 种子：允许以 `seed_vars={"q_ls":[question]}` 方式注入，Runner 需在 IO 抽取前预置。
**Phase A：资产盘点与价值甄别（优先完成）**
- 列出所有遗留 Search‑o1 相关文件：`servers/router/src/router.py`、`servers/custom/src/custom.py`、`prompt/search_o1_*.jinja`、`examples/search_o1*.yaml`、`script/run_search_o1.py`、`src/ultrarag/client.py`、相关测试与文档引用。
- 将可复用逻辑（router 判定、query 抽取、retriever、client 框架等）标记为“保留待改造”；将盒装输出、停词兜底、examples YAML 等列入“待移除”。
- 输出盘点清单，确保后续改动范围透明且无遗漏。

**Phase B：主线路实现（按顺序执行）**
1. **契约注入**：
   - 重构 router/query extractor 以接受 TokenContract；参数来源于 `pipelines/search_o1/token_contract.yaml`。
   - 新增 `custom.output_passthrough` 并替换所有 boxed 相关调用；删除 `search_o1_ensure_stop` 与 `output_extract_from_boxed`。
2. **模板与配置**：
   - 重写 `prompt/search_o1_{reasoning,refinement,finalize}.jinja` 为纯 Markdown 指令；去除任何盒装/停词字面量。
   - 在 `pipelines/search_o1/` 内建立 `run.yaml`、`template_plan.yaml`、`parameters.yaml`，仅引用上述模板与工具。
3. **API 封装**：
   - 实现 `SearchO1Pipeline`（`src/ultrarag/api.py`），加载配置与 TokenContract，提供 `query()`/`batch_query()` 等接口并作为唯一入口。
4. **观测与测试**：
   - 扩充单测：router/token_contract、query extractor 空查询、`output_passthrough`、API 缺 env fail-fast、DEBUG 开关写 memory。
   - 静态校验：`rg '<\|begin_search_query\|>' src` 仅命中配置或模板变量；`rg '\\boxed'` 确认无残留。

**Phase C：彻底清理（实现后立即执行）**
- 删除 `examples/search_o1.yaml` 及相关参数文件；移除文档、README、笔记中的旧引用。
- 移除代码中所有盒装/兜底停词实现与测试；确保新版 Markdown 输出成为唯一路径。
- 清理输出产物：删除历史 `output_extract_from_boxed`、`search_o1_ensure_stop` 相关日志描述；归档必要的老文件到 `docs/sop/archive/`。
- 在完成清理后复跑 `SearchO1Pipeline.query()` 验证 Markdown 输出正确，并更新盘点清单状态为“已迁移/已删除”。

## 13. 验收场景（Real Scenarios）
基于 ModelScope README 语料（HuggingFace 镜像），用于小而真实的验证；要求模板与代码中无任何领域枚举或品牌白名单。

- 场景 A：获取 Qwen 系列的全部模型（曾被硬编码的案例）
  - 问题示例：`请列出 Qwen 系列公开发布的模型，并按类别简要说明。`
  - 期望：
    - 至少触发 1 次检索；`ret_psg` 来源为 ModelScope README 片段；
    - 最终输出为 Markdown 段落/列表，条目与检索片段一致（允许表述差异，不允许凭空捏造）；
    - 模板中无任何“Qwen 枚举清单”字面量；
    - memory 快照可见 `<<SRCH_Q_BEGIN>>` 与 `<<SRCH_R_BEGIN>>…`；
    - 替换标记（`<search>`/`</search>`）后流程仍成立。

- 场景 B：确认嵌入模型用途（检索→整合）
  - 问题示例：`BAAI/bge-m3 适用于哪些任务？有何输入限制？`
  - 期望：
    - 检索命中文档的用途描述被整合进推理；
    - 若证据不足，`refinement` 输出 `**Next Search Plan**` 并继续；
    - 终止时输出要点式 Markdown 答案（不要求盒装符号）。

- 场景 C：无效查询与早停
  - 问题示例：`给出一个不存在的模型 X 的详细参数。`
  - 期望：
    - 经 1～N 轮检索无有效证据时，输出明确结论（无法在公开 README 中确认）并停止；
    - 仍按 `end_answer` 停止标记终止；最终 Markdown 输出不包含停词本体。

执行方式（唯一入口：Python API）
```python
from dotenv import load_dotenv
from ultrarag.api import SearchO1Pipeline

load_dotenv()
p = SearchO1Pipeline("pipelines/search_o1/run.yaml")
result = p.query("question is ...")  # returns {"format":"markdown","text": ...}
print(result["text"])  # Markdown 文本
```
