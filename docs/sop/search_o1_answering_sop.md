# SOP: Search‑o1 回答阶段（Answering）

**版本**：2025-10-04  
**负责人**：<待指定>

## 背景与目标
- 既有工作回顾：
  - README 向量库已接入并成为默认检索源，工具 `retriever_init_readme` / `retriever_search_readme` 上线，旧名别名保持可用（向后兼容）。
  - 检索返回契约采用最小化字段：`repo_author`、`repo_name`、`score`（可选 `clean_state`）；不包含 `source_url/revision/path` 等非必要信息。
  - Search‑o1 相关组件已具备：
    - 查询抽取：`servers/custom/src/custom.py: search_o1_query_extract`
    - 循环路由：`servers/router/src/router.py: search_o1_check`
    - 提示模板：`prompt/search_o1_reasoning.jinja`、`prompt/search_o1_refinement.jinja`
    - 示例管线：`examples/search_o1.yaml`
  - 生成服务支持通过 `sampling_params` 传入 `stop` 等参数（`servers/generation/src/generation.py`）。
- 本 SOP 目标：
  - 规范 Search‑o1“回答阶段”的协议与流程，确保“检索—插入—再推理—终止”的闭环稳定可复现。
  - 明确停止条件与抽取机制，保证最终答案可被稳定提取与评测。
  - 给出端到端验收清单与操作指引（以真实可运行结果为准）。
- 非目标：
  - 不改动检索索引构建与字段定义；不引入 `source_url/revision` 运行时兜底。
  - 不涉及 UI/前端集成；不绑定特定大模型供应商。

## 协议与接口
- 控制标记（tokens）：
  - 触发检索：`<|begin_search_query|> ... <|end_search_query|>`
  - 回填结果：`<|begin_search_result|> ... <|end_search_result|>`
  - 结束信号：`<|im_end|>`（判定最终停止）
- 关键工具与职责：
  - `prompt.search_o1_init`：首轮构建“思考+可选检索请求”提示；要求最终答案格式为 `\boxed{...}`。
  - `router.search_o1_check`：
    - 若文本包含 `<|im_end|>` → `state = stop`
    - 若文本包含 `<|end_search_query|>` → `state = retrieve`
    - 否则默认 `state = retrieve`（继续循环）
  - `custom.search_o1_query_extract`：从最新回答中提取最近一次 `<|begin_search_query|>...<|end_search_query|>` 的内容，若无则返回占位字符串。
  - `retriever.retriever_search_readme`：以抽取的查询执行检索，返回 `ret_psg` 与最小化 `metadata`。
  - `prompt.searcho1_reasoning_indocument`：基于“前序思考 + 当前查询 + 命中文档”产出精炼信息。
  - `prompt.search_o1_insert`：将精炼信息包裹进 `<|begin_search_result|>...<|end_search_result|>` 追加到对话上下文。
  - `generation.generate`：按采样参数生成；应允许在遇到 `stop` 词时包含停止词本身（见下）。
  - `custom.output_extract_from_boxed`（推荐）：从 `ans_ls` 中抽取 `\boxed{...}` 作为 `pred_ls` 供评测使用。
- 生成参数建议（`servers/generation/parameter.yaml`）：
  - 在 `sampling_params.extra_body` 中设置：
    - `include_stop_str_in_output: true`
    - `stop: ["<|im_end|>", "<|end_search_query|>"]`
  - 目的：
    - 保证路由器能在模型输出中看到 `<|end_search_query|>`/`<|im_end|>` 以进行状态判定；
    - 每轮生成在遇到上述标记时即刻截断，降低无效 token。

## 流程说明（对应 examples/search_o1.yaml）
1. `benchmark.get_data` 读取问题与金标。
2. `retriever.retriever_init_readme` 初始化检索端。
3. `prompt.search_o1_init` → `generation.generate`：产生首轮“思考文本”，可能包含一次检索请求。
4. 进入循环（默认 3 轮，可调）：
   - `router.search_o1_check`：
     - `stop` → 退出循环，进入评测；
     - `retrieve` → 继续：
       - `custom.search_o1_query_extract` → `retriever.retriever_search_readme`
       - `prompt.searcho1_reasoning_indocument` → `generation.generate`
       - `prompt.search_o1_insert` → `generation.generate`
5. 终止后（建议）：`custom.output_extract_from_boxed` 生成 `pred_ls`，用于 `evaluation.evaluate`。

## 输入/输出契约（摘录）
- 检索：`{"ret_psg": List[List[str]], "metadata": List[List[{repo_author, repo_name, score, clean_state?}]]}`
- 生成：`{"ans_ls": List[str]}`；评测使用 `pred_ls`（从 `\boxed{...}` 抽取）。
- 内存快照（自动）：运行时在 `output/memory_<benchmark>_<pipeline>_<ts>.json` 中记录 `memory_*` 键的最新值，便于排障与验收。

## 质量验收清单（真实可运行）
- 配置与准备：
  - `.venv` 已激活；完成 `pip/uv install -e .`。
  - `.env`/环境变量：已设置 `LLM_API_KEY/BASE_URL/MODEL_NAME`（生成），`CHROMA_PATH/CHROMA_COLLECTION/EMBEDDING_API_URL/EMBEDDING_API_KEY`（检索）。
  - `servers/generation/parameter.yaml` 已按建议启用 `stop` 与 `include_stop_str_in_output`。
- 功能行为：
  - 首轮 `generation.generate` 能输出 `<|end_search_query|>` 或直接给出思考；
  - 路由器对含 `<|end_search_query|>` 的输出判定为 `retrieve`，对含 `<|im_end|>` 的输出判定为 `stop`；
  - 至少一次完成“抽取查询 → 检索 → 文内精炼 → 插入结果 → 再生成”的闭环；
  - 最终一轮输出包含 `\boxed{...}`，可由 `custom.output_extract_from_boxed` 抽取到非空 `pred_ls`；
  - `evaluation.evaluate` 正常产出指标文件（默认写入 `output/`）。
- 一致性与无噪声：
  - 提示模板不引用检索 `metadata` 的扩展字段，仅使用 `ret_psg` 文本片段；
  - `ret_psg` 文本可读，无明显转义/HTML 杂质（清洗状态可在 `metadata.clean_state` 诊断）。
- 健壮性：
  - 缺少 `<|end_search_query|>` 时仍可继续循环而不中断；
  - 当 `top_k <= 0`、缺少必需 env 时，工具抛出清晰错误信息（fail‑fast）。
- 性能（小规模验收）：
  - 每轮生成/检索在可接受延迟内完成；`stop` 截断能显著减少无效生成。

## 观测与排障
- 查看最新内存快照：`output/memory_*search_o1*<ts>.json`
  - 关注键：`memory_prompt_ls`、`memory_ans_ls`、`memory_ret_psg`、`memory_metadata`。
- 日志：各服务器通过 MCP 标准输出记录关键信息；检索/生成失败会包含明确错误上下文。

## 回滚与兼容性
- 回滚：如遇回答阶段异常，可将 `examples/search_o1.yaml` 切换为一次性 RAG 流（去掉循环与路由），保留相同检索工具与模板。
- 兼容性：检索层保持别名与最小化契约不变；本 SOP 不引入新增必填字段。

## 里程碑与分工
1. 参数固化：将 `stop`/`include_stop_str_in_output` 默认启用（generation 参数文件）。
2. 管线完善：在 `examples/search_o1.yaml` 终止前追加 `custom.output_extract_from_boxed`，评测改读 `pred_ls`。
3. 覆盖测试：
   - 路由判定单元测试（含三种情形：`im_end` / `end_search_query` / 无标记）。
   - `output_extract_from_boxed` 抽取稳定性测试（嵌套与转义场景）。
4. 端到端验收：以 10 条样例跑通并保存快照与评测报告。

## 验收操作示例
- 生成 server/parameter（如首次或参数变更）：
  - `ultrarag build examples/search_o1.yaml`
- 真实运行（需联网与可用模型）：
  - `ultrarag run examples/search_o1.yaml`
- 可选：若使用 `pred_ls` 评测，在管线中插入 `custom.output_extract_from_boxed` 后再运行。

---
注：本 SOP 遵循“Specification‑First”。任何实现改动（例如默认启用 `stop`/抽取步骤变更）须先更新本文档再进行编码与提交。

