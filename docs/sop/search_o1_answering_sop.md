# SOP: Search‑o1 Answering Loop

版本：2025-10-25  
负责人：<待指定>

本文件对齐当前代码实现，移除历史方案与过期设计。仅保留现行的结构、契约与运行方式。

## 1. 目标与边界
- 目标：提供一条可复用的“检索‑推理迭代”流水线，输入问题，输出最终 Markdown 文本。
- 非目标：数据集构建、评测评分、可视化与报告在其他文档中定义。

## 2. 总体架构
- API 封装：`SearchO1Pipeline`（`src/ultrarag/api.py`）
  - 加载 `.env`，校验生成端关键环境（见 §4）。
  - 默认运行配置：`pipelines/search_o1/run.yaml`。
  - 提供 `query(str)->{"format":"markdown","text":...}` 与 `aquery(str)`。
- 执行引擎：`client.run`（`src/ultrarag/client.py`）
  - 读取 `run.yaml`，配套 `server/run_server.yaml`、`parameter/run_parameter.yaml`。
  - 启动 MCP 服务器：generation/prompt/retriever/router/custom（均为 Python 进程）。
  - 执行 `pipeline` 步骤，维护变量池与 memory 快照，返回工具聚合的 `data`。
  - 以 `sys.executable` 和 `env=os.environ.copy()` 启动各 MCP 服务器进程，继承父进程所用解释器与 `.env` 环境。
- 服务器实现：`servers/*/src/*.py`
  - generation：OpenAI 兼容生成调用（async）。
  - prompt：Jinja 模板渲染。
  - retriever：Chroma + 远程嵌入；返回段落与最小元数据。
  - router：基于 TokenContract 的分支判定。
  - custom：查询抽取与最终输出透传。

## 3. 配置文件与目录
- 流水线：`pipelines/search_o1/run.yaml`
- 服务器声明：`pipelines/search_o1/server/run_server.yaml`
- 参数：`pipelines/search_o1/parameter/run_parameter.yaml`
- 模板计划：`pipelines/search_o1/template_plan.yaml`（为模板路径提供别名，`SearchO1Pipeline` 自动注入 `template_plan`）
- 模板：
  - `prompt/search_o1_reasoning.jinja`
  - `prompt/search_o1_refinement.jinja`
  - `prompt/search_o1_finalize.jinja`

## 4. 运行时环境（Env-first）
- 统一在进程启动时 `dotenv.load_dotenv()`。
- 生成端（必需）：
  - `LLM_BASE_URL`、`LLM_MODEL_NAME`；可选 `LLM_API_KEY`。
  - 兼容别名（仅 generation 服务器会识别）：`OPENAI_BASE_URL`/`BASE_URL`、`MODEL_NAME`/`LLM_MODEL`。当通过 `SearchO1Pipeline` 调用时，仍要求主键存在。
- 检索端（按需）：
  - `CHROMA_PATH`、`CHROMA_COLLECTION`
  - `EMBEDDING_API_URL`、`EMBEDDING_API_KEY`
  - `EMBEDDING_TIMEOUT`（秒，默认 60）
  - `EMBEDDING_MODEL`（可选，默认 `BAAI/bge-m3`）

## 5. TokenContract（最小集）
- `begin_query`：`<<SRCH_Q_BEGIN>>`
- `end_query`：`<<SRCH_Q_END>>`
- `begin_result`：`<<SRCH_R_BEGIN>>`
- `end_result`：`<<SRCH_R_END>>`
- `end_answer`：`<<FINAL_ANSWER_END>>`
来源：`run_parameter.yaml` 的 `prompt/router/custom.token_contract`。服务器代码仅按合同取值，不硬编码字面量。

## 6. 采样与停止规则
- 不在采样参数中设置控制标记为 `stop`；允许模型显式产出上述标记，由路由与透传处理。
- 建议参数位于 `generation.sampling_params`：`timeout`、`max_tokens`、`temperature`、`top_p`；`extra_body.stop` 留空；`include_stop_str_in_output: true`（保留标记供路由/剥离）。

## 7. 流水线语义（与 run.yaml 一致）
1) `retriever.retriever_init_readme`：初始化 Chroma 与远程嵌入端点（env-first）。
2) `prompt.search_o1_init`：渲染首轮提示（`init_template`）。
3) `generation.generate`：生成首轮回答。
4) loop（`times=2`）：
   - `router.search_o1_check`：
     - 含 `end_answer` → stop；
     - 含/默认 `end_query` → retrieve。
   - retrieve 分支：
     - `custom.search_o1_query_extract`：提取查询片段；
     - `retriever.retriever_search_readme`：远程嵌入→Chroma 相似检索；
     - `prompt.searcho1_reasoning_indocument`：证据内推理（`reasoning_template`）；
     - `generation.generate` → `prompt.search_o1_insert` → `generation.generate`：插入结果并再次生成。
5) `prompt.search_o1_finalize` → `generation.generate`：终稿提示与生成（`finalize_template`）。
6) `custom.output_passthrough`：剥离合同标记，输出 `markdown_ls`。

## 8. 工具契约与中立性
- `retriever.retriever_search_readme` 输出：
  - `ret_psg: List[List[str]]`（清洗后的段落，`normalize_readme_text`）
  - `metadata: List[List[{repo_author,repo_name,score,clean_state}]]`
- 严格最小合同：不做品牌/仓库的白/黑名单；不在检索层注入领域词表或指导词；仅根据查询与向量召回结果返回文本与最小元数据。

## 9. 观测与审计
- 日志：`logs/<ts>.log`（`ULTRARAG_LOG_LEVEL` 或 `log_level` 控制等级）。
- Memory 快照：始终写入 `output/memory_<benchmark>_<pipeline>_<timestamp>.json`（仅包含本步更新的 memory 最新值；无 benchmark 时留空）。

## 10. 入口与执行
- Python API（唯一入口）
```python
from dotenv import load_dotenv
from ultrarag.api import SearchO1Pipeline

load_dotenv()
p = SearchO1Pipeline("pipelines/search_o1/run.yaml")
result = p.query("question is ...")
print(result["text"])  # Markdown 文本
```
- 运行时请在项目根激活 `.venv` 或显式使用 `.venv/bin/python`，确保父子进程解释器一致。

## 11. 与仓库铁律对齐
- 解释器规则：如存在 `.venv/`，父/子进程均使用 `.venv/bin/python`（代码通过 `sys.executable` 继承当前解释器路径，调用时请在 `.venv` 内执行）。
- 环境变量规则：所有入口均在启动即加载 `.env`；运行配置以环境变量为唯一权威来源（本流水线的 API 路径对生成端主键强校验）。
- 单一路径：Search‑o1 的输出协议为 Markdown（经 `custom.output_passthrough`）；不保留替代输出格式。

## 12. 验收要点（最小集）
- `.env` 配齐生成与检索所需键；`SearchO1Pipeline().query(...)` 能够成功返回 Markdown。
- 至少一次 loop 能触发 `retrieve` 分支，并产出 `ret_psg` 与最小 `metadata`。
- 最终返回结构：
  - 工具层：`{"markdown_ls": ["..."]}`；
  - API 层：`{"format":"markdown","text":"..."}`。

（完）
