# Repository Guidelines

## Project Structure & Module Organization
UltraRAG’s Python package lives in `src/ultrarag`, containing the CLI entry (`client.py`), shared utilities, and the MCP server/client scaffolding. Ready-to-run MCP servers are organized under `servers/` (e.g., `servers/retriever`, `servers/router`), while `examples/` holds pipeline YAMLs that illustrate orchestration patterns. Shared prompts are stored in `prompt/`, reusable datasets in `data/`, and long-form docs plus diagrams under `docs/`. Keep demo scripts and helper notebooks inside `script/` or `examples/` rather than `src/`.

## Build, Test, and Development Commands
`uv pip install -e .` performs an editable install with the recommended dependency manager; `pip install -e .` is acceptable when uv is unavailable. Use `conda env create -f environment.yml` to mirror the full GPU-ready stack, then `conda activate ultrarag`. Validate the CLI with `ultrarag run examples/sayhello.yaml` before starting feature work. During MCP development, point the client at a custom YAML via `ultrarag run servers/<module>/pipeline.yaml` to smoke-test flows.

### Python Interpreter Rule (.venv first) — Hard Requirement
- If a project-local virtualenv exists at `.venv/`, all Python entrypoints and spawned subprocesses MUST use its interpreter.
  - Shell: prefer `. .venv/bin/activate` or invoke explicitly via `.venv/bin/python ...` (e.g., `.venv/bin/python -m pytest`).
  - Subprocess (code): when launching MCP servers or helper scripts, resolve the interpreter to `.venv/bin/python` if present; otherwise fall back to `sys.executable`.
  - CI/scripts: never mix Conda Python and `.venv` within the same run. The parent process and all children must share the same interpreter path.
- Rationale: prevents “parent can import, child cannot” failures (e.g., `ModuleNotFoundError: ultrarag/jsonlines`) caused by interpreter splits; ensures reproducible imports and dependency isolation.
- Smoke guidance: if `.venv/` is absent, use the current interpreter consistently and install dependencies into it (`python -m pip install -e .`).

## Coding Style & Naming Conventions
Follow standard Python 3.11 guidelines: four-space indents, double quotes for user-facing strings, and type hints on new public functions. Name modules and servers in lowercase with underscores (`retriever_server.py`), and align YAML step names with their tool intent (`retrieve_passages`, `rerank_answers`). Prefer extracting shared logic into `src/ultrarag/utils.py` instead of duplicating code inside server directories.

## Testing Guidelines
Author tests with `pytest`; place them in `tests/` mirroring the package path (`tests/servers/test_router.py`). Use descriptive `test_<scenario>_<expected>()` names and fixture files in `data/` where appropriate. Run `pytest` locally before opening a pull request, and include CLI smoke-tests (`ultrarag run ...`) in your manual checklist when adding new pipelines or servers. Target meaningful coverage of branching logic, especially MCP tool dispatch and error handling.

## Performance & Observability Expectations
- Treat吞吐量和QPS为一等指标：在设计新 pipeline/接口时，先评估目标速率与上游限额（API 并发、服务超时），并让规范文件（SOP）明确写出并发上限、退避策略与日志要求。
- 在不牺牲数据质量的前提下压榨速度：允许增加 fetch/处理并发，但必须精准记录失败与超时，确保可追溯、可补跑，严禁“静默忽略”异常。
- 任何影响速率/质量的改动都遵循 Specification-First：先在 SOP 中写清楚目标与安全阀，再着手编码。
- 观测优先：新增并发或节流机制时同步补充指标（StageMetrics、日志、Prometheus 埋点等），让其他工程师能快速定位瓶颈与故障。


## Retrieval Data Sources
- README ingestion writes to the shared stores `output/ingestion/sqlite/docs.sqlite` (SQLite) and `output/ingestion/chroma` (Chroma collection `ingestion_docs`).
- Retriever pipelines必须使用 `retriever_init_readme` / `retriever_search_readme` 指向同一集合，只返回 `repo_author/repo_name/score`（可选 `clean_state`）。

## Search‑o1 Retrieval Neutrality & Anti‑Patterns
To keep Search‑o1 flows neutral, reproducible, and auditable, follow these rules:

- Neutral retrieval (must):
  - Do not inject brand/org names, model families, or expected answer lists into `retriever.query_instruction` to steer results.
  - Do not maintain allow/deny lists (e.g., by `repo_author`/`repo_name`) or perform domain‑specific gating at the retriever or router layer.
  - Keep the minimal contract: return only `ret_psg` text plus minimal `metadata` (author/name/score/clean_state) — no task‑specific fields.

- Clear responsibility boundaries (must):
  - Retriever → recall + text cleaning only (no semantic filtering by brand/source).
  - Prompt/Generation → integrate evidence, structure and deduplicate facts, and produce the final Markdown answer exactly once (no alternate formats).
- Router/Loop → decide `retrieve` vs `stop` via explicit markers (`<<SRCH_Q_END>>`, `<<FINAL_ANSWER_END>>`); loop `times` 是上限。

- Prohibited patterns (do not):
  - Injecting terms like “Qwen/Qwen2/Qwen3 系列模型列表” or similar domain hints into retriever instructions.
  - Whitelisting or blacklisting sources (e.g., `repo_author == 'Qwen'`).
  - Hard‑coding domain knowledge or heuristics into server code/parameters that bias retrieval outcomes.
  - Keeping legacy output paths or "compatibility modes" (for example boxed extractors) or weakening the single path with "optional" wording.

- Allowed safe tuning (okay):
  - Adjust generic hyper‑parameters (e.g., `top_k`, temperature, `max_tokens`, loop `times`).
- Improve prompts to request structured, deduplicated lists and to emit `<<FINAL_ANSWER_END>>` once sufficient evidence is gathered。
  - Evidence‑based deduplication/merging in refinement (on content agreement), never deletion based on source identity.

- Observability & reproducibility (must):
  - Preserve `memory_*` snapshots (prompts, answers, `ret_psg`, `metadata`) for each step; avoid hidden filters.
  - Keep logs informative without leaking secrets; never mask or strip source provenance in diagnostic output.
  - Output contract is singular: emit Markdown via `custom.output_passthrough`; any change must update the SOP first and remove the previous implementation.

- Specification‑First (must):
  - Any behavioral change to retrieval/loop/termination must be updated in `docs/sop/search_o1_answering_sop.md` before implementation.
## Specification-First Development
Every feature or significant change must have an SOP entry under `docs/sop/` before coding begins. Treat the SOP as the single source of truth: update the spec and implementation plan first, then implement code and other artifacts strictly following the SOP. If requirements change, revise the SOP prior to any code edits.

### Iron Rules
- Never modify repository files when the user only asks for strategy, analysis, or a plan. Deliver the plan first and wait for explicit implementation instructions before changing code.
- Official API First (hard requirement): Always use upstream, documented APIs and their provided fields as the primary contract; do not introduce ad‑hoc scraping, directory scanning, or heuristic fallbacks when the official API already supplies the needed data. Example: for ModelScope ingestion, prefer `ReadMeContent` from ListModels over any repo file traversal.
- Single Path Only (hard requirement): Do not leave multiple code paths, runtime flags, or “optional” modes that implement the same responsibility. Pick one path per SOP, remove legacy/alternate paths in the same change, and keep behavior explicit and auditable.
- No defensive “backup” logic: If an upstream API call fails, surface a clear error with actionable context rather than silently switching to an alternative behavior. Any alternative must be specified first in SOP and then implemented as the sole path.

## Commit & Pull Request Guidelines
Commit using Conventional Commits (`feat: add hybrid retriever`, `fix: guard empty query`). Keep changes scoped and reference issues in the footer when relevant. Pull requests should summarize the user-facing impact, list verification commands, and attach logs or screenshots for pipeline demos. When modifying benchmark servers, note any dataset or environment prerequisites so reviewers can reproduce results quickly.
