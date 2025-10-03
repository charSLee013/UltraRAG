# Repository Guidelines

## Project Structure & Module Organization
UltraRAG’s Python package lives in `src/ultrarag`, containing the CLI entry (`client.py`), shared utilities, and the MCP server/client scaffolding. Ready-to-run MCP servers are organized under `servers/` (e.g., `servers/retriever`, `servers/router`), while `examples/` holds pipeline YAMLs that illustrate orchestration patterns. Shared prompts are stored in `prompt/`, reusable datasets in `data/`, and long-form docs plus diagrams under `docs/`. Keep demo scripts and helper notebooks inside `script/` or `examples/` rather than `src/`.

## Build, Test, and Development Commands
`uv pip install -e .` performs an editable install with the recommended dependency manager; `pip install -e .` is acceptable when uv is unavailable. Use `conda env create -f environment.yml` to mirror the full GPU-ready stack, then `conda activate ultrarag`. Validate the CLI with `ultrarag run examples/sayhello.yaml` before starting feature work. During MCP development, point the client at a custom YAML via `ultrarag run servers/<module>/pipeline.yaml` to smoke-test flows.

## Coding Style & Naming Conventions
Follow standard Python 3.11 guidelines: four-space indents, double quotes for user-facing strings, and type hints on new public functions. Name modules and servers in lowercase with underscores (`retriever_server.py`), and align YAML step names with their tool intent (`retrieve_passages`, `rerank_answers`). Prefer extracting shared logic into `src/ultrarag/utils.py` instead of duplicating code inside server directories.

## Testing Guidelines
Author tests with `pytest`; place them in `tests/` mirroring the package path (`tests/servers/test_router.py`). Use descriptive `test_<scenario>_<expected>()` names and fixture files in `data/` where appropriate. Run `pytest` locally before opening a pull request, and include CLI smoke-tests (`ultrarag run ...`) in your manual checklist when adding new pipelines or servers. Target meaningful coverage of branching logic, especially MCP tool dispatch and error handling.


## Retrieval Data Sources
README ingestion writes to `output/modelscope_docs/chroma`; retriever pipelines must use `retriever_init_readme` / `retriever_search_readme`, which return README segments plus metadata (`repo_type/owner/name/path/source_url/revision`).
## Specification-First Development
Every feature or significant change must have an SOP entry under `docs/sop/` before coding begins. Treat the SOP as the single source of truth: update the spec and implementation plan first, then implement code and other artifacts strictly following the SOP. If requirements change, revise the SOP prior to any code edits.

## Commit & Pull Request Guidelines
Commit using Conventional Commits (`feat: add hybrid retriever`, `fix: guard empty query`). Keep changes scoped and reference issues in the footer when relevant. Pull requests should summarize the user-facing impact, list verification commands, and attach logs or screenshots for pipeline demos. When modifying benchmark servers, note any dataset or environment prerequisites so reviewers can reproduce results quickly.
