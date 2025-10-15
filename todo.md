# TODO — ModelScope Models Readmes: Quick Refactor & Acceptance

Scope: ingestion_pipeline/ingest_modelscope_models_readmes.py and sources/modelscope_models.py. Goal: mirror the successful datasets production path and fully align with SOP.

## Execute (Research → Plan → Execute → Reflect)

- Research: diff models script vs datasets script; list gaps: pre-scan catalog, item-wise yield, constructor dedupe, register_ingested_hash, env fallbacks, verbose logs, no page Semaphore/timeout, SQLite filter uses enum str.
- Plan: keep entry script minimal (load .env, enforce EMBEDDING_*, optional MODELSCOPE_MODELS_TARGET, build pipeline/runner/stores, print summary). Move dedupe to Runner, make fetch page-wise with in-page Semaphore+wait_for, identity content_hash, remove legacy hooks.
- Execute:
  1) Entry script (ingestion_pipeline/ingest_modelscope_models_readmes.py):
     - load_dotenv() at top; require EMBEDDING_API_URL/KEY/MODEL only; drop OPENAI_/LLM_ fallbacks.
     - read MODELSCOPE_MODELS_TARGET; if int >=1 pass as `target_repo_count` to pipeline.
     - construct SQLiteStore + ChromaStore + SqliteChromaIngestor; limits=max_workers=8, max_embed_concurrency=8, ingest_batch_size=8, chunk_max_size=32768.
     - logging: `logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")`; page级进度由 Runner 的 tqdm 负责。
     - build ModelScopeModelsPipeline(page_size=100, timeout=60.0, target_repo_count=env) and IngestionRunner(embed=get_default_embed_fn(), ingest=ingestor.ingest/_many, fetch_force=False).
     - run → print minimal summary (repos_ingested, chunks_created)。
     - fatal handler（与 datasets 一致）：
       - `httpx.HTTPStatusError` → `[fatal] HTTP {code} {reason} url={url}`
       - 其它 `httpx.HTTPError` → `[fatal] httpx error: {ExcName} url={url}`
       - 其余异常 → `[fatal] {exc}`

  2) Pipeline (ingestion_pipeline/sources/modelscope_models.py):
     - declare `source_type = SourceType.MODELS`.
     - change `fetch(...) -> AsyncIterable[list[RawDocument]]` (page-wise yield).
     - page loop via HubApi.list_models(page_number, page_size<=100); build candidates per page.
     - dedupe: `content_hash = repo_id = models:{owner}/{name}`; skip when in `existing_hashes` (from Runner) or in local `seen` for the page.
     - page concurrency: Semaphore(16); wrap README build with asyncio.wait_for(timeout) and small jitter 0.1–1.5s; gather with return_exceptions=True; count succeeded/timeouts; yield page_docs once per page; stop when target_repo_count reached.
     - remove pre-scan `list_catalog/estimate_total_models` and `register_ingested_hash` (旧钩子已失效)。
     - keep process() identical to datasets path (clean→split→embed→uuid5 assemble)。

  3) Runner: no changes (already injects existing_hashes using `pipeline.source_type.value`, shows page-level tqdm, micro-batch writer).

- Reflect: verify O(1) startup (no full-catalog pass), dedupe works across runs, logs clean, and 1:1 SQLite↔Chroma alignment. If any deviation, fix code then update SOP before re-run.

## Acceptance Checklist (manual, .venv required)

- Env + clean state:
  - `. .venv/bin/activate` and ensure `.env` has EMBEDDING_API_URL/KEY/MODEL, CHROMA_PATH/CHROMA_COLLECTION (optional), INGESTION_SQLITE_PATH (optional).
  - Remove old stores:
    - `rm -f output/ingestion/sqlite/docs.sqlite`
    - `python -c 'from dotenv import load_dotenv; load_dotenv(); import os,shutil; p=os.getenv("CHROMA_PATH","output/ingestion/chroma"); shutil.rmtree(p, ignore_errors=True); print(p)'`

- Run 1 (target=1):
  - `MODELSCOPE_MODELS_TARGET=1 .venv/bin/python ingestion_pipeline/ingest_modelscope_models_readmes.py`
  - Expect summary shows repos_ingested=1, chunks_created>0; no verbose per-repo prints; on upstream failure, fatal shows `HTTP <code> <reason> url=...`.

- Verify DB/Chroma counts after Run 1:
  - SQLite repos (models):
    - `.venv/bin/python -c "from dotenv import load_dotenv; load_dotenv(); import os,sqlite3; db=os.getenv('INGESTION_SQLITE_PATH','output/ingestion/sqlite/docs.sqlite'); con=sqlite3.connect(db); print(con.execute(\"select count(*) from repo where source_type='models'\").fetchone()[0])"`
  - SQLite chunks (models only):
    - `.venv/bin/python -c "from dotenv import load_dotenv; load_dotenv(); import os,sqlite3; db=os.getenv('INGESTION_SQLITE_PATH','output/ingestion/sqlite/docs.sqlite'); con=sqlite3.connect(db); print(con.execute(\"select count(*) from chunks where repo_id like 'models:%'\").fetchone()[0])"`
  - Chroma total:
    - `.venv/bin/python -c "from dotenv import load_dotenv; load_dotenv(); import os; from chromadb import PersistentClient; c=PersistentClient(path=os.getenv('CHROMA_PATH','output/ingestion/chroma')); col=c.get_or_create_collection(os.getenv('CHROMA_COLLECTION','ingestion_docs')); print(col.count())"`

- Run 2 (target=1 again, dedupe):
  - `MODELSCOPE_MODELS_TARGET=1 .venv/bin/python ingestion_pipeline/ingest_modelscope_models_readmes.py`
  - Expect repos_ingested increases by 0; SQLite repo/chunk counts unchanged; Chroma count unchanged.

- Run 3 (target=10):
  - `MODELSCOPE_MODELS_TARGET=10 .venv/bin/python ingestion_pipeline/ingest_modelscope_models_readmes.py`
  - Expect repos_ingested increases up to new unique repos (≤10); re-run with same target yields 0 new.

- 1:1 alignment spot-check:
  - Before and after each run, record SQLite chunks total and Chroma `col.count()`; delta must match.
  - Optional per-repo: pick one `repo_id like 'models:%'`, ensure at least one chunk text non-empty in SQLite and same number of vectors returned by `col.get(where={'repo_id': {'$eq': rid}})`.

- Logging cleanliness:
  - 默认级别 WARNING；仅 Runner 的页级 tqdm；除最终 summary 与致命异常外不额外打印。
  - 致命异常格式按上文 fatal handler 约定输出，避免跑偏日志。

- SOP conformity:
  - fetch returns per-page List[RawDocument]; dedupe via Runner existing_hashes; `source_type` persisted as `.value` in both stores; 旧钩子已失效。

Notes:
- Ensure entry script path remains `ingestion_pipeline/ingest_modelscope_models_readmes.py`; remove any stale/renamed duplicates after refactor.

## ModelScope 模型 README 获取（强约束，官方路径，仅此一条）

- 官方顺序（禁止自由发挥 / 禁止多路径）：
  - 第一步：`HubApi.get_model(model_id)`，读取详情里的 `ReadMeContent`。
  - 第二步（仅当第一步缺失或判定为“站点占位模板”时）：
    - 调用 `HubApi.get_model_files(model_id, revision, recursive=True)`；
    - 选择 `README.md`；
    - 通过 `get_file_download_url(...)` 获取直链并下载正文（`Accept: text/plain`）。
  - 列表接口 `list_models` 仅用于枚举，不得作为 README 正文来源（其 `ReadMeContent` 允许为空/占位/摘要，不可靠）。

- 占位模板判定（最小必要，避免把模板当正文）：
  - 关键片段命中即判定占位（例如“当前模型的贡献者未提供更加详细”“SDK下载”“Git下载”“模型文件…页面获取”以及浅灰提示段）。
  - 判定为占位后必须走 README.md 直链下载路径；不得保留“兼容/兜底”模式。

- 请求头与防爬：
  - 每次请求都重建唯一 `X-Request-ID`（UUID v4），禁止复用；
  - `Accept: text/plain, */*`；必要时附带 `ModelScopeConfig.get_cookies()`；
  - 仅使用官方域名与 Hub API；不引入抓取、镜像或私有直链。

- 编码风格与约束：
  - 绝不防御式编程，不留“TODO 兜底/备用版本/调试开关”；
  - 变更先更 SOP 再实现；
  - 严格遵循 AGENTS.md：Official API First / Single Path Only / 无兜底 / 可观测。

## 移除错误逻辑（立即执行，留下 TODO 等待后续实现）

- 删除/禁用错误路径（ingestion_pipeline/sources/modelscope_models.py）：
  - 移除对 `list_models` 返回项 `raw_item["ReadMeContent"]` 的任何正文使用；该字段仅用于枚举，不得入库。
  - 在 `_fetch_readme_text_sync` 顶部，删除“非空即返回”的分支；改为占位：
    - `# TODO(models): 实现 get_model(model_id).ReadMeContent →（占位判定）→ README.md 直链下载`
    - 临时返回 `None, None`，让上层逻辑跳过该条（不入库伪内容）。
  - 保留分页与去重逻辑不变（Runner 注入 existing_hashes，页内并发/超时仍生效）。

- Header 重建（占位）：
  - 在后续实现时，于每次 Hub API 请求前重建 `X-Request-ID`（UUID v4）；本阶段仅在 `# TODO` 中标注，不先落代码以免遗留临时实现。

- 验收（移除阶段）：
  - 运行 MODELSCOPE_MODELS_TARGET=1 时，如未完成新路径实现，应不写入 models:% 记录（因为 `fetch` 对该条返回 None）。
  - 运行 datasets 脚本不受影响；models 脚本的 README 获取将在后续步骤按官方路径补齐后再恢复入库。
