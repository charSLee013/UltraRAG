## TODO — SQLite schema, SOP alignment, env cleanup

Owner: ingestion
Date: 2025-10-16

Tasks
- Drop extra columns from SQLite `chunks` schema.
- Update insert path to write minimal fields only.
- Align SOP default `CHROMA_COLLECTION` to `modelscope_docs`.
- Trim `.env.example` to effective keys and add `INGESTION_SQLITE_PATH`.
- Remove stale tests that target legacy interfaces.

Acceptance Criteria
- Schema: databases must have `chunks` with exactly five columns: `chunk_uuid, repo_id, content_hash, chunk_index, text`. If an existing DB deviates, the code fails fast with a clear error; engineers must recreate or run an explicit, one‑off migration (no automatic runtime migration).
- Writes: ingestion completes without referencing or storing `locator_*` or `embedding` in SQLite; Chroma still receives complete metadata/embeddings.
- SOP: docs/sop/ingestion_pipeline_sop.md lists `modelscope_docs` as the recommended default for `CHROMA_COLLECTION`.
- Env: `.env.example` contains only effective variables (embedding settings, `INGESTION_SQLITE_PATH`, `CHROMA_PATH`, `CHROMA_COLLECTION`) and no legacy `CHUNK_*` entries.
- Tests: legacy test files removed from `tests/` so running pytest won’t fail due to mismatched interfaces.

Verification Steps
- Fresh run creates DB with minimal schema: delete `output/ingestion/sqlite/docs.sqlite` then run either `ingestion_pipeline/ingest_modelscope_datasets_readmes.py` or `ingestion_pipeline/ingest_modelscope_models_readmes.py` with valid embedding env; inspect schema via `PRAGMA table_info(chunks)`.
- End-to-end: confirm StageMetrics prints and Chroma upserts succeed; query Chroma (via retriever_init_readme + retriever_search_readme) returns results.
- Docs: open SOP and confirm `modelscope_docs` default.
- Env: open `.env.example` and confirm presence/absence per above.

Notes
- Migration runs inside SQLiteStore initialization; it recreates `chunks` and copies minimal fields, then reindexes `idx_chunks_repo_content`.
