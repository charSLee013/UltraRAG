# Community Intelligent Agent Design

## Goals
- Tailor UltraRAG as a community-oriented assistant capable of answering model usage, technical debugging, platform features, and project guidance questions.
- Deliver accurate, traceable responses under strict latency budgets (< 3s avg, < 10s complex queries).
- Maintain the framework's "less is more" minimalism while enabling fast iteration for competition data.

## High-Level Flow
1. **Corpus Preparation**
   - Ingest community assets (FAQ, tutorials, code labs, forum threads).
   - Normalize into `corpus.meta.jsonl` + `corpus.texts.tar.gz` manifests to preserve order and compression.
   - Generate vectors via SiliconFlow or local encoder, persist to `corpus.vectors.npy` + `corpus.faiss.index`.
2. **Pipeline Execution**
   - `benchmark.get_data`: serve inline or dataset-driven multi-turn queries.
   - `conversation.context_manager` (new) stitches previous turns + retrieved evidence.
   - `remote_retriever.retrieve`: lazily loads FAISS index, fetches top-k documents.
   - `remote_reranker.rerank`: refine ranking with SiliconFlow reranker.
   - `prompt.community_template`: emits instruction-tuned prompts (with cite, follow-up suggestions).
   - `generation.generate`: call ModelScope chat model (temperature 0 for deterministic replies).
   - `custom.output_extract_from_boxed`: trim final response + citations.
   - `evaluation.evaluate`: compute ROUGE/F1 for regression tests.
   - `llm_evaluator.evaluate_with_llm`: semantic scoring during QA sign-off.
3. **Response Delivery**
   - Return final answer + citations + optional next-step guidance.
   - Persist logs to `output/memory_*` for traceability.

## Key Modules & Enhancements
- **Conversation Memory**: new server managing session state, storing prior QA pairs, handing context to prompt generation.
- **Inline vs Dataset Input**: `servers/benchmark` already supports JSONL and inline queries; competition datasets can override via `path + key_map`.
- **Retrieval**: FAISS cache ensures sub-second lookups; fallback to fresh embedding only when corpus changes.
- **Evaluation**: LLM judge (ModelScope) complements hard metrics to approximate human scoring.
- **Extensibility**: Additional servers (e.g., `code_executor`, `error_diagnoser`) can be slotted between retriever and prompt for specialized diagnostics.

## Latency Strategy
- Warm load FAISS index at service boot.
- Use lightweight prompt + deterministic decoding (temperature = 0.0, top_p = 0.1).
- Batch embedding/rerank requests and leverage retry logic (30s × 3) to absorb remote hiccups without manual restarts.

## Accuracy & UX Notes
- Prompts must include document excerpts + citation tags (`[doc:path#chunk]`).
- Encourage follow-up suggestions when confidence < threshold (LLM judge output).
- For multi-modal data, store metadata in manifest and fetch attachments on demand.

## Stability & Ops
- All generated artifacts live under `data/` (corpus) and `output/` (memory, metrics); no temporary files persist after successful builds.
- Continuous runs compare timestamps to detect stale caches.
- Memory logs double as audit trail for competition submissions.

## Next Steps
1. Implement `conversation.context_manager` server.
2. Import competition datasets into compressed manifest + FAISS format.
3. Craft community-specific prompt templates with citation scaffolding.
4. Add specialized diagnostics servers (code execution, configuration tips) as required by problem categories.
5. Wire automated regression suites leveraging LLM judge to monitor answer quality.
