# SOP: README Retriever Integration

**版本**：2025-09-29  
**负责人**：<待指定>

## 目标
在 MCP 框架下，针对 README/说明类文档构建检索能力，为默认 RAG 以及 Search-o1/S1 等范式提供直接命中说明段落的能力。

## 约束与现状
- 范围与来源：严格对齐《docs/sop/ingestion_pipeline_sop.md》，仅消费官方来源的说明类文本（README/CHANGELOG/docs 等）。
- 数据现状：README 向量索引位于 `output/ingestion/chroma`（集合名为 `modelscope_docs`）；SQLite 位于 `output/ingestion/sqlite/docs.sqlite` 并保存最小审计信息。
- 检索工具已就位：`retriever_init_readme` / `retriever_search_readme` 已在 `servers/retriever/src/retriever.py` 实现；旧名 `retriever_init_chroma` / `retriever_search_chroma` 作为别名直连新实现（向后兼容）。
- YAML 已切换：`examples/rag.yaml`、`pipelines/search_o1/run.yaml` 使用 README 检索工具；Search‑o1 所需模板由 `pipelines/search_o1/parameter/run_parameter.yaml` 管理。
- 编程风格：最小化 + fail-fast；不做运行时兜底生成冗余元数据。

## 环境变量与参数
- 必需：`CHROMA_PATH`、`CHROMA_COLLECTION`（默认 `modelscope_docs`）、`EMBEDDING_API_URL`、`EMBEDDING_API_KEY`、`EMBEDDING_MODEL`（默认 `BAAI/bge-m3`）、`EMBEDDING_TIMEOUT`（秒，默认 60）。
- 仅加入最小必要项；其余配置通过 YAML/参数文件传入，避免冗余。

## 规格（Spec）
1. **工具设计**
   - 工具：`retriever_init_readme(chroma_path?, chroma_collection?, embedding_api_url?, embedding_api_key?, embedding_model?, embedding_timeout?)` 初始化 Chroma 与嵌入端点；`retriever_search_readme(query_list, top_k=5, query_instruction="")` 返回 README 段落+元数据。
   - 返回结构（最小合同）：`{"ret_psg": [[...]], "metadata": [[{"repo_author","repo_name","score"}, ...]]}`。
   - 可选诊断字段：`clean_state` 用于标注清洗路径（`html_stripped/json_decoded/raw`）。不返回 `source_url/revision/path` 等非必需字段。
2. **YAML 对接**
   - 更新 `examples/rag.yaml`、`pipelines/search_o1/run.yaml` 等，使检索步骤调用新工具。
   - 若 Search-o1/S1 需要多轮检索，保持现有 loop 结构不变，仅替换底层检索工具。
3. **配置**
   - `.env`、`servers/retriever/parameter.yaml` 仅包含 README 检索必需项；不引入与溯源 URL 相关的生成逻辑或兜底参数。
   - AGENTS.md 需明确“检索 README 索引前提已建立”。

## 实施计划
1. **代码改造**
   - `servers/retriever/src/retriever.py`：新增 README 工具，载入 Chroma 集合，返回段落 + metadata。
   - 复用现有令牌桶/批量方式，删除无用代码路径。
2. **配置 / 文档同步**
   - 调整 YAML / 参数文件；更新 AGENTS.md、design 文档说明 README 检索入口。
3. **测试**
   - 先运行任一 ingestion 脚本完成入库（示例）：
     - `python -m ingestion_pipeline.ingest_modelscope_models_readmes`
     - 或 `python ingestion_pipeline/ingest_github_modelscope_readmes.py`
   - 执行 `ultrarag run examples/rag.yaml`，确认命中 README 段落。
   - 运行 Search‑o1 流程，验证多轮检索能稳定命中 README 片段。

## 验证
- README 工具返回的 metadata 至少包含 `repo_author/repo_name`；内容与得分与查询一致。
- pipeline 输出中引用的文本应来自 README；随机抽样检查可读性（无明显 JSON 转义/HTML 杂质）。
- 大规模运行时性能不回退（受到 API 限速时表现与同步阶段一致）。
