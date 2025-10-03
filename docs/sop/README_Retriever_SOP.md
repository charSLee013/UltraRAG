# SOP: README Retriever Integration

**版本**：2025-09-29  
**负责人**：<待指定>

## 目标
在 MCP 框架下，针对 README/说明类文档构建检索能力，为默认 RAG 以及 Search-o1/S1 等范式提供直接命中说明段落的能力。

## 约束与现状
- 范围仅限 `script/modelscope_docs_sync.py` 同步的 README 集合，不接入模型二进制、代码文件或其他非说明文本。
- 数据现状：README 已写入 `output/modelscope_docs/chroma`，`repo_state` 缓存 `(repo_type, owner, name)` 与最新版 revision。
- 检索逻辑仍调用旧工具（`retriever_search_chroma` 占位）；Search-o1/S1 YAML 未切换到 README 数据源。
- 编程风格：遵循最小化与 fail-fast 原则，不引入防御式兜底、备用实现或噪声日志。任何错误应直接抛出，供上游处理。

## 规格（Spec）
1. **工具设计**
   - 在 `servers/retriever/src/retriever.py` 新增 README 专用工具（如 `retriever_init_readme` / `retriever_search_readme`）。
   - 工具需初始化 Chroma 客户端，连接集合 `modelscope_docs`，并保留令牌桶 / 批量逻辑。
   - 查询返回结构：`{"ret_psg": [[段落文本...]], "metadata": [[{"repo_type":..., "owner":..., "name":..., "source_url":..., "revision":...}, ...]]}` 或等价字段，使后续 prompt / reranker 能直接引用。
2. **YAML 对接**
   - 更新 `examples/rag.yaml`、`search_o1.yaml` 等，使检索步骤调用新工具。
   - 若 Search-o1/S1 需要多轮检索，保持现有 loop 结构不变，仅替换底层检索工具。
3. **配置**
   - `.env`、`servers/retriever/parameter.yaml` 如需新增字段仅限于 README 检索必需项，不引入额外噪声。
   - AGENTS.md 需明确“检索 README 索引前提已建立”。

## 实施计划
1. **代码改造**
   - `servers/retriever/src/retriever.py`：新增 README 工具，载入 Chroma 集合，返回段落 + metadata。
   - 复用现有令牌桶/批量方式，删除无用代码路径。
2. **配置 / 文档同步**
   - 调整 YAML / 参数文件；更新 AGENTS.md、design 文档说明 README 检索入口。
3. **测试**
   - 运行 `python script/modelscope_docs_sync.py --max-models 100 --max-datasets 20` 确保 Chroma 数据可用。
   - 执行 `ultrarag run examples/rag.yaml`，确认命中 README 段落并输出链接。
   - 运行 Search-o1/S1 流程，验证多轮检索能落到 README 片段。

## 验证
- README 工具返回的 metadata 包含 `repo_type/owner/name`，可用于后续引用/跳转。
- pipeline 输出中引用的文本应来自 README；随机抽样验证链接指向 ModelScope README。
- 大规模运行时性能不回退（受到 API 限速时表现与同步阶段一致）。

