# SOP: README 检索质量加固与验收

**版本**：2025-10-04  
**负责人**：<待指定>

## 背景与原始目标重述
- 原始目标（第一阶段）：将 README 向量库接入 retriever 主线，并保持旧别名与配置向后兼容，使现有 YAML 与调用方零改动可继续运行。
- 真实跑通要求：从“入库（同步/切片/嵌入/写库）→ 可被检索（初始化/检索/返回段落与元数据）”端到端可执行，非“逻辑通”，而是以可复现实测结果为准。

## 先前两份 SOP 摘要与阶段性成果
1) 《ModelScope 文档同步与检索集成》（docs/sop/chroma_retriever_sop.md）
- 范围：仅同步 README/说明类文档，构建 `output/modelscope_docs/chroma` 与 `docs.sqlite`（docs/chunks/repo_state 三表），启用仓库级增量跳过。
- 性能：令牌桶限速 + 批量写入 + SQLite WAL；默认 `CHUNK_WORKERS=64`。
- 配置：最小必要 `.env` 与参数文件，明确 `CHROMA_PATH/CHROMA_COLLECTION` 与嵌入端点。
- 成果：形成稳定的说明文档向量索引；可重复同步且对未变更仓库跳过。

2) 《README Retriever Integration》（docs/sop/README_Retriever_SOP.md）
- 工具：`retriever_init_readme` / `retriever_search_readme`；旧名 `retriever_init_chroma` / `retriever_search_chroma` 直连新实现（向后兼容）。
- YAML：检索步骤切换到 README 工具；Search‑o1 复用原有循环结构。
- 返回：段落 `ret_psg` + 最小元数据合同 `repo_author/repo_name/score`（可选 `clean_state`）。
- 成果：在具备网络与嵌入服务时，`examples/retriever_search_only*.yaml` 与 Search‑o1 能真实命中 README 片段（见 output/*memory_nq_retriever_search_only*.json 与 *search_o1*.json 运行记录）。

已发现的遗留问题（需在本 SOP 解决）
- 少量返回文本呈 JSON 转义/混杂页面字段，未完全规范化为 README 纯文本。
- 嵌入重试/回退参数固定，偶发网络问题时等待较长，需参数化以改善体验。
- 自动化测试覆盖不足：别名一致性、元数据契约、错误分支尚未落 CI。

## 范围与非目标
- 范围：返回文本规范化、元数据补全、嵌入重试参数化、最小必要日志、测试与验收脚本。
- 非目标：
  - 更换向量库/检索算法；
  - 扩大同步范围到代码/二进制文件；
  - 生成模型相关能力（本阶段聚焦可检索性与数据质量）。

## 规格（Spec）
1. 文档解码与清洗（retriever_search_readme）
- 识别 JSON/转义载荷：对候选段落尝试 `json.loads`（最多 2 次以处理双层转义）。
- 字段优先级：优先取 `ReadMeContent` 或 `readme`；无则回退 `content/text`；均无则返回原文（标记 `clean_state=raw`）。
- HTML→纯文本：使用最小清洗（去 `<script/style>` 与 HTML 标签，保留段落与标题换行），避免引入 Markdown 渲染差异。
- 输出：`ret_psg` 为纯文本；在对应 `metadata[i][j]` 增加 `clean_state`（enum: json_decoded/html_stripped/raw）。

2. 元数据最小化（契约）
- 目标：检索返回仅保证 `repo_author` 与 `repo_name`，其他字段不属于合同范围。
- 索引内是否存储 URL/Revision 不作强制要求；检索时不进行运行时兜底生成。

3. 嵌入请求的重试/超时参数化
- 在 `servers/retriever/src/retriever.py::_embed_remote` 暴露：
  - `embedding_max_retries`（默认 3）、`embedding_retry_interval`（默认 5s）、`embedding_backoff`（默认 2.0）、`embedding_timeout`（默认 60s）。
- YAML 与工具参数透传，`.env.example` 同步文档化；默认值不改变既有成功路径。

4. 兼容性与日志
- 旧别名继续直连：`retriever_*_chroma` 与 `retriever_*_readme` 返回相同 payload 结构。
- 日志：仅在 `INGEST_DEBUG=1` 或 `RETRIEVER_DEBUG=1` 时输出字段级诊断；默认 INFO 只记录步骤开始/耗时/命中数。

## 实施计划（按提交粒度）
1) 文本清洗实现
- 文件：`servers/retriever/src/retriever.py`（`retriever_search_readme`）。
- 行为：插入“解码→抽取→HTML 去噪→回填 clean_state”的管道；为异常添加可诊断的 warning（仅 DEBUG 显示样本片段）。

2) 元数据最小化（无需补全脚本）
- 不进行运行时兜底生成，也不强制回填 URL/Revision；仅保证检索返回包含 `repo_author/repo_name/score`。

3) 嵌入参数化
- 文件：`servers/retriever/src/retriever.py`（`_embed_remote` 与 `retriever_init_readme` 参数）；`.env.example` 补充字段；`servers/retriever/parameter.yaml` 对应键。

4) 测试与示例
- 新增：`tests/servers/test_retriever_readme.py`
  - 合同：所有命中项 `repo_author/repo_name` 非空；字段不包含 `source_url/revision/path`。
  - 别名一致性：`retriever_search_readme` 与 `retriever_search_chroma` 结果相同（top_k、排序与字段）。
  - 清洗正确性：当输入为 JSON/HTML 时 `clean_state` 合法且 `ret_psg` 为可读文本。
  - 错误路径：缺 `CHROMA_PATH`、无效 API Key、`top_k<=0`。
  - 示例：保留 `examples/retriever_search_only.yaml` 与 `examples/retriever_search_only_alias.yaml`；在 README 增加“一键验收”命令。

## 质量验收清单（真实跑通）
功能验证
- 命令：`ultrarag run examples/retriever_search_only.yaml`（需可用嵌入服务与已建索引）。
- 期望：
  - 控制台/日志出现 `retriever.retriever_search_readme` 步骤；
  - 生成 `output/memory_nq_retriever_search_only_*.json`，其中每条命中均含 `repo_author/repo_name/score`（可选 `clean_state`）；
  - `ret_psg` 为可读文本（非 JSON 转义块）。

一致性验证
- `ultrarag run examples/retriever_search_only_alias.yaml` 与上一步命中相同（允许浮点得分 1e‑6 以内差异）。

数据质量阈值
- 随机抽样 100 条命中：
  - `repo_author/repo_name` 非空覆盖率 ≥ 99%；
  - `clean_state=json_decoded/html_stripped` 的样本可读性通过人工 spot‑check（标题/段落结构合理）。

健壮性
- 人为设置 `EMBEDDING_API_URL` 50% 概率超时：重试不超过 `embedding_max_retries`，总耗时满足 `timeout + Σbackoff` 上界；日志只出现一次摘要告警。

回归保障
- `pytest -q` 全绿；关键测试标记 `requires_index`，在无索引时跳过但在 CI nightly 带缓存工件执行。

## 风险与回滚
- 风险：HTML 过度清洗导致丢失语义；参数化带来配置复杂度。
- 缓解：引入 `clean_state` 与可视化样本日志；默认参数保持当前行为。
- 回滚：保留清洗/补全的 Feature Flag（环境变量 `README_CLEAN_ENABLE`、`README_BACKFILL_ENABLE`）；关闭即恢复旧行为。

## 里程碑与时间表（建议）
- D+1：文本清洗实现 + 单测；
- D+2：元数据补全脚本 + dry‑run 报告；
- D+3：嵌入参数化 + 文档；
- D+4：端到端验收（两条 YAML）+ 回归基线固化；
- D+5：收尾（风险复盘、指标固化、SOP 归档）。
