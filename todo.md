# TODO — ModelScope Models: httpx.AsyncClient per-request refactor

目的：models 来源改为“每次独立连接”的 httpx 实现；页内并发保留；只走官方 API 单一路径；无兜底、无备用版本。datasets 来源不改。

执行步骤（一次到位）
- 作用文件：ingestion_pipeline/sources/modelscope_models.py
- 删除 HubApi/session 相关依赖与调用；仅保留 `MODELSCOPE_ENDPOINT` → `self._endpoint`。
- 列表分页（每次独立请求）：
  - 新增 `async def _list_models_page_http(page)` → `PUT {endpoint}/api/v1/models`，json：`{"Path":"","PageNumber":page,"PageSize":self.page_size}`；返回 `{"Models": [...], "TotalCount": int}`。
  - 头部：`User-Agent: UltraRAG-Community-Agent/0.1`、`X-Request-ID: <uuid4>`、`Accept: application/json`、`Connection: close`；cookies 使用 `ModelScopeConfig.get_cookies()`。
- README 单一路径（每次独立请求）：
  - `GET {endpoint}/api/v1/models/{owner}/{name}/repo/files?Recursive=true` 选 README.{md,markdown,rst,txt}；
  - `GET {endpoint}/api/v1/models/{owner}/{name}/repo?FilePath=...`，`Accept: text/plain`；
  - 文本为空或命中站点占位模板则跳过（返回 None）。
- fetch 逻辑：
  - 首页取总数推导 `max_pages`；循环分页；本页 candidates 通过 `existing_hashes ∪ seen` 去重。
  - 页内 `Semaphore(16)` 并发构建 RawDocument；`content_hash=repo_id=f"models:{owner}/{name}"`；达到 `target_repo_count` 立即停止。
  - 每页一次性 `yield list[RawDocument]`。
- 清理：移除 `_list_models_page_with_retry`、`list_catalog/estimate_total_models`、任何 HubApi.session 调用与兜底；不留 TODO 备用路径。

验收
- 运行前只清理 models：SQLite `repo.source_type='models'` 与 `chunks.repo_id like 'models:%'`；Chroma `where={'source_type':'models'}` 删除。
- 连续运行 `MODELSCOPE_MODELS_TARGET=10`、`50`：
  - 无 `Connection pool is full`/urllib3 池警告；第三方日志保持 CRITICAL；
  - SQLite models 的 repo/chunk 与 Chroma 向量总量、逐 repo 数量 1:1；
  - 随机抽查 3 个 README 文本非站点占位模板，含 Markdown/front‑matter 或标题。
- datasets 来源不改、无回归。

约束
- Official API First；Single Path Only；不做防御式编程，不留备用版本与兜底；先文档后实现（本 TODO 即规范）。
