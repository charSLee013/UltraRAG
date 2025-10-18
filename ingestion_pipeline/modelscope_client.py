from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import uuid
from html.parser import HTMLParser
from typing import AsyncGenerator, Dict, Iterable, List, Optional, Tuple

import httpx
import logging

from ingestion_pipeline.modelscope_headers import build_user_agent
from threading import Lock
from typing import Any

# Singleton cache for browser-like headers captured once via Playwright
_MCP_HEADERS_CACHE: Dict[str, str] | None = None
_MCP_HEADERS_LOCK = Lock()

def _capture_mcp_headers_via_playwright() -> Dict[str, str]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        # Fallback to minimal headers if Playwright is unavailable
        return {
            "User-Agent": build_user_agent(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Content-Type": "application/json",
        }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        captured: Dict[str, str] | None = None

        def on_request(req):
            nonlocal captured
            url = req.url
            if req.method == "PUT" and ("/mcpServers" in url or "/mcp/servers" in url):
                hdrs = dict(req.headers)
                # Normalize keys and keep relevant ones
                ua = hdrs.get("user-agent") or hdrs.get("User-Agent")
                accept = hdrs.get("accept") or hdrs.get("Accept")
                lang = (
                    hdrs.get("x-modelscope-accept-language")
                    or hdrs.get("accept-language")
                    or hdrs.get("Accept-Language")
                )
                captured = {
                    "User-Agent": ua or build_user_agent(),
                    "Accept": accept or "application/json, text/plain, */*",
                    "Accept-Language": lang or "zh-CN,zh;q=0.9,en;q=0.8",
                    "Content-Type": "application/json",
                }

        page.on("request", on_request)
        try:
            page.goto("https://modelscope.cn/mcp", wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
        finally:
            browser.close()

    if captured is None:
        # As a last resort return minimal headers
        return {
            "User-Agent": build_user_agent(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Content-Type": "application/json",
        }
    return captured

def build_mcp_openapi_headers() -> Dict[str, str]:
    """Return browser-like headers for MCP openapi, captured once per process.

    Uses a singleton cache to avoid multiple browser launches.
    """
    global _MCP_HEADERS_CACHE
    if _MCP_HEADERS_CACHE is not None:
        return dict(_MCP_HEADERS_CACHE)
    with _MCP_HEADERS_LOCK:
        if _MCP_HEADERS_CACHE is None:
            _MCP_HEADERS_CACHE = _capture_mcp_headers_via_playwright()
    return dict(_MCP_HEADERS_CACHE)

DEFAULT_ENDPOINT = "https://modelscope.cn"

logger = logging.getLogger("ingestion.modelscope_client")


class HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: List[str] = []

    def handle_data(self, data: str) -> None:
        stripped = data.strip()
        if stripped:
            self._parts.append(stripped)

    def result(self) -> str:
        return "\n".join(self._parts)


def strip_html(text: str) -> str:
    parser = HTMLStripper()
    parser.feed(text)
    return parser.result()


@dataclass
class ModelFile:
    owner: str
    name: str
    path: str
    revision: Optional[str]
    size: Optional[int]
    source_url: Optional[str]


@dataclass
class Document:
    repo_type: str  # 'model' or 'dataset'
    owner: str
    name: str
    path: str  # for dataset fixed to README.md
    content: str
    size: int
    sha256: str
    revision: Optional[str]
    source_url: str

    def key(self) -> Tuple[str, str, str, str]:
        return (self.repo_type, self.owner, self.name, self.path)


class ModelScopeClient:
    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        *,
        model_page_size: int = 200,
        dataset_page_size: int = 100,
        timeout: int = 30,
        max_retries: int = 4,
        initial_backoff: float = 1.0,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.model_page_size = model_page_size
        self.dataset_page_size = dataset_page_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "ModelScopeClient":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        # [块] 带退避请求：为每次请求重建 UA/X-Request-ID；对 429/5xx 做指数退避
        backoff = self.initial_backoff
        for attempt in range(self.max_retries):
            try:
                headers = kwargs.pop("headers", {})
                request_headers = {
                    "User-Agent": build_user_agent(),
                    "X-Request-ID": uuid.uuid4().hex,
                    **headers,
                }
                if self._client is None:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        resp = await client.request(method, url, headers=request_headers, **kwargs)
                else:
                    resp = await self._client.request(method, url, headers=request_headers, **kwargs)
                if resp.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError("server busy", request=resp.request, response=resp)
                return resp
            except (httpx.HTTPError, httpx.HTTPStatusError):
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2
        raise RuntimeError("unreachable")

    async def _models_page(self, page_number: int) -> Dict[str, object]:
        payload = {"Path": "", "PageNumber": page_number, "PageSize": self.model_page_size}
        resp = await self._request("PUT", f"{self.endpoint}/api/v1/models", json=payload)
        resp.raise_for_status()
        data = resp.json().get("Data")
        if not data:
            return {"Models": [], "TotalCount": 0}
        return data

    async def iter_models(self, limit: Optional[int] = None) -> AsyncGenerator[Dict[str, object], None]:
        # [块] 按页迭代 models：首页得总数，逐页 yield，避免预扫描全集
        page = 1
        yielded = 0
        total = None
        while True:
            data = await self._models_page(page)
            models = data.get("Models") or []
            if total is None:
                total = data.get("TotalCount", len(models))
            if not models:
                break
            for model in models:
                yield model
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            page += 1
            if limit is None and yielded >= total:
                break

    async def fetch_model_files(self, owner: str, name: str) -> List[ModelFile]:
        # [块] 获取仓库文件列表，并标准化关键字段
        assert self._client is not None
        params = {"Recursive": "true"}
        resp = await self._request("GET", f"{self.endpoint}/api/v1/models/{owner}/{name}/repo/files", params=params)
        if resp.status_code != 200:
            return []
        payload = resp.json()
        files = payload.get("Data", {}).get("Files", [])
        result: List[ModelFile] = []
        for item in files:
            path = item.get("Path")
            if not isinstance(path, str):
                continue
            revision = item.get("Revision") if isinstance(item.get("Revision"), str) else None
            size = item.get("Size")
            if isinstance(size, str) and size.isdigit():
                size = int(size)
            elif not isinstance(size, int):
                size = None
            result.append(
                ModelFile(
                    owner=owner,
                    name=name,
                    path=path,
                    revision=revision,
                    size=size,
                    source_url=item.get("DownloadUrl") if isinstance(item.get("DownloadUrl"), str) else None,
                )
            )
        return result

    async def fetch_model_file_content(self, file: ModelFile) -> Optional[Tuple[str, str]]:
        # [块] 拉取 README 文件内容；返回 (text, url)
        assert self._client is not None
        params = {"FilePath": file.path}
        if file.revision:
            params["Revision"] = file.revision
        resp = await self._request("GET", f"{self.endpoint}/api/v1/models/{file.owner}/{file.name}/repo", params=params)
        if resp.status_code != 200:
            return None
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        return resp.text, str(resp.url)

    async def fetch_model_detail(self, owner: str, name: str) -> Optional[Dict[str, object]]:
        assert self._client is not None
        resp = await self._request("GET", f"{self.endpoint}/api/v1/models/{owner}/{name}")
        if resp.status_code != 200:
            return None
        payload = resp.json()
        return payload.get("Data")

    async def _datasets_page(self, page_number: int) -> Dict[str, object]:
        assert self._client is not None
        params = {"PageNumber": page_number, "PageSize": self.dataset_page_size}
        logger.info("[modelscope_client] fetch datasets page=%s", page_number)
        resp = await self._request("GET", f"{self.endpoint}/api/v1/dolphin/datasets", params=params)
        resp.raise_for_status()
        return resp.json()

    async def iter_datasets(self, limit: Optional[int] = None) -> AsyncGenerator[Dict[str, object], None]:
        page = 1
        yielded = 0
        total = None
        while True:
            data = await self._datasets_page(page)
            datasets = data.get("Data") or []
            if total is None:
                total = data.get("TotalCount", len(datasets))
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            page += 1
            if limit is None and yielded >= total:
                break

    async def fetch_dataset_detail(self, owner: str, name: str) -> Optional[Dict[str, object]]:
        assert self._client is not None
        resp = await self._request("GET", f"{self.endpoint}/api/v1/datasets/{owner}/{name}")
        if resp.status_code != 200:
            return None
        payload = resp.json()
        return payload.get("Data")

    async def fetch_summary_fallback(self, repo_type: str, owner: str, name: str) -> Tuple[str, str]:
        assert self._client is not None
        segment = "models" if repo_type == "model" else "datasets"
        url = f"{self.endpoint}/{segment}/{owner}/{name}/summary"
        resp = await self._request("GET", url)
        resp.raise_for_status()
        return strip_html(resp.text), url

    async def _studios_page(
        self,
        page_number: int,
        *,
        page_size: int,
        criterion: Optional[list[dict[str, object]]] = None,
        sort_by: str = "Default",
    ) -> Dict[str, object]:
        assert self._client is not None
        payload = {
            "PageNumber": page_number,
            "PageSize": page_size,
            "SortBy": sort_by,
            "Criterion": criterion or [],
        }
        resp = await self._request(
            "PUT",
            f"{self.endpoint}/api/v1/dolphin/studios",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    async def fetch_studio_app(
        self,
        owner: str,
        name: str,
        *,
        file_path: str = "app.py",
        revision: str = "master",
    ) -> Optional[str]:
        assert self._client is not None
        url = f"{self.endpoint}/studio/{owner}/{name}/resolve/{revision}/{file_path}"
        resp = await self._request("GET", url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def now_utc() -> str:
        return datetime.now(timezone.utc).isoformat()


def is_textual_file(path: str, size: Optional[int]) -> bool:
    allowed_suffixes = {".md", ".markdown", ".rst", ".txt"}
    keywords = ("README", "CHANGELOG", "GUIDE", "TUTORIAL", "USAGE", "FAQ")
    upper = path.upper()
    if any(keyword in upper for keyword in keywords):
        return size is None or size <= 512 * 1024
    suffix = path.lower().rsplit(".", 1)
    if len(suffix) == 2 and f".{suffix[1]}" in allowed_suffixes:
        return size is None or size <= 512 * 1024
    return False
