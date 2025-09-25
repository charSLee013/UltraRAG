from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
import asyncio
from html.parser import HTMLParser
from typing import AsyncGenerator, Dict, Iterable, List, Optional, Tuple

import httpx

USER_AGENT = "UltraRAG-Community-Agent/0.1"
DEFAULT_ENDPOINT = "https://modelscope.cn"


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
    def __init__(self,
                 endpoint: str = DEFAULT_ENDPOINT,
                 *,
                 model_page_size: int = 200,
                 dataset_page_size: int = 100,
                 timeout: int = 30,
                 max_retries: int = 4,
                 initial_backoff: float = 1.0) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.model_page_size = model_page_size
        self.dataset_page_size = dataset_page_size
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff

    async def __aenter__(self) -> "ModelScopeClient":
        headers = {"User-Agent": USER_AGENT}
        self._client = httpx.AsyncClient(timeout=self.timeout, headers=headers)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
        self._client = None

    # ---------------------------- models ----------------------------
    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        assert self._client is not None
        backoff = self.initial_backoff
        for attempt in range(self.max_retries):
            try:
                resp = await self._client.request(method, url, **kwargs)
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
        payload = {
            "Path": "",
            "PageNumber": page_number,
            "PageSize": self.model_page_size,
        }
        resp = await self._request("PUT", f"{self.endpoint}/api/v1/models", json=payload)
        resp.raise_for_status()
        data = resp.json().get("Data")
        if not data:
            return {"Models": [], "TotalCount": 0}
        return data

    async def iter_models(self, limit: Optional[int] = None) -> AsyncGenerator[Dict[str, object], None]:
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
            result.append(ModelFile(
                owner=owner,
                name=name,
                path=path,
                revision=revision,
                size=size,
                source_url=item.get("DownloadUrl") if isinstance(item.get("DownloadUrl"), str) else None,
            ))
        return result

    async def fetch_model_file_content(self, file: ModelFile) -> Optional[Tuple[str, str]]:
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

    # ---------------------------- datasets ----------------------------
    async def _datasets_page(self, page_number: int) -> Dict[str, object]:
        assert self._client is not None
        params = {
            "PageNumber": page_number,
            "PageSize": self.dataset_page_size,
        }
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
        data = payload.get("Data")
        return data

    async def fetch_summary_fallback(self, repo_type: str, owner: str, name: str) -> Tuple[str, str]:
        assert self._client is not None
        segment = "models" if repo_type == "model" else "datasets"
        url = f"{self.endpoint}/{segment}/{owner}/{name}/summary"
        resp = await self._request("GET", url)
        resp.raise_for_status()
        text = strip_html(resp.text)
        return text, url

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
