from __future__ import annotations

import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Dict, Iterable, Iterator, List, Optional

import requests

USER_AGENT = "UltraRAG-Community-Agent/0.1"
DEFAULT_ENDPOINT = "https://modelscope.cn"


class HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._fragments: List[str] = []

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self._fragments.append(text)

    def text(self) -> str:
        return "\n".join(self._fragments)


def strip_html(html: str) -> str:
    parser = HTMLStripper()
    parser.feed(html)
    return parser.text()


class ModelScopeClient:
    def __init__(self,
                 endpoint: str = DEFAULT_ENDPOINT,
                 *,
                 model_page_size: int = 200,
                 dataset_page_size: int = 100) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.model_page_size = model_page_size
        self.dataset_page_size = dataset_page_size

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # ----------------------- Models -----------------------
    def _models_page(self, page_number: int) -> Dict[str, object]:
        payload = {
            "Path": "",
            "PageNumber": page_number,
            "PageSize": self.model_page_size,
        }
        url = f"{self.endpoint}/api/v1/models/"
        response = self.session.put(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json().get("Data")
        if not data:
            return {"Models": [], "TotalCount": 0}
        return data

    def iter_models(self, limit: Optional[int] = None) -> Iterator[Dict[str, object]]:
        page_number = 1
        seen = 0
        total_count = None
        while True:
            page = self._models_page(page_number)
            models = page.get("Models") or []
            if total_count is None:
                total_count = page.get("TotalCount", len(models))
            if not models:
                break
            for model in models:
                yield model
                seen += 1
                if limit is not None and seen >= limit:
                    return
            if limit is None and seen >= total_count:
                break
            page_number += 1

    def fetch_model_detail(self, owner: str, name: str) -> Dict[str, object]:
        url = f"{self.endpoint}/api/v1/models/{owner}/{name}"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("Data")
        if not data:
            raise RuntimeError(f"Empty model detail for {owner}/{name}")
        return data

    def fetch_model_files(self, owner: str, name: str) -> List[Dict[str, object]]:
        url = f"{self.endpoint}/api/v1/models/{owner}/{name}/repo/files"
        response = self.session.get(url, params={"Recursive": "true"}, timeout=15)
        if response.status_code != 200:
            return []
        payload = response.json()
        data = payload.get("Data") or {}
        return data.get("Files", [])

    def fetch_model_file(self,
                         owner: str,
                         name: str,
                         path: str,
                         revision: Optional[str] = None) -> requests.Response:
        params = {"FilePath": path}
        if revision:
            params["Revision"] = revision
        url = f"{self.endpoint}/api/v1/models/{owner}/{name}/repo"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response

    # ----------------------- Datasets -----------------------
    def _datasets_page(self, page_number: int) -> Dict[str, object]:
        params = {
            "PageNumber": page_number,
            "PageSize": self.dataset_page_size,
        }
        url = f"{self.endpoint}/api/v1/dolphin/datasets"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def iter_datasets(self, limit: Optional[int] = None) -> Iterator[Dict[str, object]]:
        page_number = 1
        seen = 0
        total_count = None
        while True:
            page = self._datasets_page(page_number)
            datasets = page.get("Data") or []
            if total_count is None:
                total_count = page.get("TotalCount", len(datasets))
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
                seen += 1
                if limit is not None and seen >= limit:
                    return
            if limit is None and seen >= total_count:
                break
            page_number += 1

    def fetch_dataset_detail(self, owner: str, name: str) -> Dict[str, object]:
        url = f"{self.endpoint}/api/v1/datasets/{owner}/{name}"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("Data")
        if not data:
            raise RuntimeError(f"Empty dataset detail for {owner}/{name}")
        return data

    # ----------------------- Utilities -----------------------
    def summary_page(self, repo_id: str, repo_type: str) -> Dict[str, str]:
        segment = "models" if repo_type == "model" else "datasets"
        url = f"{self.endpoint}/{segment}/{repo_id}/summary"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        text = strip_html(response.text)
        return {"content": text, "url": url}

    @staticmethod
    def header_request_id() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def now_utc() -> str:
        return datetime.now(timezone.utc).isoformat()
