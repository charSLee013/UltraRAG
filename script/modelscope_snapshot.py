#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import requests

USER_AGENT = "UltraRAG-Community-Agent/0.1"
DEFAULT_ENDPOINT = "https://modelscope.cn"
DEFAULT_OUTPUT_DIR = Path("output/modelscope_snapshots")
MODEL_PAGE_SIZE = 200
DATASET_PAGE_SIZE = 100


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


@dataclass
class SnapshotRecord:
    repo_id: str
    repo_type: str
    fetched_at: str
    endpoint: str
    summary: Dict[str, object]
    metadata: Dict[str, object]
    readme_content: str
    readme_source: str
    readme_html_url: Optional[str]
    files: Optional[List[Dict[str, object]]] = None

    def serialize(self) -> str:
        payload = {
            "repo_id": self.repo_id,
            "repo_type": self.repo_type,
            "fetched_at": self.fetched_at,
            "endpoint": self.endpoint,
            "summary": self.summary,
            "metadata": self.metadata,
            "readme": {
                "content": self.readme_content,
                "source": self.readme_source,
                "html_url": self.readme_html_url,
            },
        }
        if self.files is not None:
            payload["files"] = self.files
        return json.dumps(payload, ensure_ascii=False)


class ModelScopeClient:
    def __init__(self, endpoint: str = DEFAULT_ENDPOINT) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # ----------------------- Models -----------------------
    def _models_page(self, page_number: int, page_size: int) -> Dict[str, object]:
        payload = json.dumps({
            "Path": "",
            "PageNumber": page_number,
            "PageSize": page_size,
        })
        url = f"{self.endpoint}/api/v1/models/"
        response = self.session.put(url, data=payload, timeout=30)
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
            page = self._models_page(page_number, MODEL_PAGE_SIZE)
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

    # ----------------------- Datasets -----------------------
    def _datasets_page(self, page_number: int, page_size: int) -> Dict[str, object]:
        params = {"PageNumber": page_number, "PageSize": page_size}
        url = f"{self.endpoint}/api/v1/dolphin/datasets"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def iter_datasets(self, limit: Optional[int] = None) -> Iterator[Dict[str, object]]:
        page_number = 1
        seen = 0
        total_count = None
        while True:
            page = self._datasets_page(page_number, DATASET_PAGE_SIZE)
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
    def summary_page(self, repo_id: str, repo_type: str) -> Tuple[str, str]:
        segment = "models" if repo_type == "model" else "datasets"
        url = f"{self.endpoint}/{segment}/{repo_id}/summary"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        text = strip_html(response.text)
        return text, url


def to_iso(value: Optional[int | str]) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, str) and value.isdigit():
        timestamp = int(value)
    elif isinstance(value, int):
        timestamp = value
    else:
        try:
            timestamp = int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return str(value)
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def build_model_summary(detail: Dict[str, object]) -> Dict[str, object]:
    return {
        "name": detail.get("Name"),
        "display_name": detail.get("ChineseName"),
        "description": detail.get("Description"),
        "downloads": detail.get("Downloads"),
        "stars": detail.get("Stars"),
        "tags": detail.get("Tags"),
        "tasks": detail.get("Tasks"),
        "frameworks": detail.get("Frameworks"),
        "languages": detail.get("Language"),
        "created_at": to_iso(detail.get("CreatedTime")),
        "updated_at": to_iso(detail.get("LastUpdatedTime")),
    }


def build_dataset_summary(detail: Dict[str, object]) -> Dict[str, object]:
    return {
        "name": detail.get("Name"),
        "display_name": detail.get("ChineseName"),
        "description": detail.get("Description"),
        "downloads": detail.get("Downloads"),
        "likes": detail.get("Likes"),
        "tags": detail.get("Tags"),
        "created_at": to_iso(detail.get("GmtCreate")),
        "updated_at": to_iso(detail.get("GmtModified") or detail.get("GmtCreate")),
    }


def prune_model_metadata(detail: Dict[str, object]) -> Dict[str, object]:
    metadata = dict(detail)
    metadata.pop("ReadMeContent", None)
    metadata.pop("ReadMeTips", None)
    metadata.pop("widgets", None)
    return metadata


def prune_dataset_metadata(detail: Dict[str, object]) -> Dict[str, object]:
    metadata = dict(detail)
    metadata.pop("ReadmeContent", None)
    return metadata


def serialise_files(items: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    files = []
    for item in items:
        files.append(
            {
                "path": item.get("Path"),
                "size": item.get("Size"),
                "sha256": item.get("Sha256"),
                "type": item.get("Type"),
                "committed_date": to_iso(item.get("CommittedDate")),
                "commit_message": item.get("CommitMessage"),
            }
        )
    return files


def snapshot(client: ModelScopeClient,
             output_path: Path,
             max_models: Optional[int],
             max_datasets: Optional[int]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    model_count = 0
    dataset_count = 0

    with output_path.open("w", encoding="utf-8") as fp:
        # Models
        for model in client.iter_models(limit=max_models):
            owner = model.get("Path")
            name = model.get("Name")
            if not owner or not name:
                continue
            detail = client.fetch_model_detail(owner, name)
            readme = detail.get("ReadMeContent") or ""
            readme_source = "api"
            readme_url = None
            if not readme.strip():
                readme, readme_url = client.summary_page(f"{owner}/{name}", "model")
                readme_source = "html"
            files = client.fetch_model_files(owner, name)
            record = SnapshotRecord(
                repo_id=f"{owner}/{name}",
                repo_type="model",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                endpoint=client.endpoint,
                summary=build_model_summary(detail),
                metadata=prune_model_metadata(detail),
                readme_content=readme,
                readme_source=readme_source,
                readme_html_url=readme_url,
                files=serialise_files(files),
            )
            fp.write(record.serialize() + "\n")
            model_count += 1

        # Datasets
        for dataset in client.iter_datasets(limit=max_datasets):
            owner = dataset.get("Namespace") or dataset.get("Owner")
            name = dataset.get("Name")
            if not owner or not name:
                continue
            detail = client.fetch_dataset_detail(owner, name)
            readme = detail.get("ReadmeContent") or ""
            readme_source = "api"
            readme_url = None
            if not readme.strip():
                readme, readme_url = client.summary_page(f"{owner}/{name}", "dataset")
                readme_source = "html"
            record = SnapshotRecord(
                repo_id=f"{owner}/{name}",
                repo_type="dataset",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                endpoint=client.endpoint,
                summary=build_dataset_summary(detail),
                metadata=prune_dataset_metadata(detail),
                readme_content=readme,
                readme_source=readme_source,
                readme_html_url=readme_url,
            )
            fp.write(record.serialize() + "\n")
            dataset_count += 1

    print(f"Models captured: {model_count}")
    print(f"Datasets captured: {dataset_count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot ModelScope models and datasets")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--output", type=Path, default=None,
                        help="Output file path. If omitted a timestamped file is created under output/modelscope_snapshots/")
    parser.add_argument("--max-models", type=int, default=None,
                        help="Optional limit for number of models (useful for smoke tests)")
    parser.add_argument("--max-datasets", type=int, default=None,
                        help="Optional limit for number of datasets")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = ModelScopeClient(endpoint=args.endpoint)
    if args.output is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = DEFAULT_OUTPUT_DIR
        output_path = output_dir / f"modelscope_snapshot_{timestamp}.jsonl"
    else:
        output_path = args.output
    snapshot(client, output_path, args.max_models, args.max_datasets)
    print(f"Saved snapshot to {output_path}")


if __name__ == "__main__":
    main()
