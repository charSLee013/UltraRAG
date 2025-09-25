#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from modelscope_client import DEFAULT_ENDPOINT, ModelScopeClient

DEFAULT_OUTPUT_DIR = Path("output/modelscope_snapshots")


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

    def serialise(self) -> str:
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
    result: List[Dict[str, object]] = []
    for item in items:
        result.append(
            {
                "path": item.get("Path"),
                "size": item.get("Size"),
                "sha256": item.get("Sha256"),
                "type": item.get("Type"),
                "committed_date": to_iso(item.get("CommittedDate")),
                "commit_message": item.get("CommitMessage"),
            }
        )
    return result


def snapshot(client: ModelScopeClient,
             output_path: Path,
             max_models: Optional[int],
             max_datasets: Optional[int]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    model_count = 0
    dataset_count = 0

    with output_path.open("w", encoding="utf-8") as fp:
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
                fallback = client.summary_page(f"{owner}/{name}", "model")
                readme = fallback["content"]
                readme_source = "html"
                readme_url = fallback["url"]
            files = client.fetch_model_files(owner, name)
            record = SnapshotRecord(
                repo_id=f"{owner}/{name}",
                repo_type="model",
                fetched_at=ModelScopeClient.now_utc(),
                endpoint=client.endpoint,
                summary=build_model_summary(detail),
                metadata=prune_model_metadata(detail),
                readme_content=readme,
                readme_source=readme_source,
                readme_html_url=readme_url,
                files=serialise_files(files),
            )
            fp.write(record.serialise() + "\n")
            model_count += 1

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
                fallback = client.summary_page(f"{owner}/{name}", "dataset")
                readme = fallback["content"]
                readme_source = "html"
                readme_url = fallback["url"]
            record = SnapshotRecord(
                repo_id=f"{owner}/{name}",
                repo_type="dataset",
                fetched_at=ModelScopeClient.now_utc(),
                endpoint=client.endpoint,
                summary=build_dataset_summary(detail),
                metadata=prune_dataset_metadata(detail),
                readme_content=readme,
                readme_source=readme_source,
                readme_html_url=readme_url,
            )
            fp.write(record.serialise() + "\n")
            dataset_count += 1

    print(f"Models captured: {model_count}")
    print(f"Datasets captured: {dataset_count}")
    print(f"Saved snapshot to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Snapshot ModelScope metadata")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--output", type=Path, default=None,
                        help="Output file path; defaults to timestamped JSONL under output/modelscope_snapshots/")
    parser.add_argument("--max-models", type=int, default=None)
    parser.add_argument("--max-datasets", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = ModelScopeClient(endpoint=args.endpoint)
    if args.output is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_path = DEFAULT_OUTPUT_DIR / f"modelscope_snapshot_{timestamp}.jsonl"
    else:
        output_path = args.output
    snapshot(client, output_path, args.max_models, args.max_datasets)


if __name__ == "__main__":
    main()
