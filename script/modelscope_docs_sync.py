#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from modelscope_client import DEFAULT_ENDPOINT, ModelScopeClient

STATE_DIR = Path("output/modelscope_docs/.state")

DEFAULT_OUTPUT_DIR = Path("output/modelscope_docs")
ALLOWED_SUFFIXES = {
    ".md",
    ".markdown",
    ".rst",
    ".txt",
}
MAX_TEXT_BYTES = 512 * 1024
KEYWORDS = ("README", "CHANGELOG", "GUIDE", "TUTORIAL", "USAGE", "FAQ")


def is_textual(path: str, size: Optional[int]) -> bool:
    upper = path.upper()
    if any(keyword in upper for keyword in KEYWORDS):
        if size is None or size <= MAX_TEXT_BYTES:
            return True
    suffix = Path(path).suffix.lower()
    if suffix in ALLOWED_SUFFIXES:
        return size is None or size <= MAX_TEXT_BYTES
    return False


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def model_documents(client: ModelScopeClient,
                    owner: str,
                    name: str,
                    files: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    documents: List[Dict[str, object]] = []
    for item in files:
        path = item.get("Path")
        if not isinstance(path, str):
            continue
        size = item.get("Size")
        if isinstance(size, str) and size.isdigit():
            size = int(size)
        if isinstance(size, int) and size > MAX_TEXT_BYTES:
            continue
        if not is_textual(path, size if isinstance(size, int) else None):
            continue
        revision = item.get("Revision")
        response = client.fetch_model_file(owner, name, path, revision if isinstance(revision, str) else None)
        content = response.text
        documents.append(
            {
                "repo_id": f"{owner}/{name}",
                "repo_type": "model",
                "path": path,
                "fetched_at": ModelScopeClient.now_utc(),
                "endpoint": client.endpoint,
                "source_url": response.url,
                "size_bytes": len(content.encode("utf-8")),
                "sha256": sha256_text(content),
                "content": content,
            }
        )
    return documents


def dataset_documents(endpoint: str,
                      owner: str,
                      name: str,
                      detail: Dict[str, object]) -> List[Dict[str, object]]:
    readme = detail.get("ReadmeContent")
    if not isinstance(readme, str) or not readme.strip():
        return []
    content = readme.strip()
    return [
        {
            "repo_id": f"{owner}/{name}",
            "repo_type": "dataset",
            "path": "README.md",
            "fetched_at": ModelScopeClient.now_utc(),
            "endpoint": endpoint,
            "source_url": f"{endpoint}/datasets/{owner}/{name}",
            "size_bytes": len(content.encode("utf-8")),
            "sha256": sha256_text(content),
            "content": content,
        }
    ]


def load_previous_hashes(state_file: Path) -> Dict[str, str]:
    if not state_file.exists():
        return {}
    with state_file.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def save_hashes(state_file: Path, hashes: Dict[str, str]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("w", encoding="utf-8") as fp:
        json.dump(hashes, fp, ensure_ascii=False, indent=2)


def sync_documents(client: ModelScopeClient,
                   output_path: Path,
                   max_models: Optional[int],
                   max_datasets: Optional[int]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    state_file = STATE_DIR / "doc_hashes.json"
    previous_hashes = load_previous_hashes(state_file)
    current_hashes: Dict[str, str] = {}

    model_docs = 0
    dataset_docs = 0
    skipped = 0

    with output_path.open("w", encoding="utf-8") as fp:
        for model in client.iter_models(limit=max_models):
            owner = model.get("Path")
            name = model.get("Name")
            if not owner or not name:
                continue
            files = client.fetch_model_files(owner, name)
            docs = model_documents(client, owner, name, files)
            for doc in docs:
                key = f"{doc['repo_id']}|{doc['path']}"
                current_hashes[key] = doc["sha256"]
                if previous_hashes.get(key) == doc["sha256"]:
                    skipped += 1
                    continue
                fp.write(json.dumps(doc, ensure_ascii=False) + "\n")
                model_docs += 1

        for dataset in client.iter_datasets(limit=max_datasets):
            owner = dataset.get("Namespace") or dataset.get("Owner")
            name = dataset.get("Name")
            if not owner or not name:
                continue
            detail = client.fetch_dataset_detail(owner, name)
            docs = dataset_documents(client.endpoint, owner, name, detail)
            for doc in docs:
                key = f"{doc['repo_id']}|{doc['path']}"
                current_hashes[key] = doc["sha256"]
                if previous_hashes.get(key) == doc["sha256"]:
                    skipped += 1
                    continue
                fp.write(json.dumps(doc, ensure_ascii=False) + "\n")
                dataset_docs += 1

    save_hashes(state_file, current_hashes)
    print(f"Model documents captured: {model_docs}")
    print(f"Dataset documents captured: {dataset_docs}")
    print(f"Skipped unchanged: {skipped}")
    print(f"Saved documents to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect ModelScope documentation files")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--output", type=Path, default=None,
                        help="Output JSONL path; defaults to timestamped file under output/modelscope_docs/")
    parser.add_argument("--max-models", type=int, default=None)
    parser.add_argument("--max-datasets", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = ModelScopeClient(endpoint=args.endpoint)
    if args.output is None:
        output_dir = DEFAULT_OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = ModelScopeClient.now_utc().replace(":", "").replace("-", "")
        output_path = output_dir / f"modelscope_docs_{timestamp}.jsonl"
    else:
        output_path = args.output
    sync_documents(client, output_path, args.max_models, args.max_datasets)


if __name__ == "__main__":
    main()
