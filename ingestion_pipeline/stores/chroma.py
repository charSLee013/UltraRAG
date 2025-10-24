from __future__ import annotations

import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv
import chromadb

from ..types import ChunkRecord


load_dotenv()


class ChromaStore:
    def __init__(self, path: str | None = None, collection: str | None = None) -> None:
        # Env-first, no silent fallback: require CHROMA_PATH when path is not provided
        self.path = path or os.environ.get("CHROMA_PATH")
        if not self.path:
            raise RuntimeError(
                "CHROMA_PATH is not set. Configure it in your .env to point to the active collection directory."
            )
        self.collection_name = collection or os.environ.get("CHROMA_COLLECTION", "modelscope_docs")
        Path(self.path).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.path)
        self.col = self.client.get_or_create_collection(self.collection_name)

    def delete_repo(self, repo_id: str) -> None:
        self.col.delete(where={"repo_id": repo_id})

    def upsert_records(self, records: List[ChunkRecord]) -> None:
        if not records:
            return
        ids = [r.chunk_uuid for r in records]
        docs = [r.text for r in records]
        metas = [
            {
                "repo_id": r.repo_id,
                "content_hash": r.content_hash,
                "chunk_index": r.chunk_index,
                "source_type": r.locator.source_type.value,
                "owner_repo": r.locator.owner_repo,
                "source_url": r.locator.source_url,
                "fetched_at": (r.fetched_at.isoformat() if getattr(r, "fetched_at", None) else None),
            }
            for r in records
        ]
        embs = [r.embedding for r in records]
        self.col.upsert(ids=ids, documents=docs, metadatas=metas, embeddings=embs)
