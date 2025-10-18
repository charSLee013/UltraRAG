from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion_pipeline.sources.datasets_pipeline import ModelScopeDatasetsPipeline
from ingestion_pipeline.types import SourceLocator, SourceType, RawDocument
from ingestion_pipeline.embed.local import embed_local


def test_structure_aware_chunks_heading_split():
    p = ModelScopeDatasetsPipeline(existing_content_hashes=[])
    text = """# Title\npara1 line1\n\n## Sec\nline2\n\nline3"""
    chunks = p._structure_aware_chunks(text, max_size=32)
    # Expect at least two chunks due to heading boundary and small max_size
    assert len(chunks) >= 2
    assert chunks[0].startswith("# Title")


def test_process_with_local_embed():
    p = ModelScopeDatasetsPipeline(existing_content_hashes=[])
    locator = SourceLocator(source_type=SourceType.DATASETS, owner_repo="o/n", source_url="u")
    payload = {
        "owner": "o",
        "name": "n",
        "readme": {
            "text_clean": "# H\nA\nB\nC",
            "clean_state": "blanklines_stripped",
        },
    }
    raw = RawDocument(
        locator=locator,
        repo_id="datasets:o/n",
        payload=__import__("json").dumps(payload),
        fetched_at=datetime.now(timezone.utc),
        content_hash="datasets:o/n",
    )

    async def run():
        return await p.process(raw, chunk_max_size=64, embed=embed_local)

    records = asyncio.get_event_loop().run_until_complete(run())
    assert records, "should produce records"
    assert all(r.repo_id == raw.repo_id for r in records)
    assert all(r.content_hash == raw.content_hash for r in records)
