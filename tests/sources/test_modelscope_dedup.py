import asyncio
from datetime import datetime, timezone

import pytest

from ingestion_pipeline.sources.modelscope_models import ModelScopeModelsPipeline
from ingestion_pipeline.types import RawDocument, SourceLocator, SourceType


class _DummyPipeline(ModelScopeModelsPipeline):
    def __init__(self, existing):
        # page_size=2 to simulate multiple pages in the stub
        super().__init__(page_size=2, existing_content_hashes=existing)
        self._calls = {"list": 0, "build": 0}

    def _list_models_page_with_retry(self, page_number: int, page_size: int):
        # Three pages with duplicates across/within
        self._calls["list"] += 1
        if page_number == 1:
            return [
                ("alice", "m1", {}),
                ("alice", "m2", {}),
                ("alice", "m1", {}),  # dup within page
            ], 9999
        if page_number == 2:
            return [
                ("alice", "m2", {}),  # dup across pages
                ("bob", "m3", {}),
            ], 9999
        if page_number == 3:
            return [
                ("carol", "m4", {}),
            ], 9999
        return [], 9999

    async def _build_raw_document_async(self, *, owner, name, raw_item, locator, repo_id):
        # Always produce a RawDocument to isolate the "set-diff planning" behaviour
        self._calls["build"] += 1
        payload = {
            "owner": owner,
            "name": name,
            "repo_id": f"models:{owner}/{name}",
            "readme": {"text_clean": "stub"},
        }
        return RawDocument(
            locator=locator,
            repo_id=repo_id,
            payload=str(payload),
            fetched_at=datetime.now(timezone.utc),
            content_hash=repo_id,
        )


@pytest.mark.asyncio
async def test_set_diff_prefetch_and_fetch_only_new():
    # Pretend models:alice/m1 is already in SQLite (existing)
    existing = {"models:alice/m1"}
    p = _DummyPipeline(existing)

    # Planned targets should be unique and exclude existing: {alice/m2, bob/m3, carol/m4}
    # Enable prefetch mode for exact planning total in test
    p.prefetch_planning = True
    total = p.estimate_total_models()
    assert total == 3

    got = []
    async for raw in p.fetch():
        got.append(raw.repo_id)
    assert got == [
        "models:alice/m2",
        "models:bob/m3",
        "models:carol/m4",
    ]
