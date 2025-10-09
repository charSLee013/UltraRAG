"""Unit tests for the high-level SearchO1Pipeline API."""

from __future__ import annotations

import sys
from pathlib import Path
import types

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
import pytest

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

if "fastmcp" not in sys.modules:
    stub_fastmcp = types.ModuleType("fastmcp")

    class _StubClient:  # pragma: no cover - networking not required for tests
        def __init__(self, *_, **__):
            pass

    stub_fastmcp.Client = _StubClient
    sys.modules["fastmcp"] = stub_fastmcp

from ultrarag.api import SearchO1Pipeline


def _clear_generation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "LLM_BASE_URL",
        "OPENAI_BASE_URL",
        "BASE_URL",
        "LLM_MODEL_NAME",
        "MODEL_NAME",
        "LLM_MODEL",
        "LLM_API_KEY",
        "OPENAI_API_KEY",
        "API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)


def test_query_fail_without_generation_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_generation_env(monkeypatch)
    monkeypatch.setattr("ultrarag.client.load_dotenv", lambda *_, **__: False)
    monkeypatch.setattr("ultrarag.api.load_dotenv", lambda *_, **__: False)
    pipeline = SearchO1Pipeline()

    async def fake_run(self, question: str):  # pragma: no cover - replaced in tests
        return {"markdown_ls": ["unused"]}

    monkeypatch.setattr(SearchO1Pipeline, "_run_async", fake_run)

    with pytest.raises(RuntimeError):
        pipeline.query("hello")


def test_query_uses_env_when_present(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_generation_env(monkeypatch)
    monkeypatch.setattr("ultrarag.client.load_dotenv", lambda *_, **__: False)
    monkeypatch.setattr("ultrarag.api.load_dotenv", lambda *_, **__: False)
    monkeypatch.setenv("LLM_BASE_URL", "https://api.example.com/v1")
    monkeypatch.setenv("LLM_MODEL_NAME", "example-model")
    monkeypatch.setenv("LLM_API_KEY", "secret")

    pipeline = SearchO1Pipeline()

    async def fake_run(self, question: str):
        assert question == "who built python"
        return {"markdown_ls": ["answer"]}

    monkeypatch.setattr(SearchO1Pipeline, "_run_async", fake_run)

    result = pipeline.query("who built python")
    assert result == {"format": "markdown", "text": "answer"}
