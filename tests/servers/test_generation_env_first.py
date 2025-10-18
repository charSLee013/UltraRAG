"""Env-first behaviour tests for generation server."""

from __future__ import annotations

import asyncio
import os
import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from fastmcp.exceptions import ToolError
from servers.generation.src import generation


class _DummyCompletions:
    def __init__(self, recorder: dict[str, str]):
        self._recorder = recorder

    async def create(self, *, model: str, messages, **kwargs):
        self._recorder["model"] = model
        self._recorder["messages"] = messages
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="ok"))]
        )


class _DummyClient:
    def __init__(self, *, base_url: str, api_key: str):
        self.recorder = {"base_url": base_url, "api_key": api_key}
        self.chat = types.SimpleNamespace(completions=_DummyCompletions(self.recorder))


def _run(coro):
    return asyncio.run(coro)


def _generate_call(**kwargs):
    target = getattr(generation.generate, "fn", generation.generate)
    return target(**kwargs)


def test_env_overrides_parameter(monkeypatch):
    monkeypatch.setenv("LLM_BASE_URL", "https://remote.example.com/v1")
    monkeypatch.setenv("LLM_MODEL", "EnvModel")
    monkeypatch.setenv("LLM_API_KEY", "env-key")
    holder: dict[str, dict[str, str]] = {}

    def _factory(*, base_url: str, api_key: str):
        client = _DummyClient(base_url=base_url, api_key=api_key)
        holder["recorder"] = client.recorder
        return client

    monkeypatch.setattr(generation, "AsyncOpenAI", _factory)
    monkeypatch.setattr(
        generation.app,
        "logger",
        types.SimpleNamespace(info=lambda *args, **kwargs: None),
        raising=False,
    )

    result = _run(
        _generate_call(
            prompt_ls=["hello"],
            model_name="ParamModel",
            base_url="http://localhost:8000/v1",
            sampling_params={},
            api_key="",
        )
    )

    assert result["ans_ls"] == ["ok"]
    recorder = holder["recorder"]
    assert recorder["base_url"] == "https://remote.example.com/v1"
    assert recorder["api_key"] == "env-key"
    assert recorder["model"] == "EnvModel"


def test_parameter_used_when_env_missing(monkeypatch):
    for key in ["LLM_BASE_URL", "OPENAI_BASE_URL", "BASE_URL", "LLM_MODEL", "LLM_MODEL_NAME", "MODEL_NAME", "LLM_API_KEY"]:
        monkeypatch.delenv(key, raising=False)

    holder: dict[str, dict[str, str]] = {}

    def _factory(*, base_url: str, api_key: str):
        client = _DummyClient(base_url=base_url, api_key=api_key)
        holder["recorder"] = client.recorder
        return client

    monkeypatch.setattr(generation, "AsyncOpenAI", _factory)
    monkeypatch.setattr(
        generation.app,
        "logger",
        types.SimpleNamespace(info=lambda *args, **kwargs: None),
        raising=False,
    )

    result = _run(
        _generate_call(
            prompt_ls=["hi"],
            model_name="ParamModel",
            base_url="http://param-base",
            sampling_params={},
            api_key="PARAM_KEY",
        )
    )

    assert result["ans_ls"] == ["ok"]
    recorder = holder["recorder"]
    assert recorder["base_url"] == "http://param-base"
    assert recorder["api_key"] == "PARAM_KEY"
    assert recorder["model"] == "ParamModel"


def test_missing_env_and_parameter_fail_fast(monkeypatch):
    for key in ["LLM_BASE_URL", "OPENAI_BASE_URL", "BASE_URL", "LLM_MODEL", "LLM_MODEL_NAME", "MODEL_NAME", "LLM_API_KEY"]:
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setattr(generation, "AsyncOpenAI", lambda **_: None)
    monkeypatch.setattr(
        generation.app,
        "logger",
        types.SimpleNamespace(info=lambda *args, **kwargs: None),
        raising=False,
    )

    with pytest.raises(ToolError, match="LLM base URL not provided"):
        _run(
            _generate_call(
                prompt_ls=["hi"],
                model_name=None,
                base_url=None,
                sampling_params={},
                api_key="",
            )
        )

    monkeypatch.setenv("LLM_BASE_URL", "https://remote")

    with pytest.raises(ToolError, match="LLM model name not provided"):
        _run(
            _generate_call(
                prompt_ls=["hi"],
                model_name=None,
                base_url=None,
                sampling_params={},
                api_key="",
            )
        )
