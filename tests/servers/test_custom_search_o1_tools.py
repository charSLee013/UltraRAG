"""Unit tests for custom Search-o1 utilities (query extract & passthrough)."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

if "fastmcp" not in sys.modules:
    fastmcp_stub = types.ModuleType("fastmcp")

    class _StubClient:  # pragma: no cover - placeholder
        def __init__(self, *_, **__):
            pass

    fastmcp_stub.Client = _StubClient
    fastmcp_stub.server = types.SimpleNamespace()
    sys.modules["fastmcp"] = fastmcp_stub

if "fastmcp.server" not in sys.modules:
    stub_server_pkg = types.ModuleType("fastmcp.server")
    sys.modules["fastmcp.server"] = stub_server_pkg

if "fastmcp.server.server" not in sys.modules:
    stub_server_mod = types.ModuleType("fastmcp.server.server")

    class _StubFastMCP:  # pragma: no cover - placeholder
        def __init__(self, *_, **__):
            pass

    stub_server_mod.FastMCP = _StubFastMCP
    sys.modules["fastmcp.server.server"] = stub_server_mod

if "fastmcp.prompts" not in sys.modules:
    stub_prompts = types.ModuleType("fastmcp.prompts")

    class _StubPrompt:  # pragma: no cover - placeholder
        def __init__(self, *_, **__):
            pass

    class _StubPromptManager:  # pragma: no cover - placeholder
        def __init__(self, *_, **__):
            pass

    stub_prompts.Prompt = _StubPrompt
    stub_prompts.PromptManager = _StubPromptManager
    sys.modules["fastmcp.prompts"] = stub_prompts

ultrarag_module = sys.modules.setdefault("ultrarag", types.ModuleType("ultrarag"))

if "ultrarag.server" not in sys.modules:
    stub_server_module = types.ModuleType("ultrarag.server")

    class _StubServer:  # noqa: D401 - simple stub
        def __init__(self, *_, **__):
            self.logger = types.SimpleNamespace(
                info=lambda *a, **k: None,
                debug=lambda *a, **k: None,
                warning=lambda *a, **k: None,
                error=lambda *a, **k: None,
            )

        def tool(self, *_, **__):
            def decorator(fn):
                return fn

            return decorator

        def prompt(self, *_, **__):  # pragma: no cover - unused but keeps parity
            def decorator(fn):
                return fn

            return decorator

        def run(self, *_, **__):  # pragma: no cover - not used in tests
            return None

    stub_server_module.UltraRAG_MCP_Server = _StubServer
    sys.modules["ultrarag.server"] = stub_server_module
    ultrarag_module.server = stub_server_module  # type: ignore[attr-defined]


def _load_custom_module():
    module_path = Path(__file__).resolve().parent.parent.parent / "servers" / "custom" / "src" / "custom.py"
    spec = importlib.util.spec_from_file_location("custom_module", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load custom module")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("custom_module", module)
    spec.loader.exec_module(module)
    return module


custom_module = _load_custom_module()
custom_module.app.logger = types.SimpleNamespace(  # type: ignore[attr-defined]
    info=lambda *a, **k: None,
    debug=lambda *a, **k: None,
    warning=lambda *a, **k: None,
    error=lambda *a, **k: None,
)


TOKEN_CONTRACT = {
    "begin_query": "<<SRCH_Q_BEGIN>>",
    "end_query": "<<SRCH_Q_END>>",
    "end_answer": "<<FINAL_ANSWER_END>>",
    "begin_result": "<<SRCH_R_BEGIN>>",
    "end_result": "<<SRCH_R_END>>",
}


def test_search_o1_query_extract_with_markers():
    reasoning = [
        "first step <<SRCH_Q_BEGIN>>who built python<<SRCH_Q_END>>",
        "second <<SRCH_Q_BEGIN>>history of python<<SRCH_Q_END>>",
    ]
    result = custom_module.search_o1_query_extract(reasoning, TOKEN_CONTRACT)
    assert result["extract_query_list"] == [
        "who built python",
        "history of python",
    ]


def test_search_o1_query_extract_no_marker_returns_empty():
    reasoning = ["I have enough information already."]
    result = custom_module.search_o1_query_extract(reasoning, TOKEN_CONTRACT)
    assert result["extract_query_list"] == [""]


def test_output_passthrough_strips_contract_tokens():
    answers = [
        "- bullet one\n- bullet two<<FINAL_ANSWER_END>>",
        "summary<<SRCH_Q_END>><<FINAL_ANSWER_END>>",
    ]
    result = custom_module.output_passthrough(answers, TOKEN_CONTRACT)
    assert result["markdown_ls"] == [
        "- bullet one\n- bullet two",
        "summary",
    ]


def test_search_o1_query_extract_debug_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple, dict]] = []
    original_logger = custom_module.app.logger
    custom_module.app.logger = types.SimpleNamespace(
        debug=lambda *args, **kwargs: calls.append((args, kwargs)),
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    monkeypatch.setenv("SEARCH_O1_DEBUG", "1")
    try:
        reasoning = ["<<SRCH_Q_BEGIN>>foo<<SRCH_Q_END>>"]
        custom_module.search_o1_query_extract(reasoning, TOKEN_CONTRACT)
    finally:
        custom_module.app.logger = original_logger
        monkeypatch.delenv("SEARCH_O1_DEBUG", raising=False)

    assert calls, "expected debug log when SEARCH_O1_DEBUG enabled"
