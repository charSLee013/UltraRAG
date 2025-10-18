"""Unit tests for router.search_o1_check stop/retrieve handling."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

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

    class _StubPrompt:  # minimal placeholder
        def __init__(self, *_, **__):
            pass

    class _StubPromptManager:  # pragma: no cover - harmless
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


def _load_router_module():
    module_path = Path(__file__).resolve().parent.parent.parent / "servers" / "router" / "src" / "router.py"
    spec = importlib.util.spec_from_file_location("router_module", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load router module")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("router_module", module)
    spec.loader.exec_module(module)
    return module


router_module = _load_router_module()
router_module.app.logger = types.SimpleNamespace(  # type: ignore[attr-defined]
    info=lambda *a, **k: None,
    debug=lambda *a, **k: None,
    warning=lambda *a, **k: None,
    error=lambda *a, **k: None,
)


TOKEN_CONTRACT = {
    "begin_query": "<<SRCH_Q_BEGIN>>",
    "end_query": "<<SRCH_Q_END>>",
    "end_answer": "<<FINAL_ANSWER_END>>",
}


def test_search_o1_check_stop_state():
    result = router_module.search_o1_check(
        ["final answer <<FINAL_ANSWER_END>>"], TOKEN_CONTRACT
    )
    assert result["ans_ls"][0]["state"] == "stop"


def test_search_o1_check_retrieve_on_query_marker():
    result = router_module.search_o1_check(
        ["need docs <<SRCH_Q_END>>"], TOKEN_CONTRACT
    )
    assert result["ans_ls"][0]["state"] == "retrieve"


def test_search_o1_check_retrieve_by_default():
    result = router_module.search_o1_check(
        ["thinking without markers"], TOKEN_CONTRACT
    )
    assert result["ans_ls"][0]["state"] == "retrieve"
