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

import ultrarag  # noqa: E402

if "ultrarag.server" not in sys.modules:
    stub_server_module = types.ModuleType("ultrarag.server")

    class _StubServer:  # noqa: D401 - simple stub
        def __init__(self, *_, **__):
            pass

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
    ultrarag.server = stub_server_module  # type: ignore[attr-defined]


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


def test_search_o1_check_stop_state():
    result = router_module.search_o1_check(["final answer <|im_end|>"])
    assert result["ans_ls"][0]["state"] == "stop"


def test_search_o1_check_retrieve_on_query_marker():
    result = router_module.search_o1_check(["need docs <|end_search_query|>"])
    assert result["ans_ls"][0]["state"] == "retrieve"


def test_search_o1_check_retrieve_by_default():
    result = router_module.search_o1_check(["thinking without markers"])
    assert result["ans_ls"][0]["state"] == "retrieve"
