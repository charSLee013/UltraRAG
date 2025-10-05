"""Unit tests for custom.output_extract_from_boxed extraction variants."""

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


def test_output_extract_from_boxed_basic():
    result = custom_module.output_extract_from_boxed([r"\boxed{answer}"])
    assert result["pred_ls"][0] == "answer"


def test_output_extract_from_boxed_nested_braces():
    result = custom_module.output_extract_from_boxed([r"\boxed{outer {inner}}"])
    assert result["pred_ls"][0] == "outer {inner}"


def test_output_extract_from_boxed_math_wrappers():
    result = custom_module.output_extract_from_boxed([r"$\boxed{\( a + b \)}$"])
    assert result["pred_ls"][0] == "a + b"


def test_output_extract_from_boxed_backslash_cleanup():
    result = custom_module.output_extract_from_boxed([r"\boxed{line\break}"])
    assert result["pred_ls"][0] == "line break"


def test_output_extract_from_boxed_no_box():
    raw_text = "plain answer"
    result = custom_module.output_extract_from_boxed([raw_text])
    assert result["pred_ls"][0] == raw_text
