"""Unit tests for Search-o1 prompt helpers (templates & inserter)."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# Stub dependencies expected by prompt module when running outside MCP runtime.
if "ultrarag.server" not in sys.modules:
    stub_server_module = types.ModuleType("ultrarag.server")

    class _StubServer:  # noqa: D401 - minimal decorator stub
        def __init__(self, *_, **__):
            pass

        def tool(self, *_, **__):  # pragma: no cover - unused in this test
            def decorator(fn):
                return fn

            return decorator

        def prompt(self, *_, **__):
            def decorator(fn):
                return fn

            return decorator

        def run(self, *_, **__):  # pragma: no cover - not used in tests
            return None

    stub_server_module.UltraRAG_MCP_Server = _StubServer
    sys.modules["ultrarag.server"] = stub_server_module

if "fastmcp.prompts" not in sys.modules:
    stub_prompts = types.ModuleType("fastmcp.prompts")

    class PromptMessage:  # minimal structure used by search_o1 prompts
        def __init__(self, content: str | types.SimpleNamespace):
            if isinstance(content, str):
                self.content = types.SimpleNamespace(text=content)
            else:
                self.content = content

    stub_prompts.PromptMessage = PromptMessage
    sys.modules["fastmcp.prompts"] = stub_prompts


def _load_prompt_module():
    module_path = (
        Path(__file__).resolve().parent.parent.parent
        / "servers"
        / "prompt"
        / "src"
        / "prompt.py"
    )
    spec = importlib.util.spec_from_file_location("prompt_module", module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load prompt module")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("prompt_module", module)
    spec.loader.exec_module(module)
    return module


prompt_module = _load_prompt_module()

TOKEN_CONTRACT = {
    "begin_query": "<<SRCH_Q_BEGIN>>",
    "end_query": "<<SRCH_Q_END>>",
    "begin_result": "<<SRCH_R_BEGIN>>",
    "end_result": "<<SRCH_R_END>>",
    "end_answer": "<<FINAL_ANSWER_END>>",
}


def test_search_o1_init_renders_contract_tokens(tmp_path):
    template_path = tmp_path / "init.jinja"
    template_path.write_text(
        "{{ token_contract.begin_query }}{{ question }}{{ token_contract.end_query }}"
    )
    rendered = prompt_module.search_o1_init(
        ["hello"],
        template_path,
        token_contract=TOKEN_CONTRACT,
    )
    assert rendered == ["<<SRCH_Q_BEGIN>>hello<<SRCH_Q_END>>"]


def test_searcho1_reasoning_indocument_passes_contract(tmp_path):
    template_path = tmp_path / "reasoning.jinja"
    template_path.write_text(
        "{{ token_contract.begin_result }}\n{{ prev_reasoning }}\n{{ token_contract.end_result }}"
    )
    prompt_obj = types.SimpleNamespace(content=types.SimpleNamespace(text="state"))
    rendered = prompt_module.searcho1_reasoning_indocument(
        [prompt_obj],
        ["query"],
        [["doc1", "doc2"]],
        template_path,
        token_contract=TOKEN_CONTRACT,
    )
    assert "<<SRCH_R_BEGIN>>" in rendered[0]
    assert "<<SRCH_R_END>>" in rendered[0]


def test_search_o1_insert_respects_token_contract():
    prompt_obj = types.SimpleNamespace(content=types.SimpleNamespace(text="context "))
    merged = prompt_module.search_o1_insert(
        [prompt_obj],
        ["retrieved"],
        token_contract=TOKEN_CONTRACT,
    )
    assert merged == ["context <<SRCH_R_BEGIN>>retrieved<<SRCH_R_END>>"]


def test_search_o1_insert_without_contract():
    prompt_obj = types.SimpleNamespace(content=types.SimpleNamespace(text="context "))
    merged = prompt_module.search_o1_insert([prompt_obj], ["retrieved"], token_contract=None)
    assert merged == ["context retrieved"]


def test_search_o1_finalize_appends_end_token(tmp_path):
    template_path = tmp_path / "final.jinja"
    template_path.write_text("{{ conversation }}{{ token_contract.end_answer }}")
    prompt_obj = types.SimpleNamespace(content=types.SimpleNamespace(text="answer"))
    rendered = prompt_module.search_o1_finalize(
        [prompt_obj],
        template_path,
        token_contract=TOKEN_CONTRACT,
    )
    assert rendered == ["answer<<FINAL_ANSWER_END>>"]


def test_search_o1_parameter_uses_sentinel_tokens():
    param_path = PROJECT_ROOT / "pipelines" / "search_o1" / "parameter" / "run_parameter.yaml"
    cfg = yaml.safe_load(param_path.read_text())

    for section in ("prompt", "router", "custom"):
        contract = cfg[section]["token_contract"]
        assert contract["begin_query"] == "<<SRCH_Q_BEGIN>>"
        assert contract["end_query"] == "<<SRCH_Q_END>>"
        assert contract["end_answer"] == "<<FINAL_ANSWER_END>>"

    extra_body = cfg["generation"]["sampling_params"].get("extra_body", {})
    assert extra_body.get("stop", []) == []
