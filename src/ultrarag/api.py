"""Public API surface for the Search-o1 answering loop."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from dotenv import load_dotenv
import yaml

from .client import Configuration, run


logger = logging.getLogger(__name__)


class SearchO1Pipeline:
    """High-level helper to execute the Search-o1 pipeline via Python."""

    def __init__(self, run_config: str | Path = "pipelines/search_o1/run.yaml") -> None:
        self.config_path = Path(run_config)
        if not self.config_path.is_file():
            raise FileNotFoundError(f"Search-o1 config not found: {self.config_path}")

        self.configuration = Configuration()
        self.parameters = self._load_parameters()

    def _load_parameters(self) -> Dict[str, Any]:
        param_path = (
            self.config_path.parent
            / "parameter"
            / f"{self.config_path.stem}_parameter.yaml"
        )
        params = self.configuration.load_parameter_config(param_path) or {}

        template_plan_path = self.config_path.parent / "template_plan.yaml"
        if template_plan_path.is_file():
            template_plan = yaml.safe_load(template_plan_path.read_text()) or {}
            if template_plan:
                params.setdefault("template_plan", template_plan)

        return params

    @staticmethod
    def _sanitise_base_url(url: str | None) -> str:
        if not url:
            return ""
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return url

    def _resolve_generation_settings(self) -> Dict[str, str | None]:
        generation_cfg = self.parameters.get("generation", {}) or {}
        env = os.environ
        base_url = (
            env.get("LLM_BASE_URL")
            or env.get("OPENAI_BASE_URL")
            or env.get("BASE_URL")
            or generation_cfg.get("base_url")
        )
        model_name = (
            env.get("LLM_MODEL_NAME")
            or env.get("MODEL_NAME")
            or env.get("LLM_MODEL")
            or generation_cfg.get("model_name")
        )
        api_key = (
            env.get("LLM_API_KEY")
            or env.get("OPENAI_API_KEY")
            or env.get("API_KEY")
            or generation_cfg.get("api_key")
        )
        return {
            "base_url": base_url,
            "model_name": model_name,
            "api_key": api_key,
        }

    def _ensure_generation_prereqs(self) -> Dict[str, str | None]:
        settings = self._resolve_generation_settings()
        missing = [
            key
            for key in ("base_url", "model_name")
            if not (settings.get(key) or "")
        ]
        if missing:
            missing_str = ", ".join(missing)
            raise RuntimeError(
                "Missing generation settings: "
                f"{missing_str}. Set the appropriate environment variables ("
                "LLM_BASE_URL, LLM_MODEL_NAME) or provide overrides in "
                "pipelines/search_o1/parameter/run_parameter.yaml."
            )
        return settings

    def _prepare_environment(self) -> Dict[str, str | None]:
        load_dotenv()
        return self._ensure_generation_prereqs()

    async def _run_async(self, question: str) -> Dict[str, Any]:
        seed_vars: Dict[str, Any] = {"q_ls": [question]}
        result = await run(str(self.config_path), seed_vars=seed_vars)
        if isinstance(result, dict):
            return result
        return {}

    def query(self, question: str) -> Dict[str, str]:
        """Synchronously execute the Search-o1 loop for a single question."""

        settings = self._prepare_environment()
        start = time.perf_counter()
        response: Dict[str, Any] = asyncio.run(self._run_async(question))
        duration = time.perf_counter() - start
        logger.info(
            "[SearchO1Pipeline] completed query in %.2fs (model=%s, base_url=%s)",
            duration,
            settings.get("model_name") or "<unset>",
            self._sanitise_base_url(settings.get("base_url")),
        )
        markdown_ls: List[str] = response.get("markdown_ls", []) if isinstance(response, dict) else []
        text = markdown_ls[0] if markdown_ls else ""
        return {"format": "markdown", "text": text}

    async def aquery(self, question: str) -> Dict[str, str]:
        """Async variant of :meth:`query`."""

        settings = self._prepare_environment()
        start = time.perf_counter()
        response = await self._run_async(question)
        duration = time.perf_counter() - start
        logger.info(
            "[SearchO1Pipeline] completed async query in %.2fs (model=%s, base_url=%s)",
            duration,
            settings.get("model_name") or "<unset>",
            self._sanitise_base_url(settings.get("base_url")),
        )
        markdown_ls: List[str] = response.get("markdown_ls", []) if isinstance(response, dict) else []
        text = markdown_ls[0] if markdown_ls else ""
        return {"format": "markdown", "text": text}
