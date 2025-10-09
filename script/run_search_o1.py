#!/usr/bin/env python
"""Minimal Search‑o1 runner based on the public Python API.

Usage:
  python script/run_search_o1.py --question "请列出 Qwen 系列公开发布的模型"
  python script/run_search_o1.py --config pipelines/search_o1/run.yaml \
      --question "who built python?"

Notes:
  - This script intentionally avoids legacy client build/run paths.
  - Environment must provide generation + retriever settings (env‑first).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# Prefer local venv if present (quality‑of‑life only; not required)
venv_bin = PROJECT_ROOT / ".venv" / "bin"
if venv_bin.exists():
    os.environ["PATH"] = f"{venv_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    os.environ.setdefault("VIRTUAL_ENV", str(PROJECT_ROOT / ".venv"))

from ultrarag.mcp_logging import get_logger  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Search‑o1 via Python API")
    parser.add_argument(
        "--config",
        default="pipelines/search_o1/run.yaml",
        help="path to Search‑o1 run.yaml (default: %(default)s)",
    )
    parser.add_argument(
        "--question",
        required=True,
        help="question to run through the pipeline",
    )
    args = parser.parse_args()

    # Align with CLI default logging behaviour
    get_logger("Client", os.environ.get("log_level", "info"))

    from ultrarag.api import SearchO1Pipeline  # noqa: E402

    try:
        pipeline = SearchO1Pipeline(args.config)
        result = pipeline.query(args.question)
    except Exception as e:  # surface env‑first or runtime errors cleanly
        print(f"[SearchO1] error: {e}", file=sys.stderr)
        raise SystemExit(2)

    print(result.get("text", ""))


if __name__ == "__main__":
    asyncio.run(main())

