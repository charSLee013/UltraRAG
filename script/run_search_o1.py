#!/usr/bin/env python
"""Utility to build or run a pipeline without installing the package."""

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

existing_pythonpath = os.environ.get("PYTHONPATH")
if existing_pythonpath:
    os.environ["PYTHONPATH"] = f"{SRC_ROOT}{os.pathsep}{existing_pythonpath}"
else:
    os.environ["PYTHONPATH"] = str(SRC_ROOT)

venv_bin = PROJECT_ROOT / ".venv" / "bin"
if venv_bin.exists():
    os.environ["PATH"] = f"{venv_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    os.environ.setdefault("VIRTUAL_ENV", str(PROJECT_ROOT / ".venv"))

from ultrarag.client import build, run, logger as CLIENT_LOGGER  # noqa: E402
from ultrarag.mcp_logging import get_logger  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Execute UltraRAG pipeline via Python")
    parser.add_argument("action", choices=["build", "run"], help="pipeline operation")
    parser.add_argument("pipeline", help="path to YAML pipeline")
    args = parser.parse_args()

    if CLIENT_LOGGER is None:
        # align with CLI default logging behaviour
        ultrarag_logger = get_logger("Client", "info")
        import ultrarag.client as client_mod  # noqa: E402

        client_mod.logger = ultrarag_logger

    if args.action == "build":
        await build(args.pipeline)
    else:
        await run(args.pipeline)


if __name__ == "__main__":
    asyncio.run(main())
