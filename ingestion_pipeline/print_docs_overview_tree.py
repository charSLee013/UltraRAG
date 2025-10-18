from __future__ import annotations

import asyncio
import os
import sys
from dotenv import load_dotenv


# Allow running by absolute path: add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ingestion_pipeline.docs_client import fetch_overview_links, build_overview_tree  # noqa: E402


load_dotenv()


async def _main() -> None:
    base = os.environ.get("MODELSCOPE_ENDPOINT", "https://www.modelscope.cn").rstrip("/")
    links = await fetch_overview_links(base)
    tree = build_overview_tree(links)
    for line in tree.format():
        print(line)


if __name__ == "__main__":
    asyncio.run(_main())
