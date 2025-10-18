from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

import httpx
from dotenv import load_dotenv


load_dotenv()


# Fixed defaults per request (no env overrides)
API_MAIN = "https://modelscope.cn/api/v1/document/main_doc"
OUT_ROOT = Path("output/docs_md")


async def fetch_json(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    r = await client.get(url, headers={"Accept": "application/json, text/plain, */*"})
    r.raise_for_status()
    return r.json()


def walk_nodes(root: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Collect all nodes that declare markdown content (md=True), including
    directory-index nodes (dir=True) which should map to path+'/index.md'."""
    out: List[Dict[str, Any]] = []
    stack = [root]
    while stack:
        n = stack.pop()
        if n.get("md") is True and isinstance(n.get("path"), str):
            out.append(n)
        for c in (n.get("children") or []):
            if isinstance(c, dict):
                stack.append(c)
    return out


async def download_md(client: httpx.AsyncClient, base: str, node: Dict[str, Any], out_dir: Path) -> Tuple[str, bool, str]:
    # Resolve fetch path:
    # - leaf md nodes usually end with .md
    # - directory md nodes (dir=True) map to '.../index.md'
    raw_path = str(node["path"]).rstrip("/")
    fetch_path = raw_path
    if node.get("dir") is True:
        fetch_path = f"{raw_path}/index.md"
    elif not raw_path.endswith(".md"):
        # Be tolerant: append .md if missing
        fetch_path = f"{raw_path}.md"

    url = f"{base}/dist/{quote(fetch_path)}"
    try:
        r = await client.get(url, headers={"Accept": "text/markdown, text/plain, */*"})
        r.raise_for_status()
        text = r.text
    except Exception as exc:  # noqa: BLE001
        return fetch_path, False, f"{exc}"

    out_path = out_dir / fetch_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return fetch_path, True, ""


async def main() -> None:
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        meta = await fetch_json(client, API_MAIN)
        data = meta.get("Data") or {}
        target_prefix = str(data.get("TargetPrefix") or "").rstrip("/")
        version = str(data.get("Version") or "").strip()
        if not target_prefix or not version:
            raise RuntimeError("main_doc missing TargetPrefix/Version")

        index_url = f"{target_prefix}/dist/index.json"
        index_data = await fetch_json(client, index_url)
        leaves = walk_nodes(index_data)

        out_dir = OUT_ROOT / version
        out_dir.mkdir(parents=True, exist_ok=True)

        # Fixed concurrency (no env override)
        sem = asyncio.Semaphore(12)
        results: List[Tuple[str, bool, str]] = []

        async def task(n: Dict[str, Any]) -> None:
            async with sem:
                rel, ok, err = await download_md(client, target_prefix, n, out_dir)
                results.append((rel, ok, err))

        await asyncio.gather(*(task(n) for n in leaves))

        total = len(leaves)
        success = sum(1 for _, ok, _ in results if ok)
        failed = [(rel, err) for rel, ok, err in results if not ok]

        print("ModelScope docs markdown fetch summary:")
        print(f"  version     : {version}")
        print(f"  target      : {target_prefix}")
        print(f"  total md    : {total}")
        print(f"  succeeded   : {success}")
        print(f"  failed      : {len(failed)}")
        if failed:
            print("  failed list :")
            for rel, err in failed[:20]:
                print(f"   - {rel} :: {err}")


if __name__ == "__main__":
    asyncio.run(main())
