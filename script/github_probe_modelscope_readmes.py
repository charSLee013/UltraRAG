#!/usr/bin/env python3
"""
Verify GitHub paging and README retrieval for the modelscope org.

Usage (venv first):
  .venv/bin/python script/github_probe_modelscope_readmes.py [pages]

Requirements & Rules
- Loads .env at start (AGENTS.md env-first rule).
- Single-path: fixed owner 'modelscope'; official GitHub REST only; no token required.
- To avoid GitHub unauthenticated rate limits (60 req/hr), this probe caps
  README fetches to at most 20 per page.
- Verifies two points: (1) paging works; (2) README fetch works.
"""

from __future__ import annotations

import base64
import logging
import os
import sys
import time
from typing import Any, Dict, List, Tuple

import httpx
from dotenv import load_dotenv


ORG = "modelscope"
API_BASE = "https://api.github.com"


def _new_client() -> httpx.Client:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "UltraRAG-Ingestion/1.0 (+github.com/OpenBMB/UltraRAG)",
    }
    return httpx.Client(base_url=API_BASE, headers=headers, timeout=30.0)


def _maybe_wait_rate_limit(resp: httpx.Response) -> None:
    # Honor GitHub rate limit reset when possible; keep it bounded in this probe.
    try:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining == "0" and reset:
            reset_ts = int(reset)
            now = int(time.time())
            wait = max(0, reset_ts - now) + 1
            wait = min(wait, 120)
            logging.warning("Rate limited (403). Sleeping %ss until reset...", wait)
            time.sleep(wait)
    except Exception:
        pass


def list_repos_page(client: httpx.Client, page: int, per_page: int = 100) -> List[Dict[str, Any]]:
    r = client.get(f"/orgs/{ORG}/repos", params={"per_page": per_page, "page": page})
    if r.status_code == 403:
        _maybe_wait_rate_limit(r)
    r.raise_for_status()
    return r.json()  # type: ignore[return-value]


def get_readme_text(client: httpx.Client, repo: str) -> Tuple[str, int, str]:
    """Return (text, length, sha). 404 -> ("", 0, "")."""
    r = client.get(f"/repos/{ORG}/{repo}/readme")
    if r.status_code == 404:
        return "", 0, ""
    if r.status_code == 403:
        _maybe_wait_rate_limit(r)
    r.raise_for_status()
    data = r.json()
    sha = data.get("sha") or ""
    enc = data.get("encoding")
    content = data.get("content") or ""
    if enc == "base64" and content:
        try:
            raw = base64.b64decode(content, validate=True)
        except Exception:
            raw = base64.b64decode(content)
        text = raw.decode("utf-8", errors="replace")
        return text, len(text), sha
    # Fallback to download_url when encoding/content unexpected
    dl = data.get("download_url")
    if dl:
        r2 = client.get(dl)
        r2.raise_for_status()
        text = r2.text
        return text, len(text), sha
    return "", 0, sha


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    pages = 2
    if len(sys.argv) > 1:
        try:
            pages = max(1, int(sys.argv[1]))
        except ValueError:
            raise SystemExit("Usage: github_probe_modelscope_readmes.py [pages]")

    client = _new_client()

    total_listed = 0
    readme_ok = 0
    readme_missing = 0
    samples: List[Tuple[str, int, str, str]] = []  # (repo, length, sha, snippet)

    PER_PAGE_LIMIT = 20  # cap to stay under unauthenticated rate limit
    for page in range(1, pages + 1):
        repos = list_repos_page(client, page=page, per_page=100)
        logging.info("page=%d repos=%d", page, len(repos))
        if not repos:
            break
        total_listed += len(repos)
        for r in repos[:PER_PAGE_LIMIT]:
            name = r.get("name")
            if not name:
                continue
            text, n, sha = get_readme_text(client, name)
            if n < 7:
                readme_missing += 1
                logging.info("README missing/short: %s/%s length=%d", ORG, name, n)
            else:
                readme_ok += 1
                if len(samples) < 2:
                    snippet = text[:300].replace("\n", " ").replace("\r", " ")
                    samples.append((name, n, sha, snippet))

    print("\nVerification Summary")
    print(f"  org           : {ORG}")
    print(f"  pages_queried : {pages}")
    print(f"  repos_listed  : {total_listed}")
    print(f"  readme_ok     : {readme_ok}")
    print(f"  readme_missing: {readme_missing}")
    if samples:
        print("\nSample READMEs (2):")
        for repo, n, sha, snip in samples:
            print(f"- {ORG}/{repo} len={n} sha={sha}\n  snippet: {snip}")


if __name__ == "__main__":
    main()
