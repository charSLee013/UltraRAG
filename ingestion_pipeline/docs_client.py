from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv

from .modelscope_headers import build_user_agent


load_dotenv()


@dataclass
class DocLink:
    text: str
    href: str


@dataclass
class DocNode:
    name: str
    href: Optional[str] = None
    children: Dict[str, "DocNode"] = field(default_factory=dict)

    def add(self, parts: List[str], href: str) -> None:
        if not parts:
            self.href = href
            return
        head, *tail = parts
        child = self.children.get(head)
        if child is None:
            child = DocNode(name=head)
            self.children[head] = child
        child.add(tail, href)

    def format(self, prefix: str = "") -> List[str]:
        lines: List[str] = []
        label = self.name if self.href is None else f"{self.name} -> {self.href}"
        if self.name:
            lines.append(prefix + label)
        keys = sorted(self.children.keys())
        for k in keys:
            lines.extend(self.children[k].format(prefix + "  "))
        return lines


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._stack: List[str] = []
        self.links: List[DocLink] = []
        self._curr_href: Optional[str] = None
        self._curr_text_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        self._stack.append(tag)
        if tag.lower() == "a":
            href = None
            for k, v in attrs:
                if k.lower() == "href":
                    href = v
                    break
            self._curr_href = href
            self._curr_text_parts = []

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag.lower() == "a" and self._curr_href:
            text = "".join(self._curr_text_parts).strip()
            self.links.append(DocLink(text=text, href=self._curr_href))
            self._curr_href = None
            self._curr_text_parts = []
        if self._stack:
            self._stack.pop()

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._curr_href is not None:
            self._curr_text_parts.append(data)


def _headers() -> dict:
    return {
        "User-Agent": build_user_agent(),
        "X-Request-ID": uuid.uuid4().hex,
        "Accept": "text/html,application/xhtml+xml",
    }


async def fetch_overview_links(base: str = "https://www.modelscope.cn") -> List[DocLink]:
    """Fetch /docs/overview and extract sidebar-like /docs/* links.

    - No JS; relies on server-rendered anchors.
    - Returns unique links under the /docs/ path.
    """
    url = base.rstrip("/") + "/docs/overview"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=_headers())
        resp.raise_for_status()
        html = resp.text

    parser = _AnchorExtractor()
    parser.feed(html)

    # Keep only /docs/* links; normalize and deduplicate by href
    seen: set[str] = set()
    results: List[DocLink] = []
    for link in parser.links:
        href = (link.href or "").strip()
        if not href.startswith("/docs/"):
            continue
        # Exclude anchors that are obviously not part of docs content
        if any(seg in href.lower() for seg in ("/protocol/",)):
            continue
        if href in seen:
            continue
        seen.add(href)
        text = re.sub(r"\s+", " ", (link.text or "").strip())
        results.append(DocLink(text=text, href=href))
    return results


def build_overview_tree(links: List[DocLink]) -> DocNode:
    """Build a hierarchical tree from /docs/* href paths.

    The tree roots at an empty node; each segment after '/docs/' becomes a level.
    """
    root = DocNode(name="")
    for link in links:
        # strip '/docs/' prefix, keep raw casing/spaces as-is
        path = link.href[len("/docs/") :]
        if not path:
            continue
        parts = [p for p in path.split("/") if p]
        root.add(parts, link.href)
    return root


__all__ = [
    "DocLink",
    "DocNode",
    "fetch_overview_links",
    "build_overview_tree",
]

