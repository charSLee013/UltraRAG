from __future__ import annotations

import sys
import os
from dataclasses import dataclass
from typing import Dict, List


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@dataclass
class Node:
    name: str
    href: str | None = None
    children: Dict[str, "Node"] | None = None

    def add_path(self, path: str, href: str) -> None:
        parts = [p for p in path.split("/") if p]
        if not parts:
            self.href = href
            return
        if self.children is None:
            self.children = {}
        head, *tail = parts
        child = self.children.get(head)
        if child is None:
            child = Node(head)
            self.children[head] = child
        child.add_path("/".join(tail), href)

    def dump(self, indent: str = "") -> List[str]:
        out: List[str] = []
        if self.name:
            label = self.name if self.href is None else f"{self.name} -> {self.href}"
            out.append(indent + label)
        if self.children:
            for key in sorted(self.children.keys()):
                out.extend(self.children[key].dump(indent + "  "))
        return out


from ingestion_pipeline.docs_overview_seed import get_static_overview_links  # noqa: E402


def main() -> None:
    root = Node("")
    for link in get_static_overview_links():
        path = link.href[len("/docs/") :] if link.href.startswith("/docs/") else link.href
        root.add_path(path, link.href)
    for line in root.dump():
        print(line)


if __name__ == "__main__":
    main()

