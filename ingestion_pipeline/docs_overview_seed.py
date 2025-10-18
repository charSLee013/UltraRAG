from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class SeedLink:
    text: str
    href: str


def get_static_overview_links() -> List[SeedLink]:
    """Fixed set of docs/overview links observed from the official nav.

    This is used when dynamic JS rendering is unavailable.
    """
    return [
        SeedLink("Home", "/docs/home"),
        SeedLink("Overview", "/docs/Overview"),
        # Beginner's Guide
        SeedLink("Beginner's Guide/Quick Start", "/docs/Beginner-s-Guide/Quick-Start"),
        SeedLink("Beginner's Guide/Environment Setup", "/docs/Beginner-s-Guide/Environment-Setup"),
        # Models
        SeedLink("Models/Model Introduction", "/docs/Models/Model-Introduction"),
        SeedLink("Models/Upload Model", "/docs/Models/Upload-Model"),
        SeedLink("Models/Download Model", "/docs/Models/Download-Model"),
        # OpenAPI
        SeedLink("OpenAPI", "/docs/openapi"),
    ]


__all__ = ["SeedLink", "get_static_overview_links"]

