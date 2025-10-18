from __future__ import annotations

import json
import time
from typing import Dict

import httpx
from playwright.sync_api import sync_playwright


def main() -> None:
    captured: Dict[str, str] | None = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def on_request(req):
            nonlocal captured
            url = req.url
            if req.method == "PUT" and ("/mcpServers" in url or "/mcp/servers" in url):
                hdrs = dict(req.headers)
                # Keep only client headers likely relevant downstream
                keep = [
                    "user-agent",
                    "accept",
                    "accept-language",
                    "x-modelscope-trace-id",
                    "x-csrf-token",
                    "x-request-id",
                    "x-modelscope-accept-language",
                ]
                captured = {k: v for k, v in hdrs.items() if k.lower() in keep}

        page.on("request", on_request)
        page.goto("https://modelscope.cn/mcp", wait_until="domcontentloaded")
        # wait a bit for XHRs
        time.sleep(2.0)
        browser.close()

    if not captured:
        print("NO_CAPTURED_HEADERS")
        return

    print("CAPTURED_HEADERS")
    print(json.dumps(captured, ensure_ascii=False, indent=2))

    # Build openapi call using subset
    headers = {
        "User-Agent": captured.get("user-agent", "playwright/1"),
        "Accept": captured.get("accept", "application/json"),
        "Accept-Language": captured.get("x-modelscope-accept-language")
        or captured.get("accept-language", "zh-CN,zh;q=0.9,en;q=0.8"),
        "Content-Type": "application/json",
    }

    body = {"filter": {}, "page_number": 2, "page_size": 100, "search": ""}
    url = "https://modelscope.cn/openapi/v1/mcp/servers"
    with httpx.Client(timeout=30) as client:
        r = client.put(url, headers=headers, json=body)
    print("OPENAPI_STATUS", r.status_code)
    print("OPENAPI_CT", r.headers.get("content-type"))
    if r.headers.get("content-type", "").startswith("application/json"):
        data = r.json()
        items = data.get("data", {}).get("mcp_server_list", [])
        print("OPENAPI_ITEMS", len(items))
        for item in items[:2]:
            print(json.dumps(item, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
