import json
import os
import sys
import pytest

# Ensure src/ is importable for 'ultrarag'
sys.path.insert(0, os.path.abspath("src"))

from ultrarag.utils import normalize_readme_text


def test_normalize_readme_text_json_html():
    payload = {
        "ReadMeContent": "<div><h1>Title</h1><p>Paragraph A</p><br/>Line B</div>",
        "Other": "ignored",
    }
    text = json.dumps(payload)
    out, state = normalize_readme_text(text)
    assert "Title" in out and "Paragraph A" in out
    assert "<h1>" not in out and "<p>" not in out
    assert "Line B" in out
    assert state in {"html_stripped", "json_decoded"}


def test_normalize_readme_text_double_escaped_json():
    inner = json.dumps({"ReadMeContent": "<p>Hello <b>World</b></p>"})
    double = json.dumps(inner)
    out, state = normalize_readme_text(double)
    assert "Hello World" in out
    assert state in {"html_stripped", "json_decoded"}


def test_normalize_readme_text_html_only():
    html = "<div>Alpha<br>Beta</div>"
    out, state = normalize_readme_text(html)
    assert out == "Alpha\nBeta"
    assert state == "html_stripped"


def test_normalize_readme_text_raw_passthrough():
    txt = "plain text without html"
    out, state = normalize_readme_text(txt)
    assert out == txt
    assert state == "raw"
