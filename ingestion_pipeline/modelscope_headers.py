from __future__ import annotations

import re
import uuid

# Baseline UA captured from the current environment.
_BASE_USER_AGENT = (
    "modelscope/1.30.0; python/3.11.9; session_id/2126c328195e44bd982bb70acfa32a40; "
    "platform/macOS-13.7-arm64-arm-64bit; processor/arm; env/custom; user/unknown"
)
_SESSION_PATTERN = re.compile(r"session_id/([0-9a-f]{32})")


def build_user_agent() -> str:
    """Duplicate the observed UA format while updating session_id for each request."""
    new_session = uuid.uuid4().hex
    return _SESSION_PATTERN.sub(f"session_id/{new_session}", _BASE_USER_AGENT, count=1)
