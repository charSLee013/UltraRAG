"""UltraRAG public API."""

try:
    from .api import SearchO1Pipeline as _SearchO1Pipeline
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
    if exc.name == "fastmcp":
        _SearchO1Pipeline = None
    else:  # re-raise unexpected import failures
        raise


if _SearchO1Pipeline is None:  # pragma: no cover - fallback only when fastmcp missing
    class SearchO1Pipeline:  # type: ignore[override]
        def __init__(self, *_: object, **__: object) -> None:
            raise RuntimeError(
                "SearchO1Pipeline requires the 'fastmcp' package. "
                "Install project dependencies (uv pip install -e .) to use it."
            )

    __all__ = []
else:
    SearchO1Pipeline = _SearchO1Pipeline
    __all__ = ["SearchO1Pipeline"]
