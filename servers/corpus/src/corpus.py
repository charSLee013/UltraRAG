import json
import os
from pathlib import Path
from typing import Dict, Optional, Union, List, Any
import re

from ultrarag.server import UltraRAG_MCP_Server

app = UltraRAG_MCP_Server("corpus")


def parse_documents(file_path: Union[str, Path]) -> Dict[str, str]:

    try:
        from llama_index.core import SimpleDirectoryReader
    except ImportError:
        raise ImportError(
            "Missing optional dependency 'llama-index-readers-file'. "
            "Please install it with: pip install llama-index-readers-file"
        )

    file_path = Path(file_path) if not isinstance(file_path, Path) else file_path
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    ext = file_path.suffix.lower()
    if ext in [".pdf", ".docx", ".txt", ".md"]:
        reader = SimpleDirectoryReader(input_files=[file_path])
        documents = reader.load_data()
        raw_data = "\n".join([d.text for d in documents])
    else:
        raise ValueError(
            f"Unsupported file format: {file_path.suffix}. "
            "Currently supported: .docx, .txt, .pdf, .md. "
            "Please convert your file to a supported format."
        )

    return {"raw_data": raw_data}


async def chunk_documents(
    chunk_strategy: str,
    chunk_size: int,
    raw_data: str,
    output_path: Optional[str] = None,
    tokenizer_name_or_path: Optional[str] = None,
) -> Dict[str, str]:

    try:
        import chonkie
    except ImportError:
        raise ImportError("Please install 'chonkie' via pip to use chunk_documents.")

    if output_path is None:
        current_file = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file))
        output_dir = os.path.join(project_root, "output", "corpus")
        output_path = os.path.join(output_dir, "chunks.jsonl")
    else:
        output_path = str(output_path)
        output_dir = os.path.dirname(output_path)

    os.makedirs(output_dir, exist_ok=True)

    if chunk_strategy == "token":
        chunker = chonkie.TokenChunker(
            tokenizer=tokenizer_name_or_path, chunk_size=chunk_size
        )
    elif chunk_strategy == "word":
        chunker = chonkie.TokenChunker(tokenizer="word", chunk_size=chunk_size)
    elif chunk_strategy == "sentence":
        chunker = chonkie.SentenceChunker(
            tokenizer_or_token_counter=tokenizer_name_or_path, chunk_size=chunk_size
        )
    elif chunk_strategy == "recursive":
        chunker = chonkie.RecursiveChunker(
            tokenizer_or_token_counter=tokenizer_name_or_path,
            chunk_size=chunk_size,
            min_characters_per_chunk=1,
        )
    else:
        raise ValueError(
            f"Invalid chunking method: {chunk_strategy}. Supported: token, word, sentence, recursive"
        )

    chunks = chunker(raw_data)

    chunked_documents = [
        {"id": i, "contents": chunk.text} for i, chunk in enumerate(chunks)
    ]

    with open(output_path, "w", encoding="utf-8") as f:
        for doc in chunked_documents:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    return {"status": "save chunks successful"}

# ===============================
# SQLite read-only helpers (ingestion DB)
# ===============================
try:
    from ingestion_pipeline.stores.sqlite import SQLiteStore  # type: ignore
except Exception:
    SQLiteStore = None  # type: ignore


def _get_sqlite() -> SQLiteStore:  # type: ignore
    if SQLiteStore is None:
        raise ImportError(
            "ingestion_pipeline is not available; install project in editable mode."
        )
    store = SQLiteStore()
    try:
        store.conn.create_function(
            "REGEXP", 2, lambda pattern, text: 1 if re.search(pattern or "", text or "") else 0
        )
    except Exception:
        pass
    return store


@app.tool(
    name="sqlite_repo_search_filtered",
    output="source_type_in,repo_id_regex,limit->repo_ids,total",
)
def sqlite_repo_search_filtered(
    source_type_in: Optional[List[str]] = None,
    repo_id_regex: Optional[str] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    """Filter repos by source_type IN (...) and/or repo_id REGEXP.

    Notes:
      - Both filters are independent parameters; do not merge into one 'where'.
      - If both are empty/None, raise to enforce restricted pipeline contract.
    """
    # Validate: at least one filter must be provided (type list or regex)
    filtered_types = [s for s in (source_type_in or []) if str(s).strip()]
    no_type = len(filtered_types) == 0
    no_regex = not (repo_id_regex and str(repo_id_regex).strip())
    # If no filters provided, treat as 'no filtering here' and let vector layer
    # perform an unfiltered search (consistent with pipeline fallback semantics).
    if no_type and no_regex:
        return {"repo_ids": [], "total": 0}

    store = _get_sqlite()
    where_sql_parts: List[str] = []
    args: List[Any] = []

    if filtered_types:
        placeholders = ",".join(["?"] * len(filtered_types))
        where_sql_parts.append(f"source_type IN ({placeholders})")
        args.extend([str(s).strip() for s in filtered_types])

    if repo_id_regex and str(repo_id_regex).strip():
        where_sql_parts.append("repo_id REGEXP ?")
        args.append(str(repo_id_regex).strip())

    where_sql = (" WHERE " + " AND ".join(where_sql_parts)) if where_sql_parts else ""

    cur = store.conn.cursor()
    total = cur.execute(f"SELECT COUNT(1) FROM repo{where_sql}", args).fetchone()[0]
    cur = store.conn.execute(
        f"SELECT repo_id FROM repo{where_sql} LIMIT ?",
        args + [max(0, int(limit))],
    )
    repo_ids = [r[0] for r in cur.fetchall()]
    return {"repo_ids": repo_ids, "total": int(total)}


@app.tool(output="source_type,owner_regex,limit->owner_repos,repo_ids,total")
def sqlite_repo_search(
    source_type: Optional[Union[str, List[str]]] = None,
    owner_regex: Optional[Union[str, List[str]]] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    """List repos from SQLite by SourceType and/or owner_repo regex.

    Returns:
      - owner_repos: List[str]
      - repo_ids: List[str]
      - total: int (before limit)
    """
    # If no filters provided, return empty candidate set to avoid scanning full table;
    # vector layer will treat empty list as "no prefilter".
    # Unwrap list inputs from router (it wraps scalars to satisfy validators)
    if isinstance(source_type, list):
        source_type = source_type[0] if source_type else None
    if isinstance(owner_regex, list):
        owner_regex = owner_regex[0] if owner_regex else None
    if not (source_type and str(source_type).strip()) and not (owner_regex and str(owner_regex).strip()):
        return {"owner_repos": [], "repo_ids": [], "total": 0}

    store = _get_sqlite()
    where = []
    args: List[Any] = []
    if source_type:
        where.append("source_type = ?")
        args.append(str(source_type))
    if owner_regex:
        where.append("owner_repo REGEXP ?")
        args.append(str(owner_regex))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    cur = store.conn.cursor()
    total = cur.execute(
        f"SELECT COUNT(1) FROM repo{where_sql}", args
    ).fetchone()[0]
    cur = store.conn.execute(
        f"SELECT repo_id, owner_repo FROM repo{where_sql} LIMIT ?",
        args + [max(0, int(limit))],
    )
    rows = cur.fetchall()
    repo_ids = [r[0] for r in rows]
    owner_repos = [r[1] for r in rows]
    return {"owner_repos": owner_repos, "repo_ids": repo_ids, "total": int(total)}


@app.tool(output="repo_id,limit,offset->chunks")
def sqlite_chunks_by_repo(
    repo_id: str, limit: int = 2000, offset: int = 0
) -> Dict[str, Any]:
    """List chunk references for a repo from SQLite (text for QA/ops)."""
    store = _get_sqlite()
    cur = store.conn.execute(
        """
        SELECT chunk_uuid, chunk_index, text
        FROM chunks
        WHERE repo_id = ?
        ORDER BY chunk_index ASC
        LIMIT ? OFFSET ?
        """,
        (repo_id, max(0, int(limit)), max(0, int(offset))),
    )
    chunks = [
        {"chunk_uuid": row[0], "chunk_index": int(row[1]), "text": row[2]} for row in cur
    ]
    return {"chunks": chunks}


@app.tool(output="repo_id->repo_meta")
def sqlite_get_repo_meta(repo_id: str) -> Dict[str, Any]:
    """Fetch minimal repo metadata for a given repo_id."""
    store = _get_sqlite()
    row = store.conn.execute(
        """
        SELECT repo_id, source_type, owner_repo, source_url, fetched_at, content_hash
        FROM repo WHERE repo_id = ?
        """,
        (repo_id,),
    ).fetchone()
    if not row:
        return {"repo_meta": None}
    keys = [
        "repo_id",
        "source_type",
        "owner_repo",
        "source_url",
        "fetched_at",
        "content_hash",
    ]
    return {"repo_meta": {k: row[i] for i, k in enumerate(keys)}}


if __name__ == "__main__":
    app.run(transport="stdio")
