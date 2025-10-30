import os
import re
from typing import List, Dict, Any, Optional

from ultrarag.server import UltraRAG_MCP_Server
import json
import re

app = UltraRAG_MCP_Server("custom")


@app.tool(output="ans_ls->extract_query_list")
def search_r1_query_extract(ans_ls: List[str]) -> Dict[str, List[str]]:

    def get_query(text):
        import re

        pattern = re.compile(r"<search>([^<]*)", re.DOTALL)
        matches = pattern.findall(text)

        if matches:
            query = matches[-1].strip()
            if not query.endswith("?"):
                query += "?"
            return query
        else:
            return ""

    query = [get_query(answer) for answer in ans_ls]

    return {"extract_query_list": query}


@app.tool(output="ans_ls->extract_query_list")
def r1_searcher_query_extract(ans_ls: List[str]) -> Dict[str, List[str]]:

    def get_query(text):
        import re

        pattern = re.compile(r"<|begin_of_query|>([^<]*)", re.DOTALL)
        matches = pattern.findall(text)

        if matches:
            query = matches[-1].strip()
            if not query.endswith("?"):
                query += "?"
            return query
        else:
            return "There is no query."

    query = [get_query(answer) for answer in ans_ls]

    return {"extract_query_list": query}


@app.tool(output="q_ls,ret_psg->nextq_ls")
def iterretgen_nextquery(
    q_ls: List[str],
    ans_ls: List[str | Any],
) -> Dict[str, List[str]]:
    ret = []
    for q, ans in zip(q_ls, ans_ls):
        next_query = f"{q} {ans}"
        ret.append(next_query)
    return {"nextq_ls": ret}


@app.tool(output="ans_ls->q_ls")
def ircot_get_first_sent(
    ans_ls: List[str],
) -> Dict[str, List[str]]:
    ret = []
    for ans in ans_ls:
        match = re.search(r"(.+?[。！？.!?])", ans)
        if match:
            ret.append(match.group(1))
        else:
            ret.append(ans.strip())
    return {"q_ls": ret}


@app.tool(output="ans_ls->pred_ls")
def ircot_extract_ans(ans_ls: List[str]) -> Dict[str, List[str]]:
    ret = []
    pattern = re.compile(r"so the answer is[\s:]*([^\n]*)", re.IGNORECASE)
    for ans in ans_ls:
        match = pattern.search(ans)
        if match:
            ret.append(match.group(1).strip())
        else:
            ret.append(ans.strip())
    return {"pred_ls": ret}


def _contract_token(
    contract: Optional[Dict[str, Any]],
    key: str,
    default: Optional[str],
) -> Optional[str]:
    if not contract:
        return default
    value = contract.get(key)
    if isinstance(value, str) and value:
        return value
    return default


@app.tool(output="ans_ls,token_contract->extract_query_list")
def search_o1_query_extract(
    ans_ls: List[str],
    token_contract: Dict[str, Any],
) -> Dict[str, List[str]]:
    begin_query = _contract_token(
        token_contract, "begin_query", None
    )
    end_query = _contract_token(token_contract, "end_query", None)

    pattern: Optional[re.Pattern[str]] = None
    if begin_query and end_query:
        pattern = re.compile(
            re.escape(begin_query) + r"(.*?)" + re.escape(end_query), re.DOTALL
        )

    queries: List[str] = []
    for answer in ans_ls:
        if not pattern:
            queries.append("")
            continue
        matches = pattern.findall(answer)
        if matches:
            queries.append(matches[-1].strip())
        else:
            queries.append("")

    debug_flag = os.getenv("SEARCH_O1_DEBUG")
    if debug_flag and debug_flag not in {"0", "false", "False"}:
        last_query = next((q for q in reversed(queries) if q), "")
        if last_query:
            app.logger.debug(
                "[search_o1_query_extract] latest_query=%s",
                last_query[:200],
            )

    # parse optional routing hints from the model's text (any line):
    # mode=sqlite; source_type=models; owner_regex='qwen|llama'
    mode, src_type, owner_rx = None, "", ""
    for answer in ans_ls or []:
        for line in (answer or "").splitlines():
            line_stripped = line.strip()
            if not line_stripped:
                continue
            m = re.search(r"\bmode\s*=\s*([A-Za-z_]+)", line_stripped)
            if m:
                mode = (m.group(1) or "").lower()
            m2 = re.search(r"\bsource_type\s*=\s*([A-Za-z_]+)", line_stripped)
            if m2:
                src_type = m2.group(1) or ""
            m3 = re.search(r"owner_regex\s*=\s*'([^']*)'", line_stripped)
            if not m3:
                m3 = re.search(r'owner_regex\s*=\s*"([^"]*)"', line_stripped)
            if not m3:
                m3 = re.search(r"owner_regex\s*=\s*([^;\s]+)", line_stripped)
            if m3:
                owner_rx = m3.group(1) or ""

    return {"extract_query_list": queries}


@app.tool(output="ans_ls,token_contract->markdown_ls")
def output_passthrough(
    ans_ls: List[str],
    token_contract: Dict[str, Any],
) -> Dict[str, List[str]]:
    tokens = {
        token
        for token in (
            _contract_token(token_contract, "end_answer", None),
            _contract_token(token_contract, "begin_query", None),
            _contract_token(token_contract, "end_query", None),
            _contract_token(token_contract, "begin_result", None),
            _contract_token(token_contract, "end_result", None),
        )
        if token
    }

    cleaned: List[str] = []
    for ans in ans_ls:
        text = ans
        for token in tokens:
            text = text.replace(token, "")
        cleaned.append(text.strip())

    return {"markdown_ls": cleaned}


@app.tool(output="metadata->candidate_repo_ids")
def meta_to_repo_ids(
    metadata: List[List[Dict[str, Any]]],
) -> Dict[str, List[List[str]]]:
    """Extract per-query repo_id sets from metadata (aligned to query list).

    Returns list[list[str]] with unique repo_ids per query; empty list when none.
    """
    out: List[List[str]] = []
    for row in metadata or []:
        ids = []
        seen = set()
        if isinstance(row, list):
            for d in row:
                rid = (d or {}).get("repo_id")
                if isinstance(rid, str) and rid and rid not in seen:
                    ids.append(rid)
                    seen.add(rid)
        out.append(ids)
    return {"candidate_repo_ids": out}


@app.tool(output="metadata->candidate_repo_ids_flat")
def meta_to_repo_ids_flat(
    metadata: List[List[Dict[str, Any]]],
) -> Dict[str, List[str]]:
    """Extract a batch-level unique repo_id list from metadata.

    - Flattens over all queries; preserves no particular order.
    - Returns [] when none are present (downstream may fallback).
    """
    seen: set[str] = set()
    out: List[str] = []
    for row in metadata or []:
        if not isinstance(row, list):
            continue
        for d in row:
            rid = (d or {}).get("repo_id")
            if isinstance(rid, str) and rid and rid not in seen:
                seen.add(rid)
                out.append(rid)
    return {"candidate_repo_ids_flat": out}


@app.tool(output="candidate_repo_ids_flat,filtered_repo_ids,allow_fallback->final_repo_ids_flat")
def intersect_flat(
    candidate_repo_ids_flat: List[str],
    filtered_repo_ids: List[str] | None = None,
    allow_fallback: bool = True,
) -> Dict[str, List[str]]:
    """Batch-level repo intersection.

    - If filtered_repo_ids is empty/None and allow_fallback=True → return candidates as-is.
    - Else return intersection preserving candidate order.
    """
    cand = [x for x in (candidate_repo_ids_flat or []) if isinstance(x, str) and x]
    filt_set = {x for x in (filtered_repo_ids or []) if isinstance(x, str) and x}
    if not filt_set:
        return {"final_repo_ids_flat": cand if allow_fallback else []}
    inter = [x for x in cand if x in filt_set]
    return {"final_repo_ids_flat": inter}

@app.tool(output="candidate_repo_ids,filtered_repo_ids,allow_fallback->final_repo_ids")
def intersect_repo_ids(
    candidate_repo_ids: List[List[str]],
    filtered_repo_ids: List[str],
    allow_fallback: bool = True,
) -> Dict[str, List[List[str]]]:
    """Intersect per-query candidates with filtered_repo_ids.

    - If filtered_repo_ids is empty and allow_fallback=True, return candidates.
    - Always preserve per-query alignment (empty lists for empty intersections).
    """
    filt = set([x for x in (filtered_repo_ids or []) if isinstance(x, str) and x])
    out: List[List[str]] = []
    for row in candidate_repo_ids or []:
        if not filt:
            out.append(list(row or []))
        else:
            inter = [x for x in (row or []) if x in filt]
            out.append(inter)
    return {"final_repo_ids": out}


@app.tool(output="metadata->candidate_repo_ids_flat")
def meta_to_repo_ids_flat(
    metadata: List[List[Dict[str, Any]]],
) -> Dict[str, List[str]]:
    """Extract a flat unique repo_id list from metadata across the batch.

    Only keep ids that look like '<source_type>:<rest>' to avoid file paths.
    """
    seen = set()
    out: List[str] = []
    for row in metadata or []:
        if not isinstance(row, list):
            continue
        for d in row:
            rid = (d or {}).get("repo_id")
            if isinstance(rid, str) and ":" in rid and rid not in seen:
                seen.add(rid)
                out.append(rid)
    return {"candidate_repo_ids_flat": out}


@app.tool(output="candidate_repo_ids_flat,filtered_repo_ids,allow_fallback->final_repo_ids_flat")
def intersect_repo_ids_flat(
    candidate_repo_ids_flat: List[str],
    filtered_repo_ids: List[str],
    allow_fallback: bool = True,
) -> Dict[str, List[str]]:
    """Intersect flat candidates with filtered_repo_ids (both flat lists).

    - If filtered list empty and allow_fallback=True, return candidates.
    - Always return a flat list (can be empty if fallback disabled).
    """
    cand = [x for x in (candidate_repo_ids_flat or []) if isinstance(x, str) and x]
    filt = set([x for x in (filtered_repo_ids or []) if isinstance(x, str) and x])
    if not filt:
        return {"final_repo_ids_flat": cand if allow_fallback else []}
    inter = [x for x in cand if x in filt]
    if not inter and allow_fallback:
        return {"final_repo_ids_flat": cand}
    return {"final_repo_ids_flat": inter}


@app.tool(output="ans_ls->candidate_query_list")
def parse_json_queries(ans_ls: List[str]) -> Dict[str, Any]:
    """Parse strict JSON from multiview query generator.

    Input format per item:
      {"queries": [{"id": 1, "facet": "...", "query": "..."}, ...]}
    Returns a batch-level flat candidate_query_list (deduped, max 20).
    """
    import json
    seen = set()
    out: List[str] = []
    for s in ans_ls or []:
        try:
            obj = json.loads(s)
            items = obj.get("queries") or []
            for it in items:
                q = str((it or {}).get("query") or "").strip()
                if q and q not in seen:
                    seen.add(q)
                    out.append(q)
        except Exception:
            continue
    return {"candidate_query_list": out[:20]}


def _bigrams(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    # character bigrams for CJK; fall back to whitespace tokens for long english strings
    if any("\u4e00" <= ch <= "\u9fff" for ch in s):
        return [s[i : i + 2] for i in range(len(s) - 1)]
    toks = s.split()
    return [toks[i] + " " + toks[i + 1] for i in range(len(toks) - 1)] if len(toks) > 1 else toks


@app.tool(output="q_ls,candidate_query_list,metadata,hits,alpha,beta,gamma,allow_fallback,top_n->selected_repo_ids_flat")
def rank_queries_by_retrieval(
    q_ls: List[str],
    candidate_query_list: List[str],
    metadata: List[List[Dict[str, Any]]],
    hits: Optional[List[Dict[str, Any]]] = None,
    alpha: float = 0.5,
    beta: float = 0.3,
    gamma: float = 0.2,
    allow_fallback: bool = True,
    top_n: int = 1,
) -> Dict[str, Any]:
    """Rank candidate queries using retrieval signals and return repo_id set.

    coverage = unique repo_id count per candidate
    focus = 1 / (mean distance + 1e-6) from per-candidate metadata
    relevance = Jaccard bigram overlap between concatenated question(s) and concatenated docs
    score = alpha*coverage + beta*relevance + gamma*focus
    """
    import math
    alpha = float(alpha or 0.5)
    beta = float(beta or 0.3)
    gamma = float(gamma or 0.2)
    top_n = max(1, int(top_n or 1))

    # Build a per-candidate view from metadata (aligned by candidate_query_list order)
    cand_metrics: List[Dict[str, Any]] = []
    global_repo_set: set[str] = set()
    q_concat = " ".join([q for q in (q_ls or []) if isinstance(q, str)])
    q_bg = set(_bigrams(q_concat)) if q_concat else set()

    for idx, metas in enumerate(metadata or []):
        rid_set: set[str] = set()
        dists: List[float] = []
        doc_cat_parts: List[str] = []
        for m in metas or []:
            if not isinstance(m, dict):
                continue
            rid = m.get("repo_id")
            if isinstance(rid, str):
                rid_set.add(rid)
            score = m.get("score")
            if isinstance(score, (int, float)):
                dists.append(float(score))
        # approximate doc text overlap by repo_id presence; if ret_psg available, could join texts, but we keep light
        # Use metadata count as proxy; for relevance we compute against candidate query text too (optional)
        coverage = float(len(rid_set))
        mean_dist = sum(dists) / len(dists) if dists else 0.0
        focus = (1.0 / (mean_dist + 1e-6)) if mean_dist > 0 else 0.0

        # Build pseudo document string from repo ids to compute a stable bigram overlap with q
        doc_str = " ".join(sorted(list(rid_set)))
        doc_bg = set(_bigrams(doc_str))
        inter = len(q_bg & doc_bg) if q_bg else 0
        uni = len(q_bg | doc_bg) if q_bg else 0
        relevance = (inter / uni) if uni > 0 else 0.0

        score = alpha * coverage + beta * relevance + gamma * focus
        cand_metrics.append(
            {
                "idx": idx,
                "coverage": coverage,
                "focus": focus,
                "relevance": relevance,
                "score": score,
                "repo_ids": list(rid_set),
            }
        )
        global_repo_set.update(rid_set)

    # Pick top_n by score
    cand_metrics.sort(key=lambda x: x["score"], reverse=True)
    selected: List[str] = []
    if cand_metrics and (cand_metrics[0]["score"] > 0 or not allow_fallback):
        for c in cand_metrics[:top_n]:
            for rid in c["repo_ids"]:
                if rid not in selected:
                    selected.append(rid)
    elif allow_fallback:
        # fallback to global union from coarse
        selected = list(global_repo_set)

    return {"selected_repo_ids_flat": selected}

@app.tool(output="ans_ls,allowed_source_types,allow_fallback->selected_source_types,selected_repo_regex,selected_filter_raw")
def parse_filter_selection(
    ans_ls: List[str],
    allowed_source_types: List[str],
    allow_fallback: bool = True,
) -> Dict[str, Any]:
    """Parse strict JSON selection from LLM output for retrieval filtering.

    Expected JSON: {"source_types": [..], "repo_id_regex": "..."}
    - source_types: must be subset of allowed_source_types; cap at 5
    - repo_id_regex: optional; length cap 256
    If invalid and allow_fallback=True, returns empty filters to trigger unfiltered search.
    """
    def _extract_json(s: str) -> str | None:
        m = re.search(r"\{[\s\S]*\}", s)
        return m.group(0) if m else None

    sel_types: List[str] = []
    sel_regex: str = ""
    raw = ""

    if ans_ls:
        raw_candidate = _extract_json(ans_ls[0] or "")
        raw = raw_candidate or (ans_ls[0] or "")[:512]
        if raw_candidate:
            try:
                obj = json.loads(raw_candidate)
                st = obj.get("source_types")
                rg = obj.get("repo_id_regex")
                if isinstance(st, list):
                    allow = set([str(a).strip() for a in allowed_source_types or []])
                    sel_types = [str(x).strip() for x in st if str(x).strip() in allow]
                    sel_types = sel_types[:5]
                if isinstance(rg, str):
                    sel_regex = rg.strip()[:256]
            except Exception:
                sel_types, sel_regex = [], ""

    no_type = len(sel_types) == 0
    no_regex = not bool(sel_regex)
    if no_type and no_regex and not allow_fallback:
        raise ValueError(
            "[UltraRAG Error] parse_filter_selection parsed empty filters and fallback is disabled"
        )

    return {
        "selected_source_types": sel_types,
        "selected_repo_regex": sel_regex,
        "selected_filter_raw": raw,
    }


if __name__ == "__main__":
    app.run(transport="stdio")
