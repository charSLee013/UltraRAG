import os
import re
from typing import List, Dict, Any, Optional

from ultrarag.server import UltraRAG_MCP_Server

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


if __name__ == "__main__":
    app.run(transport="stdio")
