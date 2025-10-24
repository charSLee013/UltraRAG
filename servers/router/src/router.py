from typing import List, Dict, Any

from ultrarag.server import UltraRAG_MCP_Server


app = UltraRAG_MCP_Server("router")


@app.tool(output="query_list")
def route1(query_list: List[str]) -> Dict[str, List[Dict[str, str]]]:
    query = [
        {"data": query, "state": "state1" if int(query) == 1 else "state2"}
        for query in query_list
    ]
    return {"query_list": query}


@app.tool(output="query_list")
def route2(query_list: List[str]) -> Dict[str, List[Dict[str, str]]]:
    query = [{"data": query, "state": "state2"} for query in query_list]
    return {"query_list": query}


@app.tool(output="ans_ls->ans_ls")
def ircot_check_end(ans_ls: List[str]) -> Dict[str, List[Dict[str, str]]]:
    ans_ls = [
        {
            "data": ans,
            "state": "complete" if "so the answer is" in ans.lower() else "incomplete",
        }
        for ans in ans_ls
    ]
    return {"ans_ls": ans_ls}


@app.tool(output="ans_ls->ans_ls")
def search_r1_check(ans_ls: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Check if the answer is complete or incomplete.
    Args:
        ans_ls (list): List of answers to check.
    Returns:
        dict: Dictionary containing the list of answers with their states.
    """

    def get_eos(text):
        import re

        if "<|endoftext|>" in text or "<|im_end|>" in text:
            return True
        else:
            return False

    ans_ls = [
        {
            "data": answer,
            "state": "complete" if get_eos(answer) else "incomplete",
        }
        for answer in ans_ls
    ]
    return {"ans_ls": ans_ls}


@app.tool(output="page_ls->page_ls")
def webnote_check_page(page_ls: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Check if the page is complete or incomplete.
    Args:
        page_ls (list): List of pages to check.
    Returns:
        dict: Dictionary containing the list of pages with their states.
    """
    page_ls = [
        {
            "data": page,
            "state": "incomplete" if "to be filled" in page.lower() else "complete",
        }
        for page in page_ls
    ]
    return {"page_ls": page_ls}


@app.tool(output="ans_ls->ans_ls")
def r1_searcher_check(ans_ls: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Check if the answer is complete or incomplete.
    Args:
        ans_ls (list): List of answers to check.
    Returns:
        dict: Dictionary containing the list of answers with their states.
    """

    def get_eos(text):
        import re

        if "<|endoftext|>" in text or "<|im_end|>" in text or "</answer>" in text:
            return True
        else:
            return False

    ans_ls = [
        {
            "data": answer,
            "state": "complete" if get_eos(answer) else "incomplete",
        }
        for answer in ans_ls
    ]
    return {"ans_ls": ans_ls}


def _contract_token(contract: Dict[str, Any], key: str) -> str:
    value = contract.get(key) if contract else None
    if isinstance(value, str) and value:
        return value
    return ""


@app.tool(output="ans_ls,token_contract->ans_ls")
def search_o1_check(
    ans_ls: List[str],
    token_contract: Dict[str, Any],
) -> Dict[str, List[Dict[str, str]]]:
    end_answer = _contract_token(token_contract, "end_answer")
    end_query = _contract_token(token_contract, "end_query")

    def route_state(text: str) -> str:
        if end_answer and end_answer in text:
            return "stop"
        if end_query and end_query in text:
            return "retrieve"
        return "retrieve"

    routed = [
        {
            "data": answer,
            "state": route_state(answer),
        }
        for answer in ans_ls
    ]
    states = [entry["state"] for entry in routed]
    app.logger.info(
        "[search_o1_check] contract_end_answer=%s, end_query=%s, states=%s",
        end_answer,
        end_query,
        states,
    )
    return {"ans_ls": routed}


@app.tool(output="ans_ls,token_contract,route,sql_source_type,sql_owner_regex->ans_ls,router_source_type,router_owner_regex")
def search_o1_route_select(
    ans_ls: List[str],
    token_contract: Dict[str, Any],
    route: str | None = None,
    sql_source_type: str | None = None,
    sql_owner_regex: str | None = None,
) -> Dict[str, List[Dict[str, str]]]:
    """Select route for Search‑o1: retrieve (vector) / retrieve_sql / stop.

    Rules:
      - end_answer → stop
      - explicit route == 'sqlite' → retrieve_sql
      - else if sql_source_type or sql_owner_regex set → retrieve_sql
      - else → retrieve (vector)
    """

    end_answer = _contract_token(token_contract, "end_answer")

    def _parse_hints(text: str) -> dict:
        # naive key=value parser for hints like: mode=sqlite; source_type=models; owner_regex='qwen'
        hints = {}
        for part in text.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                k = k.strip().lower()
                v = v.strip().strip("'\"")
                hints[k] = v
        return hints

    parsed_source_type = sql_source_type
    parsed_owner_regex = sql_owner_regex

    def route_state(text: str) -> str:
        nonlocal parsed_source_type, parsed_owner_regex
        if end_answer and end_answer in text:
            return "stop"
        hints = _parse_hints(text)
        if not parsed_source_type:
            parsed_source_type = hints.get("source_type", parsed_source_type)
        if not parsed_owner_regex:
            parsed_owner_regex = hints.get("owner_regex", parsed_owner_regex)
        hinted_mode = (hints.get("mode") or "").lower()
        if (route or "").lower() == "sqlite" or hinted_mode == "sqlite" or (parsed_source_type or parsed_owner_regex):
            return "retrieve_sql"
        return "retrieve"

    routed = [
        {
            "data": answer,
            "state": route_state(answer),
        }
        for answer in ans_ls
    ]
    # To satisfy array-typed output validators, wrap scalars into single-item lists.
    out_src = [parsed_source_type] if parsed_source_type else []
    out_rex = [parsed_owner_regex] if parsed_owner_regex else []
    return {"ans_ls": routed, "router_source_type": out_src, "router_owner_regex": out_rex}


if __name__ == "__main__":
    app.run(transport="stdio")
