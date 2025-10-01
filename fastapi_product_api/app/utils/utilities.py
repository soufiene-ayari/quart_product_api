from utils.mapping import get_fallback_chain
from quart import Response
from pydantic import BaseModel
from collections import defaultdict
from typing import Tuple, Optional, Any
import re
import orjson
def json_response(data, status=200):
    if isinstance(data, BaseModel):
        payload = data.model_dump() if hasattr(data, "model_dump") else data.dict()
    else:
        payload = data
    return Response(orjson.dumps(payload), status=status, mimetype="application/json")
def build_lang_sort_script(lang_chain: list[str]) -> str:
    parts = [f"if (lang.toLowerCase() == '{lang}') return {i};" for i, lang in enumerate(lang_chain)]
    return "def lang = doc['langIso'].value; " + " else ".join(parts) + " else return 999;"

def build_index_list(base_index: str, lang_chain: list[str]) -> list[str]:
    return [f"{base_index}{lang.lower()}" for lang in lang_chain]

def inject_fallback_sort(original_query: dict, lang: str, base_index: str = "systemair_ds_attributes_",collapse_field: str = "attributeParentId") -> tuple[list[str], dict]:
    fallback_chain = get_fallback_chain(lang)
    indices = build_index_list(base_index, fallback_chain)
    sort_script = build_lang_sort_script(fallback_chain)

    final_query = original_query.copy()
    final_query["sort"] = [
        {
            "_script": {
                "type": "number",
                "order": "desc",
                "script": {
                    "lang": "painless",
                    "source": sort_script
                }
            }
        }
    ]
    final_query["collapse"] = {
        "field": collapse_field
    }
    return indices, final_query

def shop_statistics(es_docs):
    """
    Return sorted ["MARKET-005", ...] where, for at least one parentId:
      market == 1  AND  expired == 0   (0 or null are false)
    """
    # matches "market-005" and "market-005-expired"

    _MARKET_RE = re.compile(r"^market-(\d+)(?:-expired)?$")

    market_ok = defaultdict(set)  # suffix -> {parentIds with market==1}
    not_expired = defaultdict(set)  # suffix -> {parentIds with expired==0}
    valid_suffixes = set()

    for doc in es_docs:
        src = doc.get("_source", {}) or {}
        name = src.get("name") or ""
        m = _MARKET_RE.match(name)
        if not m:
            continue

        suffix = m.group(1)  # e.g. "005"
        is_expired_name = name.endswith("-expired")
        parent_id = src.get("parentId")
        if parent_id is None or suffix in valid_suffixes:
            continue

        vals = src.get("values") or []
        raw = next((v.get("value") for v in vals if isinstance(v, dict) and "value" in v), None)

        # normalize: only 1 is true; 0 or None => false
        flag = 1 if (raw == 1 or raw == "1") else 0

        if is_expired_name:
            if flag == 0:  # expired == 0 or None
                if parent_id in market_ok[suffix]:
                    valid_suffixes.add(suffix)
                    market_ok.pop(suffix, None)
                    not_expired.pop(suffix, None)
                else:
                    not_expired[suffix].add(parent_id)
        else:
            if flag == 1:  # market == 1
                if parent_id in not_expired[suffix]:
                    valid_suffixes.add(suffix)
                    market_ok.pop(suffix, None)
                    not_expired.pop(suffix, None)
                else:
                    market_ok[suffix].add(parent_id)

    return sorted(f"MARKET-{s}" for s in valid_suffixes)








def parse_piped_value(s: Any) -> Tuple[Any, Optional[str]]:
    _LAST_BRACKET = re.compile(r"\[(.*?)\]\s*$")
    # Only operate on strings; everything else returned as-is + unit=None
    if not isinstance(s, str):
        return s, None

    m = _LAST_BRACKET.search(s)
    if not m:
        # no trailing [...] → return input unchanged + unit=None
        return s, None

    unit = m.group(1)
    base = s[:m.start()]          # drop trailing [...]
    base = base.replace("|", "")  # remove all pipes
    base = re.sub(r"\s+", " ", base).strip()
    return base, unit
