from collections.abc import Mapping
from typing import Any

_MISSING = object()

def deep_get(data: Mapping[str, Any], *keys: str, default: Any = _MISSING) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, Mapping) or key not in cur:
            return default
        cur = cur[key]
    return cur
