import json
import os
from functools import lru_cache
from typing import Any

try:
    import orjson
except ImportError:  # pragma: no cover - optional dependency
    orjson = None


_JSON_BACKEND_ENV_VAR = "MONGOECO_JSON_BACKEND"


@lru_cache(maxsize=1)
def _resolve_json_backend_name() -> str:
    configured = os.getenv(_JSON_BACKEND_ENV_VAR, "stdlib").strip().lower()
    if configured == "stdlib":
        return "stdlib"
    if configured == "auto":
        return "orjson" if orjson is not None else "stdlib"
    if configured == "orjson":
        if orjson is None:
            raise RuntimeError(
                "MONGOECO_JSON_BACKEND=orjson requires the optional 'orjson' dependency"
            )
        return "orjson"
    raise ValueError(
        f"Unsupported {_JSON_BACKEND_ENV_VAR} value: {configured!r}. "
        "Expected 'stdlib', 'auto', or 'orjson'."
    )


def get_json_backend_name() -> str:
    return _resolve_json_backend_name()


def json_dumps_compact(value: Any, *, sort_keys: bool = False) -> str:
    if get_json_backend_name() != "orjson":
        return json.dumps(value, separators=(",", ":"), sort_keys=sort_keys)
    option = orjson.OPT_SORT_KEYS if sort_keys else 0
    return orjson.dumps(value, option=option).decode("utf-8")


def json_loads(value: str | bytes | bytearray | memoryview) -> Any:
    if get_json_backend_name() != "orjson":
        return json.loads(value)
    return orjson.loads(value)
