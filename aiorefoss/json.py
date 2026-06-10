"""JSON helper.

Uses orjson for better performance if available, falls back to standard library json.
"""

import json
from typing import Any

try:
    import orjson

    JSONDecodeError = orjson.JSONDecodeError
    json_loads = orjson.loads
    _has_orjson = True

    def json_encoder_default(obj: Any) -> Any:
        """Convert objects for orjson."""
        if isinstance(obj, set | tuple):
            return list(obj)
        raise TypeError

    def json_dumps(data: Any) -> str:
        """Dump json string using orjson."""
        return orjson.dumps(
            data,
            option=orjson.OPT_NON_STR_KEYS,
            default=json_encoder_default,
        ).decode("utf-8")

    def json_bytes(data: Any) -> bytes:
        """Dump json bytes using orjson."""
        return orjson.dumps(
            data,
            option=orjson.OPT_NON_STR_KEYS,
            default=json_encoder_default,
        )

except ImportError:
    # Fallback to standard library json
    JSONDecodeError = json.JSONDecodeError
    json_loads = json.loads
    _has_orjson = False

    def _json_encoder_default(obj: Any) -> Any:
        """Convert objects for standard json."""
        if isinstance(obj, set | tuple):
            return list(obj)
        raise TypeError

    def json_dumps(data: Any) -> str:
        """Dump json string using standard library json."""
        return json.dumps(data, default=_json_encoder_default)

    def json_bytes(data: Any) -> bytes:
        """Dump json bytes using standard library json."""
        return json.dumps(data, default=_json_encoder_default).encode("utf-8")
