"""JSON-safe value helpers for task payloads."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

_ALLOWED_CONTROL_CHARS = {"\n", "\r", "\t"}


def json_safe_string(value: Any) -> str:
    return "".join(
        ch
        if ch >= " " or ch in _ALLOWED_CONTROL_CHARS
        else f"\\u{ord(ch):04x}"
        for ch in str(value)
    )


def json_safe_value(value: Any) -> Any:
    if isinstance(value, str):
        return json_safe_string(value)
    if isinstance(value, dict):
        return {
            json_safe_string(k): json_safe_value(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [json_safe_value(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe_value(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
