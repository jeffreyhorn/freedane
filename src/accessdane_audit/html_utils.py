from __future__ import annotations


def html_attr_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [item for item in value if isinstance(item, str)]
        return " ".join(parts)
    return ""
