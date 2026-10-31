"""BSON value views independent of matching, storage and SDK ownership."""

from typing import Any


def binary_parts(value: Any) -> tuple[int, bytes] | None:
    """Recognize bytes (subtype zero), local, SDK and owned Binary uniformly."""
    if not isinstance(value, bytes):
        return None
    return getattr(value, "subtype", 0), bytes(value)
