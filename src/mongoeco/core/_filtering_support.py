from __future__ import annotations

import datetime
import re

from functools import lru_cache
from typing import TYPE_CHECKING, Any

from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.bson_values import binary_parts, document_mapping as path_mapping
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    Regex,
    Timestamp,
    is_object_id_like,
    normalize_object_id,
)


if TYPE_CHECKING:
    from collections.abc import Mapping


@lru_cache(maxsize=4096)
def split_path(path: str) -> tuple[str, ...]:
    if not path:
        return ()
    return tuple(path.split("."))


@lru_cache(maxsize=256)
def compile_regex(pattern: str, options: str) -> re.Pattern[str]:
    flags = 0
    supported = {
        "i": re.IGNORECASE,
        "m": re.MULTILINE,
        "s": re.DOTALL,
        "x": re.VERBOSE,
        "u": re.UNICODE,
    }
    seen: set[str] = set()
    for option in options:
        if option not in supported:
            raise OperationFailure(f"Unsupported regex option: {option}")
        if option in seen:
            raise OperationFailure(f"Duplicate regex option: {option}")
        seen.add(option)
        flags |= supported[option]
    try:
        return re.compile(pattern, flags)
    except re.error as error:
        message = f"Regular expression is invalid: {error}"
        raise server_error(message, code=51091) from error


def compile_bson_regex(value: Regex) -> re.Pattern[str]:
    # Preserve Regex.compile's treatment of BSON locale/Unicode flags.
    options = "".join(flag for flag in value.flags if flag in "imsx")
    return compile_regex(value.pattern, options)


def hashable_in_lookup_key(value: Any) -> tuple[str, Any] | None:
    if value is None:
        return ("none", None)
    if isinstance(value, bool):
        return ("bool", value)
    binary = binary_parts(value)
    if binary is not None:
        return ("bytes", binary[1]) if binary[0] == 0 else ("binary", binary)
    if isinstance(value, datetime.datetime):
        return ("date", value)
    if is_object_id_like(value):
        return ("objectid", normalize_object_id(value))
    if isinstance(value, Timestamp):
        return ("timestamp", value)
    return None


def _mapping_field(mapping: Mapping[str, Any], field: str) -> tuple[bool, Any]:
    if field not in mapping:
        return False, None
    return True, mapping[field]


def extract_values(doc: Any, path: str) -> list[Any]:
    doc_mapping = path_mapping(doc)
    if doc_mapping is not None and doc_mapping is not doc:
        return extract_values(doc_mapping, path)

    parts = split_path(path)
    if not parts:
        if isinstance(doc, list):
            values: list[Any] = [doc]
            for item in doc:
                values.extend(extract_values(item, path))
            return values
        return []

    current_level = [doc]
    for index, part in enumerate(parts):
        next_level = []
        is_digit = part.isdigit()
        idx = int(part) if is_digit else -1
        is_terminal = index == len(parts) - 1

        for item in current_level:
            if isinstance(item, list):
                if is_digit:
                    if 0 <= idx < len(item):
                        value = item[idx]
                        if isinstance(value, list):
                            next_level.append(value)
                            if is_terminal:
                                next_level.extend(value)
                        else:
                            next_level.append(value)
                else:
                    for subitem in item:
                        mapping = path_mapping(subitem)
                        if mapping is None:
                            continue
                        found, val = _mapping_field(mapping, part)
                        if not found:
                            continue
                        if isinstance(val, list):
                            next_level.append(val)
                            if is_terminal:
                                next_level.extend(val)
                        else:
                            next_level.append(val)
            else:
                mapping = path_mapping(item)
                if mapping is None:
                    continue
                found, val = _mapping_field(mapping, part)
                if not found:
                    continue
                if isinstance(val, list):
                    next_level.append(val)
                    if is_terminal:
                        next_level.extend(val)
                else:
                    next_level.append(val)

        if not next_level:
            return []
        current_level = next_level

    return current_level


def _resolve_indexed_value(value: list[Any], part: str) -> tuple[bool, Any]:
    if not part.isdigit():
        return False, None
    index = int(part)
    if index >= len(value):
        return False, None
    return True, value[index]


def get_field_value(doc: Any, path: str) -> tuple[bool, Any]:
    doc_mapping = path_mapping(doc)
    if doc_mapping is not None and doc_mapping is not doc:
        return get_field_value(doc_mapping, path)

    parts = split_path(path)
    if not parts:
        return True, doc

    current = doc
    for part in parts:
        if isinstance(current, list):
            found, current = _resolve_indexed_value(current, part)
        else:
            mapping = path_mapping(current)
            if mapping is None:
                return False, None
            found, current = _mapping_field(mapping, part)
        if not found:
            return False, None
    return True, current


class ArrayElementDocument(dict):
    """Bind an elemMatch operand without implicit traversal of that operand."""


_MAX_ARRAY_INDEX_DIGITS = 20


def query_field_values(doc: Any, path: str) -> list[tuple[Any, bool]]:
    """MQL terminal values and whether a named field permits array traversal.

    Missing object fields yield MISSING. An untraversable array yields no leaf,
    which is distinct for null/negation. A numeric segment can address both an
    array position and a numeric object field. Explicit positions are atomic.
    This traversal is separate from observable/index and update path access.
    """
    if isinstance(doc, ArrayElementDocument) and path == "value":
        return [(doc["value"], False)]

    def walk(value, parts, *, indexed=False):
        part, *rest = parts
        if isinstance(value, list):
            leaves = []
            canonical_index = part == "0" or (
                part and part[0] in "123456789" and part.isascii() and part.isdecimal()
            )
            if canonical_index and len(part) <= _MAX_ARRAY_INDEX_DIGITS:
                position = int(part)
                if position < len(value):
                    leaves.extend(
                        walk(value[position], rest, indexed=True)
                        if rest
                        else [(value[position], False)]
                    )
            for item in value:
                if path_mapping(item) is not None:
                    leaves.extend(walk(item, parts))
            return leaves
        mapping = path_mapping(value)
        if mapping is None:
            return [] if indexed else [(MISSING, False)]
        if part not in mapping:
            return [(MISSING, False)]
        child = mapping[part]
        return walk(child, rest) if rest else [(child, True)]

    return walk(doc, path.split("."))


def query_values(doc: Any, path: str) -> list[Any]:
    candidates = []
    for value, traverse in query_field_values(doc, path):
        candidates.append(value)
        if traverse and isinstance(value, list):
            candidates.extend(value)
    return candidates
