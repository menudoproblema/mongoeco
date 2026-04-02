from __future__ import annotations

import datetime
from functools import lru_cache
import re
import uuid
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.types import DBRef, Timestamp, is_object_id_like, normalize_object_id


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
    }
    seen: set[str] = set()
    for option in options:
        if option not in supported:
            raise OperationFailure(f"Unsupported regex option: {option}")
        if option in seen:
            raise OperationFailure(f"Duplicate regex option: {option}")
        seen.add(option)
        flags |= supported[option]
    return re.compile(pattern, flags)


def hashable_in_lookup_key(value: Any) -> tuple[str, Any] | None:
    if value is None:
        return ("none", None)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, bytes):
        return ("bytes", value)
    if isinstance(value, datetime.datetime):
        return ("date", value)
    if isinstance(value, uuid.UUID):
        return ("uuid", value)
    if is_object_id_like(value):
        return ("objectid", normalize_object_id(value))
    if isinstance(value, Timestamp):
        return ("timestamp", value)
    return None


def path_mapping(value: Any) -> dict[str, Any] | None:
    if isinstance(value, DBRef):
        return value.as_document()
    if isinstance(value, dict):
        return value
    return None


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
                        if mapping is not None and part in mapping:
                            val = mapping[part]
                            if isinstance(val, list):
                                next_level.append(val)
                                if is_terminal:
                                    next_level.extend(val)
                            else:
                                next_level.append(val)
            else:
                mapping = path_mapping(item)
                if mapping is None or part not in mapping:
                    continue
                val = mapping[part]
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
            if not part.isdigit():
                return False, None
            idx = int(part)
            if 0 <= idx < len(current):
                current = current[idx]
            else:
                return False, None
        else:
            mapping = path_mapping(current)
            if mapping is None or part not in mapping:
                return False, None
            current = mapping[part]
    return True, current


def extract_all_candidates(doc: Any, field: str) -> list[Any]:
    parts = split_path(field)
    if not parts:
        return []

    current_level = [doc]
    for index, part in enumerate(parts):
        next_level: list[Any] = []
        is_digit = part.isdigit()
        idx = int(part) if is_digit else -1
        is_last = index == len(parts) - 1

        for item in current_level:
            if isinstance(item, list):
                if is_digit:
                    if 0 <= idx < len(item):
                        value = item[idx]
                        if is_last:
                            if isinstance(value, list):
                                next_level.extend(value)
                            else:
                                next_level.append(value)
                        elif isinstance(value, list):
                            next_level.append(value)
                            next_level.extend(value)
                        else:
                            next_level.append(value)
                else:
                    for subitem in item:
                        mapping = path_mapping(subitem)
                        if mapping is not None and part in mapping:
                            value = mapping[part]
                            if is_last:
                                if isinstance(value, list):
                                    next_level.extend(value)
                                else:
                                    next_level.append(value)
                            elif isinstance(value, list):
                                next_level.append(value)
                                next_level.extend(value)
                            else:
                                next_level.append(value)
            else:
                mapping = path_mapping(item)
                if mapping is None or part not in mapping:
                    continue
                value = mapping[part]
                if is_last:
                    if isinstance(value, list):
                        next_level.extend(value)
                    else:
                        next_level.append(value)
                elif isinstance(value, list):
                    next_level.append(value)
                    next_level.extend(value)
                else:
                    next_level.append(value)

        if not next_level:
            return []
        current_level = next_level

    return current_level
