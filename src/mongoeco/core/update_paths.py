from dataclasses import dataclass
import re
from typing import Callable, Literal

from mongoeco.errors import OperationFailure


_ARRAY_FILTER_IDENTIFIER_RE = re.compile(r"^[a-z][A-Za-z0-9]*$")
_MISSING = object()


@dataclass(frozen=True, slots=True)
class UpdatePathSegment:
    kind: Literal["field", "index", "positional", "all_positional", "filtered_positional"]
    raw: str
    index: int | None = None
    identifier: str | None = None


def parse_update_path(path: str) -> tuple[UpdatePathSegment, ...]:
    if not isinstance(path, str):
        raise OperationFailure("update field names must be strings")
    if path == "":
        raise OperationFailure("update field names must not be empty")

    segments = path.split(".")
    if any(segment == "" for segment in segments):
        raise OperationFailure("update field names must not contain empty path segments")

    parsed: list[UpdatePathSegment] = []
    for segment in segments:
        if segment.isdigit():
            parsed.append(UpdatePathSegment("index", segment, index=int(segment)))
            continue
        if segment == "$":
            parsed.append(UpdatePathSegment("positional", segment))
            continue
        if segment == "$[]":
            parsed.append(UpdatePathSegment("all_positional", segment))
            continue
        if segment.startswith("$[") and segment.endswith("]"):
            identifier = segment[2:-1]
            if not _ARRAY_FILTER_IDENTIFIER_RE.match(identifier):
                raise OperationFailure("array filter identifiers must begin with a lowercase letter and contain only alphanumerics")
            parsed.append(
                UpdatePathSegment(
                    "filtered_positional",
                    segment,
                    identifier=identifier,
                )
            )
            continue
        parsed.append(UpdatePathSegment("field", segment))
    return tuple(parsed)


def is_valid_array_filter_identifier(identifier: str) -> bool:
    return bool(_ARRAY_FILTER_IDENTIFIER_RE.match(identifier))


def update_path_has_numeric_segment(path: str) -> bool:
    return any(segment.kind == "index" for segment in parse_update_path(path))


def update_path_has_positional_segment(path: str) -> bool:
    return any(
        segment.kind in {"positional", "all_positional", "filtered_positional"}
        for segment in parse_update_path(path)
    )


def expand_positional_update_paths(
    doc: object,
    path: str,
    *,
    filtered_matcher: Callable[[str, object], bool],
) -> list[str]:
    segments = parse_update_path(path)
    if not update_path_has_positional_segment(path):
        return [path]

    expanded: list[str] = []

    def _walk(current: object, index: int, built: list[str]) -> None:
        if index == len(segments):
            expanded.append(".".join(built))
            return

        segment = segments[index]
        if segment.kind == "field":
            next_current = _MISSING
            if isinstance(current, dict) and segment.raw in current:
                next_current = current[segment.raw]
            built.append(segment.raw)
            _walk(next_current, index + 1, built)
            built.pop()
            return

        if segment.kind == "index":
            next_current = _MISSING
            if isinstance(current, list) and segment.index is not None and segment.index < len(current):
                next_current = current[segment.index]
            built.append(segment.raw)
            _walk(next_current, index + 1, built)
            built.pop()
            return

        if segment.kind == "positional":
            raise OperationFailure("Legacy positional '$' update paths are not supported")

        if not isinstance(current, list):
            return

        if segment.kind == "all_positional":
            for item_index, item in enumerate(current):
                built.append(str(item_index))
                _walk(item, index + 1, built)
                built.pop()
            return

        if segment.kind == "filtered_positional":
            for item_index, item in enumerate(current):
                if segment.identifier is None or not filtered_matcher(segment.identifier, item):
                    continue
                built.append(str(item_index))
                _walk(item, index + 1, built)
                built.pop()
            return

        raise AssertionError(f"Unexpected update path segment kind: {segment.kind}")

    _walk(doc, 0, [])
    return expanded
