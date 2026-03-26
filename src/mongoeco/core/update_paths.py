from dataclasses import dataclass
import re
from typing import Literal

from mongoeco.errors import OperationFailure


_ARRAY_FILTER_IDENTIFIER_RE = re.compile(r"^[a-z][A-Za-z0-9]*$")


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


def update_path_has_numeric_segment(path: str) -> bool:
    return any(segment.kind == "index" for segment in parse_update_path(path))


def update_path_has_positional_segment(path: str) -> bool:
    return any(
        segment.kind in {"positional", "all_positional", "filtered_positional"}
        for segment in parse_update_path(path)
    )
