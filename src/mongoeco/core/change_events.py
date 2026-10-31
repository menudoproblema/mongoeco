"""Committed change contents shared by storage engines, separate from delivery."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mongoeco.core.bson_identity import same_bson_update_value
from mongoeco.core.bson_size import (
    bson_cstring_size,
    bson_document_size,
    bson_element_size,
)
from mongoeco.core.bson_values import document_mapping
from mongoeco.core.operation_context import ChangePublicationPolicy
from mongoeco.core.paths import get_document_value
from mongoeco.core.server_errors import server_error


if TYPE_CHECKING:
    from mongoeco.compat import MongoDialect
    from mongoeco.core.operation_context import OperationContext
    from mongoeco.core.operators import ResolvedInstructionApplication


# Framing of the native delta entry beyond its embedded change document.
_DELTA_METADATA_BYTES = 15
_MISSING = object()
type _Path = tuple[str | int, ...]


def captures_changes(context: OperationContext | None) -> bool:
    return context is not None and context.publication is ChangePublicationPolicy.EMIT


def validate_post_image_options(value):
    if value is None:
        return
    if not isinstance(value, dict):
        message = "changeStreamPreAndPostImages must be an object"
        raise server_error(message, code=14)
    if value.keys() - {"enabled"}:
        message = "Unknown changeStreamPreAndPostImages field"
        raise server_error(message, code=40415)
    if value.get("enabled") is None:
        message = "changeStreamPreAndPostImages requires enabled"
        raise server_error(message, code=40414)
    if not isinstance(value["enabled"], bool):
        message = "changeStreamPreAndPostImages.enabled must be a bool"
        raise server_error(message, code=14)


def post_image_enabled(options: Mapping) -> bool:
    images = options.get("changeStreamPreAndPostImages")
    return isinstance(images, Mapping) and images.get("enabled") is True


def build_change_payload(  # noqa: PLR0913 - complete internal change payload
    *,
    operation_type: str,
    db_name: str,
    coll_name: str,
    document_key: dict,
    full_document: dict | None = None,
    update_description: dict | None = None,
    post_image_available: bool = False,
    to_namespace: dict | None = None,
) -> dict:
    payload = {
        "operation_type": operation_type,
        "db_name": db_name,
        "coll_name": coll_name,
        "document_key": deepcopy(document_key),
        "full_document": deepcopy(full_document),
        "update_description": deepcopy(update_description),
    }
    # Omit the default to keep legacy outbox fingerprints and journal payloads.
    if operation_type == "update" and post_image_available:
        payload["post_image_available"] = True
    if to_namespace is not None:
        payload["to_namespace"] = deepcopy(to_namespace)
    return payload


def namespace_change_payloads(operation_type, db_name, coll_name="", *, new_name=None):
    """DDL effect and its scope-specific invalidation share one commit order."""
    target = None if new_name is None else {"db": db_name, "coll": new_name}
    return [
        build_change_payload(
            operation_type=kind,
            db_name=db_name,
            coll_name=coll_name,
            document_key={},
            to_namespace=target,
        )
        for kind in (operation_type, "invalidate")
    ]


@dataclass(slots=True)
class _Changes:
    array: bool = False
    updated: dict = field(default_factory=dict)
    inserted: dict = field(default_factory=dict)
    removed: list = field(default_factory=list)
    children: dict = field(default_factory=dict)
    length: int | None = None

    @property
    def size(self) -> int:
        if self.array:
            size = 9 + (7 if self.length is not None else 0)
            size += sum(
                1 + bson_element_size(str(i), v) for i, v in self.updated.items()
            )
        else:
            size = 5
            for entries in (self.updated, self.inserted):
                if entries:
                    size += 8 + sum(bson_element_size(k, v) for k, v in entries.items())
            if self.removed:
                size += 8 + sum(
                    bson_element_size(k, False)  # noqa: FBT003 - BSON boolean payload
                    for k in self.removed
                )
        return size + sum(
            2 + bson_cstring_size(str(k)) + child.size
            for k, child in self.children.items()
        )

    def append_description(self, path: _Path, output: dict) -> None:
        def name(parts):
            return ".".join(map(str, parts))

        if self.array:
            if self.length is not None:
                output["truncatedArrays"].append(
                    {"field": name(path), "newSize": self.length}
                )
            for index in sorted(self.updated.keys() | self.children.keys()):
                if index in self.updated:
                    output["updatedFields"][name((*path, index))] = self.updated[index]
                else:
                    self.children[index].append_description((*path, index), output)
            return
        for entries in (self.updated, self.inserted):
            for key, value in entries.items():
                output["updatedFields"][name((*path, key))] = value
        output["removedFields"].extend(name((*path, key)) for key in self.removed)
        for key, child in self.children.items():
            child.append_description((*path, key), output)

    def description(self) -> dict:
        output = {"updatedFields": {}, "removedFields": [], "truncatedArrays": []}
        self.append_description((), output)
        return output


def _pipeline_changes(before: Any, after: Any, *, bounded=True) -> _Changes | None:
    old, new = document_mapping(before), document_mapping(after)
    if old is not None and new is not None:
        node = _Changes()
        old_items, new_items = list(old.items()), list(new.items())
        i = j = 0
        deferred_deletes = set()
        while i < len(old_items) and j < len(new_items):
            key, value = old_items[i]
            new_key, new_value = new_items[j]
            if key != new_key:
                deferred_deletes.add(key)
                i += 1
                continue
            if not same_bson_update_value(value, new_value):
                _pipeline_value(node, key, value, new_value)
            i += 1
            j += 1
        node.removed.extend(key for key, _ in old_items[i:])
        for key, value in new_items[j:]:
            node.inserted[key] = value
            deferred_deletes.discard(key)
        node.removed.extend(sorted(deferred_deletes))
        post_size = bson_document_size(new)
    elif isinstance(before, list) and isinstance(after, list):
        node = _Changes(array=True)
        for i, value in enumerate(after):
            if i >= len(before):
                node.updated[i] = value
            elif not same_bson_update_value(before[i], value):
                _pipeline_value(node, i, before[i], value)
        if len(after) < len(before):
            node.length = len(after)
        post_size = 5 + sum(bson_element_size(str(i), v) for i, v in enumerate(after))
    else:
        return None
    return node if not bounded or node.size < post_size else None


def _pipeline_value(node: _Changes, key: str | int, before: Any, after: Any) -> None:
    child = _pipeline_changes(before, after)
    if child is None:
        node.updated[key] = after
    else:
        node.children[key] = child


def make_mutation_change(
    before: dict | None,
    after: dict,
    operation_type: str,
    description: dict | None = None,
    *,
    calculated: dict | None = None,
) -> dict:
    key_document = before if before is not None else after
    key = {"_id": key_document["_id"]} if "_id" in key_document else {}
    kind = "insert" if before is None else operation_type
    if kind == "update" and description is None:
        comparison_image = after if calculated is None else calculated
        changes = _pipeline_changes(before, comparison_image, bounded=False)
        if (
            changes is None
            or changes.size + _DELTA_METADATA_BYTES
            >= bson_document_size(comparison_image)
        ):
            kind = "replace"
        else:
            description = changes.description()
    return {
        "operation_type": kind,
        "document_key": key,
        "full_document": after,
        "update_description": description if kind == "update" else None,
    }


def committed_change_content(
    context: OperationContext | None,
    before: dict | None,
    after: dict,
    operation_type: str,
    change: dict | None = None,
) -> dict:
    if captures_changes(context):
        return (
            change
            if change is not None
            else make_mutation_change(before, after, operation_type)
        )
    key_document = before if before is not None else after
    return {
        "operation_type": "insert" if before is None else operation_type,
        "document_key": {"_id": key_document["_id"]} if "_id" in key_document else {},
        "full_document": after,
        "update_description": None,
    }


def _path_value(document: Any, path: _Path) -> Any:
    current = document
    for part in path:
        mapping = document_mapping(current)
        if mapping is not None and isinstance(part, str) and part in mapping:
            current = mapping[part]
        elif (
            isinstance(current, list)
            and isinstance(part, int)
            and 0 <= part < len(current)
        ):
            current = current[part]
        else:
            return _MISSING
    return current


def _typed_path(document: dict, path: str) -> tuple[_Path, _Path | None]:
    current = document
    route = []
    first_new = None
    for part in path.split("."):
        component = int(part) if isinstance(current, list) and part.isdigit() else part
        route.append(component)
        current = _path_value(document, tuple(route))
        if current is _MISSING and first_new is None:
            first_new = tuple(route)
    return tuple(route), first_new


def _record(node: _Changes, route: _Path, value: Any, *, inserted=False) -> None:
    for i, part in enumerate(route[:-1]):
        # An atomic record for a created/replaced parent covers its descendants.
        if part in node.updated or part in node.inserted:
            return
        node = node.children.setdefault(
            part, _Changes(array=isinstance(route[i + 1], int))
        )
    key = route[-1]
    if value is _MISSING:
        if key not in node.removed:
            node.removed.append(key)
    elif node.array or not inserted:
        node.updated[key] = value
    else:
        node.inserted[key] = value
    node.children.pop(key, None)


def classic_update_description(  # noqa: PLR0912 - native classic operator descriptions
    before: dict,
    after: dict,
    applications: Sequence[ResolvedInstructionApplication],
    *,
    dialect: MongoDialect,
    applied_paths: set[str],
) -> dict:
    records = []
    positional = {}
    for application in applications:
        instruction = application.instruction
        paths = [target.concrete_path for target in application.targets]
        for path in paths:
            route, created = _typed_path(before, path)
            old, new = _path_value(before, route), _path_value(after, route)
            for i, segment in enumerate(instruction.path.segments):
                if segment.kind in {"all_positional", "filtered_positional"}:
                    group = positional.setdefault(route[:i], [set(), set()])
                    group[0].add(route[i])
                    if path in applied_paths:
                        group[1].add(route[i])
            if path not in applied_paths:
                continue
            if (
                instruction.operator == "$push"
                and isinstance(old, list)
                and isinstance(new, list)
            ):
                modifiers = (
                    instruction.value if isinstance(instruction.value, Mapping) else {}
                )
                if not modifiers.keys() & {"$slice", "$sort", "$position"}:
                    records.extend(
                        ((*route, i), new[i], False) for i in range(len(old), len(new))
                    )
                    continue
            if (
                instruction.operator == "$addToSet"
                and dialect.key in {"8.0", "9.0"}
                and isinstance(old, list)
                and isinstance(new, list)
            ):
                records.extend(
                    ((*route, i), new[i], False) for i in range(len(old), len(new))
                )
                continue
            target = created or route
            records.append((target, _path_value(after, target), created is not None))
        if (
            instruction.target_path is not None
            and instruction.path.raw in applied_paths
        ):
            found, _ = get_document_value(before, instruction.path.raw)
            if found:
                route, created = _typed_path(before, instruction.target_path.raw)
                target = created or route
                records.append(
                    (target, _path_value(after, target), created is not None)
                )
    for prefix, (matched, changed) in sorted(
        positional.items(), key=lambda item: len(item[0])
    ):
        if len(matched) <= 1 or not changed:
            continue
        target = prefix if len(changed) > 1 else (*prefix, next(iter(changed)))
        records = [record for record in records if record[0][: len(target)] != target]
        records.append((target, _path_value(after, target), False))
    node = _Changes()
    for route, value, inserted in records:
        _record(node, route, value, inserted=inserted)
    return node.description()
