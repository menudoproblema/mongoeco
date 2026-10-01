"""Pure specification parsing shared by preparation and information handlers."""

from mongoeco.errors import OperationFailure


INFORMATION_STAGES = frozenset(
    {"$collStats", "$indexStats", "$currentOp", "$planCacheStats", "$listSessions"}
)


def parse_information_spec(operator: str, spec: object) -> tuple[bool, int]:
    """Return count selection and storage scale without executing a handler."""
    if not isinstance(spec, dict):
        message = f"{operator} requires a document specification"
        raise OperationFailure(message)
    if operator != "$collStats":
        if spec:
            message = f"{operator} local runtime supports only an empty document"
            raise OperationFailure(message)
        return False, 1
    unsupported = sorted(set(spec) - {"count", "storageStats"})
    if unsupported:
        message = (
            "$collStats local runtime supports only count and storageStats; "
            "unsupported keys: " + ", ".join(unsupported)
        )
        raise OperationFailure(message)
    if not spec:
        message = "$collStats requires at least one of count or storageStats"
        raise OperationFailure(message)
    include_count = "count" in spec
    if include_count and spec["count"] != {}:
        message = "$collStats.count must be an empty document"
        raise OperationFailure(message)
    scale = 1
    if "storageStats" in spec:
        storage_spec = spec["storageStats"]
        if not isinstance(storage_spec, dict):
            message = "$collStats.storageStats must be a document"
            raise OperationFailure(message)
        unsupported_storage = sorted(set(storage_spec) - {"scale"})
        if unsupported_storage:
            message = (
                "$collStats.storageStats local runtime supports only scale; "
                "unsupported keys: " + ", ".join(unsupported_storage)
            )
            raise OperationFailure(message)
        scale = storage_spec.get("scale", 1)
        if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
            message = "$collStats.storageStats.scale must be a positive integer"
            raise OperationFailure(message)
    return include_count, scale
