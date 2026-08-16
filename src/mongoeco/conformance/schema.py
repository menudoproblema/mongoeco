from __future__ import annotations

import json

from copy import deepcopy
from importlib.resources import files
from typing import Any


CONFORMANCE_REPORT_SCHEMA_RESOURCE = "schemas/conformance-report-v1.json"


def conformance_report_schema() -> dict[str, Any]:
    """Return an owned copy of the public conformance report JSON Schema."""
    resource = files("mongoeco.conformance").joinpath(
        CONFORMANCE_REPORT_SCHEMA_RESOURCE,
    )
    document = json.loads(resource.read_text(encoding="utf-8"))
    if not isinstance(document, dict):  # pragma: no cover - packaged invariant
        message = "conformance report schema must be a JSON object"
        raise TypeError(message)
    return deepcopy(document)
