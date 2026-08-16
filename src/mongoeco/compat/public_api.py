from __future__ import annotations

import inspect

from dataclasses import MISSING, fields, is_dataclass
from enum import Enum
from importlib import import_module
from importlib.resources import files

from mongoeco._version import __version__
from mongoeco.compat.deprecations import DEPRECATION_CATALOG_SCHEMA_VERSION
from mongoeco.conformance.models import CONFORMANCE_REPORT_SCHEMA_VERSION


PUBLIC_API_MANIFEST_SCHEMA_VERSION = "mongoeco-public-api/v1"
PUBLIC_API_MODULES = (
    "mongoeco",
    "mongoeco.api",
    "mongoeco.compat",
    "mongoeco.cxp",
    "mongoeco.engines",
    "mongoeco.conformance",
)


def _annotation_document(annotation: object) -> dict[str, object]:
    if annotation is inspect.Signature.empty:
        return {"rendered": None, "nullable": None}
    rendered = inspect.formatannotation(annotation)
    return {"rendered": rendered, "nullable": "None" in rendered}


def _default_document(value: object) -> dict[str, object]:
    if value is inspect.Signature.empty or value is MISSING:
        return {"required": True, "rendered": None}
    if value is None or isinstance(value, str | int | float | bool):
        rendered = repr(value)
    elif isinstance(value, Enum):
        rendered = f"{type(value).__module__}.{type(value).__qualname__}.{value.name}"
    else:
        rendered = f"<{type(value).__module__}.{type(value).__qualname__}>"
    return {"required": False, "rendered": rendered}


def _callable_document(value: object) -> dict[str, object]:
    try:
        signature = inspect.signature(value)
    except (TypeError, ValueError):
        return {
            "async": inspect.iscoroutinefunction(value),
            "parameters": [],
            "return": _annotation_document(inspect.Signature.empty),
            "signatureAvailable": False,
        }
    parameters = []
    for parameter in signature.parameters.values():
        default = _default_document(parameter.default)
        parameters.append(
            {
                "name": parameter.name,
                "kind": parameter.kind.name.lower(),
                "annotation": _annotation_document(parameter.annotation),
                "required": default["required"],
                "default": default["rendered"],
            }
        )
    return {
        "async": inspect.iscoroutinefunction(value),
        "parameters": parameters,
        "return": _annotation_document(signature.return_annotation),
        "signatureAvailable": True,
    }


def _class_members(value: type[object]) -> dict[str, object]:
    if issubclass(value, Enum):
        return {}
    classes = value.__mro__ if getattr(value, "_is_protocol", False) else (value,)
    names = {
        name for base in classes for name in vars(base) if not name.startswith("_")
    }
    members: dict[str, object] = {}
    for name in sorted(names):
        try:
            static = inspect.getattr_static(value, name)
        except AttributeError:
            continue
        descriptor_kind = "method"
        if isinstance(static, staticmethod | classmethod):
            static = static.__func__
        elif isinstance(static, property):
            members[name] = {"kind": "property"}
            continue
        if not callable(static):
            descriptor_kind = "attribute"
            members[name] = {"kind": descriptor_kind}
            continue
        members[name] = {"kind": descriptor_kind, **_callable_document(static)}
    return members


def _dataclass_fields(value: type[object]) -> list[dict[str, object]]:
    if not is_dataclass(value):
        return []
    result = []
    for item in fields(value):
        default = _default_document(item.default)
        if item.default_factory is not MISSING:
            default = {
                "required": False,
                "rendered": (
                    f"<factory:{item.default_factory.__module__}."
                    f"{item.default_factory.__qualname__}>"
                ),
            }
        result.append(
            {
                "name": item.name,
                "annotation": _annotation_document(item.type),
                "required": default["required"],
                "default": default["rendered"],
            }
        )
    return result


def _symbol_document(value: object) -> dict[str, object]:
    document: dict[str, object] = {
        "definedIn": getattr(value, "__module__", type(value).__module__),
        "qualifiedName": getattr(value, "__qualname__", type(value).__qualname__),
    }
    if inspect.isclass(value):
        document.update(
            {
                "kind": "class",
                "callable": _callable_document(value),
                "dataclass": is_dataclass(value),
                "fields": _dataclass_fields(value),
                "protocol": bool(getattr(value, "_is_protocol", False)),
                "members": _class_members(value),
            }
        )
        if issubclass(value, Enum):
            document["enumValues"] = [
                {"name": item.name, "value": item.value} for item in value
            ]
        return document
    if callable(value):
        return {"kind": "function", **document, **_callable_document(value)}
    return {"kind": "value", **document, "valueType": type(value).__qualname__}


def public_api_manifest() -> dict[str, object]:
    modules: dict[str, object] = {}
    for module_name in PUBLIC_API_MODULES:
        module = import_module(module_name)
        exports = tuple(sorted(getattr(module, "__all__", ())))
        modules[module_name] = {
            "exports": list(exports),
            "symbols": {
                name: _symbol_document(getattr(module, name)) for name in exports
            },
        }
    package_root = files("mongoeco")
    resources = [
        path
        for path in (
            "py.typed",
            "compat/resources/deprecations-v1.json",
            "compat/schemas/deprecations-v1.schema.json",
            "conformance/schemas/conformance-report-v1.json",
        )
        if package_root.joinpath(path).is_file()
    ]
    return {
        "schemaVersion": PUBLIC_API_MANIFEST_SCHEMA_VERSION,
        "packageVersion": __version__,
        "contracts": {
            "conformanceReport": CONFORMANCE_REPORT_SCHEMA_VERSION,
            "deprecationCatalog": DEPRECATION_CATALOG_SCHEMA_VERSION,
            "engineSpi": [1, 2],
            "search": ["search-v1"],
        },
        "resources": resources,
        "modules": modules,
    }


def _change(
    classification: str,
    path: str,
    before: object,
    after: object,
) -> dict[str, object]:
    return {
        "classification": classification,
        "path": path,
        "before": before,
        "after": after,
    }


def _compare_parameters(
    path: str,
    before: dict[str, object],
    after: dict[str, object],
) -> list[dict[str, object]]:
    changes = []
    before_parameters = before.get("parameters")
    after_parameters = after.get("parameters")
    before_shapes = [(item["name"], item["kind"]) for item in before_parameters or []]
    after_shapes = [(item["name"], item["kind"]) for item in after_parameters or []]
    if before_shapes != after_shapes:
        changes.append(
            _change(
                "signature-change",
                f"{path}.parameters",
                before_parameters,
                after_parameters,
            )
        )
    else:
        for previous, current in zip(
            before_parameters or [],
            after_parameters or [],
            strict=True,
        ):
            parameter_path = f"{path}.parameters.{previous['name']}"
            if previous["annotation"] != current["annotation"]:
                changes.append(
                    _change(
                        "type-change",
                        f"{parameter_path}.annotation",
                        previous["annotation"],
                        current["annotation"],
                    )
                )
            previous_default = (previous["required"], previous["default"])
            current_default = (current["required"], current["default"])
            if previous_default != current_default:
                changes.append(
                    _change(
                        "default-change",
                        f"{parameter_path}.default",
                        previous_default,
                        current_default,
                    )
                )
    if before.get("return") != after.get("return"):
        changes.append(
            _change(
                "type-change",
                f"{path}.return",
                before.get("return"),
                after.get("return"),
            )
        )
    if before.get("async") != after.get("async"):
        changes.append(
            _change(
                "async-change",
                f"{path}.async",
                before.get("async"),
                after.get("async"),
            )
        )
    return changes


def _compare_fields(
    path: str,
    before: list[dict[str, object]],
    after: list[dict[str, object]],
) -> list[dict[str, object]]:
    changes = []
    before_by_name = {field["name"]: field for field in before}
    after_by_name = {field["name"]: field for field in after}
    if tuple(before_by_name) != tuple(after_by_name):
        return [_change("signature-change", path, before, after)]
    for name, previous in before_by_name.items():
        current = after_by_name[name]
        if previous["annotation"] != current["annotation"]:
            changes.append(
                _change(
                    "type-change",
                    f"{path}.{name}.annotation",
                    previous["annotation"],
                    current["annotation"],
                )
            )
        previous_default = (previous["required"], previous["default"])
        current_default = (current["required"], current["default"])
        if previous_default != current_default:
            changes.append(
                _change(
                    "default-change",
                    f"{path}.{name}.default",
                    previous_default,
                    current_default,
                )
            )
    return changes


def compare_public_api_manifests(
    before: dict[str, object],
    after: dict[str, object],
) -> tuple[dict[str, object], ...]:
    changes: list[dict[str, object]] = []
    changes.extend(
        _change(
            "schema-change"
            if field in {"schemaVersion", "contracts"}
            else "manual-review",
            field,
            before.get(field),
            after.get(field),
        )
        for field in ("schemaVersion", "packageVersion", "contracts", "resources")
        if before.get(field) != after.get(field)
    )
    before_modules = before.get("modules", {})
    after_modules = after.get("modules", {})
    if not isinstance(before_modules, dict) or not isinstance(after_modules, dict):
        message = "public API manifests must contain module documents"
        raise TypeError(message)
    for module_name in sorted(before_modules.keys() | after_modules.keys()):
        if module_name not in after_modules:
            changes.append(
                _change(
                    "removal",
                    f"modules.{module_name}",
                    before_modules[module_name],
                    None,
                )
            )
            continue
        if module_name not in before_modules:
            changes.append(
                _change(
                    "addition-compatible",
                    f"modules.{module_name}",
                    None,
                    after_modules[module_name],
                )
            )
            continue
        before_symbols = before_modules[module_name]["symbols"]
        after_symbols = after_modules[module_name]["symbols"]
        for symbol_name in sorted(before_symbols.keys() | after_symbols.keys()):
            path = f"modules.{module_name}.symbols.{symbol_name}"
            if symbol_name not in after_symbols:
                changes.append(
                    _change("removal", path, before_symbols[symbol_name], None)
                )
                continue
            if symbol_name not in before_symbols:
                changes.append(
                    _change(
                        "addition-compatible", path, None, after_symbols[symbol_name]
                    )
                )
                continue
            previous = before_symbols[symbol_name]
            current = after_symbols[symbol_name]
            if previous.get("kind") != current.get("kind"):
                changes.append(
                    _change(
                        "manual-review",
                        f"{path}.kind",
                        previous.get("kind"),
                        current.get("kind"),
                    )
                )
                continue
            previous_callable = previous.get("callable", previous)
            current_callable = current.get("callable", current)
            changes.extend(
                _compare_parameters(path, previous_callable, current_callable)
            )
            changes.extend(
                _compare_fields(
                    f"{path}.fields",
                    previous.get("fields", []),
                    current.get("fields", []),
                )
            )
            for field in ("enumValues", "protocol", "members"):
                if previous.get(field) != current.get(field):
                    classification = {
                        "enumValues": "type-change",
                        "protocol": "manual-review",
                        "members": "signature-change",
                    }[field]
                    changes.append(
                        _change(
                            classification,
                            f"{path}.{field}",
                            previous.get(field),
                            current.get(field),
                        )
                    )
    return tuple(changes)
