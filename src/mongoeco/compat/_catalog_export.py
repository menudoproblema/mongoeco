from __future__ import annotations

import json

from mongoeco.compat._catalog_constants import (
    AUTO_INSTALLED_PYMONGO_PROFILE,
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    MONGODB_DIALECT_HOOK_NAMES,
    PYMONGO_PROFILE_HOOK_NAMES,
    STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
)
from mongoeco.compat._catalog_data import (
    DATABASE_COMMAND_SUPPORT_CATALOG,
    DATABASE_COMMAND_OPTION_SUPPORT_CATALOG,
    MONGODB_DIALECT_CATALOG,
    OPERATION_OPTION_SUPPORT_CATALOG,
    PYMONGO_PROFILE_CATALOG,
    SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES,
    SUPPORTED_GROUP_ACCUMULATORS,
    SUPPORTED_MONGODB_MAJORS,
    SUPPORTED_PYMONGO_MAJORS,
    SUPPORTED_QUERY_FIELD_OPERATORS,
    SUPPORTED_QUERY_TOP_LEVEL_OPERATORS,
    SUPPORTED_UPDATE_OPERATORS,
    SUPPORTED_WINDOW_ACCUMULATORS,
)
from mongoeco.cxp import (
    export_cxp_capability_catalog,
    export_cxp_operation_catalog as _export_cxp_operation_catalog_from_cxp,
    export_cxp_profile_catalog as _export_cxp_profile_catalog_from_cxp,
    export_cxp_profile_support_catalog as _export_cxp_profile_support_catalog_from_cxp,
)
from cxp.exchange import Document, evaluate_requirements_detailed
from mongoeco.cxp.capabilities import export_legacy_runtime_subset_catalog
from mongoeco.cxp.exchange import (
    load_mongodb_declared_snapshot,
    load_mongodb_profile,
    mongodb_catalog_store,
)

_MOCK_SAFE_PROFILE_NAME = "mongoeco-mock-safe"
_MOCK_SAFE_PROFILE_DESCRIPTION = (
    "Strict profile for mock/test runtime usage with deterministic local "
    "contract expectations for read/write/search/platform tooling."
)
def export_mongodb_dialect_catalog() -> dict[str, dict[str, object]]:
    return {
        key: {
            "server_version": entry.server_version,
            "label": entry.label,
            "aliases": list(entry.aliases),
            "behavior_flags": dict(entry.behavior_flags),
            "policy_spec": (
                {
                    "null_query_matches_undefined": entry.policy_spec.null_query_matches_undefined,
                    "expression_truthiness": entry.policy_spec.expression_truthiness,
                    "projection_flag_mode": entry.policy_spec.projection_flag_mode,
                    "update_path_sort_mode": entry.policy_spec.update_path_sort_mode,
                    "equality_mode": entry.policy_spec.equality_mode,
                    "comparison_mode": entry.policy_spec.comparison_mode,
                }
                if entry.policy_spec is not None
                else None
            ),
            "capabilities": sorted(entry.capabilities),
            "query_field_operators": sorted(entry.query_field_operators or SUPPORTED_QUERY_FIELD_OPERATORS),
            "query_top_level_operators": sorted(SUPPORTED_QUERY_TOP_LEVEL_OPERATORS),
            "update_operators": sorted(entry.update_operators or SUPPORTED_UPDATE_OPERATORS),
            "system_variables": sorted(entry.system_variables),
            "aggregation_expression_operators": sorted(SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS),
            "aggregation_stages": sorted(SUPPORTED_AGGREGATION_STAGES),
            "group_accumulators": sorted(SUPPORTED_GROUP_ACCUMULATORS),
            "window_accumulators": sorted(SUPPORTED_WINDOW_ACCUMULATORS),
        }
        for key, entry in MONGODB_DIALECT_CATALOG.items()
    }


def export_pymongo_profile_catalog() -> dict[str, dict[str, object]]:
    return {
        key: {
            "driver_series": entry.driver_series,
            "label": entry.label,
            "aliases": list(entry.aliases),
            "behavior_flags": dict(entry.behavior_flags),
            "capabilities": sorted(entry.capabilities),
        }
        for key, entry in PYMONGO_PROFILE_CATALOG.items()
    }


def export_operation_option_catalog() -> dict[str, dict[str, dict[str, str | None]]]:
    return {
        operation: {
            option: {
                "status": support.status.value,
                "note": support.note,
            }
            for option, support in options.items()
        }
        for operation, options in OPERATION_OPTION_SUPPORT_CATALOG.items()
    }


def export_database_command_option_catalog() -> dict[str, dict[str, dict[str, str | None]]]:
    return {
        operation: {
            option: {
                "status": support.status.value,
                "note": support.note,
            }
            for option, support in options.items()
        }
        for operation, options in DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.items()
    }


def export_database_command_catalog() -> dict[str, dict[str, object]]:
    return {
        command_name: {
            "family": support.family,
            "supports_wire": support.supports_wire,
            "supports_explain": support.supports_explain,
            "supports_comment": "comment" in DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.get(command_name, {}),
            "supported_options": sorted(DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.get(command_name, {})),
            "note": support.note,
        }
        for command_name, support in DATABASE_COMMAND_SUPPORT_CATALOG.items()
    }


def export_local_runtime_subset_catalog() -> dict[str, dict[str, object]]:
    return export_legacy_runtime_subset_catalog()


def export_cxp_catalog() -> dict[str, object]:
    return export_cxp_capability_catalog()


def export_cxp_profile_catalog() -> dict[str, dict[str, object]]:
    return _export_cxp_profile_catalog_from_cxp()


def export_cxp_profile_support_catalog() -> dict[str, dict[str, object]]:
    return _export_cxp_profile_support_catalog_from_cxp()


def export_cxp_operation_catalog() -> dict[str, list[dict[str, object]]]:
    return _export_cxp_operation_catalog_from_cxp()


def export_mock_safe_profile_catalog() -> dict[str, object]:
    snapshot = load_mongodb_declared_snapshot()
    requirements = load_mongodb_profile("mongodb-mock-safe")
    context = Document(
        {
            "document_type": "cxp.context",
            "spec_version": 2,
            "payload": {
                "subject_id": snapshot.payload["subject_id"],
                "configuration_revision": snapshot.payload[
                    "configuration_revision"
                ],
                "accepted_sources": ["declared"],
            },
        },
        expected_type="cxp.context",
    )
    result = evaluate_requirements_detailed(
        snapshot,
        requirements,
        context,
        catalogs=mongodb_catalog_store(),
    )
    return {
        "name": _MOCK_SAFE_PROFILE_NAME,
        "description": _MOCK_SAFE_PROFILE_DESCRIPTION,
        "recommendedFor": ["mock-runtime-tests", "contract-gated-tooling"],
        "requirements": [
            {
                "capabilityName": condition["capability"],
                "requiredOperations": list(condition["operations"]),
                "requiredMetadataKeys": list(condition.get("values", ())),
            }
            for condition in requirements.payload["requirement"]["conditions"]
        ],
        "supported": result.is_compatible,
        "verdict": result.verdict,
        "catalog": requirements.payload["catalog"],
        "validation": {
            "messages": [
                finding.message
                for finding in result.findings
                if finding.verdict != "compatible"
            ],
            "findings": [
                {
                    "requirementId": finding.requirement_id,
                    "verdict": finding.verdict,
                    "code": finding.code,
                    "path": finding.path,
                }
                for finding in result.findings
                if finding.verdict != "compatible"
            ],
        },
    }


def export_full_compat_catalog() -> dict[str, object]:
    return {
        "defaults": {
            "mongodb_dialect": DEFAULT_MONGODB_DIALECT,
            "pymongo_profile": DEFAULT_PYMONGO_PROFILE,
            "pymongo_auto_profile": AUTO_INSTALLED_PYMONGO_PROFILE,
            "pymongo_strict_auto_profile": STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
        },
        "hooks": {
            "mongodb_dialect": list(MONGODB_DIALECT_HOOK_NAMES),
            "pymongo_profile": list(PYMONGO_PROFILE_HOOK_NAMES),
        },
        "supported_majors": {
            "mongodb": sorted(SUPPORTED_MONGODB_MAJORS),
            "pymongo": sorted(SUPPORTED_PYMONGO_MAJORS),
        },
        "mongodb_dialects": export_mongodb_dialect_catalog(),
        "pymongo_profiles": export_pymongo_profile_catalog(),
        "database_commands": export_database_command_catalog(),
        "operation_options": export_operation_option_catalog(),
        "database_command_options": export_database_command_option_catalog(),
        "cxp": export_cxp_catalog(),
        "mock_safe_profile": export_mock_safe_profile_catalog(),
        "local_runtime_subsets": export_local_runtime_subset_catalog(),
    }


def export_full_compat_catalog_markdown() -> str:
    catalog = export_full_compat_catalog()
    lines: list[str] = ["# Compat Catalog", ""]

    defaults = catalog["defaults"]
    assert isinstance(defaults, dict)
    lines.append("## Defaults")
    for key, value in defaults.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")

    hooks = catalog["hooks"]
    assert isinstance(hooks, dict)
    lines.append("## Hooks")
    for key, values in hooks.items():
        rendered = ", ".join(f"`{value}`" for value in values) or "_none_"
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    supported_majors = catalog["supported_majors"]
    assert isinstance(supported_majors, dict)
    lines.append("## Supported Majors")
    for key, values in supported_majors.items():
        rendered = ", ".join(str(value) for value in values)
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    def _render_section(title: str, entries: dict[str, object]) -> None:
        lines.append(f"## {title}")
        for key, value in entries.items():
            assert isinstance(value, dict)
            lines.append(f"### `{key}`")
            for field_name, field_value in value.items():
                if isinstance(field_value, list):
                    rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
                else:
                    rendered = f"`{field_value}`"
                lines.append(f"- `{field_name}`: {rendered}")
            lines.append("")

    mongodb_dialects = catalog["mongodb_dialects"]
    assert isinstance(mongodb_dialects, dict)
    _render_section("MongoDB Dialects", mongodb_dialects)

    pymongo_profiles = catalog["pymongo_profiles"]
    assert isinstance(pymongo_profiles, dict)
    _render_section("PyMongo Profiles", pymongo_profiles)

    database_commands = catalog["database_commands"]
    assert isinstance(database_commands, dict)
    _render_section("Database Commands", database_commands)

    operation_options = catalog["operation_options"]
    assert isinstance(operation_options, dict)
    lines.append("## Operation Options")
    for operation, options in operation_options.items():
        assert isinstance(options, dict)
        lines.append(f"### `{operation}`")
        for option, support in options.items():
            assert isinstance(support, dict)
            rendered = ", ".join(
                f"`{field}`={json.dumps(value)}"
                for field, value in support.items()
            )
            lines.append(f"- `{option}`: {rendered}")
        lines.append("")

    database_command_options = catalog["database_command_options"]
    assert isinstance(database_command_options, dict)
    lines.append("## Database Command Options")
    for operation, options in database_command_options.items():
        assert isinstance(options, dict)
        lines.append(f"### `{operation}`")
        for option, support in options.items():
            assert isinstance(support, dict)
            rendered = ", ".join(
                f"`{field}`={json.dumps(value)}"
                for field, value in support.items()
            )
            lines.append(f"- `{option}`: {rendered}")
        lines.append("")

    cxp_catalog = catalog["cxp"]
    assert isinstance(cxp_catalog, dict)
    lines.append("## CXP")
    lines.append(f"- `interface`: `{cxp_catalog['interface']}`")
    profiles = cxp_catalog.get("profiles", {})
    assert isinstance(profiles, dict)
    if profiles:
        lines.append("### Profiles")
        for name, entry in profiles.items():
            assert isinstance(entry, dict)
            lines.append(f"#### `{name}`")
            description = entry.get("description")
            lines.append(f"- `description`: `{description}`")
            recommended_for = entry.get("recommendedFor", ())
            if isinstance(recommended_for, list):
                rendered_recommended_for = ", ".join(
                    f"`{value}`" for value in recommended_for
                ) or "_empty_"
                lines.append(
                    f"- `recommendedFor`: {rendered_recommended_for}"
                )
    capabilities = cxp_catalog["capabilities"]
    assert isinstance(capabilities, dict)
    for name, entry in capabilities.items():
        assert isinstance(entry, dict)
        lines.append(f"### `{name}`")
        description = entry.get("description")
        lines.append(f"- `description`: `{description}`")
        tiers = entry.get("tiers", ())
        rendered_tiers = ", ".join(f"`{value}`" for value in tiers) or "_empty_"
        lines.append(f"- `tiers`: {rendered_tiers}")
        operations = entry.get("operations", ())
        if isinstance(operations, list):
            rendered_operations = ", ".join(
                f"`{operation.get('name')}`"
                for operation in operations
                if isinstance(operation, dict)
            ) or "_empty_"
            lines.append(f"- `operations`: {rendered_operations}")
        metadata = entry.get("metadata", {})
        assert isinstance(metadata, dict)
        for field_name, field_value in metadata.items():
            if isinstance(field_value, list):
                rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
            else:
                rendered = f"`{field_value}`"
            lines.append(f"- `{field_name}`: {rendered}")
        lines.append("")

    extensions = cxp_catalog["extensions"]
    assert isinstance(extensions, dict)
    lines.append("## CXP Extensions")
    for name, entry in extensions.items():
        assert isinstance(entry, dict)
        lines.append(f"### `{name}`")
        for field_name, field_value in entry.items():
            if isinstance(field_value, list):
                rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
            else:
                rendered = f"`{field_value}`"
            lines.append(f"- `{field_name}`: {rendered}")
        lines.append("")

    mock_safe_profile = catalog["mock_safe_profile"]
    assert isinstance(mock_safe_profile, dict)
    lines.append("## Mock Safe Profile")
    lines.append(f"- `name`: `{mock_safe_profile.get('name')}`")
    lines.append(f"- `description`: `{mock_safe_profile.get('description')}`")
    lines.append(f"- `supported`: `{mock_safe_profile.get('supported')}`")
    recommended_for = mock_safe_profile.get("recommendedFor", ())
    if isinstance(recommended_for, list):
        rendered_recommended_for = ", ".join(
            f"`{value}`" for value in recommended_for
        ) or "_empty_"
        lines.append(f"- `recommendedFor`: {rendered_recommended_for}")
    requirements = mock_safe_profile.get("requirements", ())
    if isinstance(requirements, list):
        lines.append("### Requirements")
        for requirement in requirements:
            if not isinstance(requirement, dict):
                continue
            lines.append(
                f"- `{requirement.get('capabilityName')}`: "
                f"ops={requirement.get('requiredOperations')} "
                f"metadata={requirement.get('requiredMetadataKeys')}"
            )
    validation = mock_safe_profile.get("validation", {})
    if isinstance(validation, dict):
        lines.append("### Validation")
        for field_name, field_value in validation.items():
            if isinstance(field_value, list):
                rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
            else:
                rendered = f"`{field_value}`"
            lines.append(f"- `{field_name}`: {rendered}")
    lines.append("")

    runtime_subsets = catalog["local_runtime_subsets"]
    assert isinstance(runtime_subsets, dict)
    _render_section("Local Runtime Subsets", runtime_subsets)

    return "\n".join(lines).rstrip() + "\n"
