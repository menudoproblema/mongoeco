from __future__ import annotations

from mongoeco.compat._catalog_database_commands import DATABASE_COMMAND_SUPPORT_CATALOG
from mongoeco.compat._catalog_models import DatabaseCommandSupport
from mongoeco.compat._catalog_operation_options import DATABASE_COMMAND_OPTION_SUPPORT_CATALOG


def command_support_entry(command_name: str) -> DatabaseCommandSupport | None:
    return DATABASE_COMMAND_SUPPORT_CATALOG.get(command_name)


def command_supported_options(command_name: str) -> tuple[str, ...]:
    return tuple(sorted(DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.get(command_name, {})))


def command_supports_comment(command_name: str) -> bool:
    return "comment" in DATABASE_COMMAND_OPTION_SUPPORT_CATALOG.get(command_name, {})


def command_help_document(command_name: str) -> dict[str, object]:
    entry = command_support_entry(command_name)
    supported_options = command_supported_options(command_name)
    document: dict[str, object] = {
        "help": f"mongoeco local support for the {command_name} command",
    }
    if entry is None:
        document["supportsExplain"] = False
        document["supportsComment"] = command_supports_comment(command_name)
        if supported_options:
            document["supportedOptions"] = list(supported_options)
        return document
    document["adminFamily"] = entry.family
    document["supportsWire"] = entry.supports_wire
    document["supportsExplain"] = entry.supports_explain
    document["supportsComment"] = command_supports_comment(command_name)
    if supported_options:
        document["supportedOptions"] = list(supported_options)
    if entry.note is not None:
        document["note"] = entry.note
    return document


def list_commands_document_payload(command_names: tuple[str, ...]) -> dict[str, dict[str, object]]:
    return {
        command_name: command_help_document(command_name)
        for command_name in command_names
    }


def admin_family_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for support in DATABASE_COMMAND_SUPPORT_CATALOG.values():
        counts[support.family] = counts.get(support.family, 0) + 1
    return counts


def wire_command_surface_count() -> int:
    return sum(1 for support in DATABASE_COMMAND_SUPPORT_CATALOG.values() if support.supports_wire)


def explainable_command_count() -> int:
    return sum(1 for support in DATABASE_COMMAND_SUPPORT_CATALOG.values() if support.supports_explain)
