from __future__ import annotations

from types import MappingProxyType

from mongoeco.compat._catalog_models import DatabaseCommandSupport


DATABASE_COMMAND_SUPPORT_CATALOG = MappingProxyType(
    {
        "aggregate": DatabaseCommandSupport(
            family="admin_read",
            supports_explain=True,
            note="Compiled through database admin routing and exposed by both database.command(...) and the local wire passthrough.",
        ),
        "buildInfo": DatabaseCommandSupport(
            family="admin_introspection",
            note="Static local metadata sourced from the dialect/runtime descriptor.",
        ),
        "collStats": DatabaseCommandSupport(
            family="admin_stats",
            note="Served from local collection stats snapshots.",
        ),
        "connectionStatus": DatabaseCommandSupport(
            family="admin_status",
            note="Static local auth/runtime shape, with wire auth info patched from the active connection.",
        ),
        "count": DatabaseCommandSupport(
            family="admin_read",
            supports_explain=True,
            note="Compiled through the same find-selection path used by direct count command routing.",
        ),
        "create": DatabaseCommandSupport(
            family="admin_namespace",
            note="Namespace administration routed through database admin services.",
        ),
        "createIndexes": DatabaseCommandSupport(
            family="admin_index",
            note="Index administration routed through local collection/index services.",
        ),
        "currentOp": DatabaseCommandSupport(
            family="admin_control",
            supports_wire=True,
            note="Exposes the local active-operation registry for embedded-runtime API and wire parity; no distributed semantics are attempted.",
        ),
        "dbHash": DatabaseCommandSupport(
            family="admin_introspection",
            note="Computes stable local collection hashes for embedded-runtime verification across documents, indexes and collection options.",
        ),
        "dbStats": DatabaseCommandSupport(
            family="admin_stats",
            note="Served from local database stats snapshots.",
        ),
        "delete": DatabaseCommandSupport(
            family="admin_write",
            supports_explain=True,
            note="Write command orchestration routes each delete spec through the same write semantics as the public API.",
        ),
        "distinct": DatabaseCommandSupport(
            family="admin_read",
            supports_explain=True,
            note="Compiled through the same read-selection path used by direct distinct execution.",
        ),
        "drop": DatabaseCommandSupport(
            family="admin_namespace",
            note="Namespace administration routed through database admin services.",
        ),
        "dropDatabase": DatabaseCommandSupport(
            family="admin_namespace",
            note="Implemented as local namespace lifecycle cleanup plus runtime invalidation.",
        ),
        "dropIndexes": DatabaseCommandSupport(
            family="admin_index",
            note="Index administration routed through local collection/index services.",
        ),
        "explain": DatabaseCommandSupport(
            family="admin_explain",
            note="Delegates to routed find/aggregate/update/delete/count/distinct/findAndModify explain builders and preserves the explained command shape.",
        ),
        "find": DatabaseCommandSupport(
            family="admin_read",
            supports_explain=True,
            note="Compiled through the same find operation path as the public collection surface.",
        ),
        "findAndModify": DatabaseCommandSupport(
            family="admin_find_and_modify",
            supports_explain=True,
            note="Routes through explicit find-and-modify orchestration shared by database.command(...) and wire passthrough.",
        ),
        "getCmdLineOpts": DatabaseCommandSupport(
            family="admin_introspection",
            note="Static local process metadata.",
        ),
        "hello": DatabaseCommandSupport(
            family="admin_status",
            note="Static local handshake metadata.",
        ),
        "hostInfo": DatabaseCommandSupport(
            family="admin_introspection",
            note="Static local host/process metadata.",
        ),
        "insert": DatabaseCommandSupport(
            family="admin_write",
            note="Write command orchestration routes each insert batch through the same collection semantics as the public API.",
        ),
        "isMaster": DatabaseCommandSupport(
            family="admin_status",
            note="Legacy handshake alias sharing the local hello metadata source of truth.",
        ),
        "ismaster": DatabaseCommandSupport(
            family="admin_status",
            note="Legacy handshake alias sharing the local hello metadata source of truth.",
        ),
        "listCollections": DatabaseCommandSupport(
            family="admin_namespace",
            note="Namespace listing routed through local snapshots with filter/nameOnly support.",
        ),
        "listCommands": DatabaseCommandSupport(
            family="admin_introspection",
            note="Static inventory of the supported raw command surface, including admin family, wire availability and explain availability metadata.",
        ),
        "listDatabases": DatabaseCommandSupport(
            family="admin_namespace",
            note="Database listing routed through local snapshots with filter/nameOnly support.",
        ),
        "listIndexes": DatabaseCommandSupport(
            family="admin_index",
            note="Index metadata surfaced through local collection/index services.",
        ),
        "killOp": DatabaseCommandSupport(
            family="admin_control",
            supports_wire=True,
            note="Best-effort local cancellation over operations registered as killable in the embedded runtime; no remote or cluster cancellation exists.",
        ),
        "ping": DatabaseCommandSupport(
            family="admin_status",
            note="Static local health/availability response.",
        ),
        "profile": DatabaseCommandSupport(
            family="admin_control",
            note="Local profiling control and introspection routed through engine profiling support, including current level and recorded entry counts.",
        ),
        "renameCollection": DatabaseCommandSupport(
            family="admin_namespace",
            note="Namespace administration routed through database admin services.",
        ),
        "serverStatus": DatabaseCommandSupport(
            family="admin_status",
            note="Static local runtime metadata enriched with engine information, embedded-runtime counters and profiling summary.",
        ),
        "update": DatabaseCommandSupport(
            family="admin_write",
            supports_explain=True,
            note="Write command orchestration routes each update spec through the same write semantics as the public API.",
        ),
        "validate": DatabaseCommandSupport(
            family="admin_validate",
            note="Validation snapshots routed through local collection validation support.",
        ),
        "whatsmyuri": DatabaseCommandSupport(
            family="admin_introspection",
            note="Static local connection metadata.",
        ),
    }
)
