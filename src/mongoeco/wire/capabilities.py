from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WireCommandCapability:
    name: str
    kind: str = "passthrough"
    family: str = "admin_misc"
    binds_session: bool = True


END_SESSIONS_CAPABILITY = WireCommandCapability(
    name="endSessions",
    kind="end_sessions",
    family="sessions",
    binds_session=False,
)
GET_MORE_CAPABILITY = WireCommandCapability(
    name="getMore",
    kind="get_more",
    family="cursor",
    binds_session=False,
)
KILL_CURSORS_CAPABILITY = WireCommandCapability(
    name="killCursors",
    kind="kill_cursors",
    family="cursor",
    binds_session=False,
)
HELLO_CAPABILITY = WireCommandCapability(
    name="hello",
    kind="handshake",
    family="handshake",
    binds_session=False,
)
ISMASTER_CAPABILITY = WireCommandCapability(
    name="isMaster",
    kind="handshake",
    family="handshake",
    binds_session=False,
)
ISMASTER_LOWER_CAPABILITY = WireCommandCapability(
    name="ismaster",
    kind="handshake",
    family="handshake",
    binds_session=False,
)
COMMIT_TRANSACTION_CAPABILITY = WireCommandCapability(
    name="commitTransaction",
    kind="commit_transaction",
    family="transactions",
)
ABORT_TRANSACTION_CAPABILITY = WireCommandCapability(
    name="abortTransaction",
    kind="abort_transaction",
    family="transactions",
)
AUTHENTICATE_CAPABILITY = WireCommandCapability(
    name="authenticate",
    kind="authenticate",
    family="auth",
    binds_session=False,
)
SASL_START_CAPABILITY = WireCommandCapability(
    name="saslStart",
    kind="sasl_start",
    family="auth",
    binds_session=False,
)
SASL_CONTINUE_CAPABILITY = WireCommandCapability(
    name="saslContinue",
    kind="sasl_continue",
    family="auth",
    binds_session=False,
)
LOGOUT_CAPABILITY = WireCommandCapability(
    name="logout",
    kind="logout",
    family="auth",
    binds_session=False,
)

_ADMIN_CAPABILITIES: dict[str, WireCommandCapability] = {
    "aggregate": WireCommandCapability(name="aggregate", family="admin_read"),
    "buildInfo": WireCommandCapability(name="buildInfo", family="admin_introspection", binds_session=False),
    "collStats": WireCommandCapability(name="collStats", family="admin_stats"),
    "connectionStatus": WireCommandCapability(name="connectionStatus", family="admin_status", binds_session=False),
    "count": WireCommandCapability(name="count", family="admin_read"),
    "create": WireCommandCapability(name="create", family="admin_namespace"),
    "createIndexes": WireCommandCapability(name="createIndexes", family="admin_index"),
    "currentOp": WireCommandCapability(name="currentOp", family="admin_control", binds_session=False),
    "dbHash": WireCommandCapability(name="dbHash", family="admin_introspection", binds_session=False),
    "dbStats": WireCommandCapability(name="dbStats", family="admin_stats"),
    "delete": WireCommandCapability(name="delete", family="admin_write"),
    "distinct": WireCommandCapability(name="distinct", family="admin_read"),
    "drop": WireCommandCapability(name="drop", family="admin_namespace"),
    "dropIndexes": WireCommandCapability(name="dropIndexes", family="admin_index"),
    "explain": WireCommandCapability(name="explain", family="admin_explain"),
    "find": WireCommandCapability(name="find", family="admin_read"),
    "findAndModify": WireCommandCapability(name="findAndModify", family="admin_find_and_modify"),
    "getCmdLineOpts": WireCommandCapability(name="getCmdLineOpts", family="admin_introspection", binds_session=False),
    "hello": WireCommandCapability(name="hello", family="admin_status", binds_session=False),
    "hostInfo": WireCommandCapability(name="hostInfo", family="admin_introspection", binds_session=False),
    "insert": WireCommandCapability(name="insert", family="admin_write"),
    "listCommands": WireCommandCapability(name="listCommands", family="admin_introspection", binds_session=False),
    "listCollections": WireCommandCapability(name="listCollections", family="admin_namespace"),
    "listDatabases": WireCommandCapability(name="listDatabases", family="admin_namespace", binds_session=False),
    "listIndexes": WireCommandCapability(name="listIndexes", family="admin_index"),
    "killOp": WireCommandCapability(name="killOp", family="admin_control", binds_session=False),
    "ping": WireCommandCapability(name="ping", family="admin_status", binds_session=False),
    "profile": WireCommandCapability(name="profile", family="admin_control"),
    "serverStatus": WireCommandCapability(name="serverStatus", family="admin_status", binds_session=False),
    "update": WireCommandCapability(name="update", family="admin_write"),
    "validate": WireCommandCapability(name="validate", family="admin_validate"),
    "whatsmyuri": WireCommandCapability(name="whatsmyuri", family="admin_introspection", binds_session=False),
}

_SPECIAL_CAPABILITIES: dict[str, WireCommandCapability] = {
    capability.name: capability
    for capability in (
        END_SESSIONS_CAPABILITY,
        GET_MORE_CAPABILITY,
        KILL_CURSORS_CAPABILITY,
        HELLO_CAPABILITY,
        ISMASTER_CAPABILITY,
        ISMASTER_LOWER_CAPABILITY,
        COMMIT_TRANSACTION_CAPABILITY,
        ABORT_TRANSACTION_CAPABILITY,
        AUTHENTICATE_CAPABILITY,
        SASL_START_CAPABILITY,
        SASL_CONTINUE_CAPABILITY,
        LOGOUT_CAPABILITY,
    )
}


def resolve_wire_command_capability(command_name: str) -> WireCommandCapability:
    if command_name in _SPECIAL_CAPABILITIES:
        return _SPECIAL_CAPABILITIES[command_name]
    return _ADMIN_CAPABILITIES.get(
        command_name,
        WireCommandCapability(name=command_name),
    )
