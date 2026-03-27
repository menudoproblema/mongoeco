from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WireCommandCapability:
    name: str
    kind: str = "passthrough"
    binds_session: bool = True


END_SESSIONS_CAPABILITY = WireCommandCapability(
    name="endSessions",
    kind="end_sessions",
    binds_session=False,
)
GET_MORE_CAPABILITY = WireCommandCapability(
    name="getMore",
    kind="get_more",
    binds_session=False,
)
KILL_CURSORS_CAPABILITY = WireCommandCapability(
    name="killCursors",
    kind="kill_cursors",
    binds_session=False,
)
HELLO_CAPABILITY = WireCommandCapability(
    name="hello",
    kind="handshake",
    binds_session=False,
)
ISMASTER_CAPABILITY = WireCommandCapability(
    name="isMaster",
    kind="handshake",
    binds_session=False,
)
ISMASTER_LOWER_CAPABILITY = WireCommandCapability(
    name="ismaster",
    kind="handshake",
    binds_session=False,
)
COMMIT_TRANSACTION_CAPABILITY = WireCommandCapability(
    name="commitTransaction",
    kind="commit_transaction",
)
ABORT_TRANSACTION_CAPABILITY = WireCommandCapability(
    name="abortTransaction",
    kind="abort_transaction",
)

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
    )
}


def resolve_wire_command_capability(command_name: str) -> WireCommandCapability:
    return _SPECIAL_CAPABILITIES.get(
        command_name,
        WireCommandCapability(name=command_name),
    )
