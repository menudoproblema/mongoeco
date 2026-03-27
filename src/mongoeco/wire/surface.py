from __future__ import annotations

from dataclasses import dataclass

from mongoeco.api._async.database_commands import SUPPORTED_DATABASE_COMMANDS


WIRE_SPECIAL_COMMANDS: tuple[str, ...] = (
    "abortTransaction",
    "commitTransaction",
    "endSessions",
    "getMore",
    "isMaster",
    "ismaster",
    "killCursors",
)

WIRE_SUPPORTED_OPCODES: tuple[int, ...] = (2004, 2013)


@dataclass(frozen=True, slots=True)
class WireSurface:
    min_wire_version: int = 0
    max_wire_version: int = 20
    max_bson_object_size: int = 16 * 1024 * 1024
    max_message_size_bytes: int = 48_000_000
    max_write_batch_size: int = 100_000
    logical_session_timeout_minutes: int = 30
    compression: tuple[str, ...] = ()
    supports_sessions: bool = True
    supports_transactions: bool = True
    supported_commands: tuple[str, ...] = SUPPORTED_DATABASE_COMMANDS + WIRE_SPECIAL_COMMANDS
    supported_opcodes: tuple[int, ...] = WIRE_SUPPORTED_OPCODES

    def supports_command(self, command_name: str) -> bool:
        return command_name in self.supported_commands

    def supports_opcode(self, op_code: int) -> bool:
        return op_code in self.supported_opcodes

