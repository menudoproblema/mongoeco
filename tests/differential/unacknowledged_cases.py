"""Finite write specimens for unacknowledged wire messages and their controls."""

from dataclasses import dataclass

from tests.differential.write_preflight_cases import COMMAND_PREFLIGHT_CASES


@dataclass(frozen=True)
class UnacknowledgedCase:
    operation: str
    w: int

    @property
    def id(self):
        return f"{self.operation}_w{self.w}"


OPERATIONS = (
    "insert_one",
    "insert_many_ordered",
    "insert_many_unordered",
    "update_one",
    "update_many",
    "replace_one",
    "delete_one",
    "delete_many",
    "bulk_ordered",
    "bulk_unordered",
    "duplicate_insert",
    "duplicate_insert_many_ordered",
    "duplicate_insert_many_unordered",
    "invalid_increment",
    "partial_update_many",
    "invalid_replacement_id",
    "schema_insert",
    "duplicate_bulk_ordered",
    "duplicate_bulk_unordered",
    "command_insert",
    "command_duplicate_insert",
    "command_insert_ordered",
    "command_insert_unordered",
    "command_update",
    "command_update_many",
    "command_partial_update",
    "command_upsert",
    "command_replace",
    "command_delete",
    "command_delete_many",
    "command_invalid_increment",
    "command_invalid_update_shape",
    "command_invalid_delete_shape",
)
UNACKNOWLEDGED_CASES = tuple(
    UnacknowledgedCase(operation, w) for operation in OPERATIONS for w in (0, 1)
)
WRITE_COMMAND_CASES = (*UNACKNOWLEDGED_CASES, *COMMAND_PREFLIGHT_CASES)
