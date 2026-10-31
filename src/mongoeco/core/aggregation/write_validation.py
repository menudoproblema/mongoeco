"""Structural write-stage rules shared by preparation and direct execution."""

from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.server_errors import server_error


def validate_merge_mode_pair(spec):
    if not isinstance(spec, dict):
        return
    matched = spec.get("whenMatched", "merge")
    unmatched = spec.get("whenNotMatched", "insert")
    if (
        matches_bson_type(matched, "string")
        and matches_bson_type(unmatched, "string")
        and matched in {"keepExisting", "fail"}
        and unmatched in {"discard", "fail"}
    ):
        message = (
            f"Combination of whenMatched: {matched!r} and "
            f"whenNotMatched: {unmatched!r} is not supported"
        )
        raise server_error(message, code=51181)
