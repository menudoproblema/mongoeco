"""Command grammar, effective write acknowledgement and execution-error controls."""

from copy import deepcopy
from dataclasses import dataclass

from bson import Decimal128


@dataclass(frozen=True)
class CommandPreflightCase:
    id: str
    command: dict
    documents: tuple


DOCUMENTS = tuple({"_id": key, "value": 0} for key in ("a", "b", "c"))
COMMAND_PREFLIGHT_CASES = []
for family in ("update", "delete"):
    fields = (
        ("hint_scalar", {"hint": 1}),
        ("hint_null", {"hint": None}),
        ("hint_array", {"hint": []}),
        ("collation_scalar", {"collation": 1}),
        ("collation_array", {"collation": []}),
        ("collation_null_control", {"collation": None}),
        ("hint_missing_index_control", {"hint": "missing"}),
    )
    if family == "update":
        fields += (
            ("sort_scalar", {"sort": 1}),
            ("sort_array", {"sort": []}),
            ("sort_null_control", {"sort": None}),
        )
    for label, option in fields:
        for ordered in (True, False):
            entries = [
                {
                    "q": {"_id": key},
                    **(
                        {"u": {"$set": {"value": 1}}}
                        if family == "update"
                        else {"limit": 1}
                    ),
                }
                for key in ("a", "b", "c")
            ]
            entries[1].update(option)
            command = {
                family: "items",
                "updates" if family == "update" else "deletes": entries,
                "ordered": ordered,
            }
            COMMAND_PREFLIGHT_CASES.append(
                CommandPreflightCase(
                    f"preflight_{family}_{label}_{ordered}", command, DOCUMENTS
                )
            )

CONCERNS = (
    ("null", None),
    ("scalar", 1),
    ("negative_w", {"w": -1}),
    ("boolean_w", {"w": False}),
    ("empty_tags", {"w": {}}),
    ("invalid_j", {"w": 1, "j": "bad"}),
    ("invalid_fsync", {"fsync": "bad"}),
    ("journal_ack", {"w": 0, "j": True}),
    ("fsync_ack", {"w": 0, "fsync": True}),
    ("zero_control", {"w": 0}),
    ("journal_false_control", {"w": 0, "j": False}),
    ("fsync_false_control", {"w": 0, "fsync": False}),
    ("empty_control", {}),
)
for label, concern in CONCERNS:
    for family in ("insert", "update", "delete"):
        if family == "insert":
            command = {"insert": "items", "documents": [{"_id": "new", "value": 1}]}
        elif family == "update":
            command = {
                "update": "items",
                "updates": [{"q": {"_id": "a"}, "u": {"$set": {"value": 1}}}],
            }
        else:
            command = {"delete": "items", "deletes": [{"q": {"_id": "a"}, "limit": 1}]}
        command["writeConcern"] = deepcopy(concern)
        COMMAND_PREFLIGHT_CASES.append(
            CommandPreflightCase(f"concern_{family}_{label}", command, DOCUMENTS)
        )

COMMAND_PREFLIGHT_CASES.extend(
    CommandPreflightCase(
        f"concern_duplicate_{option}_ack",
        {
            "insert": "items",
            "documents": [{"_id": "a"}],
            "writeConcern": {"w": 0, option: True},
        },
        DOCUMENTS,
    )
    for option in ("j", "fsync")
)

EXTRA_CONCERNS = (
    ("array", []),
    ("w_array", {"w": []}),
    ("w_null", {"w": None}),
    ("w_empty_mode", {"w": ""}),
    ("w_custom_mode", {"w": "custom"}),
    ("w_two", {"w": 2}),
    ("w_maximum", {"w": 50}),
    ("w_over_maximum", {"w": 51}),
    ("w_nan", {"w": float("nan")}),
    ("w_infinity", {"w": float("inf")}),
    ("w_fraction_zero", {"w": 0.5}),
    ("w_fraction_one", {"w": 1.5}),
    ("w_decimal_zero", {"w": Decimal128("0")}),
    ("w_decimal_one", {"w": Decimal128("1")}),
    ("w_majority", {"w": "majority"}),
    ("tags_numeric", {"w": {"zone": 1.5}}),
    ("tags_zero", {"w": {"zone": 0}}),
    ("tags_negative", {"w": {"zone": -1}}),
    ("tags_boolean", {"w": {"zone": True}}),
    ("tags_string", {"w": {"zone": "one"}}),
    ("j_null", {"w": 0, "j": None}),
    ("j_numeric", {"w": 0, "j": 0.5}),
    ("j_decimal", {"w": 0, "j": Decimal128("1")}),
    ("fsync_numeric", {"w": 0, "fsync": 1}),
    ("fsync_null", {"w": 0, "fsync": None}),
    ("both_sync_true", {"j": True, "fsync": True}),
    ("both_sync_one_false", {"j": False, "fsync": True}),
    ("unknown_field", {"surprise": 1}),
    ("legacy_timeout", {"wtimeout": "bad"}),
    ("legacy_fields", {"getLastError": 1, "wOpTime": 1, "wElectionId": 1}),
    ("provenance", {"provenance": "clientSupplied"}),
)
for label, concern in EXTRA_CONCERNS:
    COMMAND_PREFLIGHT_CASES.append(
        CommandPreflightCase(
            f"concern_insert_{label}",
            {"insert": "items", "documents": [{"_id": "new"}], "writeConcern": concern},
            DOCUMENTS,
        )
    )
COMMAND_PREFLIGHT_CASES = tuple(COMMAND_PREFLIGHT_CASES)
