"""Raw query shape witnesses distinguish absent/null q from an empty document."""

from copy import deepcopy

from tests.differential.write_cases import WriteCase, WriteRequest


RAW_QUERY_CASES = []
for family in ("update", "delete"):
    for shape in ("missing", "null", "scalar", "array", "bool", "string", "empty"):
        for ordered in (True, False):
            if family == "update":
                specs = [
                    {"q": {"_id": "a"}, "u": {"$set": {"before": 1}}},
                    {"u": {"$set": {"value": 1}}, "multi": True},
                    {"q": {"_id": "b"}, "u": {"$set": {"after": 1}}},
                ]
                command = {"update": "items", "updates": specs, "ordered": ordered}
            else:
                specs = [
                    {"q": {"_id": "a"}, "limit": 1},
                    {"limit": 0},
                    {"q": {"_id": "b"}, "limit": 1},
                ]
                command = {"delete": "items", "deletes": specs, "ordered": ordered}
            if shape != "missing":
                specs[1]["q"] = {
                    "null": None,
                    "scalar": 1,
                    "array": [],
                    "bool": False,
                    "string": "bad",
                    "empty": {},
                }[shape]
            RAW_QUERY_CASES.append(
                WriteCase(
                    f"query_{family}_{shape}_{ordered}",
                    [{"_id": "a", "value": 0}, {"_id": "b", "value": 0}],
                    [WriteRequest("command", [deepcopy(command)])],
                )
            )
