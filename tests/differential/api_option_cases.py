"""Finite PyMongo API option specimens; independent of the candidate runtime."""

from dataclasses import dataclass

from bson import Int64


@dataclass(frozen=True)
class ApiOptionCase:
    id: str
    method: str
    projection: str | None = None
    comment: object | None = None
    failure: str = "none"
    shape: str = "list"
    ordered: bool = True


PROJECTIONS = {
    "fields": ["name", "profile.city"],
    "empty": [],
    "duplicates": ["name", "name"],
    "nested_array": ["items.sku"],
    "tuple": ("name", "profile.city"),
    "empty_tuple": (),
    "set": {"name", "profile.city"},
    "dict": {"name": 1, "_id": 0},
    "string": "name",
    "invalid_member": ["name", 1],
    "invalid_scalar": 1,
    "generator": None,
}
PROJECTION_METHODS = (
    "find",
    "find_one",
    "find_raw_batches",
    "find_one_and_update",
    "find_one_and_replace",
    "find_one_and_delete",
)
API_OPTION_CASES = [
    ApiOptionCase(f"projection_{method}_{name}", method, projection=name)
    for method in PROJECTION_METHODS
    for name in PROJECTIONS
]
for method in ("insert_one", "insert_many"):
    for label, comment in (
        ("text", "request-42"),
        ("document", {"request": "42", "tags": ["test"]}),
        ("number", Int64(42)),
    ):
        API_OPTION_CASES.extend(
            ApiOptionCase(
                f"comment_{method}_{label}_{failure}",
                method,
                comment=comment,
                failure=failure,
            )
            for failure in ("none", "duplicate", "schema")
        )
        if method == "insert_many":
            API_OPTION_CASES.extend(
                ApiOptionCase(
                    f"comment_{method}_{label}_{failure}_unordered",
                    method,
                    comment=comment,
                    failure=failure,
                    ordered=False,
                )
                for failure in ("duplicate", "schema")
            )
    API_OPTION_CASES.append(ApiOptionCase(f"comment_{method}_omitted", method))
for shape in ("tuple", "generator"):
    for failure, ordered in (("none", True), ("duplicate", True), ("duplicate", False)):
        API_OPTION_CASES.append(
            ApiOptionCase(
                f"iterable_{shape}_{failure}_{ordered}",
                "insert_many",
                comment={"shape": shape},
                shape=shape,
                failure=failure,
                ordered=ordered,
            )
        )
