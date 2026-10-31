"""Malformed resume-token admission through public watch entrypoints."""

import base64

from bson import Code

from tests.differential.semantic_watch_cases import _next


SEED = [{"_id": "initial"}]


def malformed_tokens(collection):
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    output = {}
    values = (
        ("number", 1),
        ("array", [1]),
        ("missing_data", {"other": 1}),
        ("null_data", {"_data": None}),
        ("number_data", {"_data": 1}),
        ("code_data", {"_data": Code("x")}),
        ("empty_data", {"_data": ""}),
        ("unicode_digit", {"_data": "²"}),
        ("decoded_array", {"_data": base64.urlsafe_b64encode(b"[]").decode()}),
        (
            "boolean_version",
            {"_data": base64.urlsafe_b64encode(b'{"v":true,"t":1}').decode()},
        ),
        (
            "negative_position",
            {"_data": base64.urlsafe_b64encode(b'{"v":1,"t":-1}').decode()},
        ),
    )
    for scope, owner in (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    ):
        for option in ("resume_after", "start_after"):
            for label, token in values:
                stream = None
                try:
                    stream = owner.watch(**{option: token}, max_await_time_ms=1)
                    result = {"phase": "open", "ok": True}
                except Exception as error:
                    result = {
                        "phase": "open", "error": type(error).__name__,
                        "code": getattr(error, "code", None),
                    }
                finally:
                    if stream is not None:
                        stream.close()
                output[f"{scope}/{option}/{label}"] = result
    return output


def valid_resume(collection):
    client = getattr(collection, "_client", None)
    if client is None:
        client = collection.database.client
    output = {}
    for scope, owner in (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    ):
        collection.delete_many({})
        stream = owner.watch(max_await_time_ms=20)
        try:
            collection.insert_one({"_id": 1})
            token = _next(stream)["_id"]
            # Keep history available in the documented local non-journal mode.
            collection.insert_one({"_id": 2})
        finally:
            stream.close()
        for option in ("resume_after", "start_after"):
            supplied = dict(token)
            stream = owner.watch(**{option: supplied}, max_await_time_ms=20)
            try:
                supplied["_data"] = "changed after opening"
                event = _next(stream)
                output[f"{scope}/{option}"] = {
                    "operationType": event["operationType"],
                    "documentKey": event["documentKey"],
                }
            finally:
                stream.close()
    return output


SEMANTIC_WATCH_TOKEN_CASES = (
    ("semantic_watch_malformed_tokens", malformed_tokens),
    ("semantic_watch_valid_resume", valid_resume),
)
