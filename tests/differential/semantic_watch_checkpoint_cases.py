"""Checkpoint ownership and resumption without comparing opaque backend bytes."""

import time

from tests.differential.semantic_watch_cases import _next
from tests.differential.semantic_watch_protocol_cases import _owners


SEED = [{"_id": "initial"}]


def checkpoints(collection):
    output = {}
    for scope, owner in _owners(collection):
        collection.delete_many({})
        stream = owner.watch(max_await_time_ms=20)
        try:
            initial = stream.resume_token
            copy = stream.resume_token
            copy["changed"] = []
            owned = stream.resume_token == initial
            collection.insert_one({"_id": 1})
            first = _next(stream)
            collection.insert_one({"_id": 2})
            saved = stream.resume_token
            resumed = owner.watch(resume_after=saved, max_await_time_ms=20)
            try:
                saved["_data"] = "mutated after open"
                event = _next(resumed)
                output[scope] = {
                    "initial_present": initial is not None,
                    "owned": owned,
                    "first": first["documentKey"],
                    "resumed": event["documentKey"],
                    "resumed_token_present": resumed.resume_token is not None,
                }
            finally:
                resumed.close()
            token = stream.resume_token
            stream.close()
            output[scope]["retained_after_close"] = stream.resume_token == token
        finally:
            stream.close()
    return output


def filtered_checkpoint(collection):
    output = {}
    for scope, owner in _owners(collection):
        collection.delete_many({})
        hidden = owner.watch(
            [{"$match": {"operationType": "update"}}], max_await_time_ms=20
        )
        try:
            collection.insert_one({"_id": 1})
            deadline = time.monotonic() + 5
            success = False
            attempt = 0
            while time.monotonic() < deadline:
                assert hidden.try_next() is None
                candidate = hidden.resume_token
                probe = owner.watch(resume_after=candidate, max_await_time_ms=20)
                marker = f"barrier-{attempt}"
                try:
                    collection.insert_one({"_id": marker})
                    seen = []
                    while True:
                        event = _next(probe)
                        seen.append(event["documentKey"]["_id"])
                        if seen[-1] == marker:
                            break
                    # Native empty-batch checkpoints can lag the latest write.
                    # A following marker fences the earlier change; do not
                    # require that all newly inserted markers were checkpointed.
                    if 1 not in seen:
                        success = True
                        break
                finally:
                    probe.close()
                attempt += 1
            if not success:
                message = "empty filtered reads did not checkpoint processed changes"
                raise AssertionError(message)
            output[scope] = {"skips_processed": success, "alive": hidden.alive}
        finally:
            hidden.close()
    return output


def invalidation_resume(collection):
    output = {}
    collection.delete_many({})
    for filtered in (False, True):
        collection.insert_one({"_id": str(filtered)})
        visible = collection.watch(max_await_time_ms=20)
        hidden = collection.watch(
            [{"$match": {"operationType": "insert"}}],
            max_await_time_ms=20,
        )
        try:
            collection.drop()
            _next(visible)
            invalidate = _next(visible)
            deadline = time.monotonic() + 3
            while hidden.alive and time.monotonic() < deadline:
                hidden.try_next()
            if hidden.alive:
                message = "hidden invalidation was not processed"
                raise AssertionError(message)
            token = hidden.resume_token if filtered else invalidate["_id"]
            result = {"token_equal": token == invalidate["_id"]}
            try:
                unexpected = collection.watch(resume_after=token, max_await_time_ms=20)
                unexpected.close()
                result["resume"] = {"opened": True}
            except Exception as error:
                result["resume"] = {
                    "phase": "open",
                    "error": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "code_name": (getattr(error, "details", None) or {}).get(
                        "codeName"
                    ),
                }
            resumed = collection.watch(start_after=token, max_await_time_ms=20)
            try:
                collection.insert_one({"_id": "following"})
                result["start_after"] = _next(resumed)["documentKey"]
            finally:
                resumed.close()
            output[str(filtered)] = result
        finally:
            visible.close()
            hidden.close()
    return output


def selected_resume_options(collection):
    output = {}
    for scope, owner in _owners(collection):
        collection.delete_many({})
        keeper = owner.watch(max_await_time_ms=20)
        try:
            collection.insert_one({"_id": 1})
            first = _next(keeper)["_id"]
            collection.insert_one({"_id": 2})
            second = _next(keeper)["_id"]
            collection.insert_one({"_id": 3})
            for name, kwargs in (
                ("both", {"resume_after": first, "start_after": second}),
                ("empty_start", {"resume_after": first, "start_after": {}}),
                (
                    "ignored_time",
                    {"resume_after": first, "start_at_operation_time": -1},
                ),
                ("empty_start_only", {"start_after": {}}),
            ):
                stream = owner.watch(**kwargs, max_await_time_ms=20)
                try:
                    if name == "empty_start_only":
                        collection.insert_one({"_id": 4})
                    output[f"{scope}/{name}"] = _next(stream)["documentKey"]
                finally:
                    stream.close()
        finally:
            keeper.close()
    return output


SEMANTIC_WATCH_CHECKPOINT_CASES = (
    ("semantic_watch_checkpoints", checkpoints),
    ("semantic_watch_filtered_checkpoint", filtered_checkpoint),
    ("semantic_watch_invalidation_resume", invalidation_resume),
    ("semantic_watch_selected_resume_options", selected_resume_options),
)
