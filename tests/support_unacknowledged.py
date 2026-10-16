"""Independent PyMongo observations of results, write effects and pool reuse."""

import asyncio
import struct

from copy import deepcopy
from urllib.parse import urlsplit

from bson import BSON
from pymongo import InsertOne, UpdateOne, monitoring
from pymongo.errors import InvalidOperation, OperationFailure
from pymongo.write_concern import WriteConcern

from tests.differential.write_preflight_cases import CommandPreflightCase
from tests.support_write_parity import BSONObservation, public_error, public_result


WIRE_OP_MSG = 2013


class PoolObservation(monitoring.ConnectionPoolListener):
    def __init__(self):
        self.checkouts = []

    def connection_checked_out(self, event):
        self.checkouts.append(event.connection_id)

    def _ignore(self, event):
        pass

    pool_created = _ignore
    pool_ready = _ignore
    pool_cleared = _ignore
    pool_closed = _ignore
    connection_created = _ignore
    connection_ready = _ignore
    connection_closed = _ignore
    connection_check_out_started = _ignore
    connection_check_out_failed = _ignore
    connection_checked_in = _ignore

    def observation(self):
        connections = len(set(self.checkouts))
        return {
            "physical_connections": connections,
            "reused": connections == 1 and len(self.checkouts) > 1,
        }


def client_options(pool):
    return {
        "maxPoolSize": 1,
        "retryReads": False,
        "retryWrites": False,
        "serverSelectionTimeoutMS": 3000,
        "socketTimeoutMS": 3000,
        "event_listeners": [pool],
    }


def seed(case):
    if isinstance(case, CommandPreflightCase):
        return deepcopy(list(case.documents)), {}
    documents = [{"_id": "a", "value": 0}]
    if case.operation in ("partial_update_many", "command_partial_update"):
        documents += [{"_id": "b", "value": "bad"}, {"_id": "c", "value": 0}]
    options = (
        {"validator": {"$jsonSchema": {"required": ["value"]}}}
        if case.operation == "schema_insert"
        else {}
    )
    return documents, options


def invoke(collection, case):
    if isinstance(case, CommandPreflightCase):
        command = deepcopy(case.command)
        command[next(iter(command))] = collection.name
        return collection.database.command(command)
    collection = collection.with_options(write_concern=WriteConcern(w=case.w))
    if case.operation.startswith("command_"):
        return collection.database.command(command_spec(collection.name, case))
    return _invoke_helper(collection, case)


def _invoke_helper(collection, case):  # noqa: PLR0911 - finite native specimens
    operation = case.operation
    if operation == "insert_one":
        return collection.insert_one({"_id": "b", "value": 1})
    if "insert_many" in operation:
        documents = [{"_id": "b"}, {"_id": "c"}]
        if operation.startswith("duplicate_"):
            documents.insert(1, {"_id": "a"})
        return collection.insert_many(documents, ordered=operation.endswith("_ordered"))
    if operation in ("update_one", "invalid_increment"):
        amount = "bad" if operation == "invalid_increment" else 1
        return collection.update_one({"_id": "a"}, {"$inc": {"value": amount}})
    if operation in ("update_many", "partial_update_many"):
        return collection.update_many({}, {"$inc": {"value": 1}})
    if operation in ("replace_one", "invalid_replacement_id"):
        replacement_id = ["bad"] if operation == "invalid_replacement_id" else "a"
        return collection.replace_one({"_id": "a"}, {"_id": replacement_id, "value": 7})
    if operation == "delete_one":
        return collection.delete_one({"_id": "a"})
    if operation == "delete_many":
        return collection.delete_many({})
    if "bulk" in operation:
        requests = [
            InsertOne({"_id": "b"}),
            UpdateOne({"_id": "a"}, {"$inc": {"value": 1}}),
        ]
        if operation.startswith("duplicate_"):
            requests = [InsertOne({"_id": value}) for value in ("b", "a", "c")]
        return collection.bulk_write(requests, ordered=operation.endswith("_ordered"))
    if operation == "duplicate_insert":
        return collection.insert_one({"_id": "a"})
    if operation == "schema_insert":
        return collection.insert_one({"_id": "b"})
    message = f"unknown write specimen: {operation}"
    raise AssertionError(message)


def command_spec(name, case):
    operation = case.operation.removeprefix("command_")
    if operation in (
        "insert",
        "duplicate_insert",
        "insert_ordered",
        "insert_unordered",
    ):
        documents = [{"_id": "a" if operation == "duplicate_insert" else "b"}]
        if operation in ("insert_ordered", "insert_unordered"):
            documents += [{"_id": "a"}, {"_id": "c"}]
        body = {
            "insert": name,
            "documents": documents,
            "ordered": operation != "insert_unordered",
        }
    elif operation in ("delete", "delete_many", "invalid_delete_shape"):
        item = {"q": {}, "limit": 0 if operation == "delete_many" else 1}
        if operation == "invalid_delete_shape":
            del item["q"]
        body = {"delete": name, "deletes": [item]}
    else:
        item = {
            "q": {"_id": "upsert"} if operation == "upsert" else {},
            "u": {"$inc": {"value": "bad" if operation == "invalid_increment" else 1}},
            "multi": operation in ("update_many", "partial_update"),
            "upsert": operation == "upsert",
        }
        if operation == "replace":
            item["u"] = {"_id": "a", "value": 7}
        if operation == "invalid_update_shape":
            del item["q"]
        body = {"update": name, "updates": [item]}
    return {**body, "writeConcern": {"w": case.w}}


def command_reply(document):
    """Preserve semantic field presence, counters, IDs and server error codes."""
    observed = {
        key: document[key]
        for key in (
            "ok",
            "n",
            "nModified",
            "upserted",
            "code",
            "codeName",
            "errorLabels",
        )
        if key in document
    }
    if "writeErrors" in document:
        observed["writeErrors"] = [
            {
                key: item[key]
                for key in ("index", "code", "codeName", "op")
                if key in item
            }
            for item in document["writeErrors"]
        ]
    if "writeConcernError" in document:
        observed["writeConcernError"] = {
            key: value
            for key, value in document["writeConcernError"].items()
            if key != "errmsg"
        }
    return observed


def outcome(result, error, bson):
    if error is not None:
        return {"error": public_error(error, bson)}
    if isinstance(result, dict):
        return {"command_reply": command_reply(result)}
    if result.acknowledged:
        return {"result": public_result(result, bson)}
    observed = {"acknowledged": False}
    kind = type(result).__name__
    if kind in ("InsertOneResult", "InsertManyResult"):
        field = "inserted_id" if kind == "InsertOneResult" else "inserted_ids"
        observed[field] = bson.value(getattr(result, field))
    else:
        field = {
            "UpdateResult": "matched_count",
            "DeleteResult": "deleted_count",
            "BulkWriteResult": "inserted_count",
        }[kind]
        try:
            getattr(result, field)
        except InvalidOperation:
            observed["counter_error"] = "InvalidOperation"
        else:
            message = "unacknowledged counters unexpectedly available"
            raise AssertionError(message)
    return {"result": observed}


def assert_observation(actual, expected):
    if "error" in expected and expected["error"]["codeName"] is None:
        actual["error"]["codeName"] = None
    assert actual == expected


def observe(client, database, name, case, pool):
    documents, options = seed(case)
    collection = client[database].create_collection(name, **options)
    collection.insert_many(documents)
    result = error = None
    try:
        result = invoke(collection, case)
    except OperationFailure as exc:
        error = exc
    # Transport failures escape: neither retry nor a second client may hide them.
    assert client.admin.command("ping")["ok"] == 1
    effects = list(collection.find().sort("_id"))
    following = collection.insert_one({"_id": "following", "value": 1})
    final = list(collection.find().sort("_id"))
    bson = BSONObservation([])
    return {
        **outcome(result, error, bson),
        "documents": bson.documents(effects),
        "following_write": public_result(following, bson),
        "final_documents": bson.documents(final),
        "connection": pool.observation(),
    }


async def observe_async(client, database, name, case, pool):
    documents, options = seed(case)
    collection = await client[database].create_collection(name, **options)
    await collection.insert_many(documents)
    result = error = None
    try:
        result = await invoke(collection, case)
    except OperationFailure as exc:
        error = exc
    assert (await client.admin.command("ping"))["ok"] == 1
    effects = await collection.find().sort("_id").to_list()
    following = await collection.insert_one({"_id": "following", "value": 1})
    final = await collection.find().sort("_id").to_list()
    bson = BSONObservation([])
    return {
        **outcome(result, error, bson),
        "documents": bson.documents(effects),
        "following_write": public_result(following, bson),
        "final_documents": bson.documents(final),
        "connection": pool.observation(),
    }


RAW_CASES = tuple((more, fails) for more in (False, True) for fails in (False, True))


def packet(body, request_id, *, more=False):
    payload = struct.pack("<i", 2 if more else 0) + b"\x00" + BSON.encode(body)
    return struct.pack("<iiii", len(payload) + 16, request_id, 0, WIRE_OP_MSG) + payload


async def reply(reader, request_id):
    header = await asyncio.wait_for(reader.readexactly(16), timeout=3)
    length, _, response_to, opcode = struct.unpack("<iiii", header)
    assert opcode == WIRE_OP_MSG
    assert response_to == request_id
    payload = await asyncio.wait_for(reader.readexactly(length - 16), timeout=3)
    assert payload[:5] == b"\x00" * 5
    return BSON(payload[5:]).decode()


async def observe_raw(uri, database, name, *, more, fails):
    address = urlsplit(uri)
    reader, writer = await asyncio.open_connection(
        address.hostname, address.port or 27017
    )
    seed_command = {
        "insert": name,
        "documents": [{"_id": "a", "value": 0}],
        "$db": database,
    }
    command = (
        {"update": name, "updates": [{"u": {"$set": {"value": 1}}}]}
        if fails
        else {"insert": name, "documents": [{"_id": "b", "value": 1}]}
    )
    command.update({"writeConcern": {"w": 0}, "$db": database})
    try:
        writer.write(packet(seed_command, 1))
        await writer.drain()
        assert (await reply(reader, 1))["ok"] == 1
        # Pipeline the next command: no timing-based absence assertion and no
        # new socket can conceal a stray response to the unacknowledged write.
        writer.write(
            packet(command, 2, more=more) + packet({"ping": 1, "$db": "admin"}, 3)
        )
        await writer.drain()
        write_response = None
        if not more:
            document = await reply(reader, 2)
            write_response = command_reply(document)
        assert (await reply(reader, 3))["ok"] == 1
        writer.write(
            packet({"find": name, "filter": {}, "sort": {"_id": 1}, "$db": database}, 4)
        )
        await writer.drain()
        documents = (await reply(reader, 4))["cursor"]["firstBatch"]
        following = {**seed_command, "documents": [{"_id": "following", "value": 1}]}
        writer.write(packet(following, 5))
        await writer.drain()
        assert (await reply(reader, 5))["n"] == 1
        return {
            "write_response": write_response,
            "documents": documents,
            "following_inserted": 1,
        }
    finally:
        writer.close()
        await writer.wait_closed()
