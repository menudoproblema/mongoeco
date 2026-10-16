"""Independent observations of phases, BSON state, indices and recovery."""

import json

from copy import deepcopy

from bson import json_util

from tests.support_write_parity import BSONObservation, public_result


def encoded(value):
    return json.loads(
        json_util.dumps(value, json_options=json_util.CANONICAL_JSON_OPTIONS)
    )


def normalize(value, ns):
    if isinstance(value, str):
        return value.replace(ns, "<namespace>").replace(
            ns.split(".")[0] + ".", "<database>."
        )
    if isinstance(value, dict):
        return {k: normalize(v, ns) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v, ns) for v in value]
    return value


def err(e, ns):
    return {
        "family": type(e).__name__,
        "module": type(e).__module__,
        "code": getattr(e, "code", None),
        "details": normalize(encoded(getattr(e, "details", None)), ns),
        "message": normalize(str(e), ns),
    }


def result(res, case):
    if isinstance(res, (list, dict, str, int, float, bool)) or res is None:
        return encoded(res)
    return public_result(res, BSONObservation(case.documents))


def indexes(rows):
    return sorted(
        [
            {
                k: v
                for k, v in row.items()
                if k
                in [
                    "name",
                    "key",
                    "unique",
                    "expireAfterSeconds",
                    "partialFilterExpression",
                    "sparse",
                    "collation",
                ]
            }
            for row in rows
        ],
        key=lambda row: row["name"],
    )


def observe_error_phases(client, dbname, cases):
    db = client[dbname]
    records = {}
    for original in cases:
        case = deepcopy(original)
        name = case.id
        ns = dbname + "." + name
        stages = []
        try:
            collection = db.create_collection(name, **case.options)
            if case.documents:
                collection.insert_many(case.documents, bypass_document_validation=True)
            for index in case.indexes:
                collection.create_index(
                    index["keys"], **{k: v for k, v in index.items() if k != "keys"}
                )
        except Exception as e:
            records[name] = {"setup_error": err(e, ns)}
            continue
        for request in case.requests:
            try:
                if request.method == "command":
                    command = deepcopy(request.args[0])
                    command[next(iter(command))] = name
                    res = db.command(command)
                else:
                    res = getattr(collection, request.method)(
                        *request.args, **request.kwargs
                    )
                    if request.method == "aggregate":
                        res = list(res)
                stage = {"result": normalize(result(res, case), ns)}
            except Exception as e:
                stage = {"error": err(e, ns)}
            stage["method"] = request.method
            try:
                stage["documents"] = encoded(list(collection.find().sort("_id", 1)))
                stage["indexes"] = encoded(indexes(list(collection.list_indexes())))
            except Exception as e:
                stage["state_error"] = err(e, ns)
            stages.append(stage)
        records[name] = stages
    return records


async def observe_async_error_phases(client, dbname, cases):
    db = client[dbname]
    records = {}
    for original in cases:
        case = deepcopy(original)
        name = case.id
        ns = dbname + "." + name
        stages = []
        try:
            collection = await db.create_collection(name, **case.options)
            if case.documents:
                await collection.insert_many(
                    case.documents, bypass_document_validation=True
                )
            for index in case.indexes:
                await collection.create_index(
                    index["keys"], **{k: v for k, v in index.items() if k != "keys"}
                )
        except Exception as e:
            records[name] = {"setup_error": err(e, ns)}
            continue
        for request in case.requests:
            try:
                if request.method == "command":
                    command = deepcopy(request.args[0])
                    command[next(iter(command))] = name
                    res = await db.command(command)
                else:
                    res = await getattr(collection, request.method)(
                        *request.args, **request.kwargs
                    )
                    if request.method == "aggregate":
                        res = await res.to_list()
                stage = {"result": normalize(result(res, case), ns)}
            except Exception as e:
                stage = {"error": err(e, ns)}
            stage["method"] = request.method
            try:
                stage["documents"] = encoded(
                    await collection.find().sort("_id", 1).to_list()
                )
                stage["indexes"] = encoded(
                    indexes(await (await collection.list_indexes()).to_list())
                )
            except Exception as e:
                stage["state_error"] = err(e, ns)
            stages.append(stage)
        records[name] = stages
    return records


def comparable(value):
    """Normalize transport diagnostics while keeping application BSON intact."""
    if isinstance(value, list):
        return [comparable(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "method" in value and ("result" in value or "error" in value):
        result = deepcopy(value)
        if "error" in result:
            result["error"] = _comparable_error(result["error"])
        if value["method"] == "command" and isinstance(result.get("result"), dict):
            for field in ("writeErrors", "writeConcernErrors"):
                if isinstance(result["result"].get(field), list):
                    result["result"][field] = [
                        _diagnostic_document(item) for item in result["result"][field]
                    ]
        for index in result.get("indexes", []):
            if "unique" in index and (index["name"] == "_id_" or not index["unique"]):
                del index["unique"]
        return result
    return {key: comparable(item) for key, item in value.items()}


def _comparable_error(error):
    result = {
        key: deepcopy(value)
        for key, value in error.items()
        if key not in {"module", "message"}
    }
    if result.get("family") == "DocumentValidationFailure":
        result["family"] = "WriteError"
    if isinstance(result.get("details"), dict):
        result["details"] = _diagnostic_document(result["details"])
    return result


def _diagnostic_document(document):
    result = deepcopy(document)
    if "errmsg" in result:
        diagnostic = result["errmsg"]
        result["errmsg"] = {
            "string": isinstance(diagnostic, str),
            "nonempty": bool(diagnostic),
            "exception_repr": isinstance(diagnostic, str)
            and ", full error:" in diagnostic,
        }
    for field in ("writeErrors", "writeConcernErrors"):
        if isinstance(result.get(field), list):
            result[field] = [_diagnostic_document(item) for item in result[field]]
    return result
