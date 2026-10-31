"""Native post-image availability and lookup at consumption, not write time."""

from bson import Binary, Int64

from tests.differential.semantic_watch_cases import _next


SEED = [{"_id": "initial"}]


def images(collection, scope):
    output = {}
    database = collection.database
    client = getattr(collection, "_client", None)
    if client is None:
        client = database.client
    for enabled in (False, True):
        for mode in ("default", "updateLookup", "whenAvailable", "required"):
            for deleted in (False, True):
                name = f"image_{scope}_{enabled}_{mode}_{deleted}"
                subject = database.create_collection(
                    name, changeStreamPreAndPostImages={"enabled": enabled}
                )
                subject.insert_one(
                    {
                        "_id": 1,
                        "v": 0,
                        "n": Int64(7),
                        "b": Binary(b"same", 5),
                        "pad": "x" * 1000,
                    }
                )
                owner = {
                    "collection": subject,
                    "database": database,
                    "client": client,
                }[scope]
                stream = owner.watch(
                    [
                        {"$match": {"ns.db": database.name, "ns.coll": name}},
                        {"$set": {"numberType": {"$type": "$fullDocument.n"}}},
                    ],
                    full_document=mode,
                    max_await_time_ms=20,
                )
                try:
                    subject.update_one({}, {"$set": {"v": 1}})
                    subject.update_one({}, {"$set": {"v": 2}})
                    if deleted:
                        subject.delete_one({})
                    try:
                        event = _next(stream)
                        image = event.get("fullDocument")
                        if image is not None:
                            image = {k: v for k, v in image.items() if k != "pad"}
                        output[name] = {
                            "kind": event["operationType"],
                            "present": "fullDocument" in event,
                            "image": image,
                            "numberType": event["numberType"],
                            "alive": stream.alive,
                        }
                    except Exception as error:
                        output[name] = {
                            "error": type(error).__name__,
                            "code": getattr(error, "code", None),
                            "alive": stream.alive,
                        }
                finally:
                    stream.close()
    return output


def collection_images(collection):
    return images(collection, "collection")


def database_images(collection):
    return images(collection, "database")


def client_images(collection):
    return images(collection, "client")


def availability_changes(collection):
    output = {}
    for initial in (False, True):
        for final in (False, True):
            for mode in ("whenAvailable", "required", "updateLookup"):
                name = f"availability_{initial}_{final}_{mode}"
                subject = collection.database.create_collection(
                    name, changeStreamPreAndPostImages={"enabled": initial}
                )
                subject.insert_one({"_id": 1, "v": 0, "pad": "x" * 1000})
                stream = subject.watch(full_document=mode, max_await_time_ms=20)
                try:
                    subject.update_one({}, {"$set": {"v": 1}})
                    collection.database.command(
                        {
                            "collMod": name,
                            "changeStreamPreAndPostImages": {"enabled": final},
                        }
                    )
                    try:
                        event = _next(stream)
                        output[name] = {
                            "kind": event["operationType"],
                            "v": None
                            if event.get("fullDocument") is None
                            else event["fullDocument"]["v"],
                            "alive": stream.alive,
                        }
                    except Exception as error:
                        output[name] = {
                            "error": type(error).__name__,
                            "code": getattr(error, "code", None),
                            "alive": stream.alive,
                        }
                finally:
                    stream.close()
    return output


def filtered_required(collection):
    output = {}
    for name, pipeline in (
        (
            "expr_false",
            [{"$match": {"$expr": {"$or": [False, {"$eq": ["$documentKey._id", 2]}]}}}],
        ),
        ("expr_type", [{"$match": {"$expr": {"$eq": ["$operationType", "insert"]}}}]),
        (
            "expr_root",
            [{"$match": {"$expr": {"$eq": ["$$ROOT.operationType", "insert"]}}}],
        ),
        ("expr_image", [{"$match": {"$expr": {"$eq": ["$fullDocument.v", 2]}}}]),
        ("insert", [{"$match": {"operationType": "insert"}}]),
        ("key", [{"$match": {"documentKey._id": 2}}]),
        ("image", [{"$match": {"fullDocument.v": 2}}]),
        ("project", [{"$project": {"_id": 1, "operationType": 1, "documentKey": 1}}]),
        ("mixed", [{"$match": {"operationType": "insert", "fullDocument.v": 2}}]),
        (
            "or",
            [
                {
                    "$match": {
                        "$or": [{"operationType": "insert"}, {"documentKey._id": 2}]
                    }
                }
            ],
        ),
        (
            "or_mixed",
            [
                {
                    "$match": {
                        "$or": [
                            {"operationType": "delete"},
                            {"operationType": "insert", "fullDocument.v": 2},
                        ]
                    }
                }
            ],
        ),
        (
            "not",
            [
                {
                    "$match": {
                        "operationType": {"$not": {"$eq": "update"}},
                        "fullDocument.v": 2,
                    }
                }
            ],
        ),
    ):
        subject = collection.database.create_collection("filtered_" + name)
        subject.insert_one({"_id": 1, "v": 0, "pad": "x" * 1000})
        stream = subject.watch(pipeline, full_document="required", max_await_time_ms=20)
        try:
            subject.update_one({}, {"$set": {"v": 1}})
            subject.insert_one({"_id": 2, "v": 2})
            try:
                event = _next(stream)
                output[name] = {
                    "kind": event["operationType"],
                    "key": event.get("documentKey"),
                    "alive": stream.alive,
                }
            except Exception as error:
                output[name] = {
                    "error": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "alive": stream.alive,
                }
        finally:
            stream.close()
    return output


def option_validation(collection):
    output = {}
    database = collection.database
    for index, value in enumerate(
        (
            None,
            False,
            [],
            {},
            {"wrong": True},
            {"enabled": False},
            {"enabled": True},
            {"enabled": 0},
            {"enabled": 1},
            {"enabled": "true"},
            {"enabled": None},
            {"enabled": True, "wrong": 0},
        )
    ):
        for command in ("create", "collMod"):
            name = f"option_{command}_{index}"
            if command == "collMod":
                database.create_collection(name)
            try:
                database.command({command: name, "changeStreamPreAndPostImages": value})
                output[name] = {"ok": True}
            except Exception as error:
                output[name] = {
                    "error": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
    return output


def filters_after_stages(collection):
    output = {}
    cases = {
        "set_unrelated": [
            {"$set": {"tag": 1}},
            {"$match": {"operationType": "insert"}},
        ],
        "set_alias": [
            {"$set": {"kind": "$operationType"}},
            {"$match": {"kind": "insert"}},
        ],
        "project_metadata": [
            {"$project": {"_id": 1, "operationType": 1, "fullDocument": 1}},
            {"$match": {"operationType": "insert"}},
        ],
        "unset_image": [
            {"$unset": "fullDocument"},
            {"$match": {"operationType": "insert"}},
        ],
        "replace_merge": [
            {"$replaceWith": {"$mergeObjects": ["$$ROOT", {"tag": 1}]}},
            {"$match": {"operationType": "insert"}},
        ],
        "set_operation_type": [
            {"$set": {"operationType": "insert"}},
            {"$match": {"operationType": "update"}},
        ],
    }
    for name, pipeline in cases.items():
        subject = collection.database.create_collection("staged_" + name)
        subject.insert_one({"_id": 1, "v": 0, "pad": "x" * 1000})
        stream = subject.watch(pipeline, full_document="required", max_await_time_ms=20)
        try:
            subject.update_one({}, {"$set": {"v": 1}})
            subject.insert_one({"_id": 2, "v": 2})
            try:
                event = _next(stream)
                output[name] = {
                    "kind": event["operationType"],
                    "key": event.get("documentKey"),
                    "hasImage": "fullDocument" in event,
                    "hasTag": "tag" in event,
                    "alias": event.get("kind"),
                    "alive": stream.alive,
                }
            except Exception as error:
                output[name] = {
                    "error": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "alive": stream.alive,
                }
        finally:
            stream.close()
    return output


def non_update_images(collection):
    output = {}
    for enabled in (False, True):
        for mode in ("default", "updateLookup", "whenAvailable", "required"):
            for operation in ("insert", "replace"):
                name = f"non_update_{enabled}_{mode}_{operation}"
                subject = collection.database.create_collection(
                    name, changeStreamPreAndPostImages={"enabled": enabled}
                )
                if operation == "replace":
                    subject.insert_one({"_id": 1, "v": 0})
                stream = subject.watch(full_document=mode, max_await_time_ms=20)
                try:
                    image = {"_id": 1, "v": Int64(7), "b": Binary(b"same", 5)}
                    if operation == "replace":
                        subject.replace_one({}, image)
                    else:
                        subject.insert_one(image)
                    event = _next(stream)
                    output[name] = {
                        "kind": event["operationType"],
                        "image": event.get("fullDocument"),
                        "alive": stream.alive,
                    }
                finally:
                    stream.close()
    return output


SEMANTIC_WATCH_IMAGE_CASES = (
    ("semantic_watch_collection_images", collection_images),
    ("semantic_watch_database_images", database_images),
    ("semantic_watch_client_images", client_images),
    ("semantic_watch_availability_changes", availability_changes),
    ("semantic_watch_filtered_required", filtered_required),
    ("semantic_watch_image_option_validation", option_validation),
    ("semantic_watch_filters_after_stages", filters_after_stages),
    ("semantic_watch_non_update_images", non_update_images),
)
