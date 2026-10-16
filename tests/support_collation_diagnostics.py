"""Capture opaque index keys from server failures without importing a candidate."""

from pymongo.errors import DuplicateKeyError

from tests.differential.collation_diagnostic_cases import (
    COLLATION_KEY_OPTIONS,
    COLLATION_KEY_VALUES,
)


def observe_collation_keys(database):
    collection = database.collation_keys
    rows = []
    for options in COLLATION_KEY_OPTIONS:
        collection.drop()
        collection.create_index("key", unique=True, collation=options)
        for value in COLLATION_KEY_VALUES:
            collection.delete_many({})
            collection.insert_one({"_id": "b", "key": value})
            try:
                collection.find_one_and_update(
                    {"_id": "a"}, {"$set": {"key": value}}, upsert=True
                )
            except DuplicateKeyError as error:
                details = dict(error.details)
                details["errmsg"] = details["errmsg"].replace(
                    collection.full_name, "$namespace"
                )
                rows.append(
                    {
                        "options": options,
                        "value": value,
                        "code": error.code,
                        "details": details,
                    }
                )
            else:
                message = "native duplicate expected"
                raise AssertionError(message)
    return rows


async def observe_async_collation_keys(database):
    collection = database.collation_keys
    rows = []
    for options in COLLATION_KEY_OPTIONS:
        await collection.drop()
        await collection.create_index("key", unique=True, collation=options)
        for value in COLLATION_KEY_VALUES:
            await collection.delete_many({})
            await collection.insert_one({"_id": "b", "key": value})
            try:
                await collection.find_one_and_update(
                    {"_id": "a"}, {"$set": {"key": value}}, upsert=True
                )
            except DuplicateKeyError as error:
                details = dict(error.details)
                details["errmsg"] = details["errmsg"].replace(
                    collection.full_name, "$namespace"
                )
                rows.append(
                    {
                        "options": options,
                        "value": value,
                        "code": error.code,
                        "details": details,
                    }
                )
            else:
                message = "native duplicate expected"
                raise AssertionError(message)
    return rows
