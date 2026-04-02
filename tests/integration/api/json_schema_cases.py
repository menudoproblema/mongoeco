from __future__ import annotations


def _rich_top_level_json_schema() -> dict[str, object]:
    return {
        "$jsonSchema": {
            "required": ["tenant", "profile", "status", "score"],
            "additionalProperties": False,
            "properties": {
                "_id": {"bsonType": "string"},
                "tenant": {"bsonType": "string", "minLength": 1, "maxLength": 4},
                "status": {"bsonType": "string", "minLength": 3, "maxLength": 8},
                "score": {"bsonType": "int", "minimum": 5, "maximum": 10},
                "profile": {
                    "bsonType": "object",
                    "required": ["name", "tags"],
                    "additionalProperties": False,
                    "properties": {
                        "name": {"bsonType": "string", "minLength": 3, "maxLength": 5},
                        "tags": {
                            "bsonType": "array",
                            "items": {"bsonType": "string", "minLength": 2},
                        },
                    },
                },
            },
        },
        "tenant": "a",
    }


def _rich_top_level_json_schema_documents() -> list[dict[str, object]]:
    return [
        {
            "_id": "1",
            "tenant": "a",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["ml", "db"]},
        },
        {
            "_id": "2",
            "tenant": "a",
            "status": "ready",
            "score": 4,
            "profile": {"name": "Ada", "tags": ["ml", "db"]},
        },
        {
            "_id": "3",
            "tenant": "a",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["x"]},
        },
        {
            "_id": "4",
            "tenant": "a",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["ml"], "extra": True},
        },
        {
            "_id": "5",
            "tenant": "b",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["ml", "db"]},
        },
    ]


async def assert_async_find_supports_richer_top_level_json_schema_filter(client) -> None:
    collection = client.validation.get_collection("rich_query_users")
    await collection.insert_many(_rich_top_level_json_schema_documents())

    result = await collection.find(
        _rich_top_level_json_schema(),
        sort=[("_id", 1)],
    ).to_list()

    assert result == [
        {
            "_id": "1",
            "tenant": "a",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["ml", "db"]},
        }
    ]


def assert_sync_find_supports_richer_top_level_json_schema_filter(client) -> None:
    collection = client.validation.get_collection("rich_query_users")
    collection.insert_many(_rich_top_level_json_schema_documents())

    result = collection.find(
        _rich_top_level_json_schema(),
        sort=[("_id", 1)],
    ).to_list()

    assert result == [
        {
            "_id": "1",
            "tenant": "a",
            "status": "ready",
            "score": 7,
            "profile": {"name": "Ada", "tags": ["ml", "db"]},
        }
    ]
