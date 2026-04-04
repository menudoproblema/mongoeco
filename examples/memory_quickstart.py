from __future__ import annotations

import asyncio

from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine


async def main() -> None:
    async with AsyncMongoClient(MemoryEngine()) as client:
        collection = client.demo.users

        await collection.insert_many(
            [
                {"_id": "1", "name": "Ada", "roles": ["math", "programming"]},
                {"_id": "2", "name": "Grace", "roles": ["compiler", "navy"]},
            ]
        )

        first_user = await collection.find_one({"name": "Ada"})
        print("find_one:", first_user)

        names = [document["name"] async for document in collection.find({}, {"name": 1, "_id": 0})]
        print("names:", names)

        await collection.update_one({"_id": "2"}, {"$set": {"active": True}})
        print("updated:", await collection.find_one({"_id": "2"}))


if __name__ == "__main__":
    asyncio.run(main())
