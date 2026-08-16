import unittest

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


class MergeFieldOrderTests(unittest.TestCase):
    def test_merge_mode_uses_update_field_order_for_insert_and_match(self):
        for engine_type in (MemoryEngine, SQLiteEngine):
            with (
                self.subTest(engine=engine_type.__name__),
                MongoClient(engine_type()) as client,
            ):
                source = client.test.source
                target = client.test.target
                source.insert_many(
                    [
                        {"_id": "existing", "z": 2, "a": 1, "newB": 2, "newA": 1},
                        {"_id": "new", "z": 2, "a": 1, "newB": 2, "newA": 1},
                    ],
                )
                target.insert_one({"_id": "existing", "existing": True, "z": 0})

                source.aggregate(
                    [
                        {
                            "$merge": {
                                "into": "target",
                                "whenMatched": "merge",
                                "whenNotMatched": "insert",
                            },
                        },
                    ],
                ).to_list()

                existing = target.find_one({"_id": "existing"})
                inserted = target.find_one({"_id": "new"})
                assert existing is not None
                assert inserted is not None
                self.assertEqual(
                    list(existing),
                    ["_id", "existing", "z", "a", "newA", "newB"],
                )
                self.assertEqual(
                    list(inserted),
                    ["_id", "a", "newA", "newB", "z"],
                )


if __name__ == "__main__":
    unittest.main()
