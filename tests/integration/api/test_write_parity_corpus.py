"""The finite write corpus must match independent native captures in every cell."""

import asyncio
import hashlib
import json
import unittest

from itertools import product
from pathlib import Path

from mongoeco import MongoClient

from tests.differential.write_cases import WRITE_CASES
from tests.support import ENGINE_FACTORIES, open_client
from tests.support_write_parity import (
    assert_write_observations,
    observe_async_write_case,
    observe_write_case,
)


TARGETS = ("7.0", "8.0", "9.0")
PROFILES = ("4.9", "4.19")


def native_observations(target):
    path = (
        Path(__file__).parents[2]
        / "fixtures"
        / f"write_parity_mongodb_{target.replace('.', '_')}.json"
    )
    capture = json.loads(path.read_text())
    assert set(capture["observations"]) == {case.id for case in WRITE_CASES}
    test_root = Path(__file__).parents[2]
    for key, relative in (
        ("catalog_sha256", "differential/write_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
    ):
        assert (
            capture[key]
            == hashlib.sha256((test_root / relative).read_bytes()).hexdigest()
        )
    return capture["observations"]


class WriteParityCorpusTests(unittest.TestCase):
    def test_sync_complete_native_write_observations(self):
        for target, profile, (engine, factory) in product(
            TARGETS, PROFILES, ENGINE_FACTORIES.items()
        ):
            expected = native_observations(target)
            with MongoClient(
                factory(), mongodb_dialect=target, pymongo_profile=profile
            ) as client:
                for case in WRITE_CASES:
                    with self.subTest(
                        target=target, profile=profile, engine=engine, case=case.id
                    ):
                        actual = observe_write_case(client.write_corpus, case.id, case)
                        assert_write_observations(actual, expected[case.id], case)

    def test_async_complete_native_write_observations(self):
        async def exercise():
            for target, profile, engine in product(TARGETS, PROFILES, ENGINE_FACTORIES):
                expected = native_observations(target)
                async with open_client(
                    engine, mongodb_dialect=target, pymongo_profile=profile
                ) as client:
                    for case in WRITE_CASES:
                        with self.subTest(
                            target=target, profile=profile, engine=engine, case=case.id
                        ):
                            actual = await observe_async_write_case(
                                client.write_corpus, case.id, case
                            )
                            assert_write_observations(actual, expected[case.id], case)

        asyncio.run(exercise())
