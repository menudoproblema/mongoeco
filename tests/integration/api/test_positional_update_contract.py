"""Positional writes preserve MongoDB errors and document atomicity on both engines."""

import unittest

from copy import deepcopy
from itertools import product

from mongoeco import MongoClient

from tests.support import ENGINE_FACTORIES, open_client
from tests.support_positional_updates import (
    POSITIONAL_UPDATE_CASES,
    observe_async_positional_update,
    observe_positional_update,
)


class PositionalUpdateSyncContractTests(unittest.TestCase):
    def test_positional_updates_match_captured_mongodb8_outcomes(self):
        snapshot = deepcopy(POSITIONAL_UPDATE_CASES)
        for (engine, factory), profile in product(
            ENGINE_FACTORIES.items(), ("4.9", "4.19")
        ):
            with MongoClient(
                factory(), mongodb_dialect="8.0", pymongo_profile=profile
            ) as client:
                for index, case in enumerate(POSITIONAL_UPDATE_CASES):
                    with self.subTest(
                        engine=engine, profile=profile, case=case["name"]
                    ):
                        actual = observe_positional_update(
                            client.regression[f"c{index}"], case
                        )
                        self.assertEqual(actual, case["expected"])
        self.assertEqual(POSITIONAL_UPDATE_CASES, snapshot)


class PositionalUpdateAsyncContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_positional_updates_match_captured_mongodb8_outcomes(self):
        snapshot = deepcopy(POSITIONAL_UPDATE_CASES)
        for engine, profile in product(ENGINE_FACTORIES, ("4.9", "4.19")):
            async with open_client(
                engine, mongodb_dialect="8.0", pymongo_profile=profile
            ) as client:
                for index, case in enumerate(POSITIONAL_UPDATE_CASES):
                    with self.subTest(
                        engine=engine, profile=profile, case=case["name"]
                    ):
                        actual = await observe_async_positional_update(
                            client.regression[f"c{index}"], case
                        )
                        self.assertEqual(actual, case["expected"])
        self.assertEqual(POSITIONAL_UPDATE_CASES, snapshot)
