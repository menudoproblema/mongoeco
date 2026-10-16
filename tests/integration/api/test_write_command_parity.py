"""Classic raw and wire write routes retain native responses and partial effects."""

import asyncio
import json
import unittest

from copy import deepcopy
from itertools import product
from pathlib import Path

import pymongo

from mongoeco import MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.write_cases import WRITE_CASES
from tests.integration.api.test_write_parity_corpus import native_observations
from tests.support import ENGINE_FACTORIES, open_client
from tests.support_write_parity import (
    RAW_CASE_IDS,
    BSONObservation,
    assert_write_observations,
    observe_async_raw_write,
    observe_async_write_case,
    observe_raw_write,
    observe_write_case,
    public_error,
)


CASES = [case for case in WRITE_CASES if case.id in RAW_CASE_IDS]


def raw_references(target):
    path = (
        Path(__file__).parents[2]
        / "fixtures"
        / f"write_raw_mongodb_{target.replace('.', '_')}.json"
    )
    data = json.loads(path.read_text())["observations"]
    assert set(data) == set(RAW_CASE_IDS)
    return data


class WriteCommandParityTests(unittest.TestCase):
    def test_raw_commands_match_native_same_route(self):
        async def exercise():
            for target, (engine, factory), surface in product(
                ("7.0", "8.0", "9.0"), ENGINE_FACTORIES.items(), ("sync", "async")
            ):
                refs = raw_references(target)
                if surface == "sync":
                    with MongoClient(
                        factory(), mongodb_dialect=target, pymongo_profile="4.19"
                    ) as client:
                        for case in CASES:
                            with self.subTest(
                                target=target,
                                engine=engine,
                                surface=surface,
                                case=case.id,
                            ):
                                self.assertEqual(
                                    observe_raw_write(client.raw_writes, case.id, case),
                                    refs[case.id],
                                )
                else:
                    async with open_client(
                        engine, mongodb_dialect=target, pymongo_profile="4.19"
                    ) as client:
                        for case in CASES:
                            with self.subTest(
                                target=target,
                                engine=engine,
                                surface=surface,
                                case=case.id,
                            ):
                                self.assertEqual(
                                    await observe_async_raw_write(
                                        client.raw_writes, case.id, case
                                    ),
                                    refs[case.id],
                                )

        asyncio.run(exercise())

    def test_wire_commands_and_helpers_match_native(self):
        async def exercise():
            for target, (engine, factory), surface in product(
                ("7.0", "8.0", "9.0"), ENGINE_FACTORIES.items(), ("sync", "async")
            ):
                refs = raw_references(target)
                helpers = native_observations(target)
                async with AsyncMongoEcoProxyServer(
                    engine=factory(), mongodb_dialect=target, pymongo_profile="4.19"
                ) as proxy:
                    options = {
                        "directConnection": True,
                        "serverSelectionTimeoutMS": 3000,
                    }

                    def sync_calls(
                        cell=(
                            options,
                            target,
                            engine,
                            surface,
                            refs,
                            helpers,
                            proxy.address.uri,
                        ),
                    ):
                        options, target, engine, surface, refs, helpers, uri = cell
                        with pymongo.MongoClient(uri, **options) as client:
                            for case in CASES:
                                with self.subTest(
                                    target=target,
                                    engine=engine,
                                    surface=surface,
                                    case=case.id,
                                ):
                                    self.assertEqual(
                                        observe_raw_write(
                                            client.raw_writes, case.id, case
                                        ),
                                        refs[case.id],
                                    )
                                    actual = observe_write_case(
                                        client.helper_writes, case.id, case
                                    )
                                    assert_write_observations(
                                        actual, helpers[case.id], case
                                    )

                    if surface == "sync":
                        await asyncio.to_thread(sync_calls)
                    else:
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, **options
                        ) as client:
                            for case in CASES:
                                with self.subTest(
                                    target=target,
                                    engine=engine,
                                    surface=surface,
                                    case=case.id,
                                ):
                                    self.assertEqual(
                                        await observe_async_raw_write(
                                            client.raw_writes, case.id, case
                                        ),
                                        refs[case.id],
                                    )
                                    actual = await observe_async_write_case(
                                        client.helper_writes, case.id, case
                                    )
                                    assert_write_observations(
                                        actual, helpers[case.id], case
                                    )

        asyncio.run(exercise())


class WriteCommandPreflightTests(unittest.TestCase):
    def test_malformed_final_spec_is_rejected_before_any_write(self):
        path = Path(__file__).parents[2] / "fixtures" / "write_command_preflight.json"
        references = json.loads(path.read_text())
        for (engine, factory), reference in product(
            ENGINE_FACTORIES.items(), references
        ):
            with (
                self.subTest(
                    engine=engine,
                    target=reference["target"],
                    request=reference["request"],
                ),
                MongoClient(
                    factory(),
                    mongodb_dialect=reference["target"],
                    pymongo_profile="4.19",
                ) as client,
            ):
                client.shapes.cases.insert_one({"_id": "a"})
                with self.assertRaises(pymongo.errors.OperationFailure) as failure:
                    client.shapes.command(deepcopy(reference["request"]))
                self.assertEqual(
                    public_error(failure.exception, BSONObservation([])),
                    reference["error"],
                )
                self.assertEqual(
                    list(client.shapes.cases.find({})), reference["documents"]
                )
