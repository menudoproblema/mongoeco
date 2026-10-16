"""Run ten fixed public concurrent histories for each declared direct API cell."""

import asyncio
import json
import os
import tempfile
import unittest

from contextlib import AsyncExitStack, ExitStack
from itertools import product
from pathlib import Path

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.sqlite import SQLiteEngine

from tests.support import ENGINE_FACTORIES
from tests.support_write_histories import (
    HISTORY_CASES,
    observe_async_history,
    observe_history,
)


def save_histories(name, histories):
    if directory := os.getenv("MONGOECO_WRITE_HISTORY_OUTPUT"):
        Path(directory).mkdir(parents=True, exist_ok=True)
        (Path(directory) / f"{name}.json").write_text(
            json.dumps(histories, indent=2) + "\n"
        )


EXPECTED_SURFACE_HISTORIES = 720


class WriteHistoryTests(unittest.TestCase):
    def test_sync_histories(self):
        histories = []
        for target, profile, (engine, factory) in product(
            ("7.0", "8.0", "9.0"), ("4.9", "4.19"), ENGINE_FACTORIES.items()
        ):
            with ExitStack() as stack:
                directory = stack.enter_context(tempfile.TemporaryDirectory())
                client = stack.enter_context(
                    MongoClient(
                        factory(), mongodb_dialect=target, pymongo_profile=profile
                    )
                )
                first = second = None
                if engine == "sqlite":
                    file = str(Path(directory) / "cas.sqlite")
                    first = stack.enter_context(
                        MongoClient(
                            SQLiteEngine(file),
                            mongodb_dialect=target,
                            pymongo_profile=profile,
                        )
                    )
                    second = stack.enter_context(
                        MongoClient(
                            SQLiteEngine(file),
                            mongodb_dialect=target,
                            pymongo_profile=profile,
                        )
                    )
                for identifier, repetition in product(HISTORY_CASES, range(10)):
                    with self.subTest(
                        target=target,
                        profile=profile,
                        engine=engine,
                        history=identifier,
                        repetition=repetition,
                    ):
                        name = f"{identifier}_{repetition}"
                        a, b = (
                            (first.history[name], second.history[name])
                            if identifier == "Q04" and engine == "sqlite"
                            else (client.history[name], client.history[name])
                        )
                        history = observe_history([a, b], identifier)
                        histories.append(
                            {
                                "target": target,
                                "profile": profile,
                                "engine": engine,
                                "surface": "sync",
                                "repetition": repetition,
                                **history,
                            }
                        )
        assert len(histories) == EXPECTED_SURFACE_HISTORIES
        save_histories("sync", histories)

    def test_async_histories(self):
        async def exercise():
            histories = []
            for target, profile, (engine, factory) in product(
                ("7.0", "8.0", "9.0"), ("4.9", "4.19"), ENGINE_FACTORIES.items()
            ):
                with tempfile.TemporaryDirectory() as directory:
                    async with AsyncExitStack() as stack:
                        client = await stack.enter_async_context(
                            AsyncMongoClient(
                                factory(),
                                mongodb_dialect=target,
                                pymongo_profile=profile,
                            )
                        )
                        first = second = None
                        if engine == "sqlite":
                            file = str(Path(directory) / "cas.sqlite")
                            first = await stack.enter_async_context(
                                AsyncMongoClient(
                                    SQLiteEngine(file),
                                    mongodb_dialect=target,
                                    pymongo_profile=profile,
                                )
                            )
                            second = await stack.enter_async_context(
                                AsyncMongoClient(
                                    SQLiteEngine(file),
                                    mongodb_dialect=target,
                                    pymongo_profile=profile,
                                )
                            )
                        for identifier, repetition in product(HISTORY_CASES, range(10)):
                            with self.subTest(
                                target=target,
                                profile=profile,
                                engine=engine,
                                history=identifier,
                                repetition=repetition,
                            ):
                                name = f"{identifier}_{repetition}"
                                a, b = (
                                    (first.history[name], second.history[name])
                                    if identifier == "Q04" and engine == "sqlite"
                                    else (client.history[name], client.history[name])
                                )
                                history = await observe_async_history(
                                    [a, b], identifier
                                )
                                histories.append(
                                    {
                                        "target": target,
                                        "profile": profile,
                                        "engine": engine,
                                        "surface": "async",
                                        "repetition": repetition,
                                        **history,
                                    }
                                )
            assert len(histories) == EXPECTED_SURFACE_HISTORIES
            save_histories("async", histories)

        asyncio.run(exercise())
