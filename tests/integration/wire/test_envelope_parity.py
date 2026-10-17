"""Transport effects, codes and connection reuse match the native oracle."""

import asyncio
import hashlib
import json

from pathlib import Path

import pytest

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.support_wire_envelopes import CASES, observe_async, observe_sync


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize(
    "capture", [(b, c) for b in ("memory", "sqlite") for c in (0, 128)]
)
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", CASES)
def test_envelopes_match_native(target, capture, surface, case, tmp_path):
    backend, capacity = capture
    root = Path(__file__).parents[3]
    capture = json.loads(
        (
            root
            / f"tests/fixtures/wire_envelopes_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for source, digest in capture["source_sha256"].items():
        assert hashlib.sha256((root / source).read_bytes()).hexdigest() == digest
    assert set(capture["case_ids"]) == set(CASES)

    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "data.sqlite"))
        )
        async with AsyncMongoEcoProxyServer(
            engine=engine, mongodb_dialect=target, diagnostic_capacity=capacity
        ) as proxy:
            if surface == "sync":
                return await asyncio.to_thread(
                    observe_sync, proxy.address.uri, "probe", case
                )
            return await observe_async(proxy.address.uri, "probe", case)

    # Round-trip JSON normalizes tuple pairs from socket observations.
    assert (
        json.loads(json.dumps(asyncio.run(exercise()))) == capture["observations"][case]
    )
