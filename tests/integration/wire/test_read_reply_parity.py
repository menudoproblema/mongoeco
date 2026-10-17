"""Native reply suppression is controlled by wire flags, including mirrored reads."""

import asyncio
import hashlib
import json

from pathlib import Path

import pytest

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.support_wire_reads import CASES, case_id, observe


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("case", list(CASES), ids=lambda case: case_id(*case))
def test_reads_match_native_reply_and_connection_contract(
    target, backend, case, tmp_path
):
    mirrored, more, filter_kind = case
    root = Path(__file__).parents[3]
    capture = json.loads(
        (
            root / f"tests/fixtures/wire_reads_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for source, digest in capture["source_sha256"].items():
        assert hashlib.sha256((root / source).read_bytes()).hexdigest() == digest
    assert set(capture["observations"]) == {case_id(*case) for case in CASES}

    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "data.sqlite"))
        )
        async with AsyncMongoEcoProxyServer(
            engine=engine, mongodb_dialect=target
        ) as proxy:
            return await observe(
                proxy.address.uri,
                "probe",
                mirrored=mirrored,
                more=more,
                filter_kind=filter_kind,
            )

    assert asyncio.run(exercise()) == capture["observations"][case_id(*case)]
