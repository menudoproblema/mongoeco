"""Native matrix entries must retain exact provenance and effective consumers."""

import hashlib

from pathlib import Path

from bson.json_util import loads

from tests.differential.index_guarantee_cases import INDEX_GUARANTEE_CASES
from tests.differential.semantic_guarantee_cases import SEMANTIC_GUARANTEE_CASES


ROOT = Path(__file__).parents[2]
MATRIX = ROOT / "docs/evidence/mongoeco-4.9.0/capture-coverage.json"


def test_current_matrix_provenance_and_native_semantic_consumers():
    matrix = loads(MATRIX.read_text())
    rows = matrix["rows"]
    assert len(rows) == len({(r["corpus"], r["dialect"], r["case"]) for r in rows})
    for row in rows:
        provenance = row["provenance"]
        fixture = ROOT / provenance["fixture"]
        assert (
            hashlib.sha256(fixture.read_bytes()).hexdigest()
            == provenance["fixture_sha256"]
        )
        payload = loads(fixture.read_text())
        assert payload["source"] == "real-mongodb"
        assert payload["targetDialect"] == row["dialect"]
        assert payload["runtime"]["build_info"]["version"] == row["server_version"]
        assert payload["runtime"]["fcv"] == row["fcv"] == {"version": row["dialect"]}
        assert payload["runtime"]["pymongo"] == row["capture_sdk"]
        assert payload["corpus"]["sha256"] == provenance["corpus_sha256"]
        assert row["case"] in payload["cases"]
        if row["disposition"] == "captured-only":
            assert row["exclusions"]
            assert not row["tests"]
        else:
            assert row["guarantee"]
            assert row["local_profiles"]
            for consumer in row["tests"]:
                assert (ROOT / consumer.split("::")[0]).is_file()
    native_rows = [r for r in rows if r["corpus"] == "semantic-guarantees"]
    assert {(r["dialect"], r["case"]) for r in native_rows} == {
        (dialect, case.name)
        for dialect in ("7.0", "8.0", "9.0")
        for case in SEMANTIC_GUARANTEE_CASES
    }
    corpus = ROOT / "tests/differential/semantic_guarantee_cases.py"
    digest = hashlib.sha256(corpus.read_bytes()).hexdigest()
    for row in native_rows:
        assert row["provenance"]["corpus_sha256"] == digest
        assert row["local_profiles"] == ["4.9", "4.18"]
        assert row["engines"] == ["memory", "sqlite"]
        assert row["surfaces"] == ["sync", "async"]
        assert row["routes"] == ["facade", "command"]

    index_rows = [r for r in rows if r["corpus"] == "index-guarantees"]
    assert {(r["dialect"], r["case"]) for r in index_rows} == {
        (dialect, case.name)
        for dialect in ("7.0", "8.0", "9.0")
        for case in INDEX_GUARANTEE_CASES
    }
    digest = hashlib.sha256(
        (ROOT / "tests/differential/index_guarantee_cases.py").read_bytes()
    ).hexdigest()
    for row in index_rows:
        assert row["provenance"]["corpus_sha256"] == digest
        assert row["local_profiles"] == ["4.9"]
        assert row["engines"] == ["memory", "sqlite"]
        assert row["surfaces"] == ["sync", "async"]
        assert row["exclusions"]
