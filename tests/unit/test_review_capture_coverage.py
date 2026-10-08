"""Stored native observations count only for their explicitly tested scope."""

import hashlib
import json

from pathlib import Path

from bson.json_util import loads

from tests.differential.review_improvement_cases import REVIEW_IMPROVEMENT_CASES
from tests.differential.version_delta_cases import VERSION_DELTA_CASES
from tests.integration.api.test_mongodb9_array_indexes import ARRAY_CASES
from tests.integration.api.test_mongodb9_conversions import CASES as CONVERSION_CASES
from tests.integration.api.test_review_improvement_parity import CASES as REVIEW_CASES


ROOT = Path(__file__).parents[2]
MATRIX = ROOT / "docs/evidence/mongodb9-pymongo418-improvements/capture-coverage.json"
DIALECTS = ("7.0", "8.0", "9.0")
CORPORA = {
    "version-deltas": ("version_delta_cases", VERSION_DELTA_CASES),
    "review-improvements": ("review_improvement_cases", REVIEW_IMPROVEMENT_CASES),
}


def test_all_native_captures_have_exact_corpus_identity_and_disposition():
    matrix = json.loads(MATRIX.read_text())
    rows = matrix["rows"]
    keys = {(row["corpus"], row["dialect"], row["case"]) for row in rows}
    assert len(keys) == len(rows)
    assert keys == {
        (corpus, dialect, case.name)
        for corpus, (_, cases) in CORPORA.items()
        for dialect in DIALECTS
        for case in cases
    }
    paths = {entry["case_set"]: entry for entry in matrix["corpora"]}
    for corpus, (module, cases) in CORPORA.items():
        digest = hashlib.sha256(
            (ROOT / f"tests/differential/{module}.py").read_bytes()
        ).hexdigest()
        assert paths[corpus]["sha256"] == digest
        stem = (
            "mongodb_version_deltas"
            if corpus == "version-deltas"
            else "mongodb_review_improvements"
        )
        for dialect in DIALECTS:
            fixture = loads(
                (
                    ROOT / f"tests/fixtures/{stem}_{dialect.replace('.', '_')}.json"
                ).read_text()
            )
            assert fixture["source"] == "real-mongodb"
            assert fixture["targetDialect"] == dialect
            assert fixture["corpus"]["sha256"] == digest
            assert fixture["runtime"]["build_info"]["version"].startswith(dialect + ".")
            assert fixture["runtime"]["fcv"] == {"version": dialect}
            assert set(fixture["cases"]) == {case.name for case in cases}
            assert [entry["name"] for entry in fixture["case_manifests"]] == [
                case.name for case in cases
            ]
    for row in rows:
        if row["disposition"] == "captured-only":
            assert row["reason"]
            assert not row["surfaces"]
            assert not row["tests"]
        else:
            assert row["tests"]
            assert row["surfaces"]
            assert row["engines"]
            assert row["routes"]


def test_full_parity_dispositions_follow_actual_fixture_consumers():
    rows = json.loads(MATRIX.read_text())["rows"]
    expected = (
        {
            ("version-deltas", "9.0", case.name)
            for case in (*ARRAY_CASES, *CONVERSION_CASES)
        }
        | {
            ("version-deltas", dialect, case.name)
            for dialect in DIALECTS
            for case in VERSION_DELTA_CASES
            if case.name.startswith("densify_bounds_")
        }
        | {
            ("review-improvements", dialect, case.name)
            for dialect, case in REVIEW_CASES
        }
    )
    actual = {
        (row["corpus"], row["dialect"], row["case"])
        for row in rows
        if row["disposition"].startswith("full-result")
    }
    assert actual == expected
