"""Numeric readers preserve native results without rewriting old index entries."""

import hashlib
import json
import shutil
import sqlite3

from contextlib import closing
from decimal import localcontext
from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine

from tests.differential.semantic_numeric_predicate_cases import SEED, query_observations


FIXTURES = Path(__file__).parents[2] / "fixtures"
LEGACY = FIXTURES / "sqlite/numeric-indexes-before-semantic-fix.sqlite"


def _snapshot(database):
    with closing(sqlite3.connect(database)) as connection:
        return {
            "schema": connection.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall(),
            "documents": connection.execute(
                "SELECT * FROM documents ORDER BY db_name,coll_name,storage_key"
            ).fetchall(),
            **{
                table: connection.execute(query).fetchall()
                for table, query in {
                    "scalar_index_entries": (
                        "SELECT * FROM scalar_index_entries ORDER BY "
                        "collection_id,index_name,storage_key,element_type,type_score,element_key"
                    ),
                    "multikey_entries": (
                        "SELECT * FROM multikey_entries ORDER BY "
                        "collection_id,index_name,storage_key,element_type,type_score,element_key"
                    ),
                }.items()
            },
        }


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("precision", [6, 34])
def test_legacy_index_readers_match_native_without_changing_storage(
    tmp_path, dialect, precision
):
    metadata = json.loads(LEGACY.with_suffix(".json").read_text())
    assert hashlib.sha256(LEGACY.read_bytes()).hexdigest() == metadata["sha256"]
    assert metadata["version"] == "4.11.0"
    corpus = Path(metadata["corpus"]["path"])
    assert (
        hashlib.sha256(corpus.read_bytes()).hexdigest() == metadata["corpus"]["sha256"]
    )
    target = tmp_path / "legacy.sqlite"
    shutil.copyfile(LEGACY, target)
    before = _snapshot(target)
    expected = loads(
        (
            FIXTURES / f"semantic_numeric_predicates_{dialect.replace('.', '_')}.json"
        ).read_text()
    )["cases"]["semantic_numeric_predicate_index"]
    for _ in range(2):
        with localcontext() as context:
            context.prec = precision
            with MongoClient(
                SQLiteEngine(str(target)), mongodb_dialect=dialect
            ) as client:
                assert (
                    query_observations(client.semantic_numeric.records, indexed=True)
                    == expected
                )
        assert _snapshot(target) == before


@pytest.mark.parametrize("dialect", ["7.0", "8.0"])
def test_public_numeric_operands_delegate_tagged_bson_comparisons(dialect):
    with MongoClient(SQLiteEngine(), mongodb_dialect=dialect) as client:
        collection = client.semantic_numeric.records
        collection.insert_many(SEED)
        for field in ("v", "a", "plain", "plainArray"):
            collection.create_index(field)
            for value in (0.1, 1000000000000000128):
                explain = collection.find({field: value}, hint=f"{field}_1").explain()
                pushdown = explain["details"]["pushdown"]
                # Public BSON admission preserves numeric types in operands.
                # Untagged legacy SQL is covered by the planner's raw-number
                # contract; this API must take its conservative tagged fallback.
                assert explain["strategy"] == "python"
                assert pushdown["fallbackReason"] == (
                    "Tagged BSON numeric comparison requires Python fallback"
                )
