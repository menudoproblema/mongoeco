"""Native type expectations reached through SQL, virtual indexes and fallback."""

import re

from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import pytest

from bson.json_util import loads

from mongoeco import MongoClient
from mongoeco.core.filtering import QueryEngine
from mongoeco.engines import SQLiteEngine

from tests.differential.semantic_type_query_cases import SPECS, VALUES


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize(
    "index", ["none", "scalar", "multikey", "partial", "partial_type"]
)
@pytest.mark.parametrize("alias", ["binData", "objectId", "date"])
@pytest.mark.parametrize("numeric", [False, True])
def test_type_queries_reach_exact_sql_or_declared_dialect_fallback(
    tmp_path, dialect, index, alias, numeric
):
    path = str(tmp_path / "types.sqlite")
    field = "a" if index == "multikey" else "v"
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.type_paths.records
        collection.insert_many(
            [
                {"_id": i, "v": deepcopy(v), "a": [deepcopy(v)], "enabled": True}
                for i, v in enumerate(VALUES, 1)
            ]
            + [{"_id": len(VALUES) + 1, "enabled": True}]
        )
        if index != "none":
            options = {}
            if index == "partial":
                options["partial_filter_expression"] = {"enabled": True}
            if index == "partial_type":
                options["partial_filter_expression"] = {"v": {"$type": "binData"}}
            collection.create_index([(field, 1)], **options)
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.type_paths.records
        spec = {"binData": 5, "objectId": 7, "date": 9}[alias] if numeric else alias
        query = {field: {"$type": spec}}
        if index == "partial":
            query["enabled"] = True
        fixture = (
            Path(__file__).parents[2]
            / "fixtures"
            / f"semantic_type_query_{dialect.replace('.', '_')}.json"
        )
        expected = loads(fixture.read_text())["cases"]["semantic_type_selection"][
            f"{SPECS.index(spec)}-{field}"
        ]
        statements = []
        engine._connection.set_trace_callback(statements.append)
        open_connection = engine._create_sqlite_connection

        def traced_connection():
            connection = open_connection()
            connection.set_trace_callback(statements.append)
            return connection

        with (
            patch.object(
                engine, "_create_sqlite_connection", side_effect=traced_connection
            ),
            patch.object(
                QueryEngine, "match_plan", wraps=QueryEngine.match_plan
            ) as matcher,
        ):
            actual = [d["_id"] for d in collection.find(query, sort=[("_id", 1)])]
        engine._connection.set_trace_callback(None)
        assert actual == expected
        assert (matcher.call_count == 0) == (dialect in {"7.0", "8.0"})
        assert any(
            sql.lstrip().startswith("SELECT")
            and re.search(r"\b(?:documents\.)?document\b", sql.partition("FROM")[0])
            for sql in statements
        )
        with patch.object(
            engine,
            "_translate_query_plan_with_multikey",
            side_effect=NotImplementedError,
        ):
            assert [
                d["_id"] for d in collection.find(query, sort=[("_id", 1)])
            ] == expected
