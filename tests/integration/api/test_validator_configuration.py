"""Validator preparation and persistence retain effective collection semantics."""

from unittest.mock import patch

import pytest

from bson import Decimal128

from mongoeco import MongoClient
from mongoeco.core.schema_validation import compile_collection_validator
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DocumentValidationFailure, OperationFailure


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_bulk_insert_prepares_one_validator_for_its_operation(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.validators.create_collection(
            "batch", validator={"$jsonSchema": {"required": ["a"]}}
        )
        with patch(
            "mongoeco.engines.semantic_core.compile_collection_validator",
            wraps=compile_collection_validator,
        ) as prepare:
            collection.insert_many([{"_id": i, "a": 1} for i in range(10)])
            assert prepare.call_count == 1


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_validator_options_reopen_with_types_order_and_replaced_rules(
    tmp_path, dialect
):
    path = str(tmp_path / "validators.sqlite")
    validator = {"$jsonSchema": {"properties": {"a": {"enum": [Decimal128("1")]}}}}
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
        collection = client.validators.create_collection("records", validator=validator)
        collection.insert_one({"_id": 1, "a": Decimal128("1")})
        client.validators.command(
            {"collMod": "records", "validator": {"a": {"y": 1, "x": 2}}}
        )
        with pytest.raises(OperationFailure):
            client.validators.command(
                {"collMod": "records", "validator": {"$jsonSchema": {"enum": []}}}
            )
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
        collection = client.validators.records
        assert list(collection.options()["validator"]["a"]) == ["y", "x"]
        collection.insert_one({"_id": 2, "a": {"y": 1, "x": 2}})
        with pytest.raises(DocumentValidationFailure):
            collection.insert_one({"_id": 3, "a": {"x": 2, "y": 1}})
        client.validators.command({"collMod": "records", "validator": validator})
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
        collection = client.validators.records
        assert isinstance(
            collection.options()["validator"]["$jsonSchema"]["properties"]["a"]["enum"][
                0
            ],
            Decimal128,
        )
        collection.insert_one({"_id": 4, "a": 1})
        with pytest.raises(DocumentValidationFailure):
            collection.insert_one({"_id": 5, "a": True})
        assert collection.count_documents({}) == 3  # noqa: PLR2004 - only accepted writes survive


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_unsupported_out_is_explicit_and_does_not_modify_documents(engine_factory):
    # $out has no productive caller in this runtime. It stays explicitly outside
    # the supported subset; schema work must not create a partial writeback route.
    with MongoClient(engine_factory()) as client:
        collection = client.validators.records
        collection.insert_one({"_id": 1, "a": 1})
        with pytest.raises(OperationFailure):
            list(collection.aggregate([{"$out": "records"}]))
        assert list(collection.find({})) == [{"_id": 1, "a": 1}]


@pytest.mark.parametrize("commit", [True, False])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_option_changes_follow_local_transaction_ownership(engine_factory, commit):
    # Local engine transaction contract; native standalone transaction support
    # is not inferred from the validator differential.
    with MongoClient(engine_factory()) as client:
        collection = client.validators.create_collection("records", validator={"a": 1})
        with client.start_session() as session:
            session.start_transaction()
            client.validators.command(
                {"collMod": "records", "validator": {"a": 2}}, session=session
            )
            if commit:
                session.commit_transaction()
            else:
                session.abort_transaction()
        assert collection.options()["validator"] == {"a": 2 if commit else 1}


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_coll_mod_optional_engine_capability_fails_without_changing_validation(
    engine_factory,
):
    engine = engine_factory()
    with MongoClient(engine) as client:
        collection = client.validators.create_collection(
            "optional", validator={"a": {"$gte": 0}}
        )
        collection.insert_one({"_id": 1, "a": 1})
        before = collection.options()
        # An SPI v2 consumer need not implement this builtin-only capability.
        with (
            patch.object(engine, "_modify_collection_options", None),
            pytest.raises(OperationFailure) as error,
        ):
            client.validators.command({"collMod": "optional", "validationLevel": "off"})
        assert error.value.code == 59  # noqa: PLR2004 - command capability diagnostic
        assert collection.options() == before
        with pytest.raises(OperationFailure) as error:
            collection.insert_one({"_id": 2, "a": -1})
        assert error.value.code == 121  # noqa: PLR2004 - document validation diagnostic
        assert list(collection.find()) == [{"_id": 1, "a": 1}]
