"""Public validation is immediate and independent of surface and engine."""

import asyncio
import importlib.util

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import (
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    PYMONGO_PROFILE_418,
    PyMongoProfile418,
    resolve_pymongo_profile,
    resolve_pymongo_profile_resolution,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import ConfigurationError, MongoEcoError, PyMongoError


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["async", "sync"])
@pytest.mark.parametrize("profile", ["4.9", "4.11", "4.13", "4.17", "4.18"])
def test_reserved_aggregation_keywords_validate_before_returning_cursor(
    backend, surface, profile
):
    def check(collection):
        expected = ConfigurationError if profile == "4.18" else TypeError
        for operation in ("aggregate", "aggregate_raw_batches", "list_search_indexes"):
            helper = getattr(collection, operation)
            positional = () if operation == "list_search_indexes" else ([],)
            with pytest.raises(expected):
                helper(*positional, aggregate="other")
            with pytest.raises(TypeError):
                helper(*positional, unknown_option=True)
            # Ordinary duplicate Python arguments remain binding errors.
            with pytest.raises(expected if not positional else TypeError):
                helper(*positional, pipeline=[])
        with pytest.raises(TypeError, match="both allowDiskUse and allow_disk_use"):
            collection.aggregate([], allowDiskUse=True, allow_disk_use=False)

    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, pymongo_profile=profile) as client:
            collection = client.test.records
            collection.insert_one({"_id": "unchanged"})
            check(collection)
            assert list(collection.aggregate([], allowDiskUse=False)) == [
                {"_id": "unchanged"}
            ]
            assert collection.count_documents({}) == 1
    else:

        async def exercise():
            async with AsyncMongoClient(engine, pymongo_profile=profile) as client:
                collection = client.test.records
                await collection.insert_one({"_id": "unchanged"})
                check(collection)
                assert await collection.aggregate([], allowDiskUse=False).to_list() == [
                    {"_id": "unchanged"}
                ]
                assert await collection.count_documents({}) == 1

        asyncio.run(exercise())


def test_profile_exports_exact_detection_and_defaults():
    assert resolve_pymongo_profile("4.18") is PYMONGO_PROFILE_418
    assert PyMongoProfile418() == PYMONGO_PROFILE_418
    assert DEFAULT_PYMONGO_PROFILE == "4.9"
    assert DEFAULT_MONGODB_DIALECT == "7.0"
    with patch(
        "mongoeco.compat.registry.importlib_metadata.version", return_value="4.18.2"
    ):
        for mode in ("auto-installed", "strict-auto-installed"):
            resolution = resolve_pymongo_profile_resolution(mode)
            assert resolution.resolved_profile is PYMONGO_PROFILE_418
            assert "fallback" not in resolution.resolution_mode


def test_configuration_error_preserves_local_hierarchy_and_labels():
    error = ConfigurationError("invalid options", error_labels=("test-label",))
    assert isinstance(error, (MongoEcoError, PyMongoError))
    assert error.error_labels == ("test-label",)
    assert str(error) == "invalid options"
    if importlib.util.find_spec("pymongo"):
        from pymongo.errors import (  # noqa: PLC0415 - optional driver bridge
            ConfigurationError as RealConfigurationError,
        )

        assert isinstance(error, RealConfigurationError)
        assert error.has_error_label("test-label")
