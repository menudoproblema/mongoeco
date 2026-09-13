from __future__ import annotations

import importlib

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines import EngineCapabilities, MemoryEngine, SQLiteEngine


@pytest.mark.parametrize(
    "symbol",
    ["LegacyEngineAdapter", "adapt_engine"],
)
def test_removed_spi_v1_adapter_symbols_cannot_be_imported(symbol: str) -> None:
    module = importlib.import_module("mongoeco.engines.adapter")

    assert not hasattr(module, symbol)


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
def test_builtin_engines_do_not_expose_spi_v1_methods(
    engine_type: type[object],
) -> None:
    engine = engine_type()

    for method in (
        "put_document",
        "put_documents_bulk",
        "search_documents",
        "explain_search_documents",
    ):
        assert not hasattr(engine, method)


def test_spi_v1_capability_declaration_is_rejected() -> None:
    with pytest.raises(ValueError, match="spi_version must be 2"):
        EngineCapabilities(spi_version=1)


def test_shape_only_engine_is_rejected_at_client_boundary() -> None:
    class ShapeOnlyEngine:
        async def connect(self) -> None:
            pass

        async def disconnect(self) -> None:
            pass

    with pytest.raises(TypeError, match="must declare EngineCapabilities"):
        AsyncMongoClient(ShapeOnlyEngine())
