import asyncio
import unittest
from enum import Enum
from unittest.mock import patch

import mongoeco.api._async.collection as async_collection_module
from mongoeco.api.operations import compile_update_operation
from mongoeco.api._async.collection import AsyncCollection
from mongoeco.compat import MongoDialect70
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.upserts import _seed_filter_value, seed_upsert_document
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, DuplicateKeyError, OperationFailure
from mongoeco.types import (
    DeleteMany, DeleteOne, DeleteResult, IndexModel, InsertOne, ReplaceOne,
    ReturnDocument, SearchIndexModel, UpdateMany, UpdateOne, UpdateResult,
)

__all__ = [
    "asyncio",
    "unittest",
    "Enum",
    "patch",
    "async_collection_module",
    "compile_update_operation",
    "AsyncCollection",
    "MongoDialect70",
    "MatchAll",
    "seed_upsert_document",
    "_seed_filter_value",
    "MemoryEngine",
    "SQLiteEngine",
    "BulkWriteError",
    "DuplicateKeyError",
    "OperationFailure",
    "DeleteMany",
    "DeleteOne",
    "DeleteResult",
    "IndexModel",
    "InsertOne",
    "ReplaceOne",
    "ReturnDocument",
    "SearchIndexModel",
    "UpdateMany",
    "UpdateOne",
    "UpdateResult",
    "_scan_stub_documents",
    "_SemanticsScanMixin",
    "AsyncCollectionHelperBase",
]


def _scan_stub_documents(documents, *, skip=0, limit=None):
    async def _scan():
        selected = documents[skip:]
        if limit is not None:
            selected = selected[:limit]
        for document in selected:
            yield document

    return _scan()


class _SemanticsScanMixin:
    _stub_documents = []

    def scan_find_semantics(self, db_name, coll_name, semantics, *, context=None):
        return _scan_stub_documents(
            list(self._stub_documents),
            skip=semantics.skip,
            limit=semantics.limit,
        )


class AsyncCollectionHelperBase(unittest.TestCase):
    def setUp(self):
        self.collection = AsyncCollection(MemoryEngine(), "db", "coll")
