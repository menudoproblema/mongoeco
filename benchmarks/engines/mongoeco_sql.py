import tempfile
import os
from typing import Any

from benchmarks.engines.base import BenchmarkEngine
from mongoeco import MongoClient


class MongoecoSQLEngine(BenchmarkEngine):
    def __init__(self, spill_threshold: int = 10000):
        self.client = None
        self.db_fd = None
        self.db_path = None
        self.spill_threshold = spill_threshold

    def setup(self) -> None:
        from mongoeco.engines.sqlite import SQLiteEngine
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(self.db_fd)

        engine = SQLiteEngine(path=self.db_path)
        self.client = MongoClient(engine=engine)

    def teardown(self) -> None:
        if self.client:
            self.client.close()
            self.client = None
        if self.db_path and os.path.exists(self.db_path):
            os.remove(self.db_path)

    def drop_collection(self, db_name: str, coll_name: str) -> None:
        self.client[db_name].drop_collection(coll_name)

    def insert_many(self, db_name: str, coll_name: str, documents: list[dict[str, Any]]) -> None:
        if not documents:
            return
        self.client[db_name][coll_name].insert_many(documents)

    def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: list[tuple[str, int]],
    ) -> None:
        self.client[db_name][coll_name].create_index(keys)

    def find(self, db_name: str, coll_name: str, filter_spec: dict[str, Any], sort: list[tuple[str, int]] | None = None, limit: int = 0) -> list[dict[str, Any]]:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        if limit:
            cursor.limit(limit)
        return list(cursor)

    def find_first(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        return cursor.first()

    def explain_find(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> dict[str, Any]:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        if limit:
            cursor.limit(limit)
        return cursor.explain()

    def aggregate(self, db_name: str, coll_name: str, pipeline: list[dict[str, Any]], allow_disk_use: bool = False) -> list[dict[str, Any]]:
        cursor = self.client[db_name][coll_name].aggregate(pipeline, allow_disk_use=allow_disk_use)
        return list(cursor)

    def aggregate_first(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].aggregate(pipeline, allow_disk_use=allow_disk_use)
        return cursor.first()

    def explain_aggregate(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any]:
        cursor = self.client[db_name][coll_name].aggregate(pipeline, allow_disk_use=allow_disk_use)
        return cursor.explain()

    @property
    def label(self) -> str:
        return "mongoeco-sqlite"
