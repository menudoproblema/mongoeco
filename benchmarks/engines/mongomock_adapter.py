from typing import Any

from benchmarks.engines.base import BenchmarkEngine

try:
    import mongomock
    HAS_MONGOMOCK = True
except ImportError:
    HAS_MONGOMOCK = False


class MongomockEngine(BenchmarkEngine):
    def __init__(self):
        if not HAS_MONGOMOCK:
            raise RuntimeError("mongomock is not installed. Run `pip install mongomock`")
        self.client = None

    def setup(self) -> None:
        self.client = mongomock.MongoClient()

    def teardown(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

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

    def create_search_index(
        self,
        db_name: str,
        coll_name: str,
        definition: dict[str, Any],
        *,
        name: str = "default",
        index_type: str = "search",
    ) -> str:
        raise RuntimeError("mongomock benchmark adapter does not support search indexes")

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
    ) -> dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        return next(iter(cursor), None)

    def explain_find(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> dict[str, Any]:
        _cursor = self.client[db_name][coll_name].find(filter_spec)
        return {
            "engine": "mongomock",
            "strategy": "python",
            "plan": "opaque",
            "physical_plan": [{"runtime": "python", "operation": "opaque", "detail": "mongomock"}],
            "metadata_source": "synthetic",
        }

    def aggregate(self, db_name: str, coll_name: str, pipeline: list[dict[str, Any]], allow_disk_use: bool = False) -> list[dict[str, Any]]:
        # mongomock might ignore allow_disk_use, but we pass it if we can or just ignore it
        cursor = self.client[db_name][coll_name].aggregate(pipeline)
        return list(cursor)

    def aggregate_first(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].aggregate(pipeline)
        return next(iter(cursor), None)

    def explain_aggregate(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any]:
        return {
            "engine_plan": {
                "engine": "mongomock",
                "strategy": "python",
                "plan": "opaque",
                "physical_plan": [{"runtime": "python", "operation": "opaque", "detail": "mongomock"}],
                "metadata_source": "synthetic",
            },
            "remaining_pipeline": list(pipeline),
            "streaming_batch_execution": False,
        }

    @property
    def label(self) -> str:
        return "mongomock"
