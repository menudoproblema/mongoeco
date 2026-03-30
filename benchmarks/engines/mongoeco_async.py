import asyncio
import os
import tempfile
from typing import Any

from benchmarks.engines.base import BenchmarkEngine
from mongoeco import AsyncMongoClient


class _MongoecoAsyncEngine(BenchmarkEngine):
    def __init__(self) -> None:
        self._runner: asyncio.Runner | None = None
        self.client = None

    @property
    def db_label(self) -> str:
        raise NotImplementedError

    def _build_engine(self):
        raise NotImplementedError

    def setup(self) -> None:
        self._runner = asyncio.Runner()
        engine = self._build_engine()
        self.client = self._runner.run(self._open_client(engine))

    async def _open_client(self, engine):
        client = AsyncMongoClient(engine=engine)
        await client._engine.connect()
        return client

    def teardown(self) -> None:
        if self.client is not None and self._runner is not None:
            self._runner.run(self.client.close())
            self.client = None
        if self._runner is not None:
            self._runner.close()
            self._runner = None

    def drop_collection(self, db_name: str, coll_name: str) -> None:
        assert self._runner is not None
        self._runner.run(self.client[db_name].drop_collection(coll_name))

    def insert_many(
        self,
        db_name: str,
        coll_name: str,
        documents: list[dict[str, Any]],
    ) -> None:
        if not documents:
            return
        assert self._runner is not None
        self._runner.run(self.client[db_name][coll_name].insert_many(documents))

    def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: list[tuple[str, int]],
    ) -> None:
        assert self._runner is not None
        self._runner.run(self.client[db_name][coll_name].create_index(keys))

    def find(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> list[dict[str, Any]]:
        assert self._runner is not None
        return self._runner.run(
            self._find_async(
                db_name=db_name,
                coll_name=coll_name,
                filter_spec=filter_spec,
                sort=sort,
                limit=limit,
            )
        )

    def find_first(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
    ) -> dict[str, Any] | None:
        assert self._runner is not None
        return self._runner.run(
            self._find_first_async(
                db_name=db_name,
                coll_name=coll_name,
                filter_spec=filter_spec,
                sort=sort,
            )
        )

    async def _find_first_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None,
    ) -> dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        return await cursor.first()

    def explain_find(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> dict[str, Any]:
        assert self._runner is not None
        return self._runner.run(
            self._explain_find_async(
                db_name=db_name,
                coll_name=coll_name,
                filter_spec=filter_spec,
                sort=sort,
                limit=limit,
            )
        )

    async def _explain_find_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None,
        limit: int,
    ) -> dict[str, Any]:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        if limit:
            cursor.limit(limit)
        return await cursor.explain()

    async def _find_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, Any],
        sort: list[tuple[str, int]] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        cursor = self.client[db_name][coll_name].find(filter_spec)
        if sort:
            cursor.sort(sort)
        if limit:
            cursor.limit(limit)
        return await cursor.to_list()

    def aggregate(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> list[dict[str, Any]]:
        assert self._runner is not None
        return self._runner.run(
            self._aggregate_async(
                db_name=db_name,
                coll_name=coll_name,
                pipeline=pipeline,
                allow_disk_use=allow_disk_use,
            )
        )

    def aggregate_first(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any] | None:
        assert self._runner is not None
        return self._runner.run(
            self._aggregate_first_async(
                db_name=db_name,
                coll_name=coll_name,
                pipeline=pipeline,
                allow_disk_use=allow_disk_use,
            )
        )

    async def _aggregate_first_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool,
    ) -> dict[str, Any] | None:
        cursor = self.client[db_name][coll_name].aggregate(
            pipeline,
            allow_disk_use=allow_disk_use,
        )
        return await cursor.first()

    def explain_aggregate(
        self,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool = False,
    ) -> dict[str, Any]:
        assert self._runner is not None
        return self._runner.run(
            self._explain_aggregate_async(
                db_name=db_name,
                coll_name=coll_name,
                pipeline=pipeline,
                allow_disk_use=allow_disk_use,
            )
        )

    async def _explain_aggregate_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool,
    ) -> dict[str, Any]:
        cursor = self.client[db_name][coll_name].aggregate(
            pipeline,
            allow_disk_use=allow_disk_use,
        )
        return await cursor.explain()

    async def _aggregate_async(
        self,
        *,
        db_name: str,
        coll_name: str,
        pipeline: list[dict[str, Any]],
        allow_disk_use: bool,
    ) -> list[dict[str, Any]]:
        cursor = self.client[db_name][coll_name].aggregate(
            pipeline,
            allow_disk_use=allow_disk_use,
        )
        return await cursor.to_list()


class MongoecoMemoryAsyncEngine(_MongoecoAsyncEngine):
    def __init__(self, spill_threshold: int = 10000) -> None:
        super().__init__()
        self.spill_threshold = spill_threshold

    def _build_engine(self):
        from mongoeco.engines.memory import MemoryEngine

        return MemoryEngine(aggregation_spill_threshold=self.spill_threshold)

    @property
    def label(self) -> str:
        return "mongoeco-memory-async"


class MongoecoSQLiteAsyncEngine(_MongoecoAsyncEngine):
    def __init__(self) -> None:
        super().__init__()
        self.db_fd: int | None = None
        self.db_path: str | None = None

    def _build_engine(self):
        from mongoeco.engines.sqlite import SQLiteEngine

        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(self.db_fd)
        return SQLiteEngine(path=self.db_path)

    def teardown(self) -> None:
        super().teardown()
        if self.db_path and os.path.exists(self.db_path):
            os.remove(self.db_path)

    @property
    def label(self) -> str:
        return "mongoeco-sqlite-async"
