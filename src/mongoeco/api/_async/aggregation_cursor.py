from collections.abc import AsyncIterator

from mongoeco.core.aggregation import Pipeline, apply_pipeline, split_pushdown_pipeline
from mongoeco.session import ClientSession
from mongoeco.types import Document


class AsyncAggregationCursor:
    """Cursor async mínimo para resultados de aggregate()."""

    def __init__(self, collection, pipeline: Pipeline, *, session: ClientSession | None = None):
        self._collection = collection
        self._pipeline = pipeline
        self._session = session

    def _collect_lookup_names(self, pipeline: Pipeline) -> set[str]:
        names: set[str] = set()
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                continue
            if "$facet" in stage:
                spec = stage["$facet"]
                if isinstance(spec, dict):
                    for subpipeline in spec.values():
                        if isinstance(subpipeline, list):
                            names.update(self._collect_lookup_names(subpipeline))
                continue
            if "$lookup" not in stage:
                continue
            spec = stage["$lookup"]
            if not isinstance(spec, dict):
                continue
            from_collection = spec.get("from")
            if isinstance(from_collection, str):
                names.add(from_collection)
            pipeline_spec = spec.get("pipeline")
            if isinstance(pipeline_spec, list):
                names.update(self._collect_lookup_names(pipeline_spec))
        return names

    async def _load_lookup_collections(self) -> dict[str, list[Document]]:
        names = self._collect_lookup_names(self._pipeline)
        loaded: dict[str, list[Document]] = {}
        for name in names:
            collection = self._collection._engine.scan_collection(
                self._collection._db_name,
                name,
                {},
                context=self._session,
            )
            loaded[name] = [document async for document in collection]
        return loaded

    async def _materialize(self) -> list[Document]:
        pushdown = split_pushdown_pipeline(self._pipeline)
        lookup_collections = await self._load_lookup_collections()
        documents = await self._collection.find(
            pushdown.filter_spec,
            pushdown.projection,
            sort=pushdown.sort,
            skip=pushdown.skip,
            limit=pushdown.limit,
            session=self._session,
        ).to_list()
        return apply_pipeline(
            documents,
            pushdown.remaining_pipeline,
            collection_resolver=lookup_collections.get,
        )

    async def to_list(self) -> list[Document]:
        return await self._materialize()

    async def first(self) -> Document | None:
        documents = await self._materialize()
        return documents[0] if documents else None

    def __aiter__(self) -> AsyncIterator[Document]:
        async def _iterate() -> AsyncIterator[Document]:
            for document in await self._materialize():
                yield document

        return _iterate()
