from collections.abc import AsyncIterator

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.aggregation import (
    Pipeline,
    _CURRENT_COLLECTION_RESOLVER_KEY,
    apply_pipeline,
    split_pushdown_pipeline,
)
from mongoeco.session import ClientSession
from mongoeco.types import Document


class AsyncAggregationCursor:
    """Cursor async mínimo para resultados de aggregate()."""

    def __init__(self, collection, pipeline: Pipeline, *, session: ClientSession | None = None):
        self._collection = collection
        self._pipeline = pipeline
        self._session = session

    def _collect_collection_names(self, pipeline: Pipeline) -> set[str]:
        names: set[str] = set()
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                continue
            if "$facet" in stage:
                spec = stage["$facet"]
                if isinstance(spec, dict):
                    for subpipeline in spec.values():
                        if isinstance(subpipeline, list):
                            names.update(self._collect_collection_names(subpipeline))
                continue
            if "$lookup" in stage:
                spec = stage["$lookup"]
                if not isinstance(spec, dict):
                    continue
                from_collection = spec.get("from")
                if isinstance(from_collection, str):
                    names.add(from_collection)
                pipeline_spec = spec.get("pipeline")
                if isinstance(pipeline_spec, list):
                    names.update(self._collect_collection_names(pipeline_spec))
                continue
            if "$unionWith" in stage:
                spec = stage["$unionWith"]
                if isinstance(spec, str):
                    if spec:
                        names.add(spec)
                    continue
                if not isinstance(spec, dict):
                    continue
                coll = spec.get("coll")
                if isinstance(coll, str):
                    names.add(coll)
                elif "pipeline" in spec:
                    names.add(_CURRENT_COLLECTION_RESOLVER_KEY)
                pipeline_spec = spec.get("pipeline")
                if isinstance(pipeline_spec, list):
                    names.update(self._collect_collection_names(pipeline_spec))
        return names

    def _collect_lookup_names(self, pipeline: Pipeline) -> set[str]:
        # Compatibilidad interna con tests y callers existentes; ahora recoge
        # también colecciones referenciadas por $unionWith.
        return {
            name
            for name in self._collect_collection_names(pipeline)
            if name != _CURRENT_COLLECTION_RESOLVER_KEY
        }

    async def _load_referenced_collections(self) -> dict[str, list[Document]]:
        names = self._collect_collection_names(self._pipeline)
        loaded: dict[str, list[Document]] = {}
        if _CURRENT_COLLECTION_RESOLVER_KEY in names:
            engine = getattr(self._collection, "_engine", None)
            db_name = getattr(self._collection, "_db_name", None)
            collection_name = getattr(self._collection, "_collection_name", None)
            if engine is not None and db_name is not None and collection_name is not None:
                current_collection = engine.scan_collection(
                    db_name,
                    collection_name,
                    {},
                    dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                    context=self._session,
                )
                loaded[_CURRENT_COLLECTION_RESOLVER_KEY] = [
                    document async for document in current_collection
                ]
        for name in names:
            if name == _CURRENT_COLLECTION_RESOLVER_KEY:
                continue
            collection = self._collection._engine.scan_collection(
                self._collection._db_name,
                name,
                {},
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
                context=self._session,
            )
            loaded[name] = [document async for document in collection]
        return loaded

    async def _materialize(self) -> list[Document]:
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        pushdown = split_pushdown_pipeline(
            self._pipeline,
            dialect=dialect,
        )
        referenced_collections = await self._load_referenced_collections()
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
            collection_resolver=referenced_collections.get,
            dialect=dialect,
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
