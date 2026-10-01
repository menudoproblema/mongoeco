"""Operation-local, namespace-scoped resources acquired by the async API."""

from __future__ import annotations

from dataclasses import dataclass, field, replace


_CURRENT_COLLECTION_KEY = "__mongoeco_current_collection__"
_RESOLVERS = {
    "collection_stats_resolver": "$collStats",
    "index_stats_resolver": "$indexStats",
    "current_op_resolver": "$currentOp",
    "plan_cache_stats_resolver": "$planCacheStats",
    "list_sessions_resolver": "$listSessions",
}


@dataclass(frozen=True, slots=True)
class AggregationResources:
    documents: dict
    collection: str | None
    snapshots: dict = field(default_factory=dict)

    def __call__(self, name):
        return self.documents.get(
            self.collection if name == _CURRENT_COLLECTION_KEY else name
        )

    def for_collection(self, collection):
        return replace(self, collection=collection)

    def resolver_kwargs(self):
        result = {"collection_resolver": self}
        for name, operator in _RESOLVERS.items():
            if operator == "$collStats":
                scales = {
                    scale: snapshot
                    for (collection, stage, scale), snapshot in self.snapshots.items()
                    if collection == self.collection and stage == operator
                }
                result[name] = scales.__getitem__ if scales else None
            else:
                key = (self.collection, operator, 1)
                result[name] = (
                    (lambda key=key: self.snapshots[key])
                    if key in self.snapshots
                    else None
                )
        return result


def scoped_resolver_kwargs(collection_resolver, collection, resolvers):
    """Derive a foreign frame, preserving explicit legacy resolver callbacks."""
    if isinstance(collection_resolver, AggregationResources):
        return collection_resolver.for_collection(collection).resolver_kwargs()
    return {"collection_resolver": collection_resolver, **resolvers}
