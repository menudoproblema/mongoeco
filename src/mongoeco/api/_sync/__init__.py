from importlib import import_module

__all__ = [
    "MongoClient",
    "Database",
    "Collection",
    "Cursor",
    "AggregationCursor",
    "IndexCursor",
    "ListingCursor",
    "SearchIndexCursor",
    "RawBatchCursor",
]

_EXPORT_MODULES = {
    "MongoClient": "mongoeco.api._sync.client",
    "Database": "mongoeco.api._sync.client",
    "Collection": "mongoeco.api._sync.collection",
    "Cursor": "mongoeco.api._sync.cursor",
    "AggregationCursor": "mongoeco.api._sync.aggregation_cursor",
    "IndexCursor": "mongoeco.api._sync.index_cursor",
    "ListingCursor": "mongoeco.api._sync.listing_cursor",
    "SearchIndexCursor": "mongoeco.api._sync.search_index_cursor",
    "RawBatchCursor": "mongoeco.api._sync.raw_batch_cursor",
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
