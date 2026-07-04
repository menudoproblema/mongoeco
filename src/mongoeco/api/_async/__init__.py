from importlib import import_module

__all__ = [
    "AsyncMongoClient",
    "AsyncDatabase",
    "AsyncCollection",
    "AsyncCursor",
    "AsyncAggregationCursor",
    "AsyncIndexCursor",
    "AsyncListingCursor",
    "AsyncSearchIndexCursor",
    "AsyncRawBatchCursor",
]

_EXPORT_MODULES = {
    "AsyncMongoClient": "mongoeco.api._async.client",
    "AsyncDatabase": "mongoeco.api._async.client",
    "AsyncCollection": "mongoeco.api._async.collection",
    "AsyncCursor": "mongoeco.api._async.cursor",
    "AsyncAggregationCursor": "mongoeco.api._async.aggregation_cursor",
    "AsyncIndexCursor": "mongoeco.api._async.index_cursor",
    "AsyncListingCursor": "mongoeco.api._async.listing_cursor",
    "AsyncSearchIndexCursor": "mongoeco.api._async.search_index_cursor",
    "AsyncRawBatchCursor": "mongoeco.api._async.raw_batch_cursor",
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
