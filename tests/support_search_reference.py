"""Select the internal Search oracle before eager sync opening or async awaiting."""

from unittest.mock import patch

from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor


def aggregate_for_search_oracle(collection, pipeline, *, reference=True):
    """Preserve one mode for initial response, continuation and later explain.

    Sync aggregate opens before returning; changing the returned cursor is too
    late. Async cursors also retain the instance setting after this scope exits.
    This is test-only access to the existing oracle, not another local executor.
    """
    with patch.object(
        AsyncAggregationCursor, "_force_full_search_execution", reference
    ):
        cursor = collection.aggregate(pipeline)
        owner = getattr(cursor, "_async_aggregation_cursor", cursor)
        owner._force_full_search_execution = reference
    return cursor
