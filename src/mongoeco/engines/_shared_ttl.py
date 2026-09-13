from __future__ import annotations

from collections.abc import Callable, Iterable
import datetime
from typing import TYPE_CHECKING, Any

from mongoeco.core.bson_scalars import normalize_utc_bson_datetime

if TYPE_CHECKING:
    from mongoeco.types import Document, EngineIndexRecord


def coerce_ttl_datetime(value: object) -> datetime.datetime | None:
    if not isinstance(value, datetime.datetime):
        return None
    return normalize_utc_bson_datetime(value).replace(tzinfo=datetime.UTC)


def ttl_expiration_datetime(
    values: Iterable[object],
    *,
    expire_after_seconds: int | None,
) -> datetime.datetime | None:
    if expire_after_seconds is None:
        return None
    ttl_candidates = [
        candidate
        for candidate in (coerce_ttl_datetime(value) for value in values)
        if candidate is not None
    ]
    if not ttl_candidates:
        return None
    try:
        return min(ttl_candidates) + datetime.timedelta(seconds=expire_after_seconds)
    except OverflowError:
        return datetime.datetime.max.replace(tzinfo=datetime.UTC)


def document_expired_by_ttl(
    document: Document,
    index: EngineIndexRecord,
    *,
    now: datetime.datetime,
    extract_values: Callable[[Document, str], list[Any]],
) -> bool:
    if index.expire_after_seconds is None or len(index.fields) != 1:
        return False
    expires_at = ttl_expiration_datetime(
        extract_values(document, index.fields[0]),
        expire_after_seconds=index.expire_after_seconds,
    )
    return expires_at is not None and expires_at <= now
