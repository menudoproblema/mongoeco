from __future__ import annotations

from dataclasses import dataclass
from typing import Any

type Document = dict[str, Any]
type Filter = dict[str, Any]
type UpdateDocument = dict[str, Any]
type UpdatePipelineStage = dict[str, Any]
type UpdatePipeline = list[UpdatePipelineStage]
type Update = UpdateDocument | UpdatePipeline
type ArrayFilters = list[Filter]
type SortSpec = list[tuple[str, int]]


@dataclass(frozen=True, slots=True)
class InsertOne:
    document: Document


@dataclass(frozen=True, slots=True)
class UpdateOne:
    filter: Filter
    update: Update
    upsert: bool = False
    sort: SortSpec | None = None
    array_filters: ArrayFilters | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UpdateMany:
    filter: Filter
    update: Update
    upsert: bool = False
    array_filters: ArrayFilters | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ReplaceOne:
    filter: Filter
    replacement: Document
    upsert: bool = False
    sort: SortSpec | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DeleteOne:
    filter: Filter
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DeleteMany:
    filter: Filter
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


type WriteModel = InsertOne | UpdateOne | UpdateMany | ReplaceOne | DeleteOne | DeleteMany


@dataclass(frozen=True, slots=True)
class InsertOneResult[T]:
    inserted_id: T
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class InsertManyResult[T]:
    inserted_ids: list[T]
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class UpdateResult[T]:
    matched_count: int
    modified_count: int
    upserted_id: T | None = None
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class DeleteResult:
    deleted_count: int
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class BulkWriteResult[T]:
    inserted_count: int
    matched_count: int
    modified_count: int
    deleted_count: int
    upserted_count: int
    upserted_ids: dict[int, T]
    acknowledged: bool = True
