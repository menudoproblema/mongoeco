from __future__ import annotations

import heapq
import os
import tempfile

from contextlib import ExitStack, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.json_compat import json_dumps_compact, json_loads
from mongoeco.core.sorting import compare_documents, sort_documents
from mongoeco.core.work_control import DeadlineCheckpoint, iter_with_deadline


if TYPE_CHECKING:
    from collections.abc import Iterator

    from mongoeco.core.collation import CollationSpec
    from mongoeco.types import Document


BLOCKING_AGGREGATION_STAGES = frozenset(
    {
        "$sort",
        "$group",
        "$bucket",
        "$bucketAuto",
        "$facet",
        "$sortByCount",
        "$setWindowFields",
    }
)
_MAX_SORT_MERGE_FAN_IN = 32


@dataclass(frozen=True, slots=True)
class AggregationSpillPolicy:
    threshold: int
    codec: type[DocumentCodec] = DocumentCodec

    def __post_init__(self) -> None:
        if self.threshold <= 0:
            raise ValueError("aggregation spill threshold must be > 0")

    def should_spill(self, stage_operator: str, documents: list[Document]) -> bool:
        return (
            stage_operator in BLOCKING_AGGREGATION_STAGES
            and len(documents) > self.threshold
        )

    def maybe_spill(
        self,
        stage_operator: str,
        documents: list[Document],
        *,
        deadline: float | None = None,
    ) -> list[Document]:
        if not self.should_spill(stage_operator, documents):
            return documents
        return self._round_trip_via_disk(documents, deadline=deadline)

    def sort_with_spill(
        self,
        documents: list[Document],
        sort: object,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        deadline: float | None = None,
    ) -> list[Document]:
        if not self.should_spill("$sort", documents):
            return sort_documents(
                documents,
                sort,
                dialect=dialect,
                collation=collation,
                deadline=deadline,
            )
        return self._external_sort(
            documents,
            sort,
            dialect=dialect,
            collation=collation,
            deadline=deadline,
        )

    def _round_trip_via_disk(
        self,
        documents: list[Document],
        *,
        deadline: float | None = None,
    ) -> list[Document]:
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".mongoeco-aggspill",
            delete=False,
        )
        path = handle.name
        try:
            with handle:
                for document in iter_with_deadline(documents, deadline):
                    payload = json_dumps_compact(
                        self.codec.encode(document),
                        sort_keys=False,
                    )
                    handle.write(payload)
                    handle.write("\n")
            reloaded: list[Document] = []
            with open(path, encoding="utf-8") as spilled:
                for line in iter_with_deadline(spilled, deadline):
                    reloaded.append(
                        self.codec.decode(
                            json_loads(line),
                            preserve_bson_wrappers=True,
                        )
                    )
            return reloaded
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    def _external_sort(
        self,
        documents: list[Document],
        sort: object,
        *,
        dialect: MongoDialect,
        collation: CollationSpec | None,
        deadline: float | None = None,
    ) -> list[Document]:
        temporary_paths: set[str] = set()
        checkpoint = DeadlineCheckpoint(deadline)

        class _HeapItem:
            __slots__ = ("document", "index")

            def __init__(self, document: Document, index: int):
                self.document = document
                self.index = index

            def __lt__(self, other: _HeapItem) -> bool:
                if deadline is not None:
                    checkpoint()
                return (
                    compare_documents(
                        self.document,
                        other.document,
                        sort,
                        dialect=dialect,
                        collation=collation,
                    )
                    < 0
                )

        def merged_documents(paths: list[str]) -> Iterator[Document]:
            with ExitStack() as stack:
                streams = [
                    stack.enter_context(Path(path).open(encoding="utf-8"))
                    for path in iter_with_deadline(paths, deadline)
                ]
                heap: list[_HeapItem] = []
                for index, stream in enumerate(streams):
                    checkpoint()
                    line = stream.readline()
                    if not line:
                        continue
                    document = self.codec.decode(
                        json_loads(line), preserve_bson_wrappers=True
                    )
                    heapq.heappush(heap, _HeapItem(document, index))
                while heap:
                    checkpoint()
                    item = heapq.heappop(heap)
                    yield item.document
                    line = streams[item.index].readline()
                    if line:
                        document = self.codec.decode(
                            json_loads(line), preserve_bson_wrappers=True
                        )
                        heapq.heappush(heap, _HeapItem(document, item.index))

        def write_run(documents_to_write: Iterator[Document]) -> str:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".mongoeco-aggsort",
                delete=False,
            ) as handle:
                path = handle.name
                temporary_paths.add(path)
                for document in iter_with_deadline(documents_to_write, deadline):
                    payload = json_dumps_compact(
                        self.codec.encode(document),
                        sort_keys=False,
                    )
                    handle.write(payload)
                    handle.write("\n")
            return path

        try:
            active_paths: list[str] = []
            for start in range(0, len(documents), self.threshold):
                checkpoint()
                chunk_documents = documents[start : start + self.threshold]
                chunk = (
                    sort_documents(
                        chunk_documents,
                        sort,
                        dialect=dialect,
                        collation=collation,
                        deadline=deadline,
                    )
                    if deadline is not None
                    else sort_documents(
                        chunk_documents,
                        sort,
                        dialect=dialect,
                        collation=collation,
                    )
                )
                active_paths.append(write_run(iter(chunk)))

            while len(active_paths) > _MAX_SORT_MERGE_FAN_IN:
                next_paths: list[str] = []
                for start in range(0, len(active_paths), _MAX_SORT_MERGE_FAN_IN):
                    checkpoint()
                    source_paths = active_paths[start : start + _MAX_SORT_MERGE_FAN_IN]
                    next_paths.append(write_run(merged_documents(source_paths)))
                    for path in source_paths:
                        Path(path).unlink()
                        temporary_paths.remove(path)
                active_paths = next_paths

            return list(merged_documents(active_paths))
        finally:
            for path in temporary_paths:
                with suppress(FileNotFoundError):
                    Path(path).unlink()
