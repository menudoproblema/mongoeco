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
    from collections.abc import Iterable, Iterator

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


class AggregationSortSpool:
    """Incrementally own sorted runs and a demand-driven merged output."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        threshold: int,
        codec: type[DocumentCodec],
        sort: object,
        dialect: MongoDialect,
        collation: CollationSpec | None,
        deadline: float | None,
    ) -> None:
        self._threshold = threshold
        self._codec = codec
        self._sort = sort
        self._dialect = dialect
        self._collation = collation
        self._deadline = deadline
        self._checkpoint = DeadlineCheckpoint(deadline)
        self._buffer: list[Document] = []
        self._active_paths: list[str] = []
        self._temporary_paths: set[str] = set()
        self._finished = False
        self._closed = False

    @property
    def buffered_documents(self) -> int:
        return len(self._buffer)

    @property
    def run_count(self) -> int:
        return len(self._active_paths)

    def add(self, documents: Iterable[Document]) -> None:
        if self._finished or self._closed:
            message = "aggregation sort spool no longer accepts input"
            raise RuntimeError(message)
        try:
            for document in iter_with_deadline(documents, self._deadline):
                if len(self._buffer) == self._threshold:
                    self._flush_run(self._threshold)
                self._buffer.append(document)
        except BaseException:
            self.close()
            raise

    def finish(self) -> Iterator[Document]:
        if self._finished or self._closed:
            message = "aggregation sort spool was already finished"
            raise RuntimeError(message)
        self._finished = True
        try:
            if not self._active_paths:
                documents = self._sort_documents(self._buffer)
                self._buffer = []

                def owned_memory_output() -> Iterator[Document]:
                    try:
                        yield from documents
                    finally:
                        self.close()

                return owned_memory_output()
            if self._buffer:
                self._flush_run(len(self._buffer))
            self._consolidate_runs()

            def owned_spilled_output() -> Iterator[Document]:
                try:
                    yield from self._merged_documents(self._active_paths)
                finally:
                    self.close()

            return owned_spilled_output()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer.clear()
        self._active_paths.clear()
        for path in tuple(self._temporary_paths):
            with suppress(FileNotFoundError):
                Path(path).unlink()
        self._temporary_paths.clear()

    def _sort_documents(self, documents: list[Document]) -> list[Document]:
        if self._deadline is None:
            return sort_documents(
                documents,
                self._sort,
                dialect=self._dialect,
                collation=self._collation,
            )
        return sort_documents(
            documents,
            self._sort,
            dialect=self._dialect,
            collation=self._collation,
            deadline=self._deadline,
        )

    def _flush_run(self, count: int) -> None:
        self._checkpoint()
        chunk = self._buffer[:count]
        del self._buffer[:count]
        self._active_paths.append(self._write_run(iter(self._sort_documents(chunk))))

    def _write_run(self, documents: Iterator[Document]) -> str:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".mongoeco-aggsort",
            delete=False,
        ) as handle:
            path = handle.name
            self._temporary_paths.add(path)
            for document in iter_with_deadline(documents, self._deadline):
                payload = json_dumps_compact(
                    self._codec.encode(document),
                    sort_keys=False,
                )
                handle.write(payload)
                handle.write("\n")
        return path

    def _merged_documents(self, paths: list[str]) -> Iterator[Document]:
        owner = self

        class _HeapItem:
            __slots__ = ("document", "index")

            def __init__(self, document: Document, index: int):
                self.document = document
                self.index = index

            def __lt__(self, other: _HeapItem) -> bool:
                owner._checkpoint()
                return (
                    compare_documents(
                        self.document,
                        other.document,
                        owner._sort,
                        dialect=owner._dialect,
                        collation=owner._collation,
                    )
                    < 0
                )

        with ExitStack() as stack:
            streams = [
                stack.enter_context(Path(path).open(encoding="utf-8"))
                for path in iter_with_deadline(paths, self._deadline)
            ]
            heap: list[_HeapItem] = []
            for index, stream in enumerate(streams):
                self._checkpoint()
                line = stream.readline()
                if not line:
                    continue
                document = self._codec.decode(
                    json_loads(line),
                    preserve_bson_wrappers=True,
                )
                heapq.heappush(heap, _HeapItem(document, index))
            while heap:
                self._checkpoint()
                item = heapq.heappop(heap)
                yield item.document
                line = streams[item.index].readline()
                if line:
                    document = self._codec.decode(
                        json_loads(line),
                        preserve_bson_wrappers=True,
                    )
                    heapq.heappush(heap, _HeapItem(document, item.index))

    def _consolidate_runs(self) -> None:
        while len(self._active_paths) > _MAX_SORT_MERGE_FAN_IN:
            next_paths: list[str] = []
            for start in range(0, len(self._active_paths), _MAX_SORT_MERGE_FAN_IN):
                self._checkpoint()
                source_paths = self._active_paths[
                    start : start + _MAX_SORT_MERGE_FAN_IN
                ]
                next_paths.append(self._write_run(self._merged_documents(source_paths)))
                for path in source_paths:
                    Path(path).unlink()
                    self._temporary_paths.remove(path)
            self._active_paths = next_paths


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
        return list(
            self.iter_sort_with_spill(
                documents,
                sort,
                dialect=dialect,
                collation=collation,
                deadline=deadline,
            )
        )

    def iter_sort_with_spill(
        self,
        documents: Iterable[Document],
        sort: object,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        deadline: float | None = None,
    ) -> Iterator[Document]:
        """Sort an iterable while keeping the merged output demand-driven."""
        spool = self.open_sort_spool(
            sort,
            dialect=dialect,
            collation=collation,
            deadline=deadline,
        )
        output: Iterator[Document] | None = None
        try:
            spool.add(documents)
            output = spool.finish()
            yield from output
        finally:
            close_output = getattr(output, "close", None)
            if callable(close_output):
                close_output()
            spool.close()

    def open_sort_spool(
        self,
        sort: object,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        deadline: float | None = None,
    ) -> AggregationSortSpool:
        return AggregationSortSpool(
            threshold=self.threshold,
            codec=self.codec,
            sort=sort,
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
