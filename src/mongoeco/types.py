import binascii
import datetime
import decimal
import os
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, NotRequired, Self, TypedDict


class UndefinedType:
    """Representa el tipo BSON `undefined` legado de MongoDB."""

    __slots__ = ()

    def __repr__(self) -> str:
        return 'UNDEFINED'

    def __hash__(self) -> int:
        return hash(UndefinedType)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, UndefinedType)


UNDEFINED = UndefinedType()


class ObjectId:
    """
    Implementacion nativa minima de ObjectId (BSON).
    12 bytes: 4 de timestamp + 5 aleatorios + 3 de contador.
    """

    _inc_lock = threading.Lock()
    _counter = int.from_bytes(os.urandom(3), "big")
    _random = os.urandom(5)

    __slots__ = ("_oid",)

    def __init__(self, oid: str | bytes | Self | None = None):
        if oid is None:
            self._oid = self._generate()
        elif isinstance(oid, ObjectId):
            self._oid = oid._oid
        elif isinstance(oid, bytes):
            if len(oid) != 12:
                raise ValueError(f"bytes de ObjectId deben ser de 12, no {len(oid)}")
            self._oid = oid
        elif isinstance(oid, str):
            if len(oid) != 24:
                raise ValueError(f"string de ObjectId debe ser de 24 hex, no {len(oid)}")
            try:
                self._oid = binascii.unhexlify(oid)
            except binascii.Error as exc:
                raise ValueError(f"'{oid}' no es un hexadecimal valido") from exc
        else:
            raise TypeError(f"ID invalido tipo {type(oid)}: {oid}")

    @classmethod
    def _generate(cls) -> bytes:
        timestamp = int(time.time())
        with cls._inc_lock:
            cls._counter = (cls._counter + 1) & 0xFFFFFF
            counter = cls._counter

        return (
            timestamp.to_bytes(4, "big")
            + cls._random
            + counter.to_bytes(3, "big")
        )

    @classmethod
    def is_valid(cls, oid: Any) -> bool:
        if isinstance(oid, ObjectId):
            return True
        if isinstance(oid, bytes):
            return len(oid) == 12
        if isinstance(oid, str) and len(oid) == 24:
            return all(ch in "0123456789abcdefABCDEF" for ch in oid)
        return False

    @property
    def binary(self) -> bytes:
        return self._oid

    @property
    def generation_time(self) -> int:
        return int.from_bytes(self._oid[:4], "big")

    def __str__(self) -> str:
        return binascii.hexlify(self._oid).decode("ascii")

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __hash__(self) -> int:
        return hash(self._oid)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ObjectId):
            return False
        return self._oid == other._oid

    def __lt__(self, other: Self) -> bool:
        if not isinstance(other, ObjectId):
            return NotImplemented
        return self._oid < other._oid


class Binary(bytes):
    """Representa BinData BSON con subtipo explícito."""

    def __new__(cls, data: bytes | bytearray | memoryview, subtype: int = 0) -> Self:
        instance = super().__new__(cls, bytes(data))
        if not 0 <= subtype <= 255:
            raise ValueError("binary subtype must be between 0 and 255")
        instance.subtype = subtype
        return instance

    def __repr__(self) -> str:
        return f"Binary({bytes(self)!r}, subtype={self.subtype})"


@dataclass(frozen=True, slots=True)
class Regex:
    pattern: str
    flags: str = ""

    def __post_init__(self) -> None:
        invalid = sorted(set(self.flags) - {"i", "m", "s", "x"})
        if invalid:
            joined = "".join(invalid)
            raise ValueError(f"unsupported regex flags: {joined}")

    def compile(self) -> re.Pattern[str]:
        compiled_flags = 0
        if "i" in self.flags:
            compiled_flags |= re.IGNORECASE
        if "m" in self.flags:
            compiled_flags |= re.MULTILINE
        if "s" in self.flags:
            compiled_flags |= re.DOTALL
        if "x" in self.flags:
            compiled_flags |= re.VERBOSE
        return re.compile(self.pattern, compiled_flags)


@dataclass(frozen=True, slots=True)
class Timestamp:
    time: int
    inc: int

    def __post_init__(self) -> None:
        if self.time < 0 or self.inc < 0:
            raise ValueError("timestamp components must be non-negative")

    def as_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.time, tz=datetime.UTC).replace(tzinfo=None)


@dataclass(frozen=True, slots=True)
class Decimal128:
    value: decimal.Decimal

    def __init__(self, value: decimal.Decimal | int | float | str) -> None:
        object.__setattr__(self, "value", decimal.Decimal(str(value)) if not isinstance(value, decimal.Decimal) else value)

    def to_decimal(self) -> decimal.Decimal:
        return self.value

    def __str__(self) -> str:
        return format(self.value, "f")

    def __repr__(self) -> str:
        return f"Decimal128('{self.value}')"


class SON(OrderedDict[str, Any]):
    """Ordered mapping compatible con bson.son.SON."""


@dataclass(frozen=True, slots=True)
class DBRef:
    collection: str
    id: Any
    database: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def as_document(self) -> SON:
        document: SON = SON([("$ref", self.collection), ("$id", self.id)])
        if self.database is not None:
            document["$db"] = self.database
        for key, value in self.extras.items():
            document[key] = value
        return document


type Document = dict[str, Any]
type Filter = dict[str, Any]
type Update = dict[str, Any]
type ArrayFilters = list[Filter]
type Projection = dict[str, Any]
type SortDirection = Literal[1, -1]
type SortSpec = list[tuple[str, SortDirection]]
type CollationDocument = dict[str, object]
type IndexKeySpec = SortSpec
type IndexDocument = dict[str, object]
type IndexInformationEntry = dict[str, object]
type IndexInformation = dict[str, IndexInformationEntry]
type SearchIndexDocument = dict[str, object]
type CollectionOptionsDocument = dict[str, object]
type DocumentScalarId = ObjectId | str | bytes | int | float | bool | None | UndefinedType
type DocumentId = DocumentScalarId | list[DocumentId] | dict[str, DocumentId]


class CollectionInfoDocument(TypedDict):
    readOnly: bool


class CollectionListingDocument(TypedDict):
    name: str
    type: str
    options: CollectionOptionsDocument
    info: CollectionInfoDocument


class DatabaseListingDocument(TypedDict):
    name: str
    sizeOnDisk: int
    empty: bool


class ProfilingCommandDocument(TypedDict):
    was: int
    slowms: int
    sampleRate: float
    ok: float


class ProfileEntryDocument(TypedDict, total=False):
    _id: int
    op: str
    ns: str
    command: dict[str, object]
    millis: float
    micros: int
    ts: str
    engine: str
    executionLineage: list["ExecutionLineageStepDocument"]
    fallbackReason: str
    ok: float
    errmsg: str


class ChangeNamespaceDocument(TypedDict):
    db: str
    coll: str


class ResumeTokenDocument(TypedDict):
    _data: str


class ChangeEventDocument(TypedDict, total=False):
    _id: ResumeTokenDocument
    operationType: str
    ns: ChangeNamespaceDocument
    documentKey: Document
    fullDocument: Document
    clusterTime: int
    updateDescription: dict[str, object]


@dataclass(frozen=True, slots=True)
class CollectionListingSnapshot:
    name: str
    type: str = "collection"
    options: CollectionOptionsDocument = field(default_factory=dict)
    read_only: bool = False

    def to_document(self) -> CollectionListingDocument:
        return {
            "name": self.name,
            "type": self.type,
            "options": self.options,
            "info": {"readOnly": self.read_only},
        }


@dataclass(frozen=True, slots=True)
class DatabaseListingSnapshot:
    name: str
    size_on_disk: int
    empty: bool

    def to_document(self) -> DatabaseListingDocument:
        return {
            "name": self.name,
            "sizeOnDisk": self.size_on_disk,
            "empty": self.empty,
        }


@dataclass(frozen=True, slots=True)
class ProfilingSettingsSnapshot:
    level: int = 0
    slow_ms: int = 100


@dataclass(frozen=True, slots=True)
class ProfilingCommandResult:
    previous_level: int
    slow_ms: int

    def to_document(self) -> ProfilingCommandDocument:
        return {
            "was": self.previous_level,
            "slowms": self.slow_ms,
            "sampleRate": 1.0,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class ProfileEntrySnapshot:
    profile_id: int
    op: str
    namespace: str
    command: dict[str, object]
    millis: float
    micros: int
    ts: str
    engine: str
    execution_lineage: tuple["ExecutionLineageStep", ...] = ()
    fallback_reason: str | None = None
    ok: float = 1.0
    errmsg: str | None = None

    def to_document(self) -> ProfileEntryDocument:
        document: ProfileEntryDocument = {
            "_id": self.profile_id,
            "op": self.op,
            "ns": self.namespace,
            "command": deepcopy(self.command),
            "millis": self.millis,
            "micros": self.micros,
            "ts": self.ts,
            "engine": self.engine,
            "ok": self.ok,
        }
        if self.execution_lineage:
            document["executionLineage"] = [step.to_document() for step in self.execution_lineage]
        if self.fallback_reason is not None:
            document["fallbackReason"] = self.fallback_reason
        if self.errmsg is not None:
            document["errmsg"] = self.errmsg
        return document


@dataclass(frozen=True, slots=True)
class ChangeEventSnapshot:
    token: int
    operation_type: str
    db_name: str
    coll_name: str
    document_key: Document
    full_document: Document | None = None
    update_description: dict[str, object] | None = None

    def to_document(self) -> ChangeEventDocument:
        document: ChangeEventDocument = {
            "_id": {"_data": str(self.token)},
            "operationType": self.operation_type,
            "ns": {"db": self.db_name, "coll": self.coll_name},
            "documentKey": deepcopy(self.document_key),
            "clusterTime": self.token,
        }
        if self.full_document is not None:
            document["fullDocument"] = deepcopy(self.full_document)
        if self.update_description is not None:
            document["updateDescription"] = deepcopy(self.update_description)
        return document


class CollectionStatsDocument(TypedDict):
    ns: str
    count: int
    size: int
    avgObjSize: float
    storageSize: int
    nindexes: int
    totalIndexSize: int
    ok: float


class DatabaseStatsDocument(TypedDict):
    db: str
    collections: int
    objects: int
    avgObjSize: float
    dataSize: int
    storageSize: int
    indexes: int
    indexSize: int
    ok: float


class CollectionValidationDocument(TypedDict):
    ns: str
    valid: bool
    nrecords: int
    nIndexes: int
    keysPerIndex: dict[str, int]
    warnings: list[object]
    ok: float


@dataclass(frozen=True, slots=True)
class EngineIndexRecord:
    name: str
    fields: list[str]
    key: IndexKeySpec
    unique: bool
    physical_name: str | None = None
    sparse: bool = False
    partial_filter_expression: Filter | None = None
    multikey: bool = False
    multikey_physical_name: str | None = None

    def __getitem__(self, key: str) -> object:
        return getattr(self, key)

    def get(self, key: str, default: object | None = None) -> object | None:
        return getattr(self, key, default)

    def to_definition(self) -> "IndexDefinition":
        return IndexDefinition(
            deepcopy(self.key),
            name=self.name,
            unique=self.unique,
            sparse=self.sparse,
            partial_filter_expression=deepcopy(self.partial_filter_expression),
        )


@dataclass(frozen=True, slots=True)
class CommandCursorResult:
    namespace: str
    first_batch: list[object]
    cursor_id: int = 0

    def to_document(self) -> dict[str, object]:
        return {
            "cursor": {
                "id": self.cursor_id,
                "ns": self.namespace,
                "firstBatch": self.first_batch,
            },
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class ListDatabasesCommandResult:
    databases: list[dict[str, object]]
    total_size: int

    def to_document(self) -> dict[str, object]:
        return {
            "databases": self.databases,
            "totalSize": self.total_size,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class OkResult:
    def to_document(self) -> dict[str, object]:
        return {"ok": 1.0}


@dataclass(frozen=True, slots=True)
class NamespaceOkResult:
    namespace: str

    def to_document(self) -> dict[str, object]:
        return {
            "ns": self.namespace,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class CountCommandResult:
    count: int

    def to_document(self) -> dict[str, object]:
        return {"n": self.count, "ok": 1.0}


@dataclass(frozen=True, slots=True)
class DistinctCommandResult:
    values: list[object]

    def to_document(self) -> dict[str, object]:
        return {"values": self.values, "ok": 1.0}


@dataclass(frozen=True, slots=True)
class WriteErrorEntry:
    index: int
    errmsg: str
    code: int | None = None
    operation: str | None = None

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "index": self.index,
            "errmsg": self.errmsg,
        }
        if self.code is not None:
            document["code"] = self.code
        if self.operation is not None:
            document["op"] = self.operation
        return document


@dataclass(frozen=True, slots=True)
class UpsertedWriteEntry:
    index: int
    document_id: object

    def to_document(self) -> dict[str, object]:
        return {
            "index": self.index,
            "_id": self.document_id,
        }


@dataclass(frozen=True, slots=True)
class BulkWriteErrorDetails:
    write_errors: list[WriteErrorEntry]
    inserted_count: int = 0
    matched_count: int = 0
    modified_count: int = 0
    removed_count: int = 0
    upserted: list[UpsertedWriteEntry] = field(default_factory=list)

    def to_document(self) -> dict[str, object]:
        return {
            "writeErrors": [entry.to_document() for entry in self.write_errors],
            "nInserted": self.inserted_count,
            "nMatched": self.matched_count,
            "nModified": self.modified_count,
            "nRemoved": self.removed_count,
            "nUpserted": len(self.upserted),
            "upserted": [entry.to_document() for entry in self.upserted],
        }


@dataclass(frozen=True, slots=True)
class WriteCommandResult:
    count: int
    modified_count: int | None = None
    upserted: list[UpsertedWriteEntry] | None = None

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {"n": self.count, "ok": 1.0}
        if self.modified_count is not None:
            document["nModified"] = self.modified_count
        if self.upserted:
            document["upserted"] = [entry.to_document() for entry in self.upserted]
        return document


@dataclass(frozen=True, slots=True)
class CollectionValidationSnapshot:
    namespace: str
    record_count: int
    index_count: int
    keys_per_index: dict[str, int]
    valid: bool = True
    warnings: list[object] = field(default_factory=list)

    def to_document(self) -> CollectionValidationDocument:
        return {
            "ns": self.namespace,
            "valid": self.valid,
            "nrecords": self.record_count,
            "nIndexes": self.index_count,
            "keysPerIndex": self.keys_per_index,
            "warnings": list(self.warnings),
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class FindAndModifyLastErrorObject:
    count: int
    updated_existing: bool | None = None
    upserted_id: object | None = None

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {"n": self.count}
        if self.updated_existing is not None:
            document["updatedExisting"] = self.updated_existing
        if self.upserted_id is not None:
            document["upserted"] = self.upserted_id
        return document


@dataclass(frozen=True, slots=True)
class FindAndModifyCommandResult:
    last_error_object: FindAndModifyLastErrorObject
    value: object

    def to_document(self) -> dict[str, object]:
        return {
            "lastErrorObject": self.last_error_object.to_document(),
            "value": self.value,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class CreateIndexesCommandResult:
    num_indexes_before: int
    num_indexes_after: int
    created_collection_automatically: bool
    note: str | None = None

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "numIndexesBefore": self.num_indexes_before,
            "numIndexesAfter": self.num_indexes_after,
            "createdCollectionAutomatically": self.created_collection_automatically,
            "ok": 1.0,
        }
        if self.note is not None:
            document["note"] = self.note
        return document


@dataclass(frozen=True, slots=True)
class DropIndexesCommandResult:
    previous_index_count: int
    message: str | None = None

    def to_document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "nIndexesWas": self.previous_index_count,
            "ok": 1.0,
        }
        if self.message is not None:
            document["msg"] = self.message
        return document


@dataclass(frozen=True, slots=True)
class DropDatabaseCommandResult:
    database_name: str

    def to_document(self) -> dict[str, object]:
        return {
            "dropped": self.database_name,
            "ok": 1.0,
        }


class BuildInfoDocument(TypedDict):
    version: str
    versionArray: list[int]
    gitVersion: str
    ok: float


class HelloDocument(BuildInfoDocument, total=False):
    helloOk: bool
    isWritablePrimary: bool
    maxBsonObjectSize: int
    maxMessageSizeBytes: int
    maxWriteBatchSize: int
    localTime: object
    logicalSessionTimeoutMinutes: int
    connectionId: int
    minWireVersion: int
    maxWireVersion: int
    readOnly: bool
    ismaster: bool


class ServerStatusConnectionsDocument(TypedDict):
    current: int
    available: int
    totalCreated: int


class ServerStatusStorageEngineDocument(TypedDict):
    name: str


class ServerStatusDocument(TypedDict):
    host: str
    version: str
    process: str
    pid: int
    uptime: float
    uptimeMillis: int
    uptimeEstimate: int
    localTime: object
    connections: ServerStatusConnectionsDocument
    storageEngine: ServerStatusStorageEngineDocument
    ok: float


class HostInfoSystemDocument(TypedDict):
    hostname: str
    cpuArch: str
    numCores: int
    memSizeMB: int


class HostInfoOsDocument(TypedDict):
    type: str
    name: str
    version: str


class HostInfoExtraDocument(TypedDict):
    pythonVersion: str


class HostInfoDocument(TypedDict):
    system: HostInfoSystemDocument
    os: HostInfoOsDocument
    extra: HostInfoExtraDocument
    ok: float


class WhatsMyUriDocument(TypedDict):
    you: str
    ok: float


class CmdLineOptsParsedNetDocument(TypedDict):
    bindIp: str
    port: int


class CmdLineOptsParsedDocument(TypedDict):
    net: CmdLineOptsParsedNetDocument
    storage: dict[str, object]


class CmdLineOptsDocument(TypedDict):
    argv: list[str]
    parsed: CmdLineOptsParsedDocument
    ok: float


class CommandHelpDocument(TypedDict):
    help: str


class ListCommandsDocument(TypedDict):
    commands: dict[str, CommandHelpDocument]
    ok: float


class ConnectionStatusAuthInfoDocument(TypedDict):
    authenticatedUsers: list[object]
    authenticatedUserRoles: list[object]
    authenticatedUserPrivileges: NotRequired[list[object]]


class ConnectionStatusDocument(TypedDict):
    authInfo: ConnectionStatusAuthInfoDocument
    ok: float


class PlanningMode(Enum):
    STRICT = "strict"
    RELAXED = "relaxed"


class PlanningIssueDocument(TypedDict):
    scope: str
    message: str


@dataclass(frozen=True, slots=True)
class PlanningIssue:
    scope: str
    message: str

    def to_document(self) -> PlanningIssueDocument:
        return {
            "scope": self.scope,
            "message": self.message,
        }


class QueryPlanExplanationDocument(TypedDict, total=False):
    engine: str
    strategy: str
    plan: str
    details: object
    planning_mode: str
    planning_issues: list[PlanningIssueDocument]
    execution_lineage: list[ExecutionLineageStepDocument]
    fallback_reason: str | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: str | IndexKeySpec | None
    hinted_index: str | None
    comment: object | None
    max_time_ms: int | None
    indexes: list[IndexDocument]


class AggregateExplanationDocument(TypedDict):
    engine_plan: QueryPlanExplanationDocument
    remaining_pipeline: list[dict[str, object]]
    hint: str | IndexKeySpec | None
    comment: object | None
    max_time_ms: int | None
    batch_size: int | None
    allow_disk_use: bool | None
    let: dict[str, object] | None
    streaming_batch_execution: bool
    planning_mode: str
    planning_issues: list[PlanningIssueDocument]


class ExecutionLineageStepDocument(TypedDict):
    runtime: str
    phase: str
    detail: str | None


@dataclass(frozen=True, slots=True)
class ExecutionLineageStep:
    runtime: str
    phase: str
    detail: str | None = None

    def to_document(self) -> ExecutionLineageStepDocument:
        return {
            "runtime": self.runtime,
            "phase": self.phase,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class QueryPlanExplanation:
    engine: str
    strategy: str
    plan: str
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: str | IndexKeySpec | None
    hinted_index: str | None
    comment: object | None
    max_time_ms: int | None
    details: object | None = None
    indexes: list[IndexDocument] | None = None
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()
    execution_lineage: tuple[ExecutionLineageStep, ...] = ()
    fallback_reason: str | None = None

    def to_document(self) -> QueryPlanExplanationDocument:
        document: QueryPlanExplanationDocument = {
            "engine": self.engine,
            "strategy": self.strategy,
            "plan": self.plan,
            "sort": self.sort,
            "skip": self.skip,
            "limit": self.limit,
            "hint": self.hint,
            "hinted_index": self.hinted_index,
            "comment": self.comment,
            "max_time_ms": self.max_time_ms,
            "planning_mode": self.planning_mode.value,
        }
        if self.details is not None:
            document["details"] = self.details
        if self.indexes is not None:
            document["indexes"] = self.indexes
        if self.planning_issues:
            document["planning_issues"] = [issue.to_document() for issue in self.planning_issues]
        if self.execution_lineage:
            document["execution_lineage"] = [step.to_document() for step in self.execution_lineage]
        if self.fallback_reason is not None:
            document["fallback_reason"] = self.fallback_reason
        return document


@dataclass(frozen=True, slots=True)
class AggregateExplanation:
    engine_plan: QueryPlanExplanation | QueryPlanExplanationDocument
    remaining_pipeline: list[dict[str, object]]
    hint: str | IndexKeySpec | None
    comment: object | None
    max_time_ms: int | None
    batch_size: int | None
    allow_disk_use: bool | None
    let: dict[str, object] | None
    streaming_batch_execution: bool
    planning_mode: PlanningMode = PlanningMode.STRICT
    planning_issues: tuple[PlanningIssue, ...] = ()

    def to_document(self) -> AggregateExplanationDocument:
        engine_plan = self.engine_plan
        if isinstance(engine_plan, QueryPlanExplanation):
            serialized_engine_plan = engine_plan.to_document()
        else:
            serialized_engine_plan = engine_plan
        return {
            "engine_plan": serialized_engine_plan,
            "remaining_pipeline": self.remaining_pipeline,
            "hint": self.hint,
            "comment": self.comment,
            "max_time_ms": self.max_time_ms,
            "batch_size": self.batch_size,
            "allow_disk_use": self.allow_disk_use,
            "let": self.let,
            "streaming_batch_execution": self.streaming_batch_execution,
            "planning_mode": self.planning_mode.value,
            "planning_issues": [issue.to_document() for issue in self.planning_issues],
        }


@dataclass(frozen=True, slots=True)
class CollectionStatsSnapshot:
    namespace: str
    count: int
    data_size: int
    index_count: int
    scale: int = 1

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("namespace must be a non-empty string")
        for field_name in ("count", "data_size", "index_count"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.scale <= 0:
            raise ValueError("scale must be > 0")

    def to_document(self) -> CollectionStatsDocument:
        scaled_size = self.data_size // self.scale
        avg_size = 0.0
        if self.count:
            avg_size = (self.data_size / self.count) / self.scale
        return {
            "ns": self.namespace,
            "count": self.count,
            "size": scaled_size,
            "avgObjSize": avg_size,
            "storageSize": scaled_size,
            "nindexes": self.index_count,
            "totalIndexSize": 0,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class DatabaseStatsSnapshot:
    db_name: str
    collection_count: int
    object_count: int
    data_size: int
    index_count: int
    scale: int = 1

    def __post_init__(self) -> None:
        if not self.db_name:
            raise ValueError("db_name must be a non-empty string")
        for field_name in ("collection_count", "object_count", "data_size", "index_count"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.scale <= 0:
            raise ValueError("scale must be > 0")

    def to_document(self) -> DatabaseStatsDocument:
        scaled_size = self.data_size // self.scale
        avg_size = 0.0
        if self.object_count:
            avg_size = (self.data_size / self.object_count) / self.scale
        return {
            "db": self.db_name,
            "collections": self.collection_count,
            "objects": self.object_count,
            "avgObjSize": avg_size,
            "dataSize": scaled_size,
            "storageSize": scaled_size,
            "indexes": self.index_count,
            "indexSize": 0,
            "ok": 1.0,
        }


class ReturnDocument(Enum):
    BEFORE = 'before'
    AFTER = 'after'


def _require_non_bool_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an int")
    return value


@dataclass(frozen=True, slots=True)
class WriteConcern:
    w: int | str | None = None
    j: bool | None = None
    wtimeout: int | None = None

    def __init__(
        self,
        w: int | str | None = None,
        *,
        j: bool | None = None,
        wtimeout: int | None = None,
    ):
        if w is not None:
            if isinstance(w, bool):
                raise TypeError("w must not be a bool")
            if isinstance(w, int):
                if w < 0:
                    raise ValueError("w must be >= 0")
            elif isinstance(w, str):
                if not w:
                    raise ValueError("w must be a non-empty string")
            else:
                raise TypeError("w must be an int, string, or None")
        if j is not None and not isinstance(j, bool):
            raise TypeError("j must be a bool or None")
        if wtimeout is not None:
            wtimeout = _require_non_bool_int(wtimeout, "wtimeout")
            if wtimeout < 0:
                raise ValueError("wtimeout must be >= 0")
        object.__setattr__(self, "w", w)
        object.__setattr__(self, "j", j)
        object.__setattr__(self, "wtimeout", wtimeout)

    @property
    def document(self) -> dict[str, object]:
        document: dict[str, object] = {}
        if self.w is not None:
            document["w"] = self.w
        if self.j is not None:
            document["j"] = self.j
        if self.wtimeout is not None:
            document["wtimeout"] = self.wtimeout
        return document


@dataclass(frozen=True, slots=True)
class ReadConcern:
    level: str | None = None

    def __init__(self, level: str | None = None):
        if level is not None and (not isinstance(level, str) or not level):
            raise ValueError("level must be a non-empty string or None")
        object.__setattr__(self, "level", level)

    @property
    def document(self) -> dict[str, object]:
        if self.level is None:
            return {}
        return {"level": self.level}


class ReadPreferenceMode(Enum):
    PRIMARY = "primary"
    PRIMARY_PREFERRED = "primaryPreferred"
    SECONDARY = "secondary"
    SECONDARY_PREFERRED = "secondaryPreferred"
    NEAREST = "nearest"


@dataclass(frozen=True, slots=True)
class ReadPreference:
    mode: ReadPreferenceMode = ReadPreferenceMode.PRIMARY
    tag_sets: tuple[dict[str, str], ...] | None = None
    max_staleness_seconds: int | None = None

    def __init__(
        self,
        mode: ReadPreferenceMode | str = ReadPreferenceMode.PRIMARY,
        *,
        tag_sets: Sequence[dict[str, str]] | None = None,
        max_staleness_seconds: int | None = None,
    ):
        if isinstance(mode, str):
            try:
                mode = ReadPreferenceMode(mode)
            except ValueError as exc:
                raise ValueError(f"unsupported read preference mode: {mode}") from exc
        elif not isinstance(mode, ReadPreferenceMode):
            raise TypeError("mode must be a ReadPreferenceMode or string")

        normalized_tag_sets: tuple[dict[str, str], ...] | None = None
        if tag_sets is not None:
            if isinstance(tag_sets, (str, bytes, bytearray)):
                raise TypeError("tag_sets must be a sequence of dictionaries")
            normalized_items: list[dict[str, str]] = []
            for tag_set in tag_sets:
                if not isinstance(tag_set, dict):
                    raise TypeError("tag_sets must contain only dictionaries")
                normalized_tag_set: dict[str, str] = {}
                for key, value in tag_set.items():
                    if not isinstance(key, str) or not key:
                        raise TypeError("tag set keys must be non-empty strings")
                    if not isinstance(value, str):
                        raise TypeError("tag set values must be strings")
                    normalized_tag_set[key] = value
                normalized_items.append(normalized_tag_set)
            normalized_tag_sets = tuple(normalized_items)

        if max_staleness_seconds is not None:
            max_staleness_seconds = _require_non_bool_int(
                max_staleness_seconds,
                "max_staleness_seconds",
            )
            if max_staleness_seconds <= 0:
                raise ValueError("max_staleness_seconds must be > 0")

        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "tag_sets", normalized_tag_sets)
        object.__setattr__(self, "max_staleness_seconds", max_staleness_seconds)

    @property
    def name(self) -> str:
        return self.mode.value

    @property
    def document(self) -> dict[str, object]:
        document: dict[str, object] = {"mode": self.mode.value}
        if self.tag_sets is not None:
            document["tag_sets"] = [dict(tag_set) for tag_set in self.tag_sets]
        if self.max_staleness_seconds is not None:
            document["maxStalenessSeconds"] = self.max_staleness_seconds
        return document


@dataclass(frozen=True, slots=True)
class CodecOptions:
    document_class: type[dict] = dict
    tz_aware: bool = False

    def __init__(
        self,
        document_class: type[dict] = dict,
        *,
        tz_aware: bool = False,
    ):
        if not isinstance(document_class, type):
            raise TypeError("document_class must be a type")
        if not issubclass(document_class, dict):
            raise TypeError("document_class must be a dict subclass")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be a bool")
        object.__setattr__(self, "document_class", document_class)
        object.__setattr__(self, "tz_aware", tz_aware)


@dataclass(frozen=True, slots=True)
class TransactionOptions:
    read_concern: ReadConcern = ReadConcern()
    write_concern: WriteConcern = WriteConcern()
    read_preference: ReadPreference = ReadPreference()
    max_commit_time_ms: int | None = None

    def __init__(
        self,
        *,
        read_concern: ReadConcern | None = None,
        write_concern: WriteConcern | None = None,
        read_preference: ReadPreference | None = None,
        max_commit_time_ms: int | None = None,
    ):
        if read_concern is None:
            read_concern = ReadConcern()
        elif not isinstance(read_concern, ReadConcern):
            raise TypeError("read_concern must be a ReadConcern")
        if write_concern is None:
            write_concern = WriteConcern()
        elif not isinstance(write_concern, WriteConcern):
            raise TypeError("write_concern must be a WriteConcern")
        if read_preference is None:
            read_preference = ReadPreference()
        elif not isinstance(read_preference, ReadPreference):
            raise TypeError("read_preference must be a ReadPreference")
        if max_commit_time_ms is not None:
            max_commit_time_ms = _require_non_bool_int(
                max_commit_time_ms,
                "max_commit_time_ms",
            )
            if max_commit_time_ms <= 0:
                raise ValueError("max_commit_time_ms must be > 0")
        object.__setattr__(self, "read_concern", read_concern)
        object.__setattr__(self, "write_concern", write_concern)
        object.__setattr__(self, "read_preference", read_preference)
        object.__setattr__(self, "max_commit_time_ms", max_commit_time_ms)


def normalize_write_concern(value: WriteConcern | None) -> WriteConcern:
    if value is None:
        return WriteConcern()
    if not isinstance(value, WriteConcern):
        raise TypeError("write_concern must be a WriteConcern")
    return value


def normalize_read_concern(value: ReadConcern | None) -> ReadConcern:
    if value is None:
        return ReadConcern()
    if not isinstance(value, ReadConcern):
        raise TypeError("read_concern must be a ReadConcern")
    return value


def normalize_read_preference(value: ReadPreference | None) -> ReadPreference:
    if value is None:
        return ReadPreference()
    if not isinstance(value, ReadPreference):
        raise TypeError("read_preference must be a ReadPreference")
    return value


def normalize_codec_options(value: CodecOptions | None) -> CodecOptions:
    if value is None:
        return CodecOptions()
    if not isinstance(value, CodecOptions):
        raise TypeError("codec_options must be a CodecOptions")
    return value


def normalize_transaction_options(value: TransactionOptions | None) -> TransactionOptions:
    if value is None:
        return TransactionOptions()
    if not isinstance(value, TransactionOptions):
        raise TypeError("transaction_options must be a TransactionOptions")
    return value


def normalize_index_keys(keys: object) -> IndexKeySpec:
    if isinstance(keys, str):
        if not keys:
            raise ValueError("index field names must be non-empty strings")
        return [(keys, 1)]

    if isinstance(keys, dict):
        if not keys:
            raise ValueError("keys must not be empty")
        normalized: IndexKeySpec = []
        for field, direction in keys.items():
            if not isinstance(field, str) or not field:
                raise TypeError("index field names must be non-empty strings")
            if direction not in (1, -1) or isinstance(direction, bool):
                raise ValueError("index directions must be 1 or -1")
            normalized.append((field, direction))
        return normalized

    if not isinstance(keys, Sequence) or isinstance(keys, (bytes, bytearray, dict)):
        raise TypeError("keys must be a string or a sequence of strings or (field, direction) tuples")

    normalized_items = list(keys)
    if not normalized_items:
        raise ValueError("keys must not be empty")

    normalized: IndexKeySpec = []
    for item in normalized_items:
        if isinstance(item, str):
            if not item:
                raise ValueError("index field names must be non-empty strings")
            normalized.append((item, 1))
            continue
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes, bytearray, dict))
            or len(item) != 2
        ):
            raise TypeError("keys must be a list of strings or (field, direction) tuples")
        field, direction = item
        if not isinstance(field, str) or not field:
            raise TypeError("index field names must be non-empty strings")
        if direction not in (1, -1) or isinstance(direction, bool):
            raise ValueError("index directions must be 1 or -1")
        normalized.append((field, direction))
    return normalized


def default_index_name(keys: IndexKeySpec) -> str:
    return "_".join(f"{field}_{direction}" for field, direction in keys)


def index_fields(keys: IndexKeySpec) -> list[str]:
    return [field for field, _direction in keys]


def index_key_document(keys: IndexKeySpec) -> dict[str, SortDirection]:
    return {field: direction for field, direction in keys}


def default_id_index_information() -> IndexInformation:
    return default_id_index_definition().to_information_entry_map()


def default_id_index_document() -> IndexDocument:
    return default_id_index_definition().to_list_document()


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    keys: IndexKeySpec
    name: str
    unique: bool = False
    sparse: bool = False
    partial_filter_expression: Filter | None = None

    def __init__(
        self,
        keys: object,
        *,
        name: str,
        unique: bool = False,
        sparse: bool = False,
        partial_filter_expression: Filter | None = None,
    ):
        normalized = normalize_index_keys(keys)
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))

    @property
    def fields(self) -> list[str]:
        return index_fields(self.keys)

    def to_list_document(self) -> IndexDocument:
        document: IndexDocument = {
            "name": self.name,
            "key": index_key_document(self.keys),
            "fields": self.fields,
            "unique": self.unique,
        }
        if self.sparse:
            document["sparse"] = True
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        return document

    def to_model_document(self) -> IndexDocument:
        document: IndexDocument = {
            "name": self.name,
            "key": index_key_document(self.keys),
        }
        if self.unique:
            document["unique"] = True
        if self.sparse:
            document["sparse"] = True
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        return document

    def to_information_entry(self) -> IndexInformationEntry:
        entry: IndexInformationEntry = {"key": list(self.keys)}
        if self.unique:
            entry["unique"] = True
        if self.sparse:
            entry["sparse"] = True
        if self.partial_filter_expression is not None:
            entry["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        return entry

    def to_information_entry_map(self) -> IndexInformation:
        return {self.name: self.to_information_entry()}


def default_id_index_definition() -> IndexDefinition:
    return IndexDefinition([("_id", 1)], name="_id_", unique=True)


@dataclass(frozen=True, slots=True)
class IndexModel:
    keys: IndexKeySpec
    name: str | None = None
    unique: bool = False
    sparse: bool = False
    partial_filter_expression: Filter | None = None

    def __init__(self, keys: object, **kwargs: Any):
        normalized = normalize_index_keys(keys)
        name = kwargs.pop("name", None)
        unique = kwargs.pop("unique", False)
        sparse = kwargs.pop("sparse", False)
        partial_filter_expression = kwargs.pop("partialFilterExpression", None)
        if partial_filter_expression is None:
            partial_filter_expression = kwargs.pop("partial_filter_expression", None)
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported IndexModel options: {unsupported}")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))

    @property
    def resolved_name(self) -> str:
        return self.name or default_index_name(self.keys)

    @property
    def definition(self) -> IndexDefinition:
        return IndexDefinition(
            self.keys,
            name=self.resolved_name,
            unique=self.unique,
            sparse=self.sparse,
            partial_filter_expression=self.partial_filter_expression,
        )

    @property
    def document(self) -> IndexDocument:
        return self.definition.to_model_document()


@dataclass(frozen=True, slots=True)
class SearchIndexDefinition:
    definition: Document
    name: str
    index_type: str = "search"

    def __init__(
        self,
        definition: Document,
        *,
        name: str,
        index_type: str = "search",
    ):
        if not isinstance(definition, dict):
            raise TypeError("definition must be a dict")
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(index_type, str) or not index_type:
            raise ValueError("index_type must be a non-empty string")
        object.__setattr__(self, "definition", deepcopy(definition))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "index_type", index_type)

    def to_document(self) -> SearchIndexDocument:
        return {
            "name": self.name,
            "type": self.index_type,
            "definition": deepcopy(self.definition),
            "latestDefinition": deepcopy(self.definition),
            "queryable": True,
            "status": "READY",
        }


@dataclass(frozen=True, slots=True)
class SearchIndexModel:
    definition: Document
    name: str | None = None
    index_type: str = "search"

    def __init__(self, definition: Document, **kwargs: Any):
        if not isinstance(definition, dict):
            raise TypeError("definition must be a dict")
        name = kwargs.pop("name", None)
        index_type = kwargs.pop("type", kwargs.pop("index_type", "search"))
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(index_type, str) or not index_type:
            raise ValueError("index_type must be a non-empty string")
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported SearchIndexModel options: {unsupported}")
        object.__setattr__(self, "definition", deepcopy(definition))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "index_type", index_type)

    @property
    def resolved_name(self) -> str:
        return self.name or "default"

    @property
    def definition_snapshot(self) -> SearchIndexDefinition:
        return SearchIndexDefinition(
            self.definition,
            name=self.resolved_name,
            index_type=self.index_type,
        )


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
