from __future__ import annotations

import base64
import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from typing import Any, NotRequired, TypedDict

type Document = dict[str, Any]
type Filter = dict[str, Any]
type CollectionOptionsDocument = dict[str, object]
type SortSpec = list[tuple[str, int]]
type IndexDirection = int | str
type IndexKeySpec = list[tuple[str, IndexDirection]]
type IndexDocument = dict[str, object]
type IndexInformationEntry = dict[str, object]
type IndexInformation = dict[str, IndexInformationEntry]


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


class ProfilingCommandDocument(TypedDict, total=False):
    was: int
    slowms: int
    sampleRate: float
    level: int
    entryCount: int
    namespaceVisible: bool
    trackedDatabases: int
    visibleNamespaces: int
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
    current_level: int | None = None
    entry_count: int | None = None
    namespace_visible: bool | None = None
    tracked_databases: int | None = None
    visible_namespaces: int | None = None

    def to_document(self) -> ProfilingCommandDocument:
        document: ProfilingCommandDocument = {
            "was": self.previous_level,
            "slowms": self.slow_ms,
            "sampleRate": 1.0,
            "ok": 1.0,
        }
        if self.current_level is not None:
            document["level"] = self.current_level
        if self.entry_count is not None:
            document["entryCount"] = self.entry_count
        if self.namespace_visible is not None:
            document["namespaceVisible"] = self.namespace_visible
        if self.tracked_databases is not None:
            document["trackedDatabases"] = self.tracked_databases
        if self.visible_namespaces is not None:
            document["visibleNamespaces"] = self.visible_namespaces
        return document


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
            "_id": {"_data": encode_change_stream_token(self.token)},
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


def encode_change_stream_token(token: int) -> str:
    payload = json.dumps({"v": 1, "t": token}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


class CollectionStatsDocument(TypedDict):
    ns: str
    count: int
    size: int
    avgObjSize: float
    storageSize: int
    nindexes: int
    totalIndexSize: int
    scaleFactor: int
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
    scaleFactor: int
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
    hidden: bool = False
    collation: dict[str, object] | None = None
    partial_filter_expression: Filter | None = None
    expire_after_seconds: int | None = None
    weights: dict[str, int] | None = None
    default_language: str | None = None
    language_override: str | None = None
    multikey: bool = False
    multikey_physical_name: str | None = None
    scalar_physical_name: str | None = None

    def __getitem__(self, key: str) -> object:
        return getattr(self, key)

    def get(self, key: str, default: object | None = None) -> object | None:
        return getattr(self, key, default)

    def to_definition(self):
        from mongoeco._types.indexes import IndexDefinition

        return IndexDefinition(
            deepcopy(self.key),
            name=self.name,
            unique=self.unique,
            sparse=self.sparse,
            hidden=self.hidden,
            collation=deepcopy(self.collation),
            partial_filter_expression=deepcopy(self.partial_filter_expression),
            expire_after_seconds=self.expire_after_seconds,
            weights=deepcopy(self.weights),
            default_language=self.default_language,
            language_override=self.language_override,
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


class DatabaseHashDocument(TypedDict):
    host: str
    collections: dict[str, str]
    md5: str
    timeMillis: int
    ok: float


@dataclass(frozen=True, slots=True)
class DatabaseHashCommandResult:
    host: str
    collections: dict[str, str]
    md5: str
    time_millis: int

    def to_document(self) -> DatabaseHashDocument:
        return {
            "host": self.host,
            "collections": dict(self.collections),
            "md5": self.md5,
            "timeMillis": self.time_millis,
            "ok": 1.0,
        }


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


class ServerStatusAssertsDocument(TypedDict):
    regular: int
    warning: int
    msg: int
    user: int
    rollovers: int


class ServerStatusOpcountersDocument(TypedDict):
    insert: int
    query: int
    update: int
    delete: int
    getmore: int
    command: int


class ServerStatusProfileDocument(TypedDict):
    trackedDatabases: int
    visibleNamespaces: int
    entryCount: int


class ServerStatusChangeStreamsDocument(TypedDict, total=False):
    implementation: str
    persistent: bool
    boundedHistory: bool
    maxRetainedEvents: int | None
    journalEnabled: bool
    journalFsync: bool
    journalMaxLogBytes: int | None
    retainedEvents: int
    currentOffset: int
    nextToken: int


class ServerStatusCollationDocument(TypedDict, total=False):
    selectedBackend: str
    availableBackends: list[str]
    unicodeAvailable: bool
    advancedOptionsAvailable: bool


class ServerStatusSdamDocument(TypedDict):
    fullSdam: bool
    topologyVersionAware: bool
    helloMemberDiscovery: bool
    serverHealthTracking: bool
    electionMetadataAware: bool
    longPollingHello: bool
    distributedMonitoring: bool


class ServerStatusMongoEcoDocument(TypedDict, total=False):
    embedded: bool
    dialectVersion: str
    adminCommandSurfaceCount: int
    wireCommandSurfaceCount: int
    adminFamilies: dict[str, int]
    explainableCommandCount: int
    jsonBackend: str
    profile: ServerStatusProfileDocument
    collation: ServerStatusCollationDocument
    sdam: ServerStatusSdamDocument
    changeStreams: ServerStatusChangeStreamsDocument


class ServerStatusDocument(TypedDict, total=False):
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
    asserts: ServerStatusAssertsDocument
    opcounters: ServerStatusOpcountersDocument
    mongoeco: ServerStatusMongoEcoDocument
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


class CommandHelpDocument(TypedDict, total=False):
    help: str
    adminFamily: str
    supportsWire: bool
    supportsExplain: bool
    supportsComment: bool
    supportedOptions: list[str]
    note: str


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


class ExecutionLineageStepDocument(TypedDict):
    runtime: str
    phase: str
    detail: str | None


class PhysicalPlanStepDocument(TypedDict):
    runtime: str
    operation: str
    detail: str | None


class QueryPlanExplanationDocument(TypedDict, total=False):
    engine: str
    strategy: str
    plan: str
    details: object
    planning_mode: str
    planning_issues: list[PlanningIssueDocument]
    execution_lineage: list[ExecutionLineageStepDocument]
    physical_plan: list[PhysicalPlanStepDocument]
    fallback_reason: str | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: str | IndexKeySpec | None
    hinted_index: str | None
    comment: object | None
    max_time_ms: int | None
    indexes: list[IndexDocument]
    collection: str
    namespace: str


class AggregateExplanationDocument(TypedDict):
    engine_plan: QueryPlanExplanationDocument
    remaining_pipeline: list[dict[str, object]]
    pushdown: "AggregatePushdownSummaryDocument"
    hint: str | IndexKeySpec | None
    comment: object | None
    max_time_ms: int | None
    batch_size: int | None
    allow_disk_use: bool | None
    let: dict[str, object] | None
    streaming_batch_execution: bool
    planning_mode: str
    planning_issues: list[PlanningIssueDocument]
    collection: str
    namespace: str


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
class PhysicalPlanStep:
    runtime: str
    operation: str
    detail: str | None = None

    def to_document(self) -> PhysicalPlanStepDocument:
        return {
            "runtime": self.runtime,
            "operation": self.operation,
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
    physical_plan: tuple[PhysicalPlanStep, ...] = ()
    fallback_reason: str | None = None

    def to_document(self) -> QueryPlanExplanationDocument:
        document: QueryPlanExplanationDocument = {
            "engine": self.engine,
            "strategy": self.strategy,
            "plan": self.plan,
            "sort": self.sort,
            "skip": self.skip,
            "limit": self.limit,
            "hint": _serialize_document_value(self.hint),
            "hinted_index": self.hinted_index,
            "comment": _serialize_document_value(self.comment),
            "max_time_ms": self.max_time_ms,
            "planning_mode": self.planning_mode.value,
        }
        if self.details is not None:
            document["details"] = _serialize_document_value(self.details)
        if self.indexes is not None:
            document["indexes"] = _serialize_document_value(self.indexes)
        if self.planning_issues:
            document["planning_issues"] = [issue.to_document() for issue in self.planning_issues]
        if self.execution_lineage:
            document["execution_lineage"] = [step.to_document() for step in self.execution_lineage]
        if self.physical_plan:
            document["physical_plan"] = [step.to_document() for step in self.physical_plan]
        if self.fallback_reason is not None:
            document["fallback_reason"] = self.fallback_reason
        return document


@dataclass(frozen=True, slots=True)
class AggregateExplanation:
    engine_plan: QueryPlanExplanation | QueryPlanExplanationDocument
    remaining_pipeline: list[dict[str, object]]
    pushdown: AggregatePushdownSummaryDocument
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
            serialized_engine_plan = _serialize_document_value(engine_plan)
        return {
            "engine_plan": serialized_engine_plan,
            "remaining_pipeline": _serialize_document_value(self.remaining_pipeline),
            "pushdown": _serialize_document_value(self.pushdown),
            "hint": _serialize_document_value(self.hint),
            "comment": _serialize_document_value(self.comment),
            "max_time_ms": self.max_time_ms,
            "batch_size": self.batch_size,
            "allow_disk_use": self.allow_disk_use,
            "let": _serialize_document_value(self.let),
            "streaming_batch_execution": self.streaming_batch_execution,
            "planning_mode": self.planning_mode.value,
            "planning_issues": [issue.to_document() for issue in self.planning_issues],
        }


def _serialize_document_value(value: object) -> object:
    to_document = getattr(value, "to_document", None)
    if callable(to_document):
        return to_document()
    if is_dataclass(value):
        return _serialize_document_value(asdict(value))
    if isinstance(value, dict):
        return {key: _serialize_document_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_document_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_document_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class CollectionStatsSnapshot:
    namespace: str
    count: int
    data_size: int
    index_count: int
    total_index_size: int = 0
    scale: int = 1

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("namespace must be a non-empty string")
        for field_name in ("count", "data_size", "index_count", "total_index_size"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.scale <= 0:
            raise ValueError("scale must be > 0")

    def to_document(self) -> CollectionStatsDocument:
        scaled_size = self.data_size // self.scale
        scaled_index_size = self.total_index_size // self.scale
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
            "totalIndexSize": scaled_index_size,
            "scaleFactor": self.scale,
            "ok": 1.0,
        }


@dataclass(frozen=True, slots=True)
class DatabaseStatsSnapshot:
    db_name: str
    collection_count: int
    object_count: int
    data_size: int
    index_count: int
    index_size: int = 0
    scale: int = 1

    def __post_init__(self) -> None:
        if not self.db_name:
            raise ValueError("db_name must be a non-empty string")
        for field_name in ("collection_count", "object_count", "data_size", "index_count", "index_size"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.scale <= 0:
            raise ValueError("scale must be > 0")

    def to_document(self) -> DatabaseStatsDocument:
        scaled_size = self.data_size // self.scale
        scaled_index_size = self.index_size // self.scale
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
            "indexSize": scaled_index_size,
            "scaleFactor": self.scale,
            "ok": 1.0,
        }


class ReturnDocument(Enum):
    BEFORE = "before"
    AFTER = "after"
class AggregatePushdownSummaryDocument(TypedDict, total=False):
    mode: str
    totalStages: int
    pushedDownStages: int
    remainingStages: int
    streamingEligible: bool
    streamableStageCount: int
    leadingSearchOperator: str
