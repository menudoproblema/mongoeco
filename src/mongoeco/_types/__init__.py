from importlib import import_module

_BSON_EXPORTS = (
    "Binary",
    "DBRef",
    "Decimal128",
    "OBJECT_ID_TYPES",
    "ObjectId",
    "ObjectIdLike",
    "Regex",
    "SON",
    "Timestamp",
    "UNDEFINED",
    "UndefinedType",
    "is_object_id_like",
    "normalize_object_id",
)

_CONCERN_EXPORTS = (
    "CodecOptions",
    "ReadConcern",
    "ReadPreference",
    "ReadPreferenceMode",
    "TransactionOptions",
    "UuidRepresentation",
    "WriteConcern",
    "normalize_codec_options",
    "normalize_read_concern",
    "normalize_read_preference",
    "normalize_transaction_options",
    "normalize_write_concern",
)

_DOCUMENT_EXPORTS = (
    "AggregateExplanation",
    "AggregateExplanationDocument",
    "BuildInfoDocument",
    "BulkWriteErrorDetails",
    "ChangeEventDocument",
    "ChangeEventSnapshot",
    "ChangeNamespaceDocument",
    "CollectionInfoDocument",
    "CollectionListingDocument",
    "CollectionListingSnapshot",
    "CollectionStatsDocument",
    "CollectionStatsSnapshot",
    "CollectionValidationDocument",
    "CollectionValidationSnapshot",
    "CmdLineOptsDocument",
    "CmdLineOptsParsedDocument",
    "CmdLineOptsParsedNetDocument",
    "CommandCursorResult",
    "CommandHelpDocument",
    "ConnectionStatusAuthInfoDocument",
    "ConnectionStatusDocument",
    "CountCommandResult",
    "CreateIndexesCommandResult",
    "DatabaseHashCommandResult",
    "DatabaseHashDocument",
    "DatabaseListingDocument",
    "DatabaseListingSnapshot",
    "DatabaseStatsDocument",
    "DatabaseStatsSnapshot",
    "DistinctCommandResult",
    "DropDatabaseCommandResult",
    "DropIndexesCommandResult",
    "EngineIndexRecord",
    "ExecutionLineageStep",
    "ExecutionLineageStepDocument",
    "FindAndModifyCommandResult",
    "FindAndModifyLastErrorObject",
    "HelloDocument",
    "HostInfoDocument",
    "HostInfoExtraDocument",
    "HostInfoOsDocument",
    "HostInfoSystemDocument",
    "ListCommandsDocument",
    "ListDatabasesCommandResult",
    "NamespaceOkResult",
    "OkResult",
    "PhysicalPlanStep",
    "PhysicalPlanStepDocument",
    "PlanningIssue",
    "PlanningIssueDocument",
    "PlanningMode",
    "ProfileEntryDocument",
    "ProfileEntrySnapshot",
    "ProfilingCommandDocument",
    "ProfilingCommandResult",
    "ProfilingSettingsSnapshot",
    "QueryPlanExplanation",
    "QueryPlanExplanationDocument",
    "ResumeTokenDocument",
    "ReturnDocument",
    "ServerStatusAssertsDocument",
    "ServerStatusConnectionsDocument",
    "ServerStatusDocument",
    "ServerStatusMongoEcoDocument",
    "ServerStatusOpcountersDocument",
    "ServerStatusProfileDocument",
    "ServerStatusStorageEngineDocument",
    "UpsertedWriteEntry",
    "WhatsMyUriDocument",
    "WriteCommandResult",
    "WriteErrorEntry",
    "encode_change_stream_token",
)

_INDEX_EXPORTS = (
    "IndexDefinition",
    "IndexModel",
    "SearchIndexDefinition",
    "SearchIndexModel",
    "default_id_index_definition",
    "default_id_index_document",
    "default_id_index_information",
    "default_index_name",
    "index_fields",
    "index_key_document",
    "is_ordered_index_direction",
    "is_ordered_index_spec",
    "is_special_index_direction",
    "normalize_index_direction",
    "normalize_index_keys",
    "special_index_directions",
)

_WRITE_EXPORTS = (
    "BulkWriteResult",
    "DeleteMany",
    "DeleteOne",
    "DeleteResult",
    "InsertManyResult",
    "InsertOne",
    "InsertOneResult",
    "ReplaceOne",
    "UpdateMany",
    "UpdateOne",
    "UpdateResult",
    "WriteModel",
)

__all__ = [
    *_BSON_EXPORTS,
    *_CONCERN_EXPORTS,
    *_DOCUMENT_EXPORTS,
    *_INDEX_EXPORTS,
    *_WRITE_EXPORTS,
]

_EXPORT_MODULES = {
    **dict.fromkeys(_BSON_EXPORTS, "mongoeco._types.bson"),
    **dict.fromkeys(_CONCERN_EXPORTS, "mongoeco._types.concerns"),
    **dict.fromkeys(_DOCUMENT_EXPORTS, "mongoeco._types.documents"),
    **dict.fromkeys(_INDEX_EXPORTS, "mongoeco._types.indexes"),
    **dict.fromkeys(_WRITE_EXPORTS, "mongoeco._types.writes"),
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
