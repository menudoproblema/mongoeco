from __future__ import annotations

from types import MappingProxyType

from mongoeco.compat._catalog_models import OperationOptionSupport, OptionSupportStatus

_EFFECTIVE = OptionSupportStatus.EFFECTIVE

OPERATION_OPTION_SUPPORT_CATALOG = MappingProxyType(
    {
        "find": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Validated against existing indexes and applied to read planning/explain where engines can honor it."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata and surfaced by explain()."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline during read execution and explain()."),
                "batch_size": OperationOptionSupport(_EFFECTIVE, "Async and sync find cursors now fetch local batches before yielding results, even though engines remain in-process."),
            }
        ),
        "count_documents": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying find() path used to count matching documents."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying find() path used to count documents."),
            }
        ),
        "distinct": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying find() path used to enumerate distinct values."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying find() path used to enumerate distinct values."),
            }
        ),
        "estimated_document_count": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying full-collection read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying full-collection read path."),
            }
        ),
        "aggregate": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the pushdown find() path used by aggregate() and surfaced in explain()."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata and propagated through aggregate explain/materialization."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Applied to referenced collection loads, pushdown reads and final pipeline materialization."),
                "batch_size": OperationOptionSupport(_EFFECTIVE, "Positive batch sizes trigger chunked execution for streamable aggregate pipelines; global stages still fall back to full materialization."),
                "allow_disk_use": OperationOptionSupport(_EFFECTIVE, "Controls whether the aggregation cursor may use the configured spill-to-disk policy for blocking stages."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into aggregate expression evaluation and subpipelines."),
            }
        ),
        "update_one": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Applied during update execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before single-document update execution."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented with profile-aware validation since PyMongo 4.11."),
            }
        ),
        "update_many": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Applied during per-document update execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted _id preselection before per-document updates."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "replace_one": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before replacement."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented with profile-aware validation since PyMongo 4.11."),
            }
        ),
        "delete_one": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before delete."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "delete_many": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted _id preselection before per-document deletes."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "find_one_and_update": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Propagated to the underlying update_one() execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection and post-update fetch."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through update_one()/find semantics with profile-aware validation."),
            }
        ),
        "find_one_and_replace": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection and post-replacement fetch."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through replace_one()/find semantics with profile-aware validation."),
            }
        ),
        "find_one_and_delete": MappingProxyType(
            {
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through find() selection semantics before delete."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before delete."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "bulk_write": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the batch write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables flow into per-operation write filters through $expr when requests do not override them."),
            }
        ),
        "list_indexes": MappingProxyType({"comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration.")}),
        "create_index": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline during index build and multikey backfill."),
            }
        ),
        "create_indexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline across the whole index batch."),
            }
        ),
        "drop_index": MappingProxyType({"comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration.")}),
        "drop_indexes": MappingProxyType({"comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration.")}),
    }
)

DATABASE_COMMAND_OPTION_SUPPORT_CATALOG = MappingProxyType(
    {
        "aggregate": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Propagated through command routing into aggregate pushdown/explain."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata and surfaced through explain/materialization."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Enforced against local aggregate execution and explain paths."),
                "allowDiskUse": OperationOptionSupport(_EFFECTIVE, "Applied to blocking aggregate stages through the same local spill policy as the public API."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into aggregate expression evaluation and subpipelines."),
                "batchSize": OperationOptionSupport(_EFFECTIVE, "Materialized into the command cursor surface for streamable pipelines."),
            }
        ),
        "collStats": MappingProxyType(
            {
                "scale": OperationOptionSupport(_EFFECTIVE, "Applied to size-oriented metrics in the local collection stats snapshot."),
            }
        ),
        "connectionStatus": MappingProxyType(
            {
                "showPrivileges": OperationOptionSupport(_EFFECTIVE, "Controls whether the local auth status document includes the privileges array."),
            }
        ),
        "count": MappingProxyType(
            {
                "query": OperationOptionSupport(_EFFECTIVE, "Compiled into the same local find-selection path used to execute the count command."),
                "skip": OperationOptionSupport(_EFFECTIVE, "Applied before materializing the local count result."),
                "limit": OperationOptionSupport(_EFFECTIVE, "Applied before materializing the local count result."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the compiled selection path used by the count command."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the command execution."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Enforced through the compiled selection path used by the count command."),
            }
        ),
        "createIndexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline across the command index batch."),
            }
        ),
        "currentOp": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Accepted for command parity and recorded in local admin profiling metadata when supplied."),
            }
        ),
        "dbHash": MappingProxyType(
            {
                "collections": OperationOptionSupport(_EFFECTIVE, "Limits hashing to the selected local collections while preserving a stable collection-order contract."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Accepted for command parity and recorded in command profiling metadata."),
            }
        ),
        "dbStats": MappingProxyType(
            {
                "scale": OperationOptionSupport(_EFFECTIVE, "Applied to size-oriented metrics in the local database stats snapshot."),
            }
        ),
        "delete": MappingProxyType(
            {
                "ordered": OperationOptionSupport(_EFFECTIVE, "Controls short-circuiting when one delete specification fails."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write command."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into per-delete filters through $expr when supplied on individual specs."),
            }
        ),
        "distinct": MappingProxyType(
            {
                "query": OperationOptionSupport(_EFFECTIVE, "Compiled into the same local selection path used to enumerate distinct values."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying read selection path used by the command."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the command execution."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying distinct selection path."),
            }
        ),
        "dropIndexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
            }
        ),
        "explain": MappingProxyType(
            {
                "verbosity": OperationOptionSupport(_EFFECTIVE, "Accepted and surfaced in explain responses for supported routed commands, including find/aggregate/update/delete/count/distinct/findAndModify."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated into the explained command where that command supports comment."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Propagated into the explained command where that command supports maxTimeMS."),
            }
        ),
        "find": MappingProxyType(
            {
                "filter": OperationOptionSupport(_EFFECTIVE, "Compiled into the same local find operation shape as the collection API."),
                "projection": OperationOptionSupport(_EFFECTIVE, "Applied to command cursor materialization using the same projection semantics as the collection API."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Applied to command read planning and result materialization."),
                "skip": OperationOptionSupport(_EFFECTIVE, "Applied before command cursor materialization."),
                "limit": OperationOptionSupport(_EFFECTIVE, "Applied before command cursor materialization."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied to command read planning and surfaced in explain."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the command execution."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Enforced during command read execution and explain."),
                "batchSize": OperationOptionSupport(_EFFECTIVE, "Materialized into the command cursor surface."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into command-level $expr evaluation."),
            }
        ),
        "findAndModify": MappingProxyType(
            {
                "arrayFilters": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying write path for supported filtered positional updates."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted selection and post-write fetch."),
                "maxTimeMS": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into command-level $expr evaluation for the write filter."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the command execution."),
                "bypassDocumentValidation": OperationOptionSupport(_EFFECTIVE, "Validated and propagated through replacement/update command routing."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying find-and-modify selection path."),
            }
        ),
        "insert": MappingProxyType(
            {
                "ordered": OperationOptionSupport(_EFFECTIVE, "Controls short-circuiting when one insert document fails."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write command."),
                "bypassDocumentValidation": OperationOptionSupport(_EFFECTIVE, "Validated and propagated through command routing."),
            }
        ),
        "listCollections": MappingProxyType(
            {
                "filter": OperationOptionSupport(_EFFECTIVE, "Applied to the local namespace snapshot before cursor materialization."),
                "nameOnly": OperationOptionSupport(_EFFECTIVE, "Controls the fields exposed by the listCollections cursor."),
                "authorizedCollections": OperationOptionSupport(_EFFECTIVE, "Accepted for wire/API parity and preserved in the normalized command options."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the admin read command."),
            }
        ),
        "listDatabases": MappingProxyType(
            {
                "filter": OperationOptionSupport(_EFFECTIVE, "Applied to the local database snapshot before materialization."),
                "nameOnly": OperationOptionSupport(_EFFECTIVE, "Controls the fields exposed by the listDatabases response."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the admin read command."),
            }
        ),
        "listIndexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
            }
        ),
        "killOp": MappingProxyType(
            {
                "op": OperationOptionSupport(_EFFECTIVE, "Identifies the locally registered operation to cancel on a best-effort basis."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Accepted for command parity and recorded in local admin profiling metadata when supplied."),
            }
        ),
        "profile": MappingProxyType(
            {
                "slowms": OperationOptionSupport(_EFFECTIVE, "Propagated through the local profiling control path when a profiling level update is requested; status queries also surface current level and entry counts."),
            }
        ),
        "update": MappingProxyType(
            {
                "ordered": OperationOptionSupport(_EFFECTIVE, "Controls short-circuiting when one update specification fails."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write command."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into per-update filters through $expr when supplied on individual specs."),
                "bypassDocumentValidation": OperationOptionSupport(_EFFECTIVE, "Validated and propagated through replacement/update command routing."),
                "arrayFilters": OperationOptionSupport(_EFFECTIVE, "Applied through per-update write execution for supported filtered positional paths."),
            }
        ),
        "validate": MappingProxyType(
            {
                "scandata": OperationOptionSupport(_EFFECTIVE, "Controls whether storage-engine level scan metadata is requested in the validation snapshot."),
                "full": OperationOptionSupport(_EFFECTIVE, "Controls whether the validation snapshot requests the expanded pass."),
                "background": OperationOptionSupport(_EFFECTIVE, "Validated and surfaced in the validation snapshot contract."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the validation command."),
            }
        ),
    }
)
