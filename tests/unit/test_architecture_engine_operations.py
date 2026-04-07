import asyncio
import inspect
import unittest
from unittest import mock

import mongoeco.api.operations as operations_module
from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async.database_commands import (
    AsyncDatabaseCommandService,
    BuildInfoResult,
)
from mongoeco.api._async.client import AsyncDatabase
from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.api.admin_parsing import (
    normalize_list_collections_options,
    normalize_validate_command_options,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    UpdateOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.api._sync.collection import Collection
from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.compat import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
    export_full_compat_catalog,
    get_operation_option_support,
    is_operation_option_effective,
)
from mongoeco.compat.operation_support import (
    MANAGED_OPERATION_OPTION_NAMES,
    OPERATION_OPTION_SIGNATURE_EXCLUSIONS,
)
from mongoeco.core.aggregation import (
    AGGREGATION_STAGE_HANDLERS,
    AGGREGATION_STAGE_SPECS,
    get_aggregation_stage_spec,
    is_streamable_aggregation_stage,
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.engines.base import (
    AsyncAdminEngine,
    AsyncDatabaseAdminEngine,
    AsyncCrudEngine,
    AsyncExplainEngine,
    AsyncIndexAdminEngine,
    AsyncLifecycleEngine,
    AsyncNamespaceAdminEngine,
    AsyncReadPlanningEngine,
    AsyncSessionEngine,
    AsyncStorageEngine,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.semantic_core import (
    EngineFindSemantics,
    EngineReadExecutionPlan,
    EngineUpdateSemantics,
    build_query_plan_explanation,
    compile_find_semantics,
    compile_update_semantics,
)
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.types import EngineIndexRecord, IndexDefinition, IndexInformation, default_id_index_definition
from mongoeco.types import (
    BulkWriteErrorDetails,
    CodecOptions,
    ExecutionLineageStep,
    PlanningMode,
    QueryPlanExplanation,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    TransactionOptions,
    WriteConcern,
    BuildInfoDocument,
    CollectionStatsSnapshot,
    CollectionValidationSnapshot,
    CollectionListingSnapshot,
    CollectionValidationDocument,
    CommandCursorResult,
    CountCommandResult,
    CreateIndexesCommandResult,
    DatabaseListingSnapshot,
    DatabaseStatsSnapshot,
    DistinctCommandResult,
    DropDatabaseCommandResult,
    DropIndexesCommandResult,
    FindAndModifyCommandResult,
    FindAndModifyLastErrorObject,
    ListDatabasesCommandResult,
    NamespaceOkResult,
    OkResult,
    UpsertedWriteEntry,
    WriteErrorEntry,
    WriteCommandResult,
)
from mongoeco.core.operators import CompiledUpdatePipelinePlan, UpdateEngine




class ArchitectureEngineOperationTests(unittest.TestCase):
    def test_engines_satisfy_split_storage_protocols(self):
        for engine in (MemoryEngine(), SQLiteEngine()):
            with self.subTest(engine=type(engine).__name__):
                self.assertIsInstance(engine, AsyncSessionEngine)
                self.assertIsInstance(engine, AsyncLifecycleEngine)
                self.assertIsInstance(engine, AsyncCrudEngine)
                self.assertIsInstance(engine, AsyncIndexAdminEngine)
                self.assertIsInstance(engine, AsyncReadPlanningEngine)
                self.assertIsInstance(engine, AsyncExplainEngine)
                self.assertIsInstance(engine, AsyncDatabaseAdminEngine)
                self.assertIsInstance(engine, AsyncNamespaceAdminEngine)
                self.assertIsInstance(engine, AsyncAdminEngine)
                self.assertIsInstance(engine, AsyncStorageEngine)

    def test_read_planning_engines_accept_compiled_find_semantics_directly(self):
        async def _exercise() -> None:
            operation = compile_find_operation({"kind": "view"}, sort=[("rank", 1)])
            semantics = compile_find_semantics(
                operation.filter_spec,
                plan=operation.plan,
                sort=operation.sort,
            )
            for engine in (MemoryEngine(), SQLiteEngine()):
                async with AsyncMongoClient(engine):
                    plan = await engine.plan_find_semantics("db", "users", semantics)
                    self.assertIsInstance(plan, EngineReadExecutionPlan)
                    self.assertEqual(plan.semantics.query_plan, semantics.query_plan)
                    self.assertTrue(plan.physical_plan)
        asyncio.run(_exercise())

    def test_engines_produce_typed_query_plan_explanations(self):
        async def _run() -> None:
            memory = MemoryEngine()
            sqlite = SQLiteEngine()
            await memory.connect()
            await sqlite.connect()
            try:
                semantics = compile_find_semantics({})
                memory_explain = await memory.explain_find_semantics("db", "users", semantics)
                sqlite_explain = await sqlite.explain_find_semantics("db", "users", semantics)
                self.assertIsInstance(memory_explain, QueryPlanExplanation)
                self.assertIsInstance(sqlite_explain, QueryPlanExplanation)
                self.assertEqual(memory_explain.to_document()["engine"], "memory")
                self.assertEqual(sqlite_explain.to_document()["engine"], "sqlite")
                self.assertTrue(memory_explain.physical_plan)
                self.assertTrue(sqlite_explain.physical_plan)
                self.assertIn("physical_plan", memory_explain.to_document())
                self.assertIn("physical_plan", sqlite_explain.to_document())
            finally:
                await memory.disconnect()
                await sqlite.disconnect()

        asyncio.run(_run())

    def test_compile_find_semantics_returns_typed_read_semantics(self):
        semantics = compile_find_semantics(
            {"name": "Ada"},
            sort=[("rank", 1)],
            skip=1,
            limit=2,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
        )

        self.assertIsInstance(semantics, EngineFindSemantics)
        self.assertEqual(semantics.filter_spec, {"name": "Ada"})
        self.assertEqual(semantics.sort, [("rank", 1)])
        self.assertEqual(semantics.skip, 1)
        self.assertEqual(semantics.limit, 2)
        self.assertEqual(semantics.hint, [("name", 1)])
        self.assertEqual(semantics.comment, "read")
        self.assertEqual(semantics.max_time_ms, 25)
        self.assertEqual(semantics.selector_filter, {"name": "Ada"})

    def test_compile_find_operation_extracts_classic_text_query_and_text_score_contract(self):
        operation = compile_find_operation(
            {"$text": {"$search": "Ada Lovelace"}, "kind": "person"},
            projection={"score": {"$meta": "textScore"}, "_id": 0},
            sort={"score": {"$meta": "textScore"}},
        )

        self.assertEqual(operation.filter_spec, {"$text": {"$search": "Ada Lovelace"}, "kind": "person"})
        self.assertEqual(operation.selector_filter, {"kind": "person"})
        self.assertIsNotNone(operation.text_query)
        self.assertEqual(operation.text_query.terms, ("ada", "lovelace"))
        self.assertEqual(operation.sort, [("__mongoeco_textScore__", -1)])

    def test_compile_find_operation_rejects_text_score_without_text_query(self):
        with self.assertRaisesRegex(Exception, "\\$meta textScore projection requires a \\$text query"):
            compile_find_operation(
                {"kind": "person"},
                projection={"score": {"$meta": "textScore"}},
            )
        with self.assertRaisesRegex(Exception, "\\$meta textScore sort requires a \\$text query"):
            compile_find_operation(
                {"kind": "person"},
                sort={"score": {"$meta": "textScore"}},
            )

    def test_compile_find_operation_rejects_hint_with_classic_text_query(self):
        with self.assertRaisesRegex(Exception, "classic \\$text local runtime does not support hint"):
            compile_find_operation(
                {"$text": {"$search": "Ada"}},
                hint="content_text",
            )

    def test_compile_update_semantics_returns_typed_write_semantics(self):
        operation = compile_update_operation(
            {"name": "Ada"},
            update_spec={"$set": {"rank": 1}},
            hint=[("name", 1)],
            comment="write",
        )

        semantics = compile_update_semantics(operation)

        self.assertIsInstance(semantics, EngineUpdateSemantics)
        self.assertEqual(semantics.filter_spec, {"name": "Ada"})
        self.assertEqual(semantics.selector_filter, {"name": "Ada"})
        self.assertEqual(semantics.compiled_update_plan.update_spec, {"$set": {"rank": 1}})
        self.assertEqual(semantics.compiled_upsert_plan.update_spec, {"$set": {"rank": 1}})

    def test_query_plan_explanation_builder_reuses_compiled_semantics(self):
        semantics = compile_find_semantics(
            {"name": "Ada"},
            sort=[("rank", 1)],
            skip=1,
            limit=2,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
        )

        explanation = build_query_plan_explanation(
            engine="memory",
            strategy="python",
            semantics=semantics,
            hinted_index="name_1",
        )

        self.assertEqual(explanation.engine, "memory")
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.sort, semantics.sort)
        self.assertEqual(explanation.skip, semantics.skip)
        self.assertEqual(explanation.limit, semantics.limit)
        self.assertEqual(explanation.hint, semantics.hint)
        self.assertEqual(explanation.hinted_index, "name_1")
        self.assertEqual(explanation.comment, semantics.comment)
        self.assertEqual(explanation.max_time_ms, semantics.max_time_ms)

    def test_query_plan_explanation_serializes_execution_lineage(self):
        semantics = compile_find_semantics({"name": "Ada"})
        explanation = build_query_plan_explanation(
            engine="sqlite",
            strategy="sql",
            semantics=semantics,
            execution_lineage=(
                ExecutionLineageStep(runtime="sql", phase="scan", detail="engine pushdown"),
                ExecutionLineageStep(runtime="python", phase="project", detail="semantic core projection"),
            ),
            fallback_reason=None,
        )

        self.assertEqual(
            explanation.to_document()["execution_lineage"],
            [
                {"runtime": "sql", "phase": "scan", "detail": "engine pushdown"},
                {"runtime": "python", "phase": "project", "detail": "semantic core projection"},
            ],
        )

    def test_dialect_policy_is_explicit_behavior_boundary(self):
        from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80

        self.assertTrue(MONGODB_DIALECT_70.policy.null_query_matches_undefined())
        self.assertFalse(MONGODB_DIALECT_80.policy.null_query_matches_undefined())
        self.assertEqual(MONGODB_DIALECT_70.compare_values(1, 2), MONGODB_DIALECT_70.policy.compare_values(1, 2))

    def test_relaxed_planning_mode_keeps_issues_visible_but_not_executable(self):
        async def _run() -> None:
            engine = MemoryEngine()
            await engine.connect()
            try:
                collection = AsyncCollection(
                    engine,
                    "db",
                    "users",
                    planning_mode=PlanningMode.RELAXED,
                )
                cursor = collection.find({"$futureTopLevel": 1})
                explanation = await cursor.explain()
                self.assertEqual(explanation["planning_mode"], "relaxed")
                self.assertTrue(explanation["planning_issues"])
                self.assertEqual(
                    explanation["details"]["reason"],
                    "operation has deferred planning issues (relaxed): query: Unsupported top-level query operator: $futureTopLevel",
                )
                with self.assertRaises(Exception):
                    await cursor.to_list()
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_relaxed_update_operation_collects_unsupported_update_operator_issue(self):
        operation = compile_update_operation(
            {"name": "Ada"},
            update_spec={"$future": {"value": 1}},
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertEqual(operation.planning_mode, PlanningMode.RELAXED)
        self.assertEqual(len(operation.planning_issues), 1)
        self.assertEqual(operation.planning_issues[0].scope, "update")

    def test_private_update_planning_issue_collector_reports_invalid_pipeline_shapes(self):
        malformed_stage = operations_module._collect_update_planning_issues(
            [1],
            dialect=operations_module.MONGODB_DIALECT_70,
            planning_mode=PlanningMode.RELAXED,
        )
        malformed_operator = operations_module._collect_update_planning_issues(
            [{"name": "Ada"}],
            dialect=operations_module.MONGODB_DIALECT_70,
            planning_mode=PlanningMode.RELAXED,
        )
        unsupported_stage = operations_module._collect_update_planning_issues(
            [{"$future": {}}],
            dialect=operations_module.MONGODB_DIALECT_70,
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertEqual(
            malformed_stage,
            (operations_module.PlanningIssue(scope="update", message="Each update pipeline stage must be a single-key document"),),
        )
        self.assertEqual(
            malformed_operator,
            (operations_module.PlanningIssue(scope="update", message="Update pipeline stage operator must start with '$'"),),
        )
        self.assertEqual(
            unsupported_stage,
            (operations_module.PlanningIssue(scope="update", message="Unsupported update pipeline stage: $future"),),
        )

    def test_private_update_planning_issue_collector_reports_invalid_update_type(self):
        issues = operations_module._collect_update_planning_issues(
            1,
            dialect=operations_module.MONGODB_DIALECT_70,
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertEqual(
            issues,
            (
                operations_module.PlanningIssue(
                    scope="update",
                    message="update specification must be a document or pipeline",
                ),
            ),
        )

        self.assertEqual(
            operations_module._collect_update_planning_issues(
                object(),
                dialect=operations_module.MONGODB_DIALECT_70,
                planning_mode=PlanningMode.RELAXED,
            ),
            (
                operations_module.PlanningIssue(
                    scope="update",
                    message="update specification must be a document or pipeline",
                ),
            ),
        )

        self.assertEqual(
            operations_module._collect_update_planning_issues(
                "bad",  # type: ignore[arg-type]
                dialect=operations_module.MONGODB_DIALECT_70,
                planning_mode=PlanningMode.RELAXED,
            ),
            (
                operations_module.PlanningIssue(
                    scope="update",
                    message="update specification must be a document or pipeline",
                ),
            ),
        )

    def test_private_update_planning_issue_collector_reports_validation_failures(self):
        with mock.patch.object(
            operations_module.UpdateEngine,
            "validate_update_pipeline",
            side_effect=operations_module.OperationFailure("broken pipeline"),
        ):
            issues = operations_module._collect_update_planning_issues(
                [{"$set": {"name": "Ada"}}],
                dialect=operations_module.MONGODB_DIALECT_70,
                planning_mode=PlanningMode.RELAXED,
            )

        self.assertEqual(
            issues,
            (operations_module.PlanningIssue(scope="update", message="broken pipeline"),),
        )

    def test_relaxed_update_operation_leaves_malformed_pipeline_stage_non_executable(self):
        operation = compile_update_operation(
            {"name": "Ada"},
            update_spec=[{"$set": 1}],
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertIsNone(operation.compiled_update_plan)
        self.assertIsNone(operation.compiled_upsert_plan)
        self.assertEqual(operation.planning_issues, ())

    def test_relaxed_update_operation_collects_invalid_pipeline_container_issues(self):
        empty_pipeline = compile_update_operation(
            {"name": "Ada"},
            update_spec=[],
            planning_mode=PlanningMode.RELAXED,
        )
        wrong_type = compile_update_operation(
            {"name": "Ada"},
            update_spec="bad",  # type: ignore[arg-type]
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertIsNone(empty_pipeline.compiled_update_plan)
        self.assertEqual(
            empty_pipeline.planning_issues,
            (operations_module.PlanningIssue(scope="update", message="update pipeline must be a non-empty list"),),
        )
        self.assertIsNone(wrong_type.compiled_update_plan)
        self.assertEqual(
            wrong_type.planning_issues,
            (operations_module.PlanningIssue(scope="update", message="update specification must be a document or pipeline"),),
        )

    def test_update_operation_skips_compilation_for_non_operator_documents_and_invalid_param_shapes(self):
        with self.assertRaisesRegex(operations_module.OperationFailure, "update_spec must contain only update operators"):
            compile_update_operation(
                {"name": "Ada"},
                update_spec={"name": "Ada"},
            )
        with self.assertRaisesRegex(operations_module.OperationFailure, "\\$set value must be a dict"):
            compile_update_operation(
                {"name": "Ada"},
                update_spec={"$set": 1},  # type: ignore[dict-item]
            )
        with self.assertRaisesRegex(operations_module.OperationFailure, "update specification must be a document or pipeline"):
            compile_update_operation(
                {"name": "Ada"},
                update_spec="bad",  # type: ignore[arg-type]
            )

    def test_relaxed_update_operation_reports_invalid_non_operator_documents_and_param_shapes(self):
        replacement_style = compile_update_operation(
            {"name": "Ada"},
            update_spec={"name": "Ada"},
            planning_mode=PlanningMode.RELAXED,
        )
        invalid_params = compile_update_operation(
            {"name": "Ada"},
            update_spec={"$set": 1},  # type: ignore[dict-item]
            planning_mode=PlanningMode.RELAXED,
        )
        invalid_pipeline_type = compile_update_operation(
            {"name": "Ada"},
            update_spec="bad",  # type: ignore[arg-type]
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertIsNone(replacement_style.compiled_update_plan)
        self.assertEqual(
            replacement_style.planning_issues,
            (operations_module.PlanningIssue(scope="update", message="update_spec must contain only update operators"),),
        )
        self.assertIsNone(invalid_params.compiled_update_plan)
        self.assertEqual(
            invalid_params.planning_issues,
            (operations_module.PlanningIssue(scope="update", message="$set value must be a dict"),),
        )
        self.assertIsNone(invalid_pipeline_type.compiled_update_plan)
        self.assertEqual(
            invalid_pipeline_type.planning_issues,
            (operations_module.PlanningIssue(scope="update", message="update specification must be a document or pipeline"),),
        )

    def test_private_update_compilation_and_normalizers_cover_invalid_inputs(self):
        with self.assertRaisesRegex(operations_module.OperationFailure, "update_spec must not be empty"):
            operations_module._compile_update_plans(
                {},
                dialect=operations_module.MONGODB_DIALECT_70,
                selector_filter={},
                collation=None,
                array_filters=None,
                variables=None,
                planning_mode=PlanningMode.STRICT,
            )
        self.assertEqual(operations_module._normalize_filter(None), {})
        self.assertIsNone(operations_module._normalize_collation(None))
        self.assertEqual(
            operations_module._normalize_collation({"locale": "en", "strength": 1}),
            {
                "locale": "en",
                "strength": 1,
                "caseLevel": False,
                "numericOrdering": False,
            },
        )
        self.assertIsNone(operations_module._normalize_projection(None))
        self.assertEqual(operations_module._normalize_hint("name_1"), "name_1")
        self.assertIsNone(operations_module._normalize_array_filters(None))
        self.assertEqual(operations_module._normalize_batch_size(0), 0)
        self.assertEqual(operations_module._normalize_skip(0), 0)
        self.assertIsNone(operations_module._normalize_limit(None))

        with self.assertRaises(TypeError):
            operations_module._normalize_filter([])
        with self.assertRaises(TypeError):
            operations_module._normalize_projection([])
        with self.assertRaises(ValueError):
            operations_module._normalize_hint("")
        with self.assertRaises(TypeError):
            operations_module._normalize_array_filters({})
        with self.assertRaises(TypeError):
            operations_module._normalize_allow_disk_use("yes")
        with self.assertRaises(TypeError):
            operations_module._normalize_skip(True)

    def test_private_update_compilation_returns_none_in_relaxed_mode_for_invalid_shapes(self):
        self.assertEqual(
            operations_module._compile_update_plans(
                {},
                dialect=operations_module.MONGODB_DIALECT_70,
                selector_filter={},
                collation=None,
                array_filters=None,
                variables=None,
                planning_mode=PlanningMode.RELAXED,
            ),
            (None, None),
        )

    def test_private_update_compilation_returns_none_in_relaxed_mode_after_engine_failure(self):
        with mock.patch.object(
            operations_module.UpdateEngine,
            "compile_update_plan",
            side_effect=operations_module.OperationFailure("cannot compile"),
        ):
            compiled = operations_module._compile_update_plans(
                {"$set": {"name": "Ada"}},
                dialect=operations_module.MONGODB_DIALECT_70,
                selector_filter={},
                collation=None,
                array_filters=None,
                variables=None,
                planning_mode=PlanningMode.RELAXED,
            )

        self.assertEqual(compiled, (None, None))

    def test_private_update_compilation_reraises_in_strict_mode_after_engine_failure(self):
        with mock.patch.object(
            operations_module.UpdateEngine,
            "compile_update_plan",
            side_effect=operations_module.OperationFailure("cannot compile"),
        ):
            with self.assertRaisesRegex(operations_module.OperationFailure, "cannot compile"):
                operations_module._compile_update_plans(
                    {"$set": {"name": "Ada"}},
                    dialect=operations_module.MONGODB_DIALECT_70,
                    selector_filter={},
                    collation=None,
                    array_filters=None,
                    variables=None,
                    planning_mode=PlanningMode.STRICT,
                )

    def test_relaxed_aggregate_operation_collects_stage_shape_and_support_issues(self):
        operation = compile_aggregate_operation(
            [{"$future": {}}, {"bad": 1}, {"$match": {}, "$sort": {}}],
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertEqual(operation.planning_mode, PlanningMode.RELAXED)
        self.assertEqual(
            [issue.scope for issue in operation.planning_issues],
            ["aggregate", "aggregate", "aggregate"],
        )
        self.assertTrue(any("Unsupported aggregation stage" in issue.message for issue in operation.planning_issues))

    def test_relaxed_aggregate_operation_accepts_registered_extension_stage(self):
        register_aggregation_stage("$future", lambda documents, _spec, _context: documents)
        try:
            operation = compile_aggregate_operation(
                [{"$future": {}}],
                planning_mode=PlanningMode.RELAXED,
            )
        finally:
            unregister_aggregation_stage("$future")

        self.assertEqual(operation.planning_issues, ())

    def test_aggregate_operation_with_overrides_preserves_planning_metadata(self):
        operation = compile_aggregate_operation(
            [{"$future": {}}],
            planning_mode=PlanningMode.RELAXED,
        ).with_overrides(batch_size=10)

        self.assertEqual(operation.batch_size, 10)
        self.assertEqual(operation.planning_mode, PlanningMode.RELAXED)
        self.assertEqual(len(operation.planning_issues), 1)

    def test_operation_with_overrides_preserves_dataclass_contracts(self):
        find = compile_find_operation({"name": "Ada"}).with_overrides(limit=1)
        update = compile_update_operation({"name": "Ada"}).with_overrides(comment="patched")
        aggregate = compile_aggregate_operation([{"$match": {"name": "Ada"}}]).with_overrides(batch_size=5)

        self.assertEqual(find.limit, 1)
        self.assertEqual(update.comment, "patched")
        self.assertEqual(aggregate.batch_size, 5)

    def test_private_aggregate_planning_issue_collector_reports_non_list_pipeline(self):
        issues = operations_module._collect_aggregate_planning_issues(
            {"$match": {"name": "Ada"}},
            dialect=operations_module.MONGODB_DIALECT_70,
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertEqual(
            issues,
            (operations_module.PlanningIssue(scope="aggregate", message="pipeline must be a list"),),
        )

    def test_engines_plan_read_execution_before_explain(self):
        async def _run() -> None:
            operation = compile_find_operation({"name": "Ada"})
            memory = MemoryEngine()
            sqlite = SQLiteEngine()
            await memory.connect()
            await sqlite.connect()
            try:
                memory_plan = await memory.plan_find_execution("db", "users", operation)
                sqlite_plan = await sqlite.plan_find_execution("db", "users", operation)
                self.assertIsInstance(memory_plan, EngineReadExecutionPlan)
                self.assertIsInstance(sqlite_plan, EngineReadExecutionPlan)
                self.assertEqual(memory_plan.strategy, "python")
                self.assertEqual(sqlite_plan.strategy, "sql")
                self.assertTrue(sqlite_plan.execution_lineage)
            finally:
                await memory.disconnect()
                await sqlite.disconnect()

        asyncio.run(_run())

    def test_sqlite_read_planner_exposes_fallback_reason_in_negotiation(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                operation = compile_find_operation({"payload": {"$gt": b"x"}})
                plan = await engine.plan_find_execution("db", "users", operation)
                self.assertEqual(plan.strategy, "python")
                self.assertIsNotNone(plan.fallback_reason)
                self.assertTrue(plan.execution_lineage)
            finally:
                await engine.disconnect()

        asyncio.run(_run())

    def test_namespace_admin_protocol_exposes_collection_options(self):
        self.assertIn("collection_options", AsyncNamespaceAdminEngine.__dict__)

    def test_find_operation_compiles_normalized_read_plan(self):
        operation = compile_find_operation(
            {"name": "Ada"},
            projection={"name": 1},
            sort=[("rank", 1)],
            skip=2,
            limit=5,
            hint=[("name", 1)],
            comment="read",
            max_time_ms=25,
            batch_size=10,
        )

        self.assertIsInstance(operation, FindOperation)
        self.assertEqual(operation.filter_spec, {"name": "Ada"})
        self.assertEqual(operation.projection, {"name": 1})
        self.assertEqual(operation.sort, [("rank", 1)])
        self.assertEqual(operation.skip, 2)
        self.assertEqual(operation.limit, 5)
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "read")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.batch_size, 10)

    def test_find_operation_rejects_invalid_skip(self):
        with self.assertRaises(TypeError):
            compile_find_operation({"name": "Ada"}, skip=-1)

    def test_find_and_aggregate_operations_reject_invalid_limit_hint_and_options(self):
        with self.assertRaises(TypeError):
            compile_find_operation({"name": "Ada"}, limit=-1)
        with self.assertRaises(TypeError):
            compile_find_operation({"name": "Ada"}, limit=True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            compile_find_operation({"name": "Ada"}, hint="")
        with self.assertRaises(TypeError):
            compile_update_operation({"name": "Ada"}, let="bad")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            compile_aggregate_operation([{"$match": {"name": "Ada"}}], batch_size=-1)

    def test_update_operation_compiles_normalized_write_plan(self):
        operation = compile_update_operation(
            {"name": "Ada"},
            sort=[("rank", 1)],
            array_filters=[{"item.score": {"$gte": 10}}],
            hint=[("name", 1)],
            comment="write",
            max_time_ms=25,
            let={"target": "Ada"},
        )

        self.assertIsInstance(operation, UpdateOperation)
        self.assertEqual(operation.filter_spec, {"name": "Ada"})
        self.assertEqual(operation.sort, [("rank", 1)])
        self.assertEqual(operation.array_filters, [{"item.score": {"$gte": 10}}])
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "write")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.let, {"target": "Ada"})

    def test_find_selection_operation_derives_from_update_operation(self):
        update_operation = compile_update_operation(
            {"name": "Ada"},
            sort=[("rank", 1)],
            hint=[("name", 1)],
            comment="write",
            max_time_ms=25,
            let={"target": "Ada"},
        )

        selection = compile_find_selection_from_update_operation(
            update_operation,
            projection={"_id": 1},
            limit=1,
        )

        self.assertIsInstance(selection, FindOperation)
        self.assertEqual(selection.filter_spec, update_operation.filter_spec)
        self.assertEqual(selection.plan, update_operation.plan)
        self.assertEqual(selection.sort, update_operation.sort)
        self.assertEqual(selection.hint, update_operation.hint)
        self.assertEqual(selection.comment, update_operation.comment)
        self.assertEqual(selection.projection, {"_id": 1})
        self.assertEqual(selection.limit, 1)

    def test_update_operation_rejects_invalid_array_filters(self):
        with self.assertRaises(TypeError):
            compile_update_operation({"name": "Ada"}, array_filters=[1])

    def test_compiled_update_plan_is_executable_object(self):
        plan = UpdateEngine.compile_update_plan({"$set": {"name": "Ada"}})
        document = {"_id": "1"}

        modified = plan.apply(document)

        self.assertTrue(modified)
        self.assertEqual(document["name"], "Ada")

    def test_compiled_update_pipeline_plan_is_executable_object(self):
        plan = UpdateEngine.compile_update_plan(
            [
                {"$set": {"name": {"$concat": ["$first", " ", "$last"]}}},
                {"$unset": ["first", "last"]},
            ]
        )
        document = {"_id": "1", "first": "Ada", "last": "Lovelace"}

        modified = plan.apply(document)

        self.assertIsInstance(plan, CompiledUpdatePipelinePlan)
        self.assertTrue(modified)
        self.assertEqual(document, {"_id": "1", "name": "Ada Lovelace"})

    def test_aggregate_operation_compiles_normalized_pipeline_plan(self):
        operation = compile_aggregate_operation(
            [{"$match": {"name": "Ada"}}],
            hint=[("name", 1)],
            comment="agg",
            max_time_ms=25,
            batch_size=10,
            allow_disk_use=True,
            let={"target": "Ada"},
        )

        self.assertIsInstance(operation, AggregateOperation)
        self.assertEqual(operation.pipeline, [{"$match": {"name": "Ada"}}])
        self.assertEqual(operation.hint, [("name", 1)])
        self.assertEqual(operation.comment, "agg")
        self.assertEqual(operation.max_time_ms, 25)
        self.assertEqual(operation.batch_size, 10)
        self.assertTrue(operation.allow_disk_use)
        self.assertEqual(operation.let, {"target": "Ada"})

    def test_aggregate_operation_rejects_invalid_pipeline(self):
        with self.assertRaises(TypeError):
            compile_aggregate_operation({"$match": {"name": "Ada"}})

    def test_aggregate_operation_rejects_invalid_allow_disk_use(self):
        with self.assertRaises(TypeError):
            compile_aggregate_operation([{"$match": {"name": "Ada"}}], allow_disk_use="yes")  # type: ignore[arg-type]

    def test_admin_cursors_share_materialized_base_classes(self):
        self.assertTrue(issubclass(AsyncIndexCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(AsyncListingCursor, AsyncMaterializedCursor))
        self.assertTrue(issubclass(IndexCursor, MaterializedCursor))
        self.assertTrue(issubclass(ListingCursor, MaterializedCursor))

    def test_aggregation_stage_registry_is_the_dispatch_source(self):
        self.assertIn("$match", AGGREGATION_STAGE_HANDLERS)
        self.assertIn("$match", AGGREGATION_STAGE_SPECS)
        self.assertIn("$group", AGGREGATION_STAGE_HANDLERS)
        self.assertIn("$lookup", AGGREGATION_STAGE_HANDLERS)
        self.assertIs(AGGREGATION_STAGE_HANDLERS["$set"], AGGREGATION_STAGE_HANDLERS["$addFields"])
        self.assertTrue(is_streamable_aggregation_stage("$match"))
        self.assertFalse(is_streamable_aggregation_stage("$group"))

    def test_registered_stage_can_publish_execution_mode(self):
        register_aggregation_stage(
            "$future",
            lambda documents, _spec, _context: documents,
            execution_mode="streamable",
        )
        try:
            spec = get_aggregation_stage_spec("$future")
        finally:
            unregister_aggregation_stage("$future")

        self.assertEqual(spec.execution_mode, "streamable")

    def test_index_definition_is_shared_source_for_public_metadata(self):
        index = IndexDefinition([("email", 1), ("created_at", -1)], name="email_created", unique=True)

        self.assertEqual(
            index.to_list_document(),
            {
                "name": "email_created",
                "key": {"email": 1, "created_at": -1},
                "unique": True,
            },
        )
        self.assertEqual(
            index.to_information_entry_map(),
            {
                "email_created": {
                    "key": [("email", 1), ("created_at", -1)],
                    "unique": True,
                }
            },
        )
        self.assertEqual(default_id_index_definition().name, "_id_")

    def test_engine_index_record_wraps_typed_internal_index_metadata(self):
        record = EngineIndexRecord(
            name="email_idx",
            physical_name="idx_email",
            fields=["email"],
            key=[("email", 1)],
            unique=True,
        )

        self.assertEqual(record["name"], "email_idx")
        self.assertEqual(record.get("physical_name"), "idx_email")
        self.assertEqual(
            record.to_definition().to_list_document()["key"],
            {"email": 1},
        )
