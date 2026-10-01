"""Logical validation, BSON equivalence and scoped resources share one contract."""

import decimal
import unittest

from copy import deepcopy
from itertools import product
from unittest.mock import Mock, patch

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup
from mongoeco.core.aggregation.extensions import (
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.core.aggregation.resources import AggregationResources
from mongoeco.core.aggregation.runtime import (
    AggregationStageContext,
    aggregation_equality_key,
    evaluate_expression,
)
from mongoeco.core.aggregation.runtime_state import apply_pipeline_states
from mongoeco.core.aggregation.stages import apply_pipeline, get_aggregation_stage_spec
from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    string_equality_key,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, Decimal128


class AggregationSemanticPreparationTests(unittest.TestCase):
    def test_relocation_rebinds_namespaces_and_logical_addresses(self):
        source = [{"$match": {}}, {"$unionWith": {"pipeline": [{"$indexStats": {}}]}}]
        prepared = prepare_pipeline(source, collection="root")
        for residual in (prepared[1:], list(prepared[1:]), deepcopy(prepared[1:])):
            moved = prepare_pipeline(
                residual,
                collection="foreign",
                scope="$lookup",
                path=(3, "$lookup.pipeline"),
            )
            self.assertEqual(moved.addresses[0].index, 0)
            self.assertEqual(moved.addresses[0].path, (3, "$lookup.pipeline", 0))
            self.assertEqual(moved.collection, "foreign")
            self.assertEqual(
                {request.collection for request in moved.requests}, {"foreign"}
            )
        self.assertEqual(
            {request.collection for request in prepared.requests}, {"root"}
        )
        unbound = prepare_pipeline(prepared, collection=None)
        self.assertIsNone(unbound.collection)
        self.assertEqual({request.collection for request in unbound.requests}, {None})

    def test_prepared_children_are_revalidated_in_their_new_scope(self):
        for runner, copied in product(
            (apply_pipeline, apply_pipeline_states), (False, True)
        ):
            child = prepare_pipeline([{"$indexStats": {}}], collection="root")
            if copied:
                child = list(child)
            with (
                self.subTest(runner=runner.__name__, copied=copied),
                self.assertRaisesRegex(OperationFailure, "not allowed inside \\$facet"),
            ):
                runner([], [{"$facet": {"x": child}}], index_stats_resolver=list)

    def test_registry_changes_invalidate_preparation_without_losing_positions(self):
        handler = Mock(return_value=[])
        register_aggregation_stage("$indexStats", handler)
        try:
            prepared = prepare_pipeline(
                [{"$match": {}}, {"$indexStats": "custom"}], collection="root"
            )
            self.assertFalse(handler.called)
        finally:
            unregister_aggregation_stage("$indexStats")
        for residual in (prepared[1:], list(prepared[1:])):
            with self.assertRaisesRegex(OperationFailure, "first pipeline stage"):
                prepare_pipeline(residual)

    def test_extension_discovery_does_not_execute_or_parse_custom_handlers(self):
        handler = Mock(return_value=[])
        for operator, spec in (
            ("$lookup", {"from": "foreign", "pipeline": "custom"}),
            ("$unionWith", {"coll": "foreign", "pipeline": {"custom": 1}}),
            ("$indexStats", "custom"),
            ("$collStats", {"storageStats": {"scale": "custom"}}),
        ):
            register_aggregation_stage(operator, handler)
            try:
                prepared = prepare_pipeline([{operator: spec}], collection="root")
                self.assertEqual(prepared[0][operator], spec)
                self.assertTrue(prepared.requests)
                self.assertFalse(handler.called)
            finally:
                unregister_aggregation_stage(operator)

    def test_facet_restrictions_follow_all_descendants_with_and_without_rows(self):
        forbidden = [
            {"$facet": {"x": []}},
            {"$indexStats": {}},
            {"$collStats": {"count": {}}},
            {"$planCacheStats": {}},
            {"$currentOp": {}},
            {"$listSessions": {}},
            {"$geoNear": {}},
        ]
        for runner, stage, documents in product(
            (apply_pipeline, apply_pipeline_states), forbidden, ([], [{"_id": 1}])
        ):
            nested = [{"$unionWith": {"coll": "foreign", "pipeline": [stage]}}]
            pipeline = [
                {
                    "$facet": {
                        "x": [
                            {
                                "$lookup": {
                                    "from": "foreign",
                                    "pipeline": nested,
                                    "as": "e",
                                }
                            }
                        ]
                    }
                }
            ]
            with (
                self.subTest(runner=runner.__name__, stage=stage, documents=documents),
                self.assertRaisesRegex(OperationFailure, "not allowed inside \\$facet"),
            ):
                runner(documents, pipeline, collection_resolver=lambda name: [])

    def test_documents_source_and_dialect_namespace_rules(self):
        documents = [{"_id": "seed"}]
        lookup = {"$lookup": {"pipeline": [{"$documents": [{"x": 1}]}], "as": "e"}}
        union = {"$unionWith": {"pipeline": [{"$documents": [{"x": 1}]}]}}
        for runner, dialect in product(
            (apply_pipeline, apply_pipeline_states),
            (MONGODB_DIALECT_70, MONGODB_DIALECT_80),
        ):
            with self.subTest(runner=runner.__name__, dialect=dialect.key):
                result = runner(documents, [lookup], dialect=dialect)
                public = [
                    row.public_document() if hasattr(row, "public_document") else row
                    for row in result
                ]
                self.assertEqual(public, [{"_id": "seed", "e": [{"x": 1}]}])
                self.assertEqual(len(runner(documents, [union], dialect=dialect)), 2)
                with self.assertRaises(OperationFailure):
                    runner(
                        [], [{"$lookup": {"pipeline": [], "as": "e"}}], dialect=dialect
                    )
        for operator, namespace in (("$lookup", "from"), ("$unionWith", "coll")):
            spec = {namespace: "foreign", "pipeline": [{"$documents": []}]}
            if operator == "$lookup":
                spec["as"] = "e"
            self.assertEqual(
                apply_pipeline(
                    [],
                    [{operator: spec}],
                    dialect=MONGODB_DIALECT_70,
                    collection_resolver=lambda name: [],
                ),
                [],
            )
            with self.assertRaises(OperationFailure):
                apply_pipeline(
                    [],
                    [{operator: spec}],
                    dialect=MONGODB_DIALECT_80,
                    collection_resolver=lambda name: [],
                )

    def test_stage_addresses_survive_slicing_copying_and_physical_lists(self):
        source = [
            {"$match": {}},
            {"$project": {"_id": 1}},
            {
                "$lookup": {
                    "from": "foreign",
                    "pipeline": [{"$indexStats": {}}],
                    "as": "e",
                }
            },
        ]
        prepared = prepare_pipeline(source, collection="root")
        for residual in (prepared[2:], deepcopy(prepared)[2:], list(prepared[2:])):
            program = prepare_pipeline(residual)
            self.assertEqual(program.addresses[0].index, 2)
            nested = program[0]["$lookup"]["pipeline"]
            self.assertEqual(nested.addresses[0].index, 0)
            self.assertEqual(nested.addresses[0].path, (2, "$lookup.pipeline", 0))
        source[2]["$lookup"]["as"] = "changed"
        self.assertEqual(prepared[2]["$lookup"]["as"], "e")
        self.assertIs(prepare_pipeline(prepared), prepared)

    def test_extensions_keep_their_parser_and_execution_contract(self):
        register_aggregation_stage(
            "$indexStats",
            lambda docs, spec, context: [{"index": context.stage_index}],
            execution_mode="streamable",
        )
        try:
            self.assertEqual(
                apply_pipeline([], [{"$match": {}}, {"$indexStats": "custom"}]),
                [{"index": 1}],
            )
        finally:
            unregister_aggregation_stage("$indexStats")

    def test_nested_invalid_shapes_fail_without_rows(self):
        pipelines = [
            [{"$facet": {1: []}}],
            [{"$facet": {"x": {}}}],
            [{"$facet": {"x": [{"$collStats": {"count": {}}}]}}],
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "pipeline": [{"$merge": "out"}],
                        "as": "e",
                    }
                }
            ],
            [{"$unionWith": {"coll": "foreign", "pipeline": [{"$out": "out"}]}}],
            [{"$lookup": {"from": None, "pipeline": [], "as": "e"}}],
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "let": {1: 2},
                        "pipeline": [],
                        "as": "e",
                    }
                }
            ],
            [{"$lookup": {"pipeline": [1], "as": "e"}}],
            [{"$collStats": {"storageStats": {"scale": 0}}}],
        ]
        for runner, pipeline in product(
            (apply_pipeline, apply_pipeline_states), pipelines
        ):
            with (
                self.subTest(runner=runner.__name__, pipeline=pipeline),
                self.assertRaises(OperationFailure),
            ):
                runner([], pipeline, collection_resolver=lambda name: [])

    def test_empty_resources_and_foreign_introspection_use_their_namespace(self):
        resources = AggregationResources(
            {"root": [{"_id": "root"}], "foreign": []},
            "root",
            {
                ("root", "$indexStats", 1): [{"name": "root"}],
                ("foreign", "$indexStats", 1): [],
                ("foreign", "$collStats", 2): {"ns": "db.foreign", "count": 3},
                ("foreign", "$planCacheStats", 1): [{"ns": "db.foreign"}],
                ("foreign", "$currentOp", 1): [],
                ("foreign", "$listSessions", 1): [],
            },
        )
        for runner in (apply_pipeline, apply_pipeline_states):
            for operator, spec in (
                ("$indexStats", {}),
                ("$currentOp", {}),
                ("$listSessions", {}),
                ("$planCacheStats", {}),
                ("$collStats", {"count": {}, "storageStats": {"scale": 2}}),
            ):
                with self.subTest(runner=runner.__name__, operator=operator):
                    rows = runner(
                        [{"_id": 1}],
                        [
                            {
                                "$lookup": {
                                    "from": "foreign",
                                    "pipeline": [{operator: spec}],
                                    "as": "e",
                                }
                            }
                        ],
                        collection_resolver=resources,
                    )
                    document = (
                        rows[0].public_document()
                        if hasattr(rows[0], "public_document")
                        else rows[0]
                    )
                    expected = (
                        [{"ns": "db.foreign"}]
                        if operator == "$planCacheStats"
                        else [
                            {
                                "ns": "db.foreign",
                                "count": 3,
                                "storageStats": {"ns": "db.foreign", "count": 3},
                            }
                        ]
                        if operator == "$collStats"
                        else []
                    )
                    self.assertEqual(document["e"], expected)
        self.assertEqual(
            resources("__mongoeco_current_collection__"), [{"_id": "root"}]
        )

    def test_expression_collation_is_recursive_and_survives_lexical_scopes(self):
        collation = CollationSpec(locale="en", strength=2)
        expressions = [
            ({"$eq": [{"x": ["Ada"]}, {"x": ["ada"]}]}, True),
            ({"$cmp": ["Ada", "ada"]}, 0),
            (
                {"$let": {"vars": {"name": "Ada"}, "in": {"$in": ["$$name", ["ada"]]}}},
                True,
            ),
            ({"$setUnion": [["Ada"], ["ada"]]}, ["Ada"]),
            ({"$setIntersection": [["Ada"], ["ada"]]}, ["Ada"]),
            ({"$setDifference": [["Ada"], ["ada"]]}, []),
            ({"$setEquals": [["Ada"], ["ada"]]}, True),
            ({"$setIsSubset": [["Ada"], ["ada"]]}, True),
            ({"$indexOfArray": [["Ada"], "ada"]}, 0),
            ({"$map": {"input": ["Ada"], "in": {"$eq": ["$$this", "ada"]}}}, [True]),
            ({"$maxN": {"input": ["z", "A"], "n": 1}}, ["z"]),
            ({"$max": ["z", "A"]}, "z"),
            ({"$sortArray": {"input": ["z", "A"], "sortBy": 1}}, ["A", "z"]),
            (
                {"$sortArray": {"input": [{"x": "z"}, {"x": "A"}], "sortBy": {"x": 1}}},
                [{"x": "A"}, {"x": "z"}],
            ),
        ]
        for expression, expected in expressions:
            with self.subTest(expression=expression):
                self.assertEqual(
                    evaluate_expression({}, expression, collation=collation), expected
                )

    def test_semantic_keys_preserve_bson_numeric_equivalence_and_precision(self):
        for left, right in (
            (1, 1.0),
            (0, -0.0),
            (0, Decimal128("-0")),
            (1, Decimal128("1.00")),
            (float("nan"), decimal.Decimal("NaN")),
            (float("inf"), decimal.Decimal("Infinity")),
            ({"x": ["Ada", 1]}, {"x": ["ada", 1.0]}),
        ):
            collation = CollationSpec(locale="en", strength=2)
            self.assertEqual(
                aggregation_equality_key(left, collation),
                aggregation_equality_key(right, collation),
            )
        self.assertNotEqual(
            aggregation_equality_key(value=True), aggregation_equality_key(1)
        )
        left = decimal.Decimal(1234567890123456789012345678901234)
        right = decimal.Decimal(1234567890123456789012345678901235)
        self.assertNotEqual(
            aggregation_equality_key(left), aggregation_equality_key(right)
        )

    def test_recursive_collation_order_and_backend_keys(self):
        spec = CollationSpec(locale="en", strength=2)
        for left, right, comparison in (
            ({"x": "Ada"}, {"x": "ada"}, 0),
            ({"a": 1}, {"b": 1}, -1),
            ({"x": "a"}, {"x": "z"}, -1),
            ({"a": 1}, {"a": 1, "b": 2}, -1),
            (["Ada"], ["ada"], 0),
            (["a"], ["z"], -1),
            ([1], [1, 2], -1),
        ):
            self.assertEqual(
                compare_with_collation(left, right, collation=spec), comparison
            )
        with (
            patch("mongoeco.core.collation._icu", None),
            patch("mongoeco.core.collation._pyuca", None),
        ):
            self.assertEqual(
                string_equality_key("Ada", spec), string_equality_key("ada", spec)
            )
            self.assertNotEqual(
                string_equality_key("Ada", CollationSpec(locale="en", strength=3)),
                string_equality_key("ada", CollationSpec(locale="en", strength=3)),
            )
            with self.assertRaises(ValueError):
                string_equality_key("x", CollationSpec(locale="en", normalization=True))

    def test_preparation_owns_facet_branches_and_can_change_dialect(self):
        source = [{"$facet": {"branch": [{"$unionWith": {"pipeline": []}}]}}]
        prepared = prepare_pipeline(source, collection="root")
        upgraded = prepare_pipeline(prepared, dialect=MONGODB_DIALECT_80)
        self.assertEqual(prepared[0]["$facet"]["branch"].collection, "root")
        self.assertIs(upgraded.dialect, MONGODB_DIALECT_80)
        self.assertEqual(upgraded[0]["$facet"]["branch"].collection, "root")
        self.assertEqual(
            source, [{"$facet": {"branch": [{"$unionWith": {"pipeline": []}}]}}]
        )
        with self.assertRaises(OperationFailure):
            prepare_pipeline([{"$facet": []}])
        with self.assertRaises(OperationFailure):
            aggregation_equality_key({1, 2})

    def test_icu_group_key_uses_the_same_backend_as_comparisons(self):
        spec = CollationSpec(locale="en", normalization=True)
        with (
            patch("mongoeco.core.collation._can_use_icu_collation", return_value=True),
            patch("mongoeco.core.collation._get_icu_collator") as collator,
        ):
            collator.return_value.getSortKey.return_value = bytearray(b"key")
            self.assertEqual(string_equality_key("Ada", spec), b"key")
            collator.return_value.getSortKey.assert_called_once_with("Ada")

    def test_direct_registered_handlers_enforce_original_stage_position(self):
        for operator, spec in (
            ("$collStats", {"count": {}}),
            ("$indexStats", {}),
            ("$currentOp", {}),
            ("$planCacheStats", {}),
            ("$listSessions", {}),
        ):
            with (
                self.subTest(operator=operator),
                self.assertRaisesRegex(OperationFailure, "first pipeline stage"),
            ):
                get_aggregation_stage_spec(operator).handler(
                    [], spec, AggregationStageContext(stage_index=1)
                )

    def test_compiled_and_interpreted_group_extrema_use_the_same_collation(self):
        spec = {"_id": None, "lo": {"$min": "$x"}, "hi": {"$max": "$x"}}
        collation = CollationSpec(locale="en", strength=2)
        documents = [{"x": "a"}, {"x": "Z"}]
        expected = [{"_id": None, "lo": "a", "hi": "Z"}]
        self.assertEqual(
            CompiledGroup(spec).apply(documents, collation=collation), expected
        )
        self.assertEqual(
            apply_pipeline(documents, [{"$group": spec}], collation=collation), expected
        )
        self.assertEqual(
            [
                state.public_document()
                for state in apply_pipeline_states(
                    documents, [{"$group": spec}], collation=collation
                )
            ],
            expected,
        )

    def test_binary_keys_preserve_subtypes_and_default_bytes_equivalence(self):
        documents = [
            {"x": value} for value in (b"x", Binary(b"x", 0), Binary(b"x", 128))
        ]
        pipeline = [{"$group": {"_id": "$x", "n": {"$sum": 1}}}]
        expected = [{"_id": b"x", "n": 2}, {"_id": Binary(b"x", 128), "n": 1}]
        for runner in (apply_pipeline, apply_pipeline_states):
            rows = runner(documents, pipeline)
            self.assertCountEqual(
                [
                    row.public_document() if hasattr(row, "public_document") else row
                    for row in rows
                ],
                expected,
            )
