from __future__ import annotations

from types import SimpleNamespace
import unittest

from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    GreaterThanCondition,
    LessThanCondition,
    MatchAll,
    NotCondition,
    OrCondition,
)
from mongoeco.engines import _sqlite_fast_paths as fast_paths
from mongoeco.engines import _sqlite_read_fast_path_runtime as read_fast_path_runtime


class SQLiteFastPathsTests(unittest.TestCase):
    def test_plan_and_comparison_fields_cover_recursive_shapes(self):
        plan = AndCondition(
            clauses=(
                EqualsCondition(field="kind", value="view"),
                NotCondition(clause=GreaterThanCondition(field="rank", value=3)),
                OrCondition(clauses=(LessThanCondition(field="score", value=5), MatchAll())),
            )
        )

        self.assertEqual(fast_paths.plan_fields(MatchAll()), set())
        self.assertEqual(fast_paths.plan_fields(plan), {"kind", "rank", "score"})
        self.assertEqual(fast_paths.comparison_fields(plan), {"rank", "score"})
        self.assertEqual(fast_paths.comparison_fields(EqualsCondition(field="kind", value="x")), set())
        self.assertEqual(fast_paths.plan_fields(SimpleNamespace(field=1)), set())

    def test_select_clause_and_custom_order_cover_limit_and_skip_branches(self):
        self.assertEqual(
            fast_paths.resolve_select_clause_for_scalar_sort("document"),
            "documents.document",
        )
        self.assertEqual(
            fast_paths.resolve_select_clause_for_scalar_sort("storage_key, document"),
            "documents.storage_key, documents.document",
        )
        self.assertEqual(
            fast_paths.resolve_select_clause_for_scalar_sort("COUNT(*)"),
            "COUNT(*)",
        )

        sql, params = fast_paths.build_select_statement_with_custom_order(
            select_clause="document",
            from_clause="documents",
            namespace_sql="db_name = ? AND coll_name = ?",
            namespace_params=("db", "coll"),
            where_fragment=("kind = ?", ["view"]),
            order_sql=" ORDER BY document",
            skip=2,
            limit=None,
        )
        self.assertIn("LIMIT -1", sql)
        self.assertIn("OFFSET ?", sql)
        self.assertEqual(params, ["db", "coll", "view", 2])

    def test_build_scalar_sort_select_sql_rejects_ineligible_cases(self):
        base_kwargs = dict(
            db_name="db",
            coll_name="coll",
            plan=MatchAll(),
            select_clause="document",
            skip=0,
            limit=None,
            field_traverses_array_in_collection=lambda *_: False,
            field_contains_tagged_bytes_in_collection=lambda *_: False,
            field_contains_tagged_undefined_in_collection=lambda *_: False,
            field_has_uniform_scalar_sort_type_in_collection=lambda *_: "string",
            translate_query_plan_with_multikey=lambda *_: ("1 = 1", []),
            find_scalar_sort_index=lambda *_: None,
            lookup_collection_id=lambda *_: 1,
            quote_identifier=lambda name: f'"{name}"',
            value_expression_sql=lambda field: f"json_extract(document, '$.{field}')",
        )

        self.assertIsNone(fast_paths.build_scalar_sort_select_sql(sort=[("kind", 2)], hint=None, **base_kwargs))
        kwargs = dict(base_kwargs)
        kwargs["field_traverses_array_in_collection"] = lambda *_: True
        self.assertIsNone(
            fast_paths.build_scalar_sort_select_sql(sort=[("kind", 1)], hint=None, **kwargs)
        )
        kwargs = dict(base_kwargs)
        kwargs["field_contains_tagged_bytes_in_collection"] = lambda *_: True
        self.assertIsNone(
            fast_paths.build_scalar_sort_select_sql(sort=[("kind", 1)], hint=None, **kwargs)
        )
        kwargs = dict(base_kwargs)
        kwargs["field_contains_tagged_undefined_in_collection"] = lambda *_: True
        self.assertIsNone(
            fast_paths.build_scalar_sort_select_sql(sort=[("kind", 1)], hint=None, **kwargs)
        )
        kwargs = dict(base_kwargs)
        kwargs["field_has_uniform_scalar_sort_type_in_collection"] = lambda *_: None
        self.assertIsNone(
            fast_paths.build_scalar_sort_select_sql(sort=[("kind", 1)], hint=None, **kwargs)
        )

    def test_build_scalar_sort_select_sql_covers_indexed_and_document_fallback_orders(self):
        base_kwargs = dict(
            db_name="db",
            coll_name="coll",
            plan=MatchAll(),
            select_clause="storage_key, document",
            sort=[("kind", 1)],
            skip=1,
            limit=3,
            hint=None,
            field_traverses_array_in_collection=lambda *_: False,
            field_contains_tagged_bytes_in_collection=lambda *_: False,
            field_contains_tagged_undefined_in_collection=lambda *_: False,
            field_has_uniform_scalar_sort_type_in_collection=lambda *_: "string",
            translate_query_plan_with_multikey=lambda *_: ("1 = 1", []),
            quote_identifier=lambda name: f'"{name}"',
            value_expression_sql=lambda field: f"json_extract(document, '$.{field}')",
        )

        sql, params = fast_paths.build_scalar_sort_select_sql(
            find_scalar_sort_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
            lookup_collection_id=lambda *_: 7,
            **base_kwargs,
        )
        self.assertIn("scalar_index_entries INDEXED BY", sql)
        self.assertIn("LIMIT ?", sql)
        self.assertEqual(params[:4], ["db", "coll", 7, "kind_1"])

        sql, params = fast_paths.build_scalar_sort_select_sql(
            find_scalar_sort_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
            lookup_collection_id=lambda *_: None,
            **base_kwargs,
        )
        self.assertIn("FROM documents", sql)
        self.assertIn("json_extract(document, '$.kind') ASC", sql)
        self.assertEqual(params[:2], ["db", "coll"])

    def test_build_select_sql_prefers_scalar_sort_and_hinted_index(self):
        sql, params = fast_paths.build_select_sql(
            db_name="db",
            coll_name="coll",
            plan=MatchAll(),
            select_clause="document",
            sort=None,
            skip=0,
            limit=None,
            hint="idx_kind",
            build_scalar_sort_select_sql_fn=lambda *_args, **_kwargs: ("SELECT 1", ["x"]),
            resolve_hint_index=lambda *_: None,
            translate_query_plan_with_multikey=lambda *_: ("1 = 1", []),
            build_select_statement=lambda **_kwargs: SimpleNamespace(sql="SELECT 2", params=("db", "coll")),
            quote_identifier=lambda name: f'"{name}"',
        )
        self.assertEqual((sql, params), ("SELECT 1", ["x"]))

        sql, _ = fast_paths.build_select_sql(
            db_name="db",
            coll_name="coll",
            plan=MatchAll(),
            select_clause="document",
            sort=None,
            skip=0,
            limit=None,
            hint="idx_kind",
            build_scalar_sort_select_sql_fn=lambda *_args, **_kwargs: None,
            resolve_hint_index=lambda *_: {"physical_name": "idx_physical"},
            translate_query_plan_with_multikey=lambda *_: ("1 = 1", []),
            build_select_statement=lambda **kwargs: SimpleNamespace(sql=kwargs["from_clause"], params=()),
            quote_identifier=lambda name: f'"{name}"',
        )
        self.assertIn('documents INDEXED BY "idx_physical"', sql)

    def test_scalar_index_and_range_helpers_cover_none_and_success_paths(self):
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_index(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                null_matches_undefined=False,
                field_is_top_level_array_in_collection=lambda *_: True,
                find_scalar_index=lambda *_: None,
                lookup_collection_id=lambda *_: None,
                multikey_signatures_for_query_value=lambda *_: (),
                multikey_type_score=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
            )
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_index(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                null_matches_undefined=False,
                field_is_top_level_array_in_collection=lambda *_: False,
                find_scalar_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
                lookup_collection_id=lambda *_: None,
                multikey_signatures_for_query_value=lambda *_: (("string", "view"),),
                multikey_type_score=lambda *_: 2,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: ("1", {"_id": "1"}),
                deserialize_document=lambda document: document,
            )
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_index(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                null_matches_undefined=False,
                field_is_top_level_array_in_collection=lambda *_: False,
                find_scalar_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
                lookup_collection_id=lambda *_: 1,
                multikey_signatures_for_query_value=lambda *_: (_ for _ in ()).throw(NotImplementedError()),
                multikey_type_score=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
            )
        )
        self.assertEqual(
            fast_paths.select_first_document_for_scalar_index(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                null_matches_undefined=False,
                field_is_top_level_array_in_collection=lambda *_: False,
                find_scalar_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
                lookup_collection_id=lambda *_: 1,
                multikey_signatures_for_query_value=lambda *_: (("string", "view"),),
                multikey_type_score=lambda *_: 2,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: ("1", {"_id": "1"}),
                deserialize_document=lambda document: {"decoded": document},
            ),
            ("1", {"decoded": {"_id": "1"}}),
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_index(
                db_name="db",
                coll_name="coll",
                field="kind",
                value="view",
                null_matches_undefined=False,
                field_is_top_level_array_in_collection=lambda *_: False,
                find_scalar_index=lambda *_: {"name": "kind_1", "scalar_physical_name": "scidx_kind"},
                lookup_collection_id=lambda *_: 1,
                multikey_signatures_for_query_value=lambda *_: (("string", "view"),),
                multikey_type_score=lambda *_: 2,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
            )
        )

        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_range(
                db_name="db",
                coll_name="coll",
                field="score",
                value=5,
                operator=">",
                can_use_scalar_range_fast_path=lambda *_: None,
                lookup_collection_id=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
            )
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_range(
                db_name="db",
                coll_name="coll",
                field="score",
                value=5,
                operator=">",
                can_use_scalar_range_fast_path=lambda *_: ({"name": "score_1"}, 2, "5"),
                lookup_collection_id=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: ("2", {"_id": "2"}),
                deserialize_document=lambda document: document,
            )
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_range(
                db_name="db",
                coll_name="coll",
                field="score",
                value=5,
                operator=">",
                can_use_scalar_range_fast_path=lambda *_: (
                    {"name": "score_1", "scalar_physical_name": "scidx_score"},
                    2,
                    "5",
                ),
                lookup_collection_id=lambda *_: None,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: ("2", {"_id": "2"}),
                deserialize_document=lambda document: document,
            )
        )
        self.assertEqual(
            fast_paths.select_first_document_for_scalar_range(
                db_name="db",
                coll_name="coll",
                field="score",
                value=5,
                operator=">",
                can_use_scalar_range_fast_path=lambda *_: ({"name": "score_1", "scalar_physical_name": "scidx_score"}, 2, "5"),
                lookup_collection_id=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: ("2", {"_id": "2"}),
                deserialize_document=lambda document: {"decoded": document},
            ),
            ("2", {"decoded": {"_id": "2"}}),
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_scalar_range(
                db_name="db",
                coll_name="coll",
                field="score",
                value=5,
                operator=">",
                can_use_scalar_range_fast_path=lambda *_: (
                    {"name": "score_1", "scalar_physical_name": "scidx_score"},
                    2,
                    "5",
                ),
                lookup_collection_id=lambda *_: 1,
                quote_identifier=lambda name: f'"{name}"',
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
            )
        )

    def test_select_first_document_for_plan_covers_id_equals_scalar_range_and_fallthrough(self):
        self.assertEqual(
            fast_paths.select_first_document_for_plan(
                db_name="db",
                coll_name="coll",
                plan=EqualsCondition(field="_id", value="1"),
                storage_key_for_id=lambda value: f"key:{value}",
                fetchone=lambda *_: ("key:1", {"_id": "1"}),
                deserialize_document=lambda document: {"decoded": document},
                select_first_document_for_scalar_index_fn=lambda *_: None,
                select_first_document_for_scalar_range_fn=lambda *_: None,
            ),
            ("key:1", {"decoded": {"_id": "1"}}),
        )
        self.assertEqual(
            fast_paths.select_first_document_for_plan(
                db_name="db",
                coll_name="coll",
                plan=EqualsCondition(field="kind", value="view"),
                storage_key_for_id=lambda value: f"key:{value}",
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
                select_first_document_for_scalar_index_fn=lambda *_: ("3", {"_id": "3"}),
                select_first_document_for_scalar_range_fn=lambda *_: None,
            ),
            ("3", {"_id": "3"}),
        )
        self.assertEqual(
            fast_paths.select_first_document_for_plan(
                db_name="db",
                coll_name="coll",
                plan=GreaterThanCondition(field="score", value=10),
                storage_key_for_id=lambda value: f"key:{value}",
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
                select_first_document_for_scalar_index_fn=lambda *_: None,
                select_first_document_for_scalar_range_fn=lambda *_args: ("4", {"_id": "4"}),
            ),
            ("4", {"_id": "4"}),
        )
        self.assertIsNone(
            fast_paths.select_first_document_for_plan(
                db_name="db",
                coll_name="coll",
                plan=EqualsCondition(field="nested.value", value=1),
                storage_key_for_id=lambda value: f"key:{value}",
                fetchone=lambda *_: None,
                deserialize_document=lambda document: document,
                select_first_document_for_scalar_index_fn=lambda *_: None,
                select_first_document_for_scalar_range_fn=lambda *_: None,
            )
        )

    def test_find_scalar_index_skips_indexes_without_scalar_support(self):
        engine = SimpleNamespace(
            _load_indexes=lambda *_: [
                {"name": "kind_text", "fields": ["kind"]},
                {"name": "kind_scalar", "fields": ["kind"]},
            ],
            _supports_scalar_index=lambda index: index["name"] == "kind_scalar",
        )

        self.assertEqual(
            read_fast_path_runtime._find_scalar_index(engine, "db", "coll", "kind"),
            {"name": "kind_scalar", "fields": ["kind"]},
        )
