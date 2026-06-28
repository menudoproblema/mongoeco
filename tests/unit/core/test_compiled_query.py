import unittest
from itertools import product

from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import AndCondition, OrCondition, compile_filter
from mongoeco.engines.semantic_core import (
    compile_find_semantics,
    iter_filtered_documents,
)
from mongoeco.types import UNDEFINED


class CompiledQueryTests(unittest.TestCase):
    def test_compiled_query_matches_query_engine_for_top_level_array_equality_and_in(
        self,
    ):
        document = {'tags': ['python', 'mongodb'], 'scores': [1, 2]}

        equality_plan = compile_filter({'tags': 'python'})
        in_plan = compile_filter({'tags': {'$in': ['python']}})
        range_plan = compile_filter({'scores': {'$gte': 2}})

        self.assertEqual(
            QueryEngine.match_plan(document, equality_plan),
            CompiledQuery(equality_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, in_plan),
            CompiledQuery(in_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, range_plan),
            CompiledQuery(range_plan).match(document),
        )

    def test_compiled_query_matches_query_engine_for_missing_field_semantics(
        self,
    ):
        document = {}

        ne_plan = compile_filter({'value': {'$ne': None}})
        nin_plan = compile_filter({'value': {'$nin': [None]}})

        self.assertEqual(
            QueryEngine.match_plan(document, ne_plan),
            CompiledQuery(ne_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, nin_plan),
            CompiledQuery(nin_plan).match(document),
        )

    def test_compiled_query_matches_query_engine_for_null_negation_with_undefined(
        self,
    ):
        documents = [
            {},
            {'value': None},
            {'value': UNDEFINED},
            {'value': [UNDEFINED]},
            {'value': [1, UNDEFINED]},
            {'value': 'present'},
        ]

        for filter_spec in (
            {'value': {'$ne': None}},
            {'value': {'$nin': [None]}},
        ):
            plan = compile_filter(filter_spec)
            compiled = CompiledQuery(plan)
            self.assertEqual(
                [
                    QueryEngine.match_plan(document, plan)
                    for document in documents
                ],
                [compiled.match(document) for document in documents],
            )

    def test_compiled_query_matches_query_engine_for_top_level_scalar_equality(
        self,
    ):
        document = {'active': True, 'city': 'Madrid', 'age': 30}

        active_plan = compile_filter({'active': True})
        city_plan = compile_filter({'city': 'Madrid'})
        age_plan = compile_filter({'age': {'$gte': 18}})

        self.assertEqual(
            QueryEngine.match_plan(document, active_plan),
            CompiledQuery(active_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, city_plan),
            CompiledQuery(city_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, age_plan),
            CompiledQuery(age_plan).match(document),
        )

    def test_compiled_query_matches_query_engine_for_exists_with_null_and_nested_paths(
        self,
    ):
        document = {'value': None, 'items': [{'kind': 'a'}]}

        exists_null_plan = compile_filter({'value': {'$exists': True}})
        nested_exists_plan = compile_filter({'items.kind': {'$exists': True}})

        self.assertEqual(
            QueryEngine.match_plan(document, exists_null_plan),
            CompiledQuery(exists_null_plan).match(document),
        )
        self.assertEqual(
            QueryEngine.match_plan(document, nested_exists_plan),
            CompiledQuery(nested_exists_plan).match(document),
        )

    def test_compiled_query_uses_unique_bindings_for_nested_boolean_siblings(
        self,
    ):
        filter_spec = {
            '$or': [
                {'planning_status': {'$exists': False}},
                {'planning_status': 'active'},
            ],
            'completed_at': None,
            'task_type': {'$in': ['work_unit', 'content_update']},
        }
        document = {
            '_id': 'task-1',
            'planning_status': 'active',
            'completed_at': None,
            'task_type': 'work_unit',
        }
        plan = compile_filter(filter_spec)

        self.assertTrue(QueryEngine.match_plan(document, plan))
        self.assertTrue(CompiledQuery(plan).match(document))

    def test_compiled_query_matches_query_engine_for_representative_logical_filters(
        self,
    ):
        documents = [
            {},
            {'value': None, 'status': 'active', 'task_type': 'work_unit'},
            {'value': UNDEFINED, 'status': 'active', 'task_type': 'work_unit'},
            {'value': 1, 'status': 'active', 'task_type': 'content_update'},
            {'value': 1, 'status': 'retired', 'task_type': 'work_unit'},
            {'items': [{'exposed_at': 'ready'}], 'status': 'active'},
        ]
        filters = [
            {
                '$or': [
                    {'status': {'$exists': False}},
                    {'status': 'active'},
                ],
                'task_type': {'$in': ['work_unit', 'content_update']},
            },
            {
                '$and': [
                    {'$or': [{'value': 1}, {'value': None}]},
                    {'status': {'$ne': 'retired'}},
                ]
            },
            {
                '$nor': [
                    {'status': 'retired'},
                    {
                        'task_type': {
                            '$nin': ['work_unit', 'content_update', None]
                        }
                    },
                ],
                'value': {'$nin': [None]},
            },
            {
                '$or': [
                    {'items.exposed_at': {'$exists': True, '$ne': None}},
                    {'value': {'$gt': 0}},
                ],
                'status': {'$ne': 'retired'},
            },
        ]

        for filter_spec in filters:
            with self.subTest(filter_spec=filter_spec):
                plan = compile_filter(filter_spec)
                compiled = CompiledQuery(plan)

                self.assertEqual(
                    [
                        QueryEngine.match_plan(document, plan)
                        for document in documents
                    ],
                    [compiled.match(document) for document in documents],
                )

    def test_compiled_query_matches_query_engine_for_logical_operator_matrix(
        self,
    ):
        documents = [
            {},
            {'a': None},
            {'a': UNDEFINED},
            {'a': []},
            {'a': [UNDEFINED]},
            {'a': [None]},
            {'a': [1, 2]},
            {'a': 0},
            {'a': 1},
            {'a': 2},
            {'a': 'x'},
            {'a': {'b': 1}},
            {'a': [{'b': 1}, {'b': 2}]},
            {'a': [{'b': None}]},
            {'b': 'active'},
            {'a': 1, 'b': 'active', 'c': 'work_unit'},
            {'a': 2, 'b': 'retired', 'c': 'work_unit'},
        ]
        atoms = [
            {'a': {'$exists': True}},
            {'a': {'$exists': False}},
            {'a': None},
            {'a': {'$ne': None}},
            {'a': {'$in': [None, 1]}},
            {'a': {'$nin': [None, 1]}},
            {'a': {'$gt': 0}},
            {'a': {'$gte': 1}},
            {'a': {'$lt': 2}},
            {'a': {'$lte': 1}},
            {'a.b': {'$exists': True}},
            {'a.b': {'$exists': False}},
            {'a.b': 1},
            {'a.b': {'$ne': None}},
            {'b': 'active'},
            {'b': {'$ne': 'retired'}},
            {'c': {'$in': ['work_unit', 'content_update']}},
        ]
        filters = list(atoms)
        for left, right in product(atoms[:8], atoms[8:]):
            filters.append({'$and': [left, right]})
            filters.append({'$or': [left, right]})
            filters.append({'$nor': [left, right]})
            if not (set(left) & set(right)):
                filters.append({**left, **right})

        for filter_spec in filters:
            with self.subTest(filter_spec=filter_spec):
                plan = compile_filter(filter_spec)
                compiled = CompiledQuery(plan)

                self.assertEqual(
                    [
                        QueryEngine.match_plan(document, plan)
                        for document in documents
                    ],
                    [compiled.match(document) for document in documents],
                )

    def test_iter_filtered_documents_uses_compiled_query_without_changing_results(
        self,
    ):
        semantics = compile_find_semantics({'tags': {'$in': ['python']}})
        documents = [
            {'_id': 1, 'tags': ['python', 'mongo']},
            {'_id': 2, 'tags': ['sql']},
        ]

        self.assertIsNotNone(semantics.compiled_query)
        self.assertEqual(
            list(iter_filtered_documents(documents, semantics)),
            [{'_id': 1, 'tags': ['python', 'mongo']}],
        )

    def test_compiled_query_covers_inline_prefix_and_empty_boolean_nodes(self):
        self.assertEqual(
            CompiledQuery(AndCondition(())).get_inline_code(prefix='tmp'),
            'True',
        )
        self.assertEqual(
            CompiledQuery(OrCondition(())).get_inline_code(), 'False'
        )
        self.assertTrue(
            CompiledQuery(compile_filter({'score': {'$lte': 5}})).match(
                {'score': 5}
            )
        )
