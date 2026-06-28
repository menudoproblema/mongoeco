from collections.abc import Callable
from dataclasses import dataclass
import datetime
import re
from typing import Any


RealParityAction = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class RealParityCase:
    name: str
    seed_documents: list[dict[str, Any]]
    action: RealParityAction
    min_version: tuple[int, int] = (7, 0)

    def supports(self, target_version: tuple[int, int]) -> bool:
        return target_version >= self.min_version


def _write_error_code(action: Callable[[], Any]) -> tuple[str, int | None]:
    try:
        action()
    except Exception as exc:
        return ('error', getattr(exc, 'code', None))
    return ('ok', None)


def _write_error_flag(action: Callable[[], Any]) -> str:
    try:
        action()
    except Exception:
        return 'error'
    return 'ok'


_BOOLEAN_FILTER_MATRIX_DOCUMENTS = [
    {
        '_id': 'active-work-open',
        'planning_status': 'active',
        'completed_at': None,
        'task_type': 'work_unit',
        'score': 8,
        'items': [
            {
                'enrollment_task_id': 'soft',
                'exposed_at': datetime.datetime(2024, 1, 1, 12, 0),
            }
        ],
    },
    {
        '_id': 'missing-status-open',
        'completed_at': None,
        'task_type': 'content_update',
        'score': 3,
        'items': [{'enrollment_task_id': 'soft'}],
    },
    {
        '_id': 'retired-work-open',
        'planning_status': 'retired',
        'completed_at': None,
        'task_type': 'work_unit',
        'score': 9,
        'items': [
            {
                'enrollment_task_id': 'soft',
                'exposed_at': datetime.datetime(2024, 1, 2, 12, 0),
            }
        ],
    },
    {
        '_id': 'active-work-done',
        'planning_status': 'active',
        'completed_at': datetime.datetime(2024, 1, 3, 12, 0),
        'task_type': 'work_unit',
        'score': 5,
        'items': [
            {
                'enrollment_task_id': 'soft',
                'exposed_at': datetime.datetime(2024, 1, 4, 12, 0),
            }
        ],
    },
    {
        '_id': 'pending-quiz-open',
        'planning_status': 'pending',
        'completed_at': None,
        'task_type': 'quiz',
        'score': 7,
        'items': [],
    },
    {
        '_id': 'active-update-open',
        'planning_status': 'active',
        'completed_at': None,
        'task_type': 'content_update',
        'score': 6,
        'items': [{'enrollment_task_id': 'soft', 'exposed_at': None}],
    },
]


def _ids_for_find(collection: Any, filter_spec: dict[str, Any]) -> list[Any]:
    return [
        document['_id']
        for document in collection.find(filter_spec, sort=[('_id', 1)])
    ]


def _ids_for_aggregate(
    collection: Any,
    pipeline: list[dict[str, Any]],
) -> list[Any]:
    return [
        document['_id']
        for document in collection.aggregate(
            [*pipeline, {'$sort': {'_id': 1}}, {'$project': {'_id': 1}}]
        )
    ]


def _boolean_filter_matrix_action(
    collection: Any,
) -> dict[str, list[Any]]:
    or_with_siblings = {
        '$or': [
            {'planning_status': {'$exists': False}},
            {'planning_status': 'active'},
        ],
        'completed_at': None,
        'task_type': {'$in': ['work_unit', 'content_update']},
    }
    expr_with_or_and_siblings = {
        '$expr': {
            '$or': [
                {'$eq': ['$task_type', 'work_unit']},
                {'$gt': ['$score', 6]},
            ]
        },
        '$or': [
            {'planning_status': 'active'},
            {'planning_status': 'pending'},
        ],
        'completed_at': None,
    }
    return {
        'find_or_with_siblings': _ids_for_find(collection, or_with_siblings),
        'find_nested_and_or': _ids_for_find(
            collection,
            {
                '$and': [
                    {'completed_at': None},
                    {
                        '$or': [
                            {'task_type': 'work_unit'},
                            {'task_type': 'content_update'},
                        ]
                    },
                    {
                        '$or': [
                            {'planning_status': 'active'},
                            {'planning_status': {'$exists': False}},
                        ]
                    },
                ]
            },
        ),
        'find_nor_with_sibling': _ids_for_find(
            collection,
            {
                '$nor': [
                    {'planning_status': 'retired'},
                    {'completed_at': {'$ne': None}},
                ],
                'task_type': {'$in': ['work_unit', 'content_update']},
            },
        ),
        'find_dotted_array_or': _ids_for_find(
            collection,
            {
                'items.exposed_at': {'$exists': True, '$ne': None},
                '$or': [
                    {'task_type': 'work_unit'},
                    {'planning_status': 'pending'},
                ],
            },
        ),
        'find_expr_with_or_and_siblings': _ids_for_find(
            collection, expr_with_or_and_siblings
        ),
        'aggregate_match_or_with_siblings': _ids_for_aggregate(
            collection,
            [{'$match': or_with_siblings}],
        ),
        'aggregate_match_expr_with_or_and_siblings': _ids_for_aggregate(
            collection,
            [{'$match': expr_with_or_and_siblings}],
        ),
        'aggregate_match_dotted_array_or': _ids_for_aggregate(
            collection,
            [
                {
                    '$match': {
                        'items.exposed_at': {'$exists': True, '$ne': None},
                        '$or': [
                            {'task_type': 'work_unit'},
                            {'planning_status': 'pending'},
                        ],
                    }
                }
            ],
        ),
    }


REAL_PARITY_CASES: tuple[RealParityCase, ...] = (
    RealParityCase(
        name='boolean_filter_matrix',
        seed_documents=_BOOLEAN_FILTER_MATRIX_DOCUMENTS,
        action=_boolean_filter_matrix_action,
    ),
    RealParityCase(
        name='find_expr_compare_fields',
        seed_documents=[
            {'_id': '1', 'spent': 12, 'budget': 10},
            {'_id': '2', 'spent': 8, 'budget': 10},
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {'$expr': {'$gt': ['$spent', '$budget']}}, sort=[('_id', 1)]
            )
        ],
    ),
    RealParityCase(
        name='find_subdocument_order_sensitive_equality',
        seed_documents=[
            {'_id': 'ordered', 'value': {'a': 1, 'b': 2}},
            {'_id': 'reordered', 'value': {'b': 2, 'a': 1}},
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {'value': {'a': 1, 'b': 2}}, sort=[('_id', 1)]
            )
        ],
    ),
    RealParityCase(
        name='find_all_with_multiple_elem_match',
        seed_documents=[
            {
                '_id': '1',
                'items': [
                    {'kind': 'a', 'qty': 1},
                    {'kind': 'b', 'qty': 2},
                    {'kind': 'b', 'qty': 5},
                ],
            },
            {
                '_id': '2',
                'items': [{'kind': 'a', 'qty': 1}, {'kind': 'b', 'qty': 2}],
            },
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {
                    'items': {
                        '$all': [
                            {'$elemMatch': {'kind': 'a'}},
                            {'$elemMatch': {'kind': 'b', 'qty': {'$gte': 5}}},
                        ]
                    }
                },
                sort=[('_id', 1)],
            )
        ],
    ),
    RealParityCase(
        name='find_implicit_regex_literal',
        seed_documents=[
            {'_id': '1', 'name': 'MongoDB'},
            {'_id': '2', 'name': 'Postgres'},
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {'name': re.compile('^mongo', re.IGNORECASE)},
                sort=[('_id', 1)],
            )
        ],
    ),
    RealParityCase(
        name='find_in_with_regex_literals',
        seed_documents=[
            {'_id': '1', 'tags': ['beta', 'stable']},
            {'_id': '2', 'tags': ['alpha', 'stable']},
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {'tags': {'$in': [re.compile('^be'), re.compile('^zz')]}},
                sort=[('_id', 1)],
            )
        ],
    ),
    RealParityCase(
        name='update_add_to_set_document_order_sensitive',
        seed_documents=[{'_id': '1', 'items': [{'kind': 'a', 'qty': 1}]}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({'_id': '1'}),
            )
        )(
            collection.update_one(
                {'_id': '1'},
                {'$addToSet': {'items': {'qty': 1, 'kind': 'a'}}},
            )
        ),
    ),
    RealParityCase(
        name='update_set_on_insert_id_matches_filter',
        seed_documents=[],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                result.upserted_id,
                collection.find_one({'_id': 'seed'}),
            )
        )(
            collection.update_one(
                {'_id': 'seed'},
                {'$setOnInsert': {'_id': 'seed', 'state': 'new'}},
                upsert=True,
            )
        ),
    ),
    RealParityCase(
        name='update_pipeline_project_preserves_id',
        seed_documents=[{'_id': '1', 'name': 'Ada', 'legacy': True}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({'_id': '1'}),
            )
        )(
            collection.update_one(
                {'_id': '1'},
                [{'$project': {'_id': 0, 'name': 1}}],
            )
        ),
    ),
    RealParityCase(
        name='update_pipeline_rejects_id_change',
        seed_documents=[{'_id': '1', 'name': 'Ada'}],
        action=lambda collection: _write_error_code(
            lambda: collection.update_one(
                {'_id': '1'},
                [{'$set': {'_id': '2'}}],
            )
        ),
    ),
    RealParityCase(
        name='insert_rejects_root_array_id',
        seed_documents=[],
        action=lambda collection: _write_error_flag(
            lambda: collection.insert_one({'_id': [1]})
        ),
    ),
    RealParityCase(
        name='find_expr_truthiness_array',
        seed_documents=[
            {'_id': 'array', 'flag': []},
            {'_id': 'zero', 'flag': 0},
            {'_id': 'false', 'flag': False},
            {'_id': 'string', 'flag': ''},
        ],
        action=lambda collection: [
            document['_id']
            for document in collection.find(
                {'$expr': '$flag'}, sort=[('_id', 1)]
            )
        ],
    ),
    RealParityCase(
        name='aggregate_project_array_traversal',
        seed_documents=[
            {'_id': '1', 'items': [{'kind': 'a'}, {'kind': 'b'}]},
            {'_id': '2', 'items': [{'kind': 'c'}]},
        ],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {'$project': {'_id': 1, 'kinds': '$items.kind'}},
                    {'$sort': {'_id': 1}},
                ]
            )
        ),
    ),
    RealParityCase(
        name='aggregate_get_field_literal_name',
        seed_documents=[{'_id': '1', 'a.b.c': 1, '$price': 2, 'x..y': 3}],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {
                        '$project': {
                            '_id': 0,
                            'dotted': {'$getField': {'field': 'a.b.c'}},
                            'dollar': {
                                '$getField': {'field': {'$literal': '$price'}}
                            },
                            'double_dot': {'$getField': {'field': 'x..y'}},
                        }
                    }
                ]
            )
        ),
    ),
)


def get_real_parity_case(name: str) -> RealParityCase:
    for case in REAL_PARITY_CASES:
        if case.name == name:
            return case
    raise KeyError(name)
