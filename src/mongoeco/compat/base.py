import datetime
import math
from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
import uuid

from mongoeco.types import ObjectId, UndefinedType

SUPPORTED_QUERY_FIELD_OPERATORS = frozenset(
    {
        '$eq',
        '$ne',
        '$gt',
        '$gte',
        '$lt',
        '$lte',
        '$in',
        '$nin',
        '$all',
        '$size',
        '$mod',
        '$regex',
        '$options',
        '$not',
        '$elemMatch',
        '$exists',
    }
)

SUPPORTED_QUERY_TOP_LEVEL_OPERATORS = frozenset({'$and', '$or', '$nor', '$expr'})

SUPPORTED_UPDATE_OPERATORS = frozenset(
    {'$set', '$unset', '$inc', '$push', '$addToSet', '$pull', '$pop'}
)

SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS = frozenset(
    {
        '$literal',
        '$eq',
        '$ne',
        '$gt',
        '$gte',
        '$lt',
        '$lte',
        '$and',
        '$or',
        '$in',
        '$ifNull',
        '$cond',
        '$add',
        '$multiply',
        '$subtract',
        '$divide',
        '$mod',
        '$floor',
        '$ceil',
        '$size',
        '$arrayElemAt',
        '$toString',
        '$let',
        '$first',
        '$concatArrays',
        '$setUnion',
        '$map',
        '$filter',
        '$reduce',
        '$arrayToObject',
        '$indexOfArray',
        '$sortArray',
        '$dateTrunc',
        '$mergeObjects',
        '$getField',
    }
)

SUPPORTED_AGGREGATION_STAGES = frozenset(
    {
        '$match',
        '$project',
        '$sort',
        '$skip',
        '$limit',
        '$addFields',
        '$set',
        '$unwind',
        '$group',
        '$bucket',
        '$bucketAuto',
        '$lookup',
        '$replaceRoot',
        '$replaceWith',
        '$facet',
        '$count',
        '$sortByCount',
        '$setWindowFields',
    }
)

SUPPORTED_GROUP_ACCUMULATORS = frozenset({'$sum', '$min', '$max', '$first', '$avg', '$push'})

SUPPORTED_WINDOW_ACCUMULATORS = frozenset({'$sum', '$min', '$max', '$avg', '$push', '$first'})


@dataclass(frozen=True, slots=True)
class MongoDialect:
    """Describe la semántica observable objetivo del servidor MongoDB."""

    key: str
    server_version: str
    label: str

    def null_query_matches_undefined(self) -> bool:
        return True

    def expression_truthy(self, value: object) -> bool:
        if value is None:
            return False
        if isinstance(value, UndefinedType):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        return True

    def supports_query_field_operator(self, name: str) -> bool:
        return name in SUPPORTED_QUERY_FIELD_OPERATORS

    def supports_query_top_level_operator(self, name: str) -> bool:
        return name in SUPPORTED_QUERY_TOP_LEVEL_OPERATORS

    def supports_update_operator(self, name: str) -> bool:
        return name in SUPPORTED_UPDATE_OPERATORS

    def supports_aggregation_expression_operator(self, name: str) -> bool:
        return name in SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS

    def supports_aggregation_stage(self, name: str) -> bool:
        return name in SUPPORTED_AGGREGATION_STAGES

    def supports_group_accumulator(self, name: str) -> bool:
        return name in SUPPORTED_GROUP_ACCUMULATORS

    def supports_window_accumulator(self, name: str) -> bool:
        return name in SUPPORTED_WINDOW_ACCUMULATORS

    def projection_flag(self, value: object) -> int | None:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int) and value in (0, 1):
            return value
        return None

    def sort_update_path_items(
        self,
        params: dict[str, object],
    ) -> list[tuple[str, object]]:
        def sort_key(item: tuple[str, object]) -> tuple[int, int | str]:
            path, _ = item
            if not isinstance(path, str):
                raise TypeError('update field names must be strings')
            if path.isdigit():
                return (0, int(path))
            return (1, path)

        return sorted(params.items(), key=sort_key)

    def values_equal(self, left: Any, right: Any) -> bool:
        if isinstance(left, dict) and isinstance(right, dict):
            left_items = list(left.items())
            right_items = list(right.items())
            if len(left_items) != len(right_items):
                return False
            return all(
                left_key == right_key and self.values_equal(left_value, right_value)
                for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items)
            )
        if isinstance(left, list) and isinstance(right, list):
            if len(left) != len(right):
                return False
            return all(self.values_equal(left_item, right_item) for left_item, right_item in zip(left, right))
        return self.compare_values(left, right) == 0

    def compare_values(self, left: Any, right: Any) -> int:
        type_order: dict[type, int] = {
            type(None): 1,
            UndefinedType: 1,
            int: 2,
            float: 2,
            str: 3,
            dict: 4,
            list: 5,
            bytes: 6,
            uuid.UUID: 6,
            ObjectId: 7,
            bool: 8,
            datetime.datetime: 9,
        }

        type_left = type_order.get(type(left), 100)
        type_right = type_order.get(type(right), 100)
        if type_left != type_right:
            return -1 if type_left < type_right else 1

        if isinstance(left, dict) and isinstance(right, dict):
            left_items = list(left.items())
            right_items = list(right.items())
            for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items):
                if left_key != right_key:
                    return -1 if left_key < right_key else 1
                value_comparison = self.compare_values(left_value, right_value)
                if value_comparison != 0:
                    return value_comparison
            if len(left_items) == len(right_items):
                return 0
            return -1 if len(left_items) < len(right_items) else 1

        if isinstance(left, list) and isinstance(right, list):
            for left_value, right_value in zip(left, right):
                comparison = self.compare_values(left_value, right_value)
                if comparison != 0:
                    return comparison
            if len(left) == len(right):
                return 0
            return -1 if len(left) < len(right) else 1

        if isinstance(left, float) and math.isnan(left):
            return 0 if isinstance(right, float) and math.isnan(right) else -1
        if isinstance(right, float) and math.isnan(right):
            return 1

        if left == right:
            return 0

        try:
            if left < right:
                return -1
            if left > right:
                return 1
            return 0
        except TypeError:
            left_repr = str(left)
            right_repr = str(right)
            if left_repr == right_repr:
                return 0
            return -1 if left_repr < right_repr else 1


class MongoDialect70(MongoDialect):
    def __init__(self):
        super().__init__(key='7.0', server_version='7.0', label='MongoDB 7.0')


class MongoDialect80(MongoDialect):
    def __init__(self):
        super().__init__(key='8.0', server_version='8.0', label='MongoDB 8.0')

    def null_query_matches_undefined(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class PyMongoProfile:
    """Describe la superficie pública objetivo compatible con PyMongo."""

    key: str
    driver_series: str
    label: str

    def supports_update_one_sort(self) -> bool:
        return False


class PyMongoProfile49(PyMongoProfile):
    def __init__(self):
        super().__init__(key='4.9', driver_series='4.x', label='PyMongo 4.9')


class PyMongoProfile411(PyMongoProfile):
    def __init__(self):
        super().__init__(key='4.11', driver_series='4.x', label='PyMongo 4.11')

    def supports_update_one_sort(self) -> bool:
        return True


class PyMongoProfile413(PyMongoProfile411):
    def __init__(self):
        super().__init__()
        object.__setattr__(self, 'key', '4.13')
        object.__setattr__(self, 'label', 'PyMongo 4.13')


MONGODB_DIALECT_70 = MongoDialect70()
MONGODB_DIALECT_80 = MongoDialect80()

PYMONGO_PROFILE_49 = PyMongoProfile49()
PYMONGO_PROFILE_411 = PyMongoProfile411()
PYMONGO_PROFILE_413 = PyMongoProfile413()

MONGODB_DIALECTS = MappingProxyType(
    {
        MONGODB_DIALECT_70.key: MONGODB_DIALECT_70,
        MONGODB_DIALECT_80.key: MONGODB_DIALECT_80,
    }
)

MONGODB_DIALECT_ALIASES = MappingProxyType(
    {
        '7': MONGODB_DIALECT_70.key,
        '7.0': MONGODB_DIALECT_70.key,
        '8': MONGODB_DIALECT_80.key,
        '8.0': MONGODB_DIALECT_80.key,
    }
)

MONGODB_DIALECT_CAPABILITIES = MappingProxyType(
    {
        MONGODB_DIALECT_70.key: frozenset(),
        MONGODB_DIALECT_80.key: frozenset(),
    }
)

MONGODB_DIALECT_BEHAVIOR_FLAGS = MappingProxyType(
    {
        MONGODB_DIALECT_70.key: MappingProxyType(
            {
                'null_query_matches_undefined': MONGODB_DIALECT_70.null_query_matches_undefined(),
            }
        ),
        MONGODB_DIALECT_80.key: MappingProxyType(
            {
                'null_query_matches_undefined': MONGODB_DIALECT_80.null_query_matches_undefined(),
            }
        ),
    }
)

PYMONGO_PROFILES = MappingProxyType(
    {
        PYMONGO_PROFILE_49.key: PYMONGO_PROFILE_49,
        PYMONGO_PROFILE_411.key: PYMONGO_PROFILE_411,
        PYMONGO_PROFILE_413.key: PYMONGO_PROFILE_413,
    }
)

PYMONGO_PROFILE_ALIASES = MappingProxyType(
    {
        '4': PYMONGO_PROFILE_49.key,
        '4.9': PYMONGO_PROFILE_49.key,
        '4.11': PYMONGO_PROFILE_411.key,
        '4.13': PYMONGO_PROFILE_413.key,
    }
)

PYMONGO_PROFILE_CAPABILITIES = MappingProxyType(
    {
        PYMONGO_PROFILE_49.key: frozenset(),
        PYMONGO_PROFILE_411.key: frozenset({'update_one.sort'}),
        PYMONGO_PROFILE_413.key: frozenset({'update_one.sort'}),
    }
)

PYMONGO_PROFILE_BEHAVIOR_FLAGS = MappingProxyType(
    {
        PYMONGO_PROFILE_49.key: MappingProxyType(
            {
                'supports_update_one_sort': PYMONGO_PROFILE_49.supports_update_one_sort(),
            }
        ),
        PYMONGO_PROFILE_411.key: MappingProxyType(
            {
                'supports_update_one_sort': PYMONGO_PROFILE_411.supports_update_one_sort(),
            }
        ),
        PYMONGO_PROFILE_413.key: MappingProxyType(
            {
                'supports_update_one_sort': PYMONGO_PROFILE_413.supports_update_one_sort(),
            }
        ),
    }
)
