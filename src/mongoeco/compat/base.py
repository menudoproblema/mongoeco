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
        '$unset',
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

DEFAULT_BSON_TYPE_ORDER = MappingProxyType(
    {
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
)

MONGODB_DIALECT_HOOK_NAMES = (
    'null_query_matches_undefined',
)

PYMONGO_PROFILE_HOOK_NAMES = (
    'supports_update_one_sort',
)


def _build_behavior_flags(instance: object, hook_names: Sequence[str]) -> MappingProxyType:
    return MappingProxyType(
        {
            hook_name: getattr(instance, hook_name)()
            for hook_name in hook_names
        }
    )


def _build_behavior_flag_catalog(
    catalog: MappingProxyType,
    hook_names: Sequence[str],
) -> MappingProxyType:
    return MappingProxyType(
        {
            key: _build_behavior_flags(instance, hook_names)
            for key, instance in catalog.items()
        }
    )


def _build_capability_catalog(catalog: MappingProxyType) -> MappingProxyType:
    return MappingProxyType(
        {
            key: instance.capabilities
            for key, instance in catalog.items()
        }
    )


@dataclass(frozen=True, slots=True)
class MongoDialect:
    """Describe la semántica observable objetivo del servidor MongoDB.

    Contrato:
    - La metadata (`key`, `server_version`, `label`) identifica el dialecto.
    - Los hooks de comportamiento registran deltas versionados explícitos.
    - Los hooks lógicos son la única puerta por la que el core debe consultar
      decisiones semánticas versionables.
    """

    key: str
    server_version: str
    label: str

    def null_query_matches_undefined(self) -> bool:
        """Controla si `null` en query iguala el BSON `undefined` legado."""
        return True

    def expression_truthy(self, value: object) -> bool:
        """Truthiness de expresiones agregadas y `$expr`."""
        if value is None:
            return False
        if isinstance(value, UndefinedType):
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        return True

    @property
    def query_field_operators(self) -> frozenset[str]:
        return SUPPORTED_QUERY_FIELD_OPERATORS

    @property
    def query_top_level_operators(self) -> frozenset[str]:
        return SUPPORTED_QUERY_TOP_LEVEL_OPERATORS

    @property
    def update_operators(self) -> frozenset[str]:
        return SUPPORTED_UPDATE_OPERATORS

    @property
    def aggregation_expression_operators(self) -> frozenset[str]:
        return SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS

    @property
    def aggregation_stages(self) -> frozenset[str]:
        return SUPPORTED_AGGREGATION_STAGES

    @property
    def group_accumulators(self) -> frozenset[str]:
        return SUPPORTED_GROUP_ACCUMULATORS

    @property
    def window_accumulators(self) -> frozenset[str]:
        return SUPPORTED_WINDOW_ACCUMULATORS

    @property
    def bson_type_order(self) -> MappingProxyType:
        return DEFAULT_BSON_TYPE_ORDER

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset()

    def supports_query_field_operator(self, name: str) -> bool:
        return name in self.query_field_operators

    def supports_query_top_level_operator(self, name: str) -> bool:
        return name in self.query_top_level_operators

    def supports_update_operator(self, name: str) -> bool:
        return name in self.update_operators

    def supports_aggregation_expression_operator(self, name: str) -> bool:
        return name in self.aggregation_expression_operators

    def supports_aggregation_stage(self, name: str) -> bool:
        return name in self.aggregation_stages

    def supports_group_accumulator(self, name: str) -> bool:
        return name in self.group_accumulators

    def supports_window_accumulator(self, name: str) -> bool:
        return name in self.window_accumulators

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
        """Igualdad observable BSON, sensible al orden de campos en documentos."""
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
        """Orden observable BSON para comparaciones y sorting."""
        type_left = self.bson_type_order.get(type(left), 100)
        type_right = self.bson_type_order.get(type(right), 100)
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

    def behavior_flags(self) -> MappingProxyType:
        """Expone metadata derivada directamente de los hooks del dialecto."""
        return _build_behavior_flags(self, MONGODB_DIALECT_HOOK_NAMES)


@dataclass(frozen=True, slots=True)
class MongoDialect70(MongoDialect):
    key: str = '7.0'
    server_version: str = '7.0'
    label: str = 'MongoDB 7.0'


@dataclass(frozen=True, slots=True)
class MongoDialect80(MongoDialect):
    key: str = '8.0'
    server_version: str = '8.0'
    label: str = 'MongoDB 8.0'

    def null_query_matches_undefined(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class PyMongoProfile:
    """Describe la superficie pública objetivo compatible con PyMongo.

    Contrato:
    - La metadata identifica la serie pública objetivo del driver.
    - Los hooks solo modelan diferencias visibles de API Python.
    - Nunca deben controlar semántica MQL o comparación BSON.
    """

    key: str
    driver_series: str
    label: str

    def supports_update_one_sort(self) -> bool:
        return False

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset()

    def behavior_flags(self) -> MappingProxyType:
        """Expone metadata derivada directamente de los hooks del perfil."""
        return _build_behavior_flags(self, PYMONGO_PROFILE_HOOK_NAMES)


@dataclass(frozen=True, slots=True)
class PyMongoProfile49(PyMongoProfile):
    key: str = '4.9'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.9'


@dataclass(frozen=True, slots=True)
class PyMongoProfile411(PyMongoProfile):
    key: str = '4.11'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.11'

    def supports_update_one_sort(self) -> bool:
        return True

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({'update_one.sort'})


@dataclass(frozen=True, slots=True)
class PyMongoProfile413(PyMongoProfile411):
    key: str = '4.13'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.13'


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

MONGODB_DIALECT_CAPABILITIES = _build_capability_catalog(MONGODB_DIALECTS)

MONGODB_DIALECT_BEHAVIOR_FLAGS = _build_behavior_flag_catalog(
    MONGODB_DIALECTS,
    MONGODB_DIALECT_HOOK_NAMES,
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

PYMONGO_PROFILE_CAPABILITIES = _build_capability_catalog(PYMONGO_PROFILES)

PYMONGO_PROFILE_BEHAVIOR_FLAGS = _build_behavior_flag_catalog(
    PYMONGO_PROFILES,
    PYMONGO_PROFILE_HOOK_NAMES,
)

SUPPORTED_MONGODB_MAJORS = frozenset(
    int(key.split('.', 1)[0])
    for key in MONGODB_DIALECTS
)

SUPPORTED_PYMONGO_MAJORS = frozenset(
    int(key.split('.', 1)[0])
    for key in PYMONGO_PROFILES
)
