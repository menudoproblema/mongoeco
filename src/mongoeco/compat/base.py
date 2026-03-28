import datetime
import decimal
import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, ClassVar
import uuid

from mongoeco.compat.catalog import (
    DEFAULT_BSON_TYPE_ORDER,
    MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED,
    MONGODB_DIALECT_ALIASES,
    MONGODB_DIALECT_CATALOG,
    MongoBehaviorPolicySpec,
    MONGODB_DIALECT_HOOK_NAMES,
    PYMONGO_CAP_UPDATE_ONE_SORT,
    PYMONGO_PROFILE_ALIASES,
    PYMONGO_PROFILE_CATALOG,
    PYMONGO_PROFILE_HOOK_NAMES,
    SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES,
    SUPPORTED_GROUP_ACCUMULATORS,
    SUPPORTED_MONGODB_MAJORS,
    SUPPORTED_PYMONGO_MAJORS,
    SUPPORTED_QUERY_FIELD_OPERATORS,
    SUPPORTED_QUERY_TOP_LEVEL_OPERATORS,
    SUPPORTED_UPDATE_OPERATORS,
    SUPPORTED_WINDOW_ACCUMULATORS,
)
from mongoeco.core.bson_scalars import compare_bson_numeric, unwrap_bson_numeric, wrap_bson_numeric
from mongoeco.types import ObjectId, UndefinedType


@dataclass(frozen=True, slots=True)
class MongoBehaviorPolicy:
    null_query_matches_undefined_fn: Callable[[], bool]
    expression_truthy_fn: Callable[[object], bool]
    projection_flag_fn: Callable[[object], int | None]
    sort_update_path_items_fn: Callable[[dict[str, object]], list[tuple[str, object]]]
    values_equal_fn: Callable[[Any, Any], bool]
    compare_values_fn: Callable[[Any, Any], int]

    def null_query_matches_undefined(self) -> bool:
        return self.null_query_matches_undefined_fn()

    def expression_truthy(self, value: object) -> bool:
        return self.expression_truthy_fn(value)

    def projection_flag(self, value: object) -> int | None:
        return self.projection_flag_fn(value)

    def sort_update_path_items(
        self,
        params: dict[str, object],
    ) -> list[tuple[str, object]]:
        return self.sort_update_path_items_fn(params)

    def values_equal(self, left: Any, right: Any) -> bool:
        return self.values_equal_fn(left, right)

    def compare_values(self, left: Any, right: Any) -> int:
        return self.compare_values_fn(left, right)


def _expression_truthy_default(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, UndefinedType):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    return True


def _projection_flag_default(value: object) -> int | None:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int) and value in (0, 1):
        return value
    return None


def _sort_update_path_items_default(params: dict[str, object]) -> list[tuple[str, object]]:
    def sort_key(item: tuple[str, object]) -> tuple[int, int | str]:
        path, _ = item
        if not isinstance(path, str):
            raise TypeError('update field names must be strings')
        if path.isdigit():
            return (0, int(path))
        return (1, path)

    return sorted(params.items(), key=sort_key)


def _compare_values_default(
    left: Any,
    right: Any,
    bson_type_order: MappingProxyType,
) -> int:
    wrapped_left = wrap_bson_numeric(left)
    wrapped_right = wrap_bson_numeric(right)
    if wrapped_left is not None and wrapped_right is not None:
        return compare_bson_numeric(wrapped_left, wrapped_right)

    normalized_left = unwrap_bson_numeric(left)
    normalized_right = unwrap_bson_numeric(right)

    type_left = bson_type_order.get(type(normalized_left), 100)
    type_right = bson_type_order.get(type(normalized_right), 100)
    if type_left != type_right:
        return -1 if type_left < type_right else 1

    if isinstance(normalized_left, dict) and isinstance(normalized_right, dict):
        left_items = list(normalized_left.items())
        right_items = list(normalized_right.items())
        for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items):
            if left_key != right_key:
                return -1 if left_key < right_key else 1
            value_comparison = _compare_values_default(left_value, right_value, bson_type_order)
            if value_comparison != 0:
                return value_comparison
        if len(left_items) == len(right_items):
            return 0
        return -1 if len(left_items) < len(right_items) else 1

    if isinstance(normalized_left, list) and isinstance(normalized_right, list):
        for left_value, right_value in zip(normalized_left, normalized_right):
            comparison = _compare_values_default(left_value, right_value, bson_type_order)
            if comparison != 0:
                return comparison
        if len(normalized_left) == len(normalized_right):
            return 0
        return -1 if len(normalized_left) < len(normalized_right) else 1

    if isinstance(normalized_left, float) and math.isnan(normalized_left):
        return 0 if isinstance(normalized_right, float) and math.isnan(normalized_right) else -1
    if isinstance(normalized_right, float) and math.isnan(normalized_right):
        return 1

    if normalized_left == normalized_right:
        return 0

    try:
        if normalized_left < normalized_right:
            return -1
        if normalized_left > normalized_right:
            return 1
        return 0
    except TypeError:
        left_repr = str(normalized_left)
        right_repr = str(normalized_right)
        if left_repr == right_repr:
            return 0
        return -1 if left_repr < right_repr else 1


def _values_equal_default(
    left: Any,
    right: Any,
    bson_type_order: MappingProxyType,
) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        left_items = list(left.items())
        right_items = list(right.items())
        if len(left_items) != len(right_items):
            return False
        return all(
            left_key == right_key and _values_equal_default(left_value, right_value, bson_type_order)
            for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items)
        )
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(
            _values_equal_default(left_item, right_item, bson_type_order)
            for left_item, right_item in zip(left, right)
        )
    return _compare_values_default(left, right, bson_type_order) == 0


def build_mongo_behavior_policy(
    spec: MongoBehaviorPolicySpec,
    *,
    bson_type_order: MappingProxyType = DEFAULT_BSON_TYPE_ORDER,
    capabilities: frozenset[str] = frozenset(),
) -> MongoBehaviorPolicy:
    if spec.expression_truthiness != "mongo-default":
        raise ValueError(f"unsupported expression truthiness strategy: {spec.expression_truthiness}")
    if spec.projection_flag_mode != "bool-or-binary-int":
        raise ValueError(f"unsupported projection flag strategy: {spec.projection_flag_mode}")
    if spec.update_path_sort_mode != "numeric-then-lex":
        raise ValueError(f"unsupported update path sort strategy: {spec.update_path_sort_mode}")
    if spec.equality_mode != "bson-structural":
        raise ValueError(f"unsupported equality strategy: {spec.equality_mode}")
    if spec.comparison_mode != "bson-total-order":
        raise ValueError(f"unsupported comparison strategy: {spec.comparison_mode}")

    return MongoBehaviorPolicy(
        null_query_matches_undefined_fn=lambda: (
            MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED in capabilities
            if capabilities
            else spec.null_query_matches_undefined
        ),
        expression_truthy_fn=_expression_truthy_default,
        projection_flag_fn=_projection_flag_default,
        sort_update_path_items_fn=_sort_update_path_items_default,
        values_equal_fn=lambda left, right: _values_equal_default(left, right, bson_type_order),
        compare_values_fn=lambda left, right: _compare_values_default(left, right, bson_type_order),
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
    catalog_behavior_flags: MappingProxyType = MappingProxyType({})
    catalog_policy_spec: MongoBehaviorPolicySpec = MongoBehaviorPolicySpec()
    catalog_capabilities: frozenset[str] = frozenset()
    _base_policy_cache: MongoBehaviorPolicy | None = field(default=None, init=False, repr=False, compare=False)
    _policy_cache: MongoBehaviorPolicy | None = field(default=None, init=False, repr=False, compare=False)
    _SEMANTIC_OVERRIDE_NAMES: ClassVar[tuple[str, ...]] = (
        "null_query_matches_undefined",
        "expression_truthy",
        "projection_flag",
        "sort_update_path_items",
        "values_equal",
        "compare_values",
    )

    def behavior_flag(self, name: str, default: bool | None = None) -> bool | None:
        if name not in MONGODB_DIALECT_HOOK_NAMES:
            return default
        value = self.catalog_behavior_flags.get(name)
        return value if isinstance(value, bool) else default

    def has_capability(self, name: str) -> bool:
        return name in self.capabilities

    def supports(self, capability: str) -> bool:
        return self.has_capability(capability)

    @property
    def policy_spec(self) -> MongoBehaviorPolicySpec:
        return self.catalog_policy_spec

    @property
    def base_policy(self) -> MongoBehaviorPolicy:
        cached = self._base_policy_cache
        if cached is not None:
            return cached
        dialect_type = type(self)
        uses_declared_capabilities = getattr(dialect_type, "policy_spec") is getattr(MongoDialect, "policy_spec")
        policy = build_mongo_behavior_policy(
            self.policy_spec,
            bson_type_order=self.bson_type_order,
            capabilities=self.capabilities if uses_declared_capabilities else frozenset(),
        )
        object.__setattr__(self, "_base_policy_cache", policy)
        return policy

    def _has_semantic_overrides(self) -> bool:
        dialect_type = type(self)
        return any(
            getattr(dialect_type, name) is not getattr(MongoDialect, name)
            for name in self._SEMANTIC_OVERRIDE_NAMES
        )

    @property
    def policy(self) -> MongoBehaviorPolicy:
        cached = self._policy_cache
        if cached is not None:
            return cached
        if not self._has_semantic_overrides():
            object.__setattr__(self, "_policy_cache", self.base_policy)
            return self.base_policy
        policy = MongoBehaviorPolicy(
            null_query_matches_undefined_fn=self.null_query_matches_undefined,
            expression_truthy_fn=self.expression_truthy,
            projection_flag_fn=self.projection_flag,
            sort_update_path_items_fn=self.sort_update_path_items,
            values_equal_fn=self.values_equal,
            compare_values_fn=self.compare_values,
        )
        object.__setattr__(self, "_policy_cache", policy)
        return policy

    def null_query_matches_undefined(self) -> bool:
        return self.base_policy.null_query_matches_undefined()

    def expression_truthy(self, value: object) -> bool:
        return self.base_policy.expression_truthy(value)

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
        return self.catalog_capabilities

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
        return self.base_policy.projection_flag(value)

    def sort_update_path_items(
        self,
        params: dict[str, object],
    ) -> list[tuple[str, object]]:
        return self.base_policy.sort_update_path_items(params)

    def values_equal(self, left: Any, right: Any) -> bool:
        if self.policy_spec.equality_mode != "bson-structural":
            raise ValueError(f"unsupported equality strategy: {self.policy_spec.equality_mode}")
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
        if self.policy_spec.comparison_mode != "bson-total-order":
            raise ValueError(f"unsupported comparison strategy: {self.policy_spec.comparison_mode}")
        wrapped_left = wrap_bson_numeric(left)
        wrapped_right = wrap_bson_numeric(right)
        if wrapped_left is not None and wrapped_right is not None:
            return compare_bson_numeric(wrapped_left, wrapped_right)

        normalized_left = unwrap_bson_numeric(left)
        normalized_right = unwrap_bson_numeric(right)

        type_left = self.bson_type_order.get(type(normalized_left), 100)
        type_right = self.bson_type_order.get(type(normalized_right), 100)
        if type_left != type_right:
            return -1 if type_left < type_right else 1

        if isinstance(normalized_left, dict) and isinstance(normalized_right, dict):
            left_items = list(normalized_left.items())
            right_items = list(normalized_right.items())
            for (left_key, left_value), (right_key, right_value) in zip(left_items, right_items):
                if left_key != right_key:
                    return -1 if left_key < right_key else 1
                value_comparison = self.compare_values(left_value, right_value)
                if value_comparison != 0:
                    return value_comparison
            if len(left_items) == len(right_items):
                return 0
            return -1 if len(left_items) < len(right_items) else 1

        if isinstance(normalized_left, list) and isinstance(normalized_right, list):
            for left_value, right_value in zip(normalized_left, normalized_right):
                comparison = self.compare_values(left_value, right_value)
                if comparison != 0:
                    return comparison
            if len(normalized_left) == len(normalized_right):
                return 0
            return -1 if len(normalized_left) < len(normalized_right) else 1

        if isinstance(normalized_left, float) and math.isnan(normalized_left):
            return 0 if isinstance(normalized_right, float) and math.isnan(normalized_right) else -1
        if isinstance(normalized_right, float) and math.isnan(normalized_right):
            return 1

        if normalized_left == normalized_right:
            return 0

        try:
            if normalized_left < normalized_right:
                return -1
            if normalized_left > normalized_right:
                return 1
            return 0
        except TypeError:
            left_repr = str(normalized_left)
            right_repr = str(normalized_right)
            if left_repr == right_repr:
                return 0
            return -1 if left_repr < right_repr else 1

    def behavior_flags(self) -> MappingProxyType:
        """Expone metadata declarativa derivada del catálogo del dialecto."""
        return self.catalog_behavior_flags


@dataclass(frozen=True, slots=True)
class MongoDialect70(MongoDialect):
    key: str = '7.0'
    server_version: str = '7.0'
    label: str = 'MongoDB 7.0'
    catalog_behavior_flags: MappingProxyType = MONGODB_DIALECT_CATALOG['7.0'].behavior_flags
    catalog_policy_spec: MongoBehaviorPolicySpec = MONGODB_DIALECT_CATALOG['7.0'].policy_spec or MongoBehaviorPolicySpec()
    catalog_capabilities: frozenset[str] = MONGODB_DIALECT_CATALOG['7.0'].capabilities


@dataclass(frozen=True, slots=True)
class MongoDialect80(MongoDialect):
    key: str = '8.0'
    server_version: str = '8.0'
    label: str = 'MongoDB 8.0'
    catalog_behavior_flags: MappingProxyType = MONGODB_DIALECT_CATALOG['8.0'].behavior_flags
    catalog_policy_spec: MongoBehaviorPolicySpec = MONGODB_DIALECT_CATALOG['8.0'].policy_spec or MongoBehaviorPolicySpec()
    catalog_capabilities: frozenset[str] = MONGODB_DIALECT_CATALOG['8.0'].capabilities


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
    catalog_behavior_flags: MappingProxyType = MappingProxyType({})
    catalog_capabilities: frozenset[str] = frozenset()

    def behavior_flag(self, name: str, default: bool | None = None) -> bool | None:
        if name not in PYMONGO_PROFILE_HOOK_NAMES:
            return default
        value = self.catalog_behavior_flags.get(name)
        return value if isinstance(value, bool) else default

    def has_capability(self, name: str) -> bool:
        return name in self.capabilities

    def supports(self, capability: str) -> bool:
        return self.has_capability(capability)

    def supports_update_one_sort(self) -> bool:
        if self.supports(PYMONGO_CAP_UPDATE_ONE_SORT):
            return True
        catalog_value = self.behavior_flag("supports_update_one_sort")
        return bool(catalog_value) if catalog_value is not None else False

    @property
    def capabilities(self) -> frozenset[str]:
        return self.catalog_capabilities

    def behavior_flags(self) -> MappingProxyType:
        """Expone metadata declarativa derivada del catálogo del perfil."""
        return self.catalog_behavior_flags


@dataclass(frozen=True, slots=True)
class PyMongoProfile49(PyMongoProfile):
    key: str = '4.9'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.9'
    catalog_behavior_flags: MappingProxyType = PYMONGO_PROFILE_CATALOG['4.9'].behavior_flags
    catalog_capabilities: frozenset[str] = PYMONGO_PROFILE_CATALOG['4.9'].capabilities


@dataclass(frozen=True, slots=True)
class PyMongoProfile411(PyMongoProfile):
    key: str = '4.11'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.11'
    catalog_behavior_flags: MappingProxyType = PYMONGO_PROFILE_CATALOG['4.11'].behavior_flags
    catalog_capabilities: frozenset[str] = PYMONGO_PROFILE_CATALOG['4.11'].capabilities


@dataclass(frozen=True, slots=True)
class PyMongoProfile413(PyMongoProfile411):
    key: str = '4.13'
    driver_series: str = '4.x'
    label: str = 'PyMongo 4.13'
    catalog_behavior_flags: MappingProxyType = PYMONGO_PROFILE_CATALOG['4.13'].behavior_flags
    catalog_capabilities: frozenset[str] = PYMONGO_PROFILE_CATALOG['4.13'].capabilities


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

MONGODB_DIALECT_CAPABILITIES = MappingProxyType(
    {key: instance.capabilities for key, instance in MONGODB_DIALECTS.items()}
)

MONGODB_DIALECT_BEHAVIOR_FLAGS = MappingProxyType(
    {key: instance.behavior_flags() for key, instance in MONGODB_DIALECTS.items()}
)

MONGODB_DIALECT_POLICY_SPECS = MappingProxyType(
    {key: instance.policy_spec for key, instance in MONGODB_DIALECTS.items()}
)

PYMONGO_PROFILES = MappingProxyType(
    {
        PYMONGO_PROFILE_49.key: PYMONGO_PROFILE_49,
        PYMONGO_PROFILE_411.key: PYMONGO_PROFILE_411,
        PYMONGO_PROFILE_413.key: PYMONGO_PROFILE_413,
    }
)

PYMONGO_PROFILE_CAPABILITIES = MappingProxyType(
    {key: instance.capabilities for key, instance in PYMONGO_PROFILES.items()}
)

PYMONGO_PROFILE_BEHAVIOR_FLAGS = MappingProxyType(
    {key: instance.behavior_flags() for key, instance in PYMONGO_PROFILES.items()}
)

assert tuple(MONGODB_DIALECTS) == tuple(MONGODB_DIALECT_CATALOG)
assert tuple(PYMONGO_PROFILES) == tuple(PYMONGO_PROFILE_CATALOG)
