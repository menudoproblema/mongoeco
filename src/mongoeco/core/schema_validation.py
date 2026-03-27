from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import datetime
import decimal
import math
import re
import uuid
from typing import Any, Mapping

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    bson_numeric_alias,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, ObjectId, UndefinedType


_JSON_TYPE_MAP: dict[str, tuple[type[Any], ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
    "bool": (bool,),
    "boolean": (bool,),
    "null": (type(None),),
}

_BSON_TYPE_ALIASES = frozenset(
    {
        "double",
        "string",
        "object",
        "array",
        "binData",
        "objectId",
        "bool",
        "date",
        "null",
        "regex",
        "int",
        "long",
        "decimal",
        "number",
    }
)


def _json_path(path: tuple[str, ...]) -> str:
    if not path:
        return "$"
    return "$." + ".".join(path)


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    path: tuple[str, ...]
    message: str

    def render(self) -> str:
        return f"{_json_path(self.path)}: {self.message}"


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    valid: bool
    issues: tuple[SchemaValidationIssue, ...] = ()

    @property
    def first_message(self) -> str:
        if not self.issues:
            return "document passed validation"
        return self.issues[0].render()


@dataclass(frozen=True, slots=True)
class CompiledJsonSchema:
    raw_schema: Document

    def __post_init__(self) -> None:
        self._validate_schema_definition(self.raw_schema)

    def validate(
        self,
        document: object,
        *,
        path: tuple[str, ...] = (),
    ) -> SchemaValidationResult:
        issues = tuple(self._validate_schema(self.raw_schema, document, path))
        return SchemaValidationResult(valid=not issues, issues=issues)

    def _validate_schema(
        self,
        schema: Mapping[str, object],
        value: object,
        path: tuple[str, ...],
    ) -> list[SchemaValidationIssue]:
        issues: list[SchemaValidationIssue] = []

        allowed_types: list[str] = []
        for key in ("bsonType", "type"):
            declared = schema.get(key)
            if declared is None:
                continue
            allowed_types.extend(self._normalize_type_names(declared, field_name=key))
        if allowed_types and not self._matches_any_type(value, allowed_types):
            rendered = ", ".join(sorted(dict.fromkeys(allowed_types)))
            issues.append(
                SchemaValidationIssue(
                    path,
                    f"type mismatch; expected {rendered}",
                )
            )
            return issues

        if "enum" in schema:
            enum_values = schema["enum"]
            if not isinstance(enum_values, list):
                raise OperationFailure("$jsonSchema enum must be a list")
            if value not in enum_values:
                issues.append(SchemaValidationIssue(path, "value is not in enum"))

        if isinstance(value, dict):
            issues.extend(self._validate_object(schema, value, path))
        elif isinstance(value, list):
            issues.extend(self._validate_array(schema, value, path))
        elif isinstance(value, str):
            issues.extend(self._validate_string(schema, value, path))
        elif self._is_numeric(value):
            issues.extend(self._validate_numeric(schema, value, path))

        return issues

    def _validate_schema_definition(self, schema: Mapping[str, object]) -> None:
        allowed_types: list[str] = []
        for key in ("bsonType", "type"):
            declared = schema.get(key)
            if declared is None:
                continue
            allowed_types.extend(self._normalize_type_names(declared, field_name=key))

        if "enum" in schema and not isinstance(schema["enum"], list):
            raise OperationFailure("$jsonSchema enum must be a list")

        required = schema.get("required")
        if required is not None:
            if not isinstance(required, list) or not all(isinstance(item, str) for item in required):
                raise OperationFailure("$jsonSchema required must be a list of strings")

        properties = schema.get("properties")
        if properties is not None:
            if not isinstance(properties, dict):
                raise OperationFailure("$jsonSchema properties must be a document")
            for sub_schema in properties.values():
                if not isinstance(sub_schema, dict):
                    raise OperationFailure("$jsonSchema property definitions must be documents")
                self._validate_schema_definition(sub_schema)

        additional_properties = schema.get("additionalProperties")
        if additional_properties is not None and not isinstance(additional_properties, bool):
            raise OperationFailure("$jsonSchema additionalProperties must be a bool")

        items_schema = schema.get("items")
        if items_schema is not None:
            if not isinstance(items_schema, dict):
                raise OperationFailure("$jsonSchema items must be a document")
            self._validate_schema_definition(items_schema)

        for field_name in ("minItems", "maxItems", "minLength", "maxLength"):
            value = schema.get(field_name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise OperationFailure(f"$jsonSchema {field_name} must be a non-negative integer")

        pattern = schema.get("pattern")
        if pattern is not None and not isinstance(pattern, str):
            raise OperationFailure("$jsonSchema pattern must be a string")

        for field_name in ("minimum", "maximum"):
            bound = schema.get(field_name)
            if bound is not None and self._as_decimal(bound) is None:
                raise OperationFailure(f"$jsonSchema {field_name} must be numeric")

    def _validate_object(
        self,
        schema: Mapping[str, object],
        value: Document,
        path: tuple[str, ...],
    ) -> list[SchemaValidationIssue]:
        issues: list[SchemaValidationIssue] = []
        required = schema.get("required")
        if required is not None:
            if not isinstance(required, list) or not all(isinstance(item, str) for item in required):
                raise OperationFailure("$jsonSchema required must be a list of strings")
            for field_name in required:
                if field_name not in value:
                    issues.append(
                        SchemaValidationIssue(
                            path + (field_name,),
                            "field is required",
                        )
                    )

        properties = schema.get("properties")
        property_schemas: dict[str, Mapping[str, object]] = {}
        if properties is not None:
            if not isinstance(properties, dict):
                raise OperationFailure("$jsonSchema properties must be a document")
            for field_name, sub_schema in properties.items():
                if not isinstance(field_name, str) or not isinstance(sub_schema, dict):
                    raise OperationFailure("$jsonSchema property definitions must be documents")
                property_schemas[field_name] = sub_schema

        additional_properties = schema.get("additionalProperties", True)
        if not isinstance(additional_properties, bool):
            raise OperationFailure("$jsonSchema additionalProperties must be a bool")

        for field_name, field_value in value.items():
            sub_schema = property_schemas.get(field_name)
            if sub_schema is None:
                if not additional_properties:
                    issues.append(
                        SchemaValidationIssue(
                            path + (field_name,),
                            "additional properties are not allowed",
                        )
                    )
                continue
            issues.extend(self._validate_schema(sub_schema, field_value, path + (field_name,)))

        return issues

    def _validate_array(
        self,
        schema: Mapping[str, object],
        value: list[object],
        path: tuple[str, ...],
    ) -> list[SchemaValidationIssue]:
        issues: list[SchemaValidationIssue] = []
        min_items = schema.get("minItems")
        if min_items is not None:
            if not isinstance(min_items, int) or isinstance(min_items, bool) or min_items < 0:
                raise OperationFailure("$jsonSchema minItems must be a non-negative integer")
            if len(value) < min_items:
                issues.append(SchemaValidationIssue(path, f"array has fewer than {min_items} items"))
        max_items = schema.get("maxItems")
        if max_items is not None:
            if not isinstance(max_items, int) or isinstance(max_items, bool) or max_items < 0:
                raise OperationFailure("$jsonSchema maxItems must be a non-negative integer")
            if len(value) > max_items:
                issues.append(SchemaValidationIssue(path, f"array has more than {max_items} items"))
        items_schema = schema.get("items")
        if items_schema is not None:
            if not isinstance(items_schema, dict):
                raise OperationFailure("$jsonSchema items must be a document")
            for index, item in enumerate(value):
                issues.extend(self._validate_schema(items_schema, item, path + (str(index),)))
        return issues

    def _validate_string(
        self,
        schema: Mapping[str, object],
        value: str,
        path: tuple[str, ...],
    ) -> list[SchemaValidationIssue]:
        issues: list[SchemaValidationIssue] = []
        min_length = schema.get("minLength")
        if min_length is not None:
            if not isinstance(min_length, int) or isinstance(min_length, bool) or min_length < 0:
                raise OperationFailure("$jsonSchema minLength must be a non-negative integer")
            if len(value) < min_length:
                issues.append(SchemaValidationIssue(path, f"string shorter than {min_length}"))
        max_length = schema.get("maxLength")
        if max_length is not None:
            if not isinstance(max_length, int) or isinstance(max_length, bool) or max_length < 0:
                raise OperationFailure("$jsonSchema maxLength must be a non-negative integer")
            if len(value) > max_length:
                issues.append(SchemaValidationIssue(path, f"string longer than {max_length}"))
        pattern = schema.get("pattern")
        if pattern is not None:
            if not isinstance(pattern, str):
                raise OperationFailure("$jsonSchema pattern must be a string")
            if re.search(pattern, value) is None:
                issues.append(SchemaValidationIssue(path, "string does not match pattern"))
        return issues

    def _validate_numeric(
        self,
        schema: Mapping[str, object],
        value: object,
        path: tuple[str, ...],
    ) -> list[SchemaValidationIssue]:
        issues: list[SchemaValidationIssue] = []
        comparable = self._as_decimal(value)
        for field_name, operator, label in (
            ("minimum", lambda current, expected: current < expected, "less than minimum"),
            ("maximum", lambda current, expected: current > expected, "greater than maximum"),
        ):
            raw_bound = schema.get(field_name)
            if raw_bound is None:
                continue
            expected = self._as_decimal(raw_bound)
            if expected is None:
                raise OperationFailure(f"$jsonSchema {field_name} must be numeric")
            if operator(comparable, expected):
                issues.append(SchemaValidationIssue(path, label))
        return issues

    @staticmethod
    def _normalize_type_names(value: object, *, field_name: str) -> list[str]:
        candidates = value if isinstance(value, list) else [value]
        normalized: list[str] = []
        for candidate in candidates:
            if not isinstance(candidate, str):
                raise OperationFailure(f"$jsonSchema {field_name} entries must be strings")
            if candidate not in _BSON_TYPE_ALIASES and candidate not in _JSON_TYPE_MAP:
                raise OperationFailure(f"$jsonSchema {field_name} value '{candidate}' is not supported")
            normalized.append(candidate)
        return normalized

    @staticmethod
    def _is_numeric(value: object) -> bool:
        return bson_numeric_alias(value) is not None and not isinstance(value, bool)

    @staticmethod
    def _as_decimal(value: object) -> decimal.Decimal | None:
        alias = bson_numeric_alias(value)
        if alias is None:
            return None
        if isinstance(value, BsonDecimal128):
            return value.value
        if isinstance(value, BsonDouble):
            if math.isnan(value.value) or math.isinf(value.value):
                return None
            return decimal.Decimal(str(value.value))
        if isinstance(value, BsonInt32 | BsonInt64):
            return decimal.Decimal(value.value)
        if isinstance(value, decimal.Decimal):
            return value
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return decimal.Decimal(str(value))
        if isinstance(value, int):
            return decimal.Decimal(value)
        return None

    @staticmethod
    def _matches_any_type(value: object, type_names: list[str]) -> bool:
        return any(CompiledJsonSchema._matches_type(value, type_name) for type_name in type_names)

    @staticmethod
    def _matches_type(value: object, type_name: str) -> bool:
        if type_name == "number":
            return bson_numeric_alias(value) is not None and not isinstance(value, bool)
        if type_name == "int":
            return bson_numeric_alias(value) == "int"
        if type_name == "long":
            return bson_numeric_alias(value) == "long"
        if type_name == "double":
            return bson_numeric_alias(value) == "double"
        if type_name == "decimal":
            return bson_numeric_alias(value) == "decimal"
        if type_name == "objectId":
            return isinstance(value, ObjectId)
        if type_name == "date":
            return isinstance(value, datetime.datetime)
        if type_name == "binData":
            return isinstance(value, (bytes, bytearray, uuid.UUID))
        if type_name == "regex":
            return isinstance(value, re.Pattern)
        expected = _JSON_TYPE_MAP.get(type_name)
        if expected is None:
            return False
        if type_name in {"object", "array"}:
            return isinstance(value, expected)
        return isinstance(value, expected) and not (
            type_name in {"bool", "boolean"} and isinstance(value, bool) is False
        )


@dataclass(frozen=True, slots=True)
class CompiledCollectionValidator:
    raw_validator: Document | None
    query_validator: QueryNode | None
    json_schema: CompiledJsonSchema | None
    validation_level: str = "strict"
    validation_action: str = "error"

    def should_validate(
        self,
        *,
        original_document: Document | None,
        is_upsert_insert: bool = False,
    ) -> bool:
        if self.validation_level == "off":
            return False
        if self.validation_level == "moderate" and original_document is not None and not is_upsert_insert:
            previous = self.validate_document(
                original_document,
                original_document=None,
                is_upsert_insert=True,
            )
            return previous.valid
        return True

    def validate_document(
        self,
        document: Document,
        *,
        original_document: Document | None = None,
        is_upsert_insert: bool = False,
        dialect: MongoDialect | None = None,
    ) -> SchemaValidationResult:
        if not self.should_validate(
            original_document=original_document,
            is_upsert_insert=is_upsert_insert,
        ):
            return SchemaValidationResult(valid=True)
        issues: list[SchemaValidationIssue] = []
        effective_dialect = dialect or MONGODB_DIALECT_70
        if self.query_validator is not None and not QueryEngine.match_plan(
            document,
            self.query_validator,
            dialect=effective_dialect,
        ):
            issues.append(SchemaValidationIssue((), "document failed validator expression"))
        if self.json_schema is not None:
            issues.extend(self.json_schema.validate(document).issues)
        return SchemaValidationResult(valid=not issues, issues=tuple(issues))


def compile_collection_validator(
    options: Mapping[str, object] | None,
    *,
    dialect: MongoDialect | None = None,
) -> CompiledCollectionValidator | None:
    if not options:
        return None
    validator_spec = options.get("validator")
    if validator_spec is None:
        return None
    if not isinstance(validator_spec, dict):
        raise OperationFailure("collection validator must be a document")

    validation_level = options.get("validationLevel", "strict")
    if not isinstance(validation_level, str) or validation_level not in {"off", "strict", "moderate"}:
        raise OperationFailure("validationLevel must be 'off', 'strict' or 'moderate'")
    validation_action = options.get("validationAction", "error")
    if not isinstance(validation_action, str) or validation_action not in {"error", "warn"}:
        raise OperationFailure("validationAction must be 'error' or 'warn'")

    validator_document = deepcopy(validator_spec)
    json_schema_spec = validator_document.pop("$jsonSchema", None)
    json_schema = None
    if json_schema_spec is not None:
        if not isinstance(json_schema_spec, dict):
            raise OperationFailure("$jsonSchema validator must be a document")
        json_schema = CompiledJsonSchema(deepcopy(json_schema_spec))

    query_validator = None
    if validator_document:
        query_validator = compile_filter(
            validator_document,
            dialect=dialect or MONGODB_DIALECT_70,
        )

    return CompiledCollectionValidator(
        raw_validator=deepcopy(validator_spec),
        query_validator=query_validator,
        json_schema=json_schema,
        validation_level=validation_level,
        validation_action=validation_action,
    )
