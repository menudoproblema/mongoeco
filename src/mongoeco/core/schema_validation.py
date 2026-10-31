from __future__ import annotations

import datetime
import decimal
import re
import uuid

from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_support import compile_regex
from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.bson_scalars import (
    _is_nan_numeric_value,
    _numeric_to_decimal,
    bson_numeric_alias,
    compare_bson_numeric,
    wrap_bson_numeric,
)
from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    SON,
    Binary,
    DBRef,
    Decimal128,
    Document,
    ObjectId,
    Regex,
    Timestamp,
)


_JSON_TYPE_MAP = {
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
        "timestamp",
        "null",
        "regex",
        "int",
        "long",
        "decimal",
        "number",
    }
)
_KEYWORDS = frozenset(
    {
        "type",
        "bsonType",
        "enum",
        "required",
        "properties",
        "additionalProperties",
        "items",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "minItems",
        "maxItems",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "uniqueItems",
        "pattern",
        "title",
        "description",
    }
)
# Known constraints outside this implemented subset must never be ignored.
_UNSUPPORTED = frozenset(
    {
        "dependencies",
        "patternProperties",
        "additionalItems",
        "minProperties",
        "maxProperties",
    }
)


def schema_equal(left: object, right: object) -> bool:
    """Schema equality: atomic arrays, unordered object fields, binary strings."""
    lnum, rnum = wrap_bson_numeric(left), wrap_bson_numeric(right)
    if lnum is not None or rnum is not None:
        return (
            lnum is not None
            and rnum is not None
            and compare_bson_numeric(left, right) == 0
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            schema_equal(v, right[k]) for k, v in left.items()
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            schema_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    return bson_equality_key(left) == bson_equality_key(right)


def _json_path(path: tuple[str, ...]) -> str:
    return "$" if not path else "$." + ".".join(path)


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    path: tuple[str, ...]
    message: str
    detail: Document | None = None

    def render(self) -> str:
        return f"{_json_path(self.path)}: {self.message}"


@dataclass(frozen=True, slots=True)
class SchemaValidationResult:
    valid: bool
    issues: tuple[SchemaValidationIssue, ...] = ()

    @property
    def first_message(self) -> str:
        return self.issues[0].render() if self.issues else "document passed validation"

    @property
    def details(self) -> list[Document]:
        return _issue_details(self.issues)


def _issue_details(issues):
    return [deepcopy(issue.detail) for issue in issues if issue.detail is not None]


def _issue_recorder(issues, schema, value, path):
    def fail(key, message, reason=None, **extra):
        detail = {"operatorName": key, "specifiedAs": {key: deepcopy(schema[key])}}
        if reason is not None:
            detail.update(reason=reason, consideredValue=deepcopy(value))
        detail.update(extra)
        issues.append(SchemaValidationIssue(path, message, detail))

    return fail


def _group(issues, detail):
    # Preserve the flat issue API and attach a native structural diagnostic once.
    return [
        SchemaValidationIssue(issue.path, issue.message, detail if i == 0 else None)
        for i, issue in enumerate(issues)
    ]


@dataclass(frozen=True, slots=True)
class CompiledJsonSchema:
    raw_schema: Document
    _patterns: dict[str, re.Pattern[str]] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_schema", deepcopy(self.raw_schema))
        self._validate_schema_definition(self.raw_schema)

    def validate(
        self, document: object, *, path: tuple[str, ...] = ()
    ) -> SchemaValidationResult:
        issues = tuple(self._validate_schema(self.raw_schema, document, path))
        return SchemaValidationResult(valid=not issues, issues=issues)

    @staticmethod
    def _normalize_type_names(value: object, *, field_name: str) -> list[str]:
        candidates = value if isinstance(value, list) else [value]
        if not candidates:
            msg = f"$jsonSchema keyword '{field_name}' must name at least one type"
            raise server_error(
                msg,
                code=9,
            )
        allowed = (
            _BSON_TYPE_ALIASES
            if field_name == "bsonType"
            else {"object", "array", "string", "number", "boolean", "null"}
        )
        for candidate in candidates:
            if not isinstance(candidate, str):
                msg = f"$jsonSchema {field_name} entries must be strings"
                raise server_error(msg, code=14)
            if candidate == "integer" and field_name == "type":
                msg = "$jsonSchema type 'integer' is not currently supported."
                raise server_error(msg, code=9)
            if candidate not in allowed:
                msg = f"Unknown type name alias: {candidate}"
                raise server_error(msg, code=2)
        for i, candidate in enumerate(candidates):
            if candidate in candidates[:i]:
                msg = (
                    f"$jsonSchema keyword '{field_name}' "
                    f"has duplicate value: {candidate}"
                )
                raise server_error(
                    msg,
                    code=9,
                )
        return list(candidates)

    def _validate_schema_definition(self, schema) -> None:
        for keyword in schema:
            if keyword in _UNSUPPORTED:
                msg = f"$jsonSchema keyword '{keyword}' is not supported by mongoeco"
                raise server_error(msg, code=2)
            if keyword not in _KEYWORDS:
                msg = f"Unknown $jsonSchema keyword: {keyword}"
                raise server_error(msg, code=9)
        if "type" in schema and "bsonType" in schema:
            msg = "Cannot specify both $jsonSchema keywords 'type' and 'bsonType'"
            raise server_error(msg, code=9)
        for key in ("type", "bsonType"):
            if key in schema:
                self._normalize_type_names(schema[key], field_name=key)
        for key in ("enum", "required"):
            if key not in schema:
                continue
            values = schema[key]
            if not isinstance(values, list):
                msg = f"$jsonSchema {key} must be a list"
                raise server_error(msg, code=14)
            if not values:
                msg = f"$jsonSchema keyword '{key}' cannot be an empty array"
                raise server_error(msg, code=9)
            if key == "required" and any(not isinstance(v, str) for v in values):
                msg = "$jsonSchema required must be a list of strings"
                raise server_error(msg, code=14)
            if any(
                schema_equal(value, earlier)
                for i, value in enumerate(values)
                for earlier in values[:i]
            ):
                suffix = "." if key == "enum" else ""
                msg = (
                    f"$jsonSchema keyword '{key}' array "
                    f"cannot contain duplicate values{suffix}"
                )
                raise server_error(
                    msg,
                    code=9,
                )
        self._validate_string_definition(schema)
        self._validate_numeric_definition(schema)
        self._validate_children_definition(schema)

    def _validate_string_definition(self, schema):
        for key in ("title", "description", "pattern"):
            if key in schema and not isinstance(schema[key], str):
                msg = f"$jsonSchema keyword '{key}' must be of type string"
                raise server_error(msg, code=14)
        if "pattern" in schema:
            try:
                self._patterns[schema["pattern"]] = compile_regex(schema["pattern"], "")
            except OperationFailure as error:
                if "unterminated character set" in str(error):
                    msg = (
                        "Regular expression is invalid: "
                        "missing terminating ] for character class"
                    )
                    raise server_error(
                        msg,
                        code=51091,
                    ) from error
                raise

    def _validate_numeric_definition(self, schema):
        for key in ("minimum", "maximum", "multipleOf"):
            if key in schema and wrap_bson_numeric(schema[key]) is None:
                msg = f"$jsonSchema {key} must be numeric"
                raise server_error(msg, code=14)
        if "multipleOf" in schema:
            divisor = wrap_bson_numeric(schema["multipleOf"]).value
            if (
                not _is_nan_numeric_value(divisor)
                and compare_bson_numeric(schema["multipleOf"], 0) <= 0
            ):
                msg = "$jsonSchema keyword 'multipleOf' must have a positive value"
                raise server_error(
                    msg,
                    code=9,
                )
        for key in ("exclusiveMinimum", "exclusiveMaximum", "uniqueItems"):
            if key not in schema:
                continue
            bound = key.removeprefix("exclusive").lower()
            if key != "uniqueItems" and bound not in schema:
                msg = (
                    f"$jsonSchema keyword '{bound}' must be a present "
                    f"if {key} is present"
                )
                raise server_error(
                    msg,
                    code=9,
                )
            if not isinstance(schema[key], bool):
                msg = f"$jsonSchema keyword '{key}' must be a boolean"
                raise server_error(msg, code=14)
        for key in ("minItems", "maxItems", "minLength", "maxLength"):
            if key in schema:
                number = wrap_bson_numeric(schema[key])
                if (
                    number is None
                    or not decimal.Decimal(str(number.value)).is_finite()
                    or number.value < 0
                    or number.value != int(number.value)
                ):
                    msg = f"$jsonSchema {key} must be a non-negative integer"
                    raise server_error(msg, code=9)

    def _validate_children_definition(self, schema):
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            msg = "$jsonSchema properties must be a document"
            raise server_error(msg, code=14)
        children = list(properties.values())
        children.extend(schema[key] for key in ("items", "not") if key in schema)
        if "additionalProperties" in schema and not isinstance(
            schema["additionalProperties"], bool
        ):
            children.append(schema["additionalProperties"])
        for key in ("allOf", "anyOf", "oneOf"):
            if key in schema:
                if not isinstance(schema[key], list) or not schema[key]:
                    msg = f"$jsonSchema {key} must be a non-empty list of documents"
                    raise server_error(
                        msg,
                        code=9,
                    )
                children.extend(schema[key])
        for child in children:
            if not isinstance(child, dict):
                msg = "$jsonSchema child schema must be a document"
                raise server_error(msg, code=14)
            self._validate_schema_definition(child)

    def _validate_schema(self, schema, value, path):
        issues = []
        fail = _issue_recorder(issues, schema, value, path)
        type_issues = []
        for key in ("bsonType", "type"):
            if key in schema and not self._matches_any_type(
                value, self._normalize_type_names(schema[key], field_name=key)
            ):
                candidates = (
                    "bool",
                    "int",
                    "long",
                    "double",
                    "decimal",
                    "string",
                    "array",
                    "object",
                    "null",
                    "binData",
                    "date",
                    "objectId",
                    "regex",
                    "timestamp",
                )
                actual = next(
                    (name for name in candidates if self._matches_type(value, name)),
                    type(value).__name__,
                )
                fail(
                    key,
                    f"type mismatch; expected {schema[key]}",
                    "type did not match",
                    consideredType=actual,
                )
                type_issues.extend(issues)
                issues.clear()
        if "enum" in schema and not any(schema_equal(value, v) for v in schema["enum"]):
            fail("enum", "value is not in enum", "value was not found in enum")
        if isinstance(value, dict):
            issues.extend(self._validate_object(schema, value, path))
        elif isinstance(value, list):
            issues.extend(self._validate_array(schema, value, path))
        elif isinstance(value, str):
            issues.extend(self._validate_string(schema, value, path))
        elif wrap_bson_numeric(value) is not None:
            issues.extend(self._validate_numeric(schema, value, path))
        issues.extend(self._validate_logical_operators(schema, value, path))
        return issues + type_issues

    def _validate_logical_operators(self, schema, value, path):
        issues = []
        for key in ("allOf", "anyOf", "oneOf"):
            if key not in schema:
                continue
            results = [
                self._validate_schema(child, value, path) for child in schema[key]
            ]
            matches = sum(not result for result in results)
            valid = (
                matches == len(results)
                if key == "allOf"
                else matches > 0
                if key == "anyOf"
                else matches == 1
            )
            if not valid:
                reason = (
                    "more than one subschema matched"
                    if key == "oneOf" and matches > 1
                    else None
                )
                detail = {"operatorName": key}
                if reason:
                    detail.update(
                        reason=reason,
                        matchingSchemaIndexes=[
                            i for i, r in enumerate(results) if not r
                        ],
                    )
                else:
                    detail["schemasNotSatisfied"] = [
                        {
                            "index": i,
                            "details": _issue_details(r),
                        }
                        for i, r in enumerate(results)
                        if r
                    ]
                issues.append(
                    SchemaValidationIssue(path, f"value does not satisfy {key}", detail)
                )
        if "not" in schema and not self._validate_schema(schema["not"], value, path):
            issues.append(
                SchemaValidationIssue(
                    path,
                    "value must not satisfy not",
                    {"operatorName": "not", "reason": "child expression matched"},
                )
            )
        return issues

    def _validate_object(self, schema, value, path):
        issues = []
        fail = _issue_recorder(issues, schema, value, path)
        properties = []
        property_issues = []
        for name, child in schema.get("properties", {}).items():
            if name not in value:
                continue
            result = self._validate_schema(child, value[name], (*path, name))
            if result:
                property_issues.extend(result)
                properties.append(
                    {
                        "propertyName": name,
                        **{
                            k: deepcopy(child[k])
                            for k in ("title", "description")
                            if k in child
                        },
                        "details": _issue_details(result),
                    }
                )
        if properties:
            issues.extend(
                _group(
                    property_issues,
                    {
                        "operatorName": "properties",
                        "propertiesNotSatisfied": properties,
                    },
                )
            )
        missing = [name for name in schema.get("required", []) if name not in value]
        if missing:
            fail("required", "field is required", missingProperties=missing)
        extra = [name for name in value if name not in schema.get("properties", {})]
        additional = schema.get("additionalProperties", True)
        if extra and additional is False:
            fail(
                "additionalProperties",
                "additional properties are not allowed",
                additionalProperties=extra,
            )
        elif isinstance(additional, dict):
            for name in extra:
                result = self._validate_schema(additional, value[name], (*path, name))
                if result:
                    issues.extend(
                        _group(
                            result,
                            {
                                "operatorName": "additionalProperties",
                                "reason": (
                                    "at least one additional property "
                                    "did not match the subschema"
                                ),
                                "failingProperty": name,
                                "details": _issue_details(result),
                            },
                        )
                    )
                    break
        return issues

    def _validate_array(self, schema, value, path):
        issues = []
        fail = _issue_recorder(issues, schema, value, path)
        for key, bad, message in (
            ("minItems", len(value) < schema.get("minItems", 0), "fewer"),
            ("maxItems", len(value) > schema.get("maxItems", len(value)), "more"),
        ):
            if bad:
                fail(
                    key,
                    f"array has {message} than {schema[key]} items",
                    "array length did not satisfy the specified bounds",
                )
        if schema.get("uniqueItems"):
            duplicate = next(
                (
                    (v,)
                    for i, v in enumerate(value)
                    if any(schema_equal(v, a) for a in value[:i])
                ),
                None,
            )
            if duplicate is not None:
                fail(
                    "uniqueItems",
                    "array items are not unique",
                    "found a duplicate item",
                    duplicatedValue=deepcopy(duplicate[0]),
                )
        if "items" in schema:
            for i, item in enumerate(value):
                result = self._validate_schema(schema["items"], item, (*path, str(i)))
                if result:
                    issues.extend(
                        _group(
                            result,
                            {
                                "operatorName": "items",
                                "reason": (
                                    "At least one item did not match the sub-schema"
                                ),
                                "itemIndex": i,
                                "details": _issue_details(result),
                            },
                        )
                    )
                    break
        return issues

    def _validate_string(self, schema, value, path):
        issues = []
        fail = _issue_recorder(issues, schema, value, path)
        for key, bad, message in (
            ("minLength", len(value) < schema.get("minLength", 0), "shorter"),
            ("maxLength", len(value) > schema.get("maxLength", len(value)), "longer"),
        ):
            if bad:
                fail(
                    key,
                    f"string {message} than {schema[key]}",
                    "specified string length was not satisfied",
                )
        if (
            "pattern" in schema
            and self._patterns[schema["pattern"]].search(value) is None
        ):
            fail(
                "pattern",
                "string does not match pattern",
                "regular expression did not match",
            )
        return issues

    def _validate_numeric(self, schema, value, path):
        issues = []
        fail = _issue_recorder(issues, schema, value, path)
        for key, direction in (("maximum", -1), ("minimum", 1)):
            if key not in schema:
                continue
            bound = schema[key]
            vnan = _is_nan_numeric_value(wrap_bson_numeric(value).value)
            bnan = _is_nan_numeric_value(wrap_bson_numeric(bound).value)
            exclusive = "exclusive" + key.capitalize()
            compared = compare_bson_numeric(value, bound) * direction
            matched = (
                (vnan and bnan and not schema.get(exclusive))
                if vnan or bnan
                else (compared > 0 or (compared == 0 and not schema.get(exclusive)))
            )
            if not matched:
                fail(
                    key,
                    "less than minimum" if key == "minimum" else "greater than maximum",
                    "comparison failed",
                )
                if exclusive in schema:
                    issues[-1].detail["specifiedAs"][exclusive] = schema[exclusive]
        if "multipleOf" in schema:
            # Schema division promotes doubles to decimal at 15 significant digits;
            # unlike bounds this is arithmetic conversion, not numeric equality.
            current = _numeric_to_decimal(wrap_bson_numeric(value).value)
            divisor = _numeric_to_decimal(wrap_bson_numeric(schema["multipleOf"]).value)
            if not divisor.is_finite():
                raise server_error(
                    "divisor cannot be NaN"
                    if divisor.is_nan()
                    else "divisor cannot be infinite",
                    code=2,
                )
            matched = False
            if current.is_finite() and divisor.is_finite():
                cn, cd = current.as_integer_ratio()
                dn, dd = divisor.as_integer_ratio()
                matched = (cn * dd) % (cd * dn) == 0
            if not matched:
                fail(
                    "multipleOf",
                    "value is not a multiple",
                    "considered value is not a multiple of the specified value",
                )
        return issues

    @staticmethod
    def _matches_any_type(value: object, type_names: list[str]) -> bool:
        return any(
            CompiledJsonSchema._matches_type(value, type_name)
            for type_name in type_names
        )

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
            return bson_numeric_alias(value) == "decimal" or isinstance(
                value, Decimal128
            )
        if type_name == "objectId":
            return isinstance(value, ObjectId)
        if type_name == "date":
            return isinstance(value, datetime.datetime)
        if type_name == "timestamp":
            return isinstance(value, Timestamp)
        if type_name == "binData":
            return isinstance(value, (bytes, bytearray, uuid.UUID, Binary))
        if type_name == "regex":
            return isinstance(value, (re.Pattern, Regex))
        expected = _JSON_TYPE_MAP.get(type_name)
        if expected is None:
            return False
        if type_name in {"object", "array"}:
            if type_name == "object":
                return isinstance(value, (dict, SON, DBRef))
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
    dialect: MongoDialect = MONGODB_DIALECT_70
    collation: CollationSpec | None = None

    def should_validate(
        self,
        *,
        original_document: Document | None,
        is_upsert_insert: bool = False,
        dialect: MongoDialect | None = None,
    ) -> bool:
        if self.validation_level == "off":
            return False
        return not (
            self.validation_level == "moderate"
            and original_document is not None
            and not is_upsert_insert
            and not self._validate_document(original_document, dialect=dialect).valid
        )

    def _validate_document(self, document, *, dialect):
        issues = []
        if self.query_validator is not None and not QueryEngine.match_plan(
            document,
            self.query_validator,
            dialect=dialect or self.dialect,
            collation=self.collation,
        ):
            issues.append(
                SchemaValidationIssue((), "document failed validator expression")
            )
        if self.json_schema is not None:
            issues.extend(self.json_schema.validate(document).issues)
        return SchemaValidationResult(valid=not issues, issues=tuple(issues))

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
            dialect=dialect,
        ):
            return SchemaValidationResult(valid=True)
        return self._validate_document(document, dialect=dialect)


def compile_collection_validator(
    options: Mapping[str, object] | None,
    *,
    dialect: MongoDialect | None = None,
) -> CompiledCollectionValidator | None:
    if not options:
        return None
    validation_level = options.get("validationLevel", "strict")
    if not isinstance(validation_level, str) or validation_level not in {
        "off",
        "strict",
        "moderate",
    }:
        msg = "validationLevel must be 'off', 'strict' or 'moderate'"
        raise server_error(msg, code=2)
    validation_action = options.get("validationAction", "error")
    if not isinstance(validation_action, str) or validation_action not in {
        "error",
        "warn",
    }:
        msg = "validationAction must be 'error' or 'warn'"
        raise server_error(msg, code=2)

    validator_spec = options.get("validator")
    if validator_spec is None:
        return None
    if not isinstance(validator_spec, dict):
        msg = "collection validator must be a document"
        raise server_error(msg, code=14)

    validator_document = deepcopy(validator_spec)
    json_schema = None
    if "$jsonSchema" in validator_document:
        json_schema_spec = validator_document.pop("$jsonSchema")
        if not isinstance(json_schema_spec, dict):
            msg = "$jsonSchema validator must be a document"
            raise server_error(msg, code=14)
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
        dialect=dialect or MONGODB_DIALECT_70,
        collation=normalize_collation(options.get("collation")),
    )


def schema_failure_details(result: SchemaValidationResult) -> list[Document]:
    """Render the recorded decision; never prepare or evaluate a second schema."""
    return result.details


if TYPE_CHECKING:
    from collections.abc import Mapping
