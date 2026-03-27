import datetime
import decimal
import math
import random
import re
import uuid
from collections.abc import Callable
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.date_expressions import _to_utc_naive
from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    INT32_MAX,
    INT32_MIN,
    INT64_MAX,
    INT64_MIN,
    is_bson_numeric,
    unwrap_bson_numeric,
    validate_bson_value,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, ObjectId, UndefinedType


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type ExpressionArgValidator = Callable[[str, object, int, int | None], list[Any]]
type Stringifier = Callable[[Any], str]
type BsonSizer = Callable[[dict[str, Any]], int]


SCALAR_EXPRESSION_OPERATORS = frozenset(
    {
        "$rand",
        "$convert",
        "$bsonSize",
        "$binarySize",
        "$isNumber",
        "$type",
        "$toString",
        "$toBool",
        "$toDate",
        "$toDecimal",
        "$toInt",
        "$toDouble",
        "$toLong",
        "$toObjectId",
        "$toUUID",
        "$isArray",
        "$cmp",
    }
)


def evaluate_scalar_expression(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    require_expression_args: ExpressionArgValidator,
    stringify_value: Stringifier,
    bson_document_size: BsonSizer,
    missing_sentinel: object,
) -> Any:
    if operator == "$rand":
        if spec != {}:
            raise OperationFailure("$rand does not accept arguments")
        return random.random()

    if operator == "$convert":
        if not isinstance(spec, dict) or "input" not in spec or "to" not in spec:
            raise OperationFailure("$convert requires input and to")
        value = evaluate_expression_with_missing(document, spec["input"], variables)
        if value is missing_sentinel or value is None:
            return evaluate_expression(document, spec["onNull"], variables) if "onNull" in spec else None
        target = evaluate_expression(document, spec["to"], variables)
        if isinstance(target, dict):
            target = target.get("type")
        if not isinstance(target, str):
            raise OperationFailure("$convert to must resolve to a string")
        aliases = {
            "bool": "bool",
            "int": "int",
            "long": "long",
            "double": "double",
            "date": "date",
            "objectId": "objectId",
            "string": "string",
        }
        try:
            if target not in aliases:
                raise OperationFailure("$convert target type is not supported")
            return _convert_aggregation_scalar(operator, value, aliases[target], stringify_value=stringify_value)
        except OperationFailure:
            if "onError" in spec:
                return evaluate_expression(document, spec["onError"], variables)
            raise

    if operator == "$bsonSize":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression_with_missing(document, args[0], variables)
        if value is missing_sentinel or value is None:
            return None
        if not isinstance(value, dict):
            raise OperationFailure("$bsonSize requires an object input")
        return bson_document_size(value)

    if operator == "$binarySize":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression_with_missing(document, args[0], variables)
        if value is missing_sentinel or value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            return len(value)
        if isinstance(value, uuid.UUID):
            return len(value.bytes)
        raise OperationFailure("$binarySize requires a BinData argument")

    if operator == "$isNumber":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression_with_missing(document, args[0], variables)
        return is_bson_numeric(value) or (isinstance(value, (int, float)) and not isinstance(value, bool))

    if operator == "$type":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression_with_missing(document, args[0], variables)
        return _aggregation_type_name(value, missing_sentinel=missing_sentinel)

    if operator == "$toString":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression(document, args[0], variables)
        if value is None:
            return None
        return stringify_value(unwrap_bson_numeric(value))

    if operator in {"$toBool", "$toDate", "$toDecimal", "$toInt", "$toDouble", "$toLong", "$toObjectId", "$toUUID"}:
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression_with_missing(document, args[0], variables)
        if value is missing_sentinel:
            return None
        target = {
            "$toBool": "bool",
            "$toDate": "date",
            "$toDecimal": "decimal",
            "$toInt": "int",
            "$toDouble": "double",
            "$toLong": "long",
            "$toObjectId": "objectId",
            "$toUUID": "uuid",
        }[operator]
        return _convert_aggregation_scalar(operator, value, target, stringify_value=stringify_value)

    if operator == "$isArray":
        args = require_expression_args(
            operator,
            [spec] if not isinstance(spec, list) else spec,
            1,
            1,
        )
        value = evaluate_expression(document, args[0], variables)
        return isinstance(value, list)

    if operator == "$cmp":
        args = require_expression_args(operator, spec, 2, 2)
        left = evaluate_expression(document, args[0], variables)
        right = evaluate_expression(document, args[1], variables)
        comparison = dialect.policy.compare_values(left, right)
        if comparison < 0:
            return -1
        if comparison > 0:
            return 1
        return 0

    raise OperationFailure(f"Unsupported scalar expression operator: {operator}")


def _aggregation_type_name(value: Any, *, missing_sentinel: object) -> str:
    if value is missing_sentinel:
        return "missing"
    if isinstance(value, UndefinedType):
        return "undefined"
    if value is None:
        return "null"
    if isinstance(value, BsonInt32):
        return "int"
    if isinstance(value, BsonInt64):
        return "long"
    if isinstance(value, BsonDouble):
        return "double"
    if isinstance(value, BsonDecimal128):
        return "decimal"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "long" if value < -(1 << 31) or value > (1 << 31) - 1 else "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, decimal.Decimal):
        return "decimal"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, (bytes, bytearray, uuid.UUID)):
        return "binData"
    if isinstance(value, ObjectId):
        return "objectId"
    if isinstance(value, datetime.datetime):
        return "date"
    if isinstance(value, re.Pattern):
        return "regex"
    return type(value).__name__


def _datetime_to_epoch_millis(value: datetime.datetime) -> int:
    normalized = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return int(normalized.timestamp() * 1000)


def _parse_base10_int_string(operator: str, value: str) -> int:
    text = value.strip()
    if not text or not re.fullmatch(r"[+-]?\d+", text):
        raise OperationFailure(f"{operator} cannot convert the string value")
    return int(text, 10)


def _convert_aggregation_scalar(
    operator: str,
    value: Any,
    target: str,
    *,
    stringify_value: Stringifier,
) -> Any:
    if value is None or isinstance(value, UndefinedType):
        return None
    preserve_numeric_wrappers = isinstance(value, BsonInt32 | BsonInt64 | BsonDouble | BsonDecimal128)
    value = unwrap_bson_numeric(value)

    if target == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        if isinstance(value, (str, list, dict, bytes, bytearray, uuid.UUID, ObjectId, datetime.datetime)):
            return True
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "int":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            if value < INT32_MIN or value > INT32_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt32(value) if preserve_numeric_wrappers else value
        if isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise OperationFailure(f"{operator} cannot convert the value")
            integer = int(value)
            if integer < INT32_MIN or integer > INT32_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt32(integer) if preserve_numeric_wrappers else integer
        if isinstance(value, str):
            integer = _parse_base10_int_string(operator, value)
            if integer < INT32_MIN or integer > INT32_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt32(integer) if preserve_numeric_wrappers else integer
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "long":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            if value < INT64_MIN or value > INT64_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt64(value) if preserve_numeric_wrappers else value
        if isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise OperationFailure(f"{operator} cannot convert the value")
            integer = int(value)
            if integer < INT64_MIN or integer > INT64_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt64(integer) if preserve_numeric_wrappers else integer
        if isinstance(value, str):
            integer = _parse_base10_int_string(operator, value)
            if integer < INT64_MIN or integer > INT64_MAX:
                raise OperationFailure(f"{operator} overflow")
            return BsonInt64(integer) if preserve_numeric_wrappers else integer
        if isinstance(value, datetime.datetime):
            integer = _datetime_to_epoch_millis(value)
            return BsonInt64(integer) if preserve_numeric_wrappers else integer
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "double":
        if isinstance(value, bool):
            result = 1.0 if value else 0.0
            return BsonDouble(result) if preserve_numeric_wrappers else result
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result = float(value)
            return BsonDouble(result) if preserve_numeric_wrappers else result
        if isinstance(value, str):
            text = value.strip()
            if not text:
                raise OperationFailure(f"{operator} cannot convert the string value")
            try:
                result = float(text)
                return BsonDouble(result) if preserve_numeric_wrappers else result
            except ValueError as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        if isinstance(value, datetime.datetime):
            result = float(_datetime_to_epoch_millis(value))
            return BsonDouble(result) if preserve_numeric_wrappers else result
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "date":
        if isinstance(value, datetime.datetime):
            return _to_utc_naive(value)
        if isinstance(value, ObjectId):
            return datetime.datetime.fromtimestamp(value.generation_time, tz=datetime.UTC).replace(tzinfo=None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if not math.isfinite(value):
                raise OperationFailure(f"{operator} cannot convert the value")
            return datetime.datetime.fromtimestamp(value / 1000, tz=datetime.UTC).replace(tzinfo=None)
        if isinstance(value, str):
            try:
                return _to_utc_naive(datetime.datetime.fromisoformat(value.replace("Z", "+00:00")))
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "objectId":
        if isinstance(value, ObjectId):
            return value
        if isinstance(value, str):
            try:
                return ObjectId(value)
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "uuid":
        if isinstance(value, uuid.UUID):
            return value
        if isinstance(value, (bytes, bytearray)):
            if len(value) != 16:
                raise OperationFailure(f"{operator} cannot convert the value")
            try:
                return uuid.UUID(bytes=bytes(value))
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the value") from exc
        if isinstance(value, str):
            try:
                return uuid.UUID(value)
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "decimal":
        if isinstance(value, bool):
            result = decimal.Decimal(int(value))
            validate_bson_value(result)
            return BsonDecimal128(result) if preserve_numeric_wrappers else result
        if isinstance(value, int):
            result = decimal.Decimal(value)
            validate_bson_value(result)
            return BsonDecimal128(result) if preserve_numeric_wrappers else result
        if isinstance(value, float):
            if not math.isfinite(value):
                raise OperationFailure(f"{operator} cannot convert the value")
            result = decimal.Decimal(str(value))
            validate_bson_value(result)
            return BsonDecimal128(result) if preserve_numeric_wrappers else result
        if isinstance(value, decimal.Decimal):
            validate_bson_value(value)
            return BsonDecimal128(value) if preserve_numeric_wrappers else value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                raise OperationFailure(f"{operator} cannot convert the string value")
            try:
                result = decimal.Decimal(text)
                validate_bson_value(result)
                return BsonDecimal128(result) if preserve_numeric_wrappers else result
            except Exception as exc:
                raise OperationFailure(f"{operator} cannot convert the string value") from exc
        raise OperationFailure(f"{operator} cannot convert the value")

    if target == "string":
        return stringify_value(value)

    raise OperationFailure(f"Unsupported conversion target for {operator}")
