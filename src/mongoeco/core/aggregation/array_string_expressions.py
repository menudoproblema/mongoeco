import re
import uuid
from collections.abc import Callable
from copy import deepcopy
from functools import cmp_to_key
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Regex, SortSpec, UndefinedType


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]
type ExpressionArgValidator = Callable[[str, object, int, int | None], list[Any]]


ARRAY_STRING_EXPRESSION_OPERATORS = frozenset(
    {
        "$size",
        "$arrayElemAt",
        "$first",
        "$concat",
        "$trim",
        "$ltrim",
        "$rtrim",
        "$replaceOne",
        "$replaceAll",
        "$strcasecmp",
        "$substr",
        "$substrBytes",
        "$substrCP",
        "$strLenBytes",
        "$strLenCP",
        "$toLower",
        "$toUpper",
        "$split",
        "$concatArrays",
        "$reverseArray",
        "$allElementsTrue",
        "$anyElementTrue",
        "$setUnion",
        "$setDifference",
        "$setIntersection",
        "$setEquals",
        "$setIsSubset",
        "$slice",
        "$map",
        "$filter",
        "$reduce",
        "$objectToArray",
        "$arrayToObject",
        "$zip",
        "$indexOfArray",
        "$indexOfBytes",
        "$indexOfCP",
        "$regexMatch",
        "$regexFind",
        "$regexFindAll",
        "$sortArray",
    }
)


def _copy_if_mutable(value: Any) -> Any:
    if isinstance(value, (dict, list, set)):
        return deepcopy(value)
    return value


def evaluate_array_string_expression(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    require_expression_args: ExpressionArgValidator,
    missing_sentinel: object,
) -> Any:
    if operator == "$size":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression(document, args[0], variables)
        if value is None:
            return None
        if not isinstance(value, list):
            raise OperationFailure("$size requires an array value")
        return len(value)

    if operator == "$arrayElemAt":
        args = require_expression_args(operator, spec, 2, 2)
        values = evaluate_expression(document, args[0], variables)
        index = evaluate_expression(document, args[1], variables)
        if values is None or index is None:
            return None
        if not isinstance(values, list):
            raise OperationFailure("$arrayElemAt requires an array as first argument")
        if not isinstance(index, int) or isinstance(index, bool):
            raise OperationFailure("$arrayElemAt requires an integer index")
        if index < 0:
            index += len(values)
        if index < 0 or index >= len(values):
            return None
        return values[index]

    if operator == "$first":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression(document, args[0], variables)
        if isinstance(value, list):
            return value[0] if value else None
        return value

    if operator == "$concat":
        args = require_expression_args(operator, spec, 1, None)
        parts: list[str] = []
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is None:
                return None
            if not isinstance(value, str):
                raise OperationFailure("$concat requires string arguments")
            parts.append(value)
        return "".join(parts)

    if operator in {"$trim", "$ltrim", "$rtrim"}:
        if not isinstance(spec, dict) or "input" not in spec:
            raise OperationFailure(f"{operator} requires an input field")
        value = evaluate_expression(document, spec["input"], variables)
        if value is None:
            return None
        if not isinstance(value, str):
            raise OperationFailure(f"{operator} requires a string input")
        chars = spec.get("chars")
        if chars is not None:
            chars = evaluate_expression(document, chars, variables)
            if chars is None:
                return None
            if not isinstance(chars, str):
                raise OperationFailure(f"{operator} chars must evaluate to a string")
        mode = {"$trim": "both", "$ltrim": "left", "$rtrim": "right"}[operator]
        return _trim_string(value, chars, mode=mode)

    if operator in {"$replaceOne", "$replaceAll"}:
        if not isinstance(spec, dict) or not {"input", "find", "replacement"} <= set(spec):
            raise OperationFailure(f"{operator} requires input, find and replacement")
        source = evaluate_expression(document, spec["input"], variables)
        find = evaluate_expression(document, spec["find"], variables)
        replacement = evaluate_expression(document, spec["replacement"], variables)
        if source is None or find is None or replacement is None:
            return None
        if not isinstance(source, str) or not isinstance(find, str) or not isinstance(replacement, str):
            raise OperationFailure(f"{operator} requires string input, find and replacement")
        return source.replace(find, replacement, 1 if operator == "$replaceOne" else -1)

    if operator == "$strcasecmp":
        args = require_expression_args(operator, spec, 2, 2)
        left = evaluate_expression(document, args[0], variables)
        right = evaluate_expression(document, args[1], variables)
        if left is None or right is None:
            return None
        if not isinstance(left, str) or not isinstance(right, str):
            raise OperationFailure("$strcasecmp requires string arguments")
        return _compare_strings_case_insensitive(left, right)

    if operator in {"$substr", "$substrBytes", "$substrCP"}:
        args = require_expression_args(operator, spec, 3, 3)
        source = evaluate_expression(document, args[0], variables)
        start = evaluate_expression(document, args[1], variables)
        length = evaluate_expression(document, args[2], variables)
        if source is None:
            return ""
        if not isinstance(source, str):
            raise OperationFailure(f"{operator} requires the first argument to evaluate to a string")
        if not isinstance(start, int) or isinstance(start, bool):
            raise OperationFailure(f"{operator} start must be an integer")
        if not isinstance(length, int) or isinstance(length, bool):
            raise OperationFailure(f"{operator} length must be an integer")
        if operator == "$substrCP":
            return _substr_code_points(source, start, length)
        return _substr_string(source, start, length)

    if operator in {"$strLenBytes", "$strLenCP"}:
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression_with_missing(document, args[0], variables)
        if value is missing_sentinel or value is None:
            raise OperationFailure(f"{operator} requires a string argument, found: null")
        if not isinstance(value, str):
            raise OperationFailure(f"{operator} requires a string argument")
        return len(value.encode("utf-8")) if operator == "$strLenBytes" else len(value)

    if operator in {"$toLower", "$toUpper"}:
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression(document, args[0], variables)
        if value is None:
            return ""
        if not isinstance(value, str):
            raise OperationFailure(f"{operator} requires a string argument")
        return value.lower() if operator == "$toLower" else value.upper()

    if operator == "$split":
        args = require_expression_args(operator, spec, 2, 2)
        source = evaluate_expression(document, args[0], variables)
        delimiter = evaluate_expression(document, args[1], variables)
        if source is None or delimiter is None:
            return None
        if not isinstance(source, str) or not isinstance(delimiter, str):
            raise OperationFailure("$split requires string arguments")
        if delimiter == "":
            raise OperationFailure("$split delimiter must not be an empty string")
        return source.split(delimiter)

    if operator == "$concatArrays":
        args = require_expression_args(operator, spec, 1, None)
        result: list[Any] = []
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is None:
                return None
            result.extend(deepcopy(_require_array(operator, value)))
        return result

    if operator == "$reverseArray":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression(document, args[0], variables)
        if value is None:
            return None
        return list(reversed(deepcopy(_require_array(operator, value))))

    if operator in {"$allElementsTrue", "$anyElementTrue"}:
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        value = evaluate_expression(document, args[0], variables)
        if value is None:
            return None
        values = _require_array(operator, value)
        predicate = all if operator == "$allElementsTrue" else any
        return predicate(dialect.policy.expression_truthy(item) for item in values)

    if operator == "$setUnion":
        args = require_expression_args(operator, spec, 1, None)
        result: list[Any] = []
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is None:
                return None
            _append_unique_values(result, _require_array(operator, value), dialect=dialect)
        return result

    if operator in {"$setDifference", "$setIntersection"}:
        args = require_expression_args(operator, spec, 2, 2)
        left = evaluate_expression(document, args[0], variables)
        right = evaluate_expression(document, args[1], variables)
        if left is None or right is None:
            return None
        left_values = _require_array(operator, left)
        right_values = _require_array(operator, right)
        left_unique: list[Any] = []
        right_unique: list[Any] = []
        _append_unique_values(left_unique, left_values, dialect=dialect)
        _append_unique_values(right_unique, right_values, dialect=dialect)
        result: list[Any] = []
        for item in left_unique:
            in_right = any(QueryEngine._values_equal(item, candidate, dialect=dialect) for candidate in right_unique)
            if operator == "$setDifference" and not in_right:
                result.append(deepcopy(item))
            if operator == "$setIntersection" and in_right:
                result.append(deepcopy(item))
        return result

    if operator in {"$setEquals", "$setIsSubset"}:
        args = require_expression_args(operator, spec, 2, 2 if operator == "$setIsSubset" else None)
        sets: list[list[Any]] = []
        for item in args:
            value = evaluate_expression(document, item, variables)
            if value is None:
                return None
            normalized: list[Any] = []
            _append_unique_values(normalized, _require_array(operator, value), dialect=dialect)
            sets.append(normalized)
        base = sets[0]
        if operator == "$setEquals":
            for candidate in sets[1:]:
                if len(base) != len(candidate):
                    return False
                if any(not any(QueryEngine._values_equal(item, other, dialect=dialect) for other in candidate) for item in base):
                    return False
            return True
        for candidate in sets[1:]:
            if any(not any(QueryEngine._values_equal(item, other, dialect=dialect) for other in candidate) for item in base):
                return False
        return True

    if operator == "$slice":
        args = require_expression_args(operator, spec, 2, 3)
        raw_value = evaluate_expression(document, args[0], variables)
        if raw_value is None:
            return None
        values = _require_array(operator, raw_value)
        return _slice_array(values, args, document, variables, evaluate_expression=evaluate_expression)

    if operator == "$map":
        if not isinstance(spec, dict) or "input" not in spec or "in" not in spec:
            raise OperationFailure("$map requires input and in")
        source = evaluate_expression(document, spec["input"], variables)
        if source is None:
            return None
        source = _require_array(operator, source)
        alias = spec.get("as", "this")
        if not isinstance(alias, str):
            raise OperationFailure("$map as must be a string")
        result = []
        for item in source:
            scoped = dict(variables or {})
            scoped_item = _copy_if_mutable(item)
            scoped[alias] = scoped_item
            if alias == "this":
                scoped["this"] = scoped_item
            else:
                scoped.pop("this", None)
            result.append(evaluate_expression(document, spec["in"], scoped))
        return result

    if operator == "$filter":
        if not isinstance(spec, dict) or "input" not in spec or "cond" not in spec:
            raise OperationFailure("$filter requires input and cond")
        source = evaluate_expression(document, spec["input"], variables)
        if source is None:
            return None
        source = _require_array(operator, source)
        alias = spec.get("as", "this")
        if not isinstance(alias, str):
            raise OperationFailure("$filter as must be a string")
        result = []
        for item in source:
            scoped = dict(variables or {})
            scoped[alias] = item
            scoped["this"] = item
            if dialect.policy.expression_truthy(evaluate_expression(document, spec["cond"], scoped)):
                result.append(deepcopy(item))
        return result

    if operator == "$reduce":
        if not isinstance(spec, dict) or not {"input", "initialValue", "in"} <= set(spec):
            raise OperationFailure("$reduce requires input, initialValue and in")
        source = evaluate_expression(document, spec["input"], variables)
        if source is None:
            return None
        source = _require_array(operator, source)
        accumulated = evaluate_expression(document, spec["initialValue"], variables)
        for item in source:
            scoped = dict(variables or {})
            scoped["value"] = accumulated
            scoped["this"] = item
            accumulated = evaluate_expression(document, spec["in"], scoped)
        return accumulated

    if operator == "$objectToArray":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        raw_value = evaluate_expression(document, args[0], variables)
        if raw_value is None:
            return None
        if not isinstance(raw_value, dict):
            raise OperationFailure("$objectToArray requires a document input")
        return [{"k": key, "v": deepcopy(value)} for key, value in raw_value.items()]

    if operator == "$arrayToObject":
        args = require_expression_args(operator, [spec] if not isinstance(spec, list) else spec, 1, 1)
        raw_values = evaluate_expression(document, args[0], variables)
        if raw_values is None:
            return None
        values = _require_array(operator, raw_values)
        result: dict[str, Any] = {}
        for item in values:
            if isinstance(item, list) and len(item) == 2:
                key, value = item
            elif isinstance(item, dict) and set(item) == {"k", "v"}:
                key, value = item["k"], item["v"]
            else:
                raise OperationFailure("$arrayToObject requires [key, value] pairs or {k, v} documents")
            if not isinstance(key, str):
                raise OperationFailure("$arrayToObject keys must be strings")
            result[key] = deepcopy(value)
        return result

    if operator == "$zip":
        if not isinstance(spec, dict) or "inputs" not in spec:
            raise OperationFailure("$zip requires inputs")
        raw_inputs = evaluate_expression(document, spec["inputs"], variables)
        if raw_inputs is None:
            return None
        inputs = _require_array(operator, raw_inputs)
        arrays: list[list[Any]] = []
        for item in inputs:
            resolved = evaluate_expression(document, item, variables) if not isinstance(item, list) else item
            if resolved is None or resolved is missing_sentinel:
                return None
            arrays.append(_require_array(operator, resolved))
        use_longest = evaluate_expression(document, spec["useLongestLength"], variables) if "useLongestLength" in spec else False
        if not isinstance(use_longest, bool):
            raise OperationFailure("$zip useLongestLength must evaluate to a boolean")
        defaults = evaluate_expression(document, spec["defaults"], variables) if "defaults" in spec else None
        if defaults is not None and not isinstance(defaults, list):
            raise OperationFailure("$zip defaults must evaluate to an array")
        if defaults is not None and len(defaults) != len(arrays):
            raise OperationFailure("$zip defaults length must match inputs length")
        target_length = max((len(array) for array in arrays), default=0) if use_longest else min((len(array) for array in arrays), default=0)
        result: list[list[Any]] = []
        for index in range(target_length):
            row: list[Any] = []
            for array_index, array in enumerate(arrays):
                if index < len(array):
                    row.append(deepcopy(array[index]))
                elif defaults is not None:
                    row.append(deepcopy(defaults[array_index]))
                else:
                    row.append(None)
            result.append(row)
        return result

    if operator == "$indexOfArray":
        args = require_expression_args(operator, spec, 2, 4)
        values = evaluate_expression(document, args[0], variables)
        if values is None:
            return None
        values = _require_array(operator, values)
        needle = evaluate_expression(document, args[1], variables)
        start = 0
        end = len(values)
        if len(args) >= 3:
            start = evaluate_expression(document, args[2], variables)
            if not isinstance(start, int) or isinstance(start, bool):
                raise OperationFailure("$indexOfArray start must be an integer")
        if len(args) == 4:
            end = evaluate_expression(document, args[3], variables)
            if not isinstance(end, int) or isinstance(end, bool):
                raise OperationFailure("$indexOfArray end must be an integer")
        for index, value in enumerate(values[max(start, 0):max(end, 0)], start=max(start, 0)):
            if QueryEngine._values_equal(value, needle, dialect=dialect):
                return index
        return -1

    if operator in {"$indexOfBytes", "$indexOfCP"}:
        args = require_expression_args(operator, spec, 2, 4)
        source = evaluate_expression(document, args[0], variables)
        substring = evaluate_expression(document, args[1], variables)
        if source is None:
            return None
        if not isinstance(source, str) or not isinstance(substring, str):
            raise OperationFailure(f"{operator} requires string arguments")
        start = evaluate_expression(document, args[2], variables) if len(args) >= 3 else None
        end = evaluate_expression(document, args[3], variables) if len(args) == 4 else None
        if operator == "$indexOfBytes":
            source_bytes = source.encode("utf-8")
            substring_bytes = substring.encode("utf-8")
            start_index, end_index = _normalize_index_bounds(operator, start, end, len(source_bytes))
            if start_index > end_index:
                return -1
            return source_bytes.find(substring_bytes, start_index, end_index)
        start_index, end_index = _normalize_index_bounds(operator, start, end, len(source))
        if start_index > end_index:
            return -1
        return source.find(substring, start_index, end_index)

    if operator in {"$regexMatch", "$regexFind", "$regexFindAll"}:
        if not isinstance(spec, dict) or "input" not in spec or "regex" not in spec:
            raise OperationFailure(f"{operator} requires input and regex")
        input_value = evaluate_expression_with_missing(document, spec["input"], variables)
        if input_value is missing_sentinel or input_value is None:
            if operator == "$regexMatch":
                return False
            if operator == "$regexFindAll":
                return []
            return None
        if not isinstance(input_value, str):
            raise OperationFailure(f"{operator} input must resolve to a string")
        regex_value = evaluate_expression(document, spec["regex"], variables)
        options_value = evaluate_expression(document, spec["options"], variables) if "options" in spec else None
        compiled = _compile_aggregation_regex(regex_value, options_value, operator=operator)
        if operator == "$regexMatch":
            return compiled.search(input_value) is not None
        if operator == "$regexFind":
            match = compiled.search(input_value)
            if match is None:
                return None
            return _build_regex_match_result(match)
        return [_build_regex_match_result(match) for match in compiled.finditer(input_value)]

    if operator == "$sortArray":
        if not isinstance(spec, dict) or "input" not in spec or "sortBy" not in spec:
            raise OperationFailure("$sortArray requires input and sortBy")
        input_value = evaluate_expression(document, spec["input"], variables)
        if input_value is None:
            return None
        values = deepcopy(_require_array(operator, input_value))
        sort_by = _normalize_sort_array_spec(spec["sortBy"])
        if isinstance(sort_by, int):
            return sorted(values, key=cmp_to_key(dialect.policy.compare_values), reverse=sort_by == -1)
        return sort_documents(values, sort_by, dialect=dialect)

    raise OperationFailure(f"Unsupported array/string expression operator: {operator}")


def _require_array(operator: str, value: object) -> list[Any]:
    if not isinstance(value, list):
        raise OperationFailure(f"{operator} requires array arguments")
    return value


def _append_unique_values(
    target: list[Any],
    values: list[Any],
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> None:
    for value in values:
        if any(QueryEngine._values_equal(value, existing, dialect=dialect) for existing in target):
            continue
        target.append(deepcopy(value))


def _slice_array(
    value: list[Any],
    spec: list[object],
    document: Document,
    variables: dict[str, Any] | None,
    *,
    evaluate_expression: ExpressionEvaluator,
) -> list[Any]:
    if len(spec) == 2:
        count = evaluate_expression(document, spec[1], variables)
        if not isinstance(count, int) or isinstance(count, bool):
            raise OperationFailure("$slice count must be an integer")
        if count == 0:
            return []
        if count > 0:
            return deepcopy(value[:count])
        return deepcopy(value[count:])

    position = evaluate_expression(document, spec[1], variables)
    count = evaluate_expression(document, spec[2], variables)
    if not isinstance(position, int) or isinstance(position, bool):
        raise OperationFailure("$slice position must be an integer")
    if not isinstance(count, int) or isinstance(count, bool):
        raise OperationFailure("$slice count must be an integer")
    if count < 0:
        raise OperationFailure("$slice count must be a non-negative integer")
    start = position if position >= 0 else len(value) + position
    start = min(max(start, 0), len(value))
    return deepcopy(value[start : start + count])


def _compare_strings_case_insensitive(left: str, right: str) -> int:
    normalized_left = left.lower()
    normalized_right = right.lower()
    if normalized_left < normalized_right:
        return -1
    if normalized_left > normalized_right:
        return 1
    return 0


def _substr_string(value: str, start: int, length: int) -> str:
    if start < 0:
        return ""
    raw = value.encode("utf-8")
    if start > len(raw):
        return ""
    end = len(raw) if length < 0 else min(len(raw), start + length)
    boundaries = {0, len(raw)}
    offset = 0
    for char in value:
        boundaries.add(offset)
        offset += len(char.encode("utf-8"))
        boundaries.add(offset)
    if start not in boundaries or end not in boundaries:
        raise OperationFailure("$substr byte offsets must align with UTF-8 code point boundaries")
    return raw[start:end].decode("utf-8")


def _substr_code_points(value: str, start: int, length: int) -> str:
    if start < 0 or start > len(value):
        return ""
    end = len(value) if length < 0 else min(len(value), start + length)
    return value[start:end]


def _normalize_index_bounds(operator: str, start: Any, end: Any, upper_bound: int) -> tuple[int, int]:
    if start is None:
        start_index = 0
    else:
        if not isinstance(start, int) or isinstance(start, bool):
            raise OperationFailure(f"{operator} start must be an integer")
        if start < 0:
            raise OperationFailure(f"{operator} start must be a non-negative integer")
        start_index = start
    if end is None:
        end_index = upper_bound
    else:
        if not isinstance(end, int) or isinstance(end, bool):
            raise OperationFailure(f"{operator} end must be an integer")
        if end < 0:
            raise OperationFailure(f"{operator} end must be a non-negative integer")
        end_index = end
    if start_index > upper_bound:
        return upper_bound, upper_bound
    return start_index, min(end_index, upper_bound)


def _trim_string(value: str, chars: str | None, *, mode: str) -> str:
    if chars is None:
        start = 0
        end = len(value)
        if mode in {"left", "both"}:
            while start < end and (value[start].isspace() or value[start] == "\x00"):
                start += 1
        if mode in {"right", "both"}:
            while end > start and (value[end - 1].isspace() or value[end - 1] == "\x00"):
                end -= 1
        return value[start:end]
    if mode == "left":
        return value.lstrip(chars)
    if mode == "right":
        return value.rstrip(chars)
    return value.strip(chars)


def _compile_aggregation_regex(regex_value: Any, options_value: Any, *, operator: str) -> re.Pattern[str]:
    supported = {"i": re.IGNORECASE, "m": re.MULTILINE, "s": re.DOTALL, "x": re.VERBOSE}
    flags = 0
    pattern: str
    if options_value is not None and not isinstance(options_value, str):
        raise OperationFailure(f"{operator} options must be a string")
    if isinstance(regex_value, Regex):
        if options_value not in {None, ""}:
            raise OperationFailure(f"{operator} cannot specify options in both regex and options")
        return regex_value.compile()
    if isinstance(regex_value, re.Pattern):
        if options_value not in {None, ""}:
            raise OperationFailure(f"{operator} cannot specify options in both regex and options")
        disallowed_flags = regex_value.flags & (re.DOTALL | re.VERBOSE)
        if disallowed_flags:
            raise OperationFailure(f"{operator} regex patterns only support embedded i and m options")
        flags = regex_value.flags & (re.IGNORECASE | re.MULTILINE)
        pattern = regex_value.pattern
    elif isinstance(regex_value, str):
        pattern = regex_value
    else:
        raise OperationFailure(f"{operator} regex must resolve to a string or regex pattern")
    if options_value:
        for option in options_value:
            if option not in supported:
                raise OperationFailure(f"Unsupported regex option: {option}")
            flags |= supported[option]
    return re.compile(pattern, flags)


def _build_regex_match_result(match: re.Match[str]) -> dict[str, Any]:
    return {
        "match": match.group(0),
        "idx": match.start(),
        "captures": list(match.groups(default=None)),
    }


def _normalize_sort_array_spec(spec: object) -> SortSpec | int:
    if spec in (1, -1) and not isinstance(spec, bool):
        return spec
    if not isinstance(spec, dict):
        raise OperationFailure("$sortArray sortBy must be 1, -1 or a document")
    sort_spec: SortSpec = []
    for field, direction in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$sortArray sort fields must be strings")
        if isinstance(direction, bool):
            raise OperationFailure("$sortArray sort directions must be 1 or -1")
        if direction not in (1, -1):
            raise OperationFailure("$sortArray sort directions must be 1 or -1")
        sort_spec.append((field, direction))
    return sort_spec
