from __future__ import annotations

import calendar
import datetime
import re

from collections.abc import Callable
from typing import Any
from zoneinfo import ZoneInfo

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.bson_scalars import (
    INT64_MAX,
    BsonInt64,
    bson_datetime_from_milliseconds,
    bson_datetime_milliseconds,
    exact_bson_int64,
)
from mongoeco.core.server_errors import server_error
from mongoeco.core.bson_values import is_bson_string
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Timestamp, is_object_id_like, normalize_object_id


type ExpressionEvaluator = Callable[[Document, object, dict[str, Any] | None], Any]


DATE_EXPRESSION_OPERATORS = frozenset(
    {
        "$dateTrunc",
        "$dateAdd",
        "$dateSubtract",
        "$dateToString",
        "$dateToParts",
        "$dateFromString",
        "$dateFromParts",
        "$year",
        "$month",
        "$dayOfMonth",
        "$dayOfWeek",
        "$dayOfYear",
        "$hour",
        "$minute",
        "$second",
        "$millisecond",
        "$isoDayOfWeek",
        "$week",
        "$isoWeek",
        "$isoWeekYear",
        "$dateDiff",
    }
)


def evaluate_date_expression(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    missing_sentinel: object,
) -> Any:
    if operator == "$dateTrunc":
        if not isinstance(spec, dict) or "date" not in spec or "unit" not in spec:
            raise OperationFailure("$dateTrunc requires date and unit")
        value = evaluate_expression(document, spec["date"], variables)
        if value is None:
            return None
        value = _date_operand(value)
        if not isinstance(value, datetime.datetime):
            raise OperationFailure("$dateTrunc requires a datetime value")
        unit = evaluate_expression(document, spec["unit"], variables)
        if unit is None:
            return None
        if not isinstance(unit, str):
            raise OperationFailure("$dateTrunc unit must be a string")
        bin_size = evaluate_expression(document, spec["binSize"], variables) if "binSize" in spec else 1
        if bin_size is None:
            return None
        bin_size = exact_bson_int64(bin_size)
        if bin_size is None:
            message = "$dateTrunc binSize must be an integer"
            raise server_error(message, code=5439017)
        if bin_size <= 0:
            message = "$dateTrunc binSize must be positive"
            raise server_error(message, code=5439018)
        fixed_milliseconds = {
            "millisecond": 1,
            "second": 1000,
            "minute": 60000,
            "hour": 3600000,
            "day": 86400000,
            "week": 604800000,
        }
        if (
            unit in fixed_milliseconds
            and bin_size > INT64_MAX // fixed_milliseconds[unit]
        ):
            message = "$dateTrunc binSize causes an overflow"
            raise server_error(message, code=5439006)
        timezone = evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None
        if "timezone" in spec and timezone is None:
            return None
        timezone = _resolve_timezone(operator, timezone)
        start_of_week = evaluate_expression(document, spec["startOfWeek"], variables) if "startOfWeek" in spec else "sunday"
        if unit == "week" and start_of_week is None:
            return None
        if unit != "week":
            start_of_week = "sunday"
        localized = _localize_datetime(value, timezone)
        return _restore_datetime_timezone(
            _truncate_datetime(localized, unit, bin_size, start_of_week), value
        )

    if operator in {"$dateAdd", "$dateSubtract"}:
        if not isinstance(spec, dict) or not {"startDate", "unit", "amount"} <= set(spec):
            raise OperationFailure(f"{operator} requires startDate, unit and amount")
        value = evaluate_expression(document, spec["startDate"], variables)
        if value is None:
            return None
        value = _date_operand(value)
        if not isinstance(value, datetime.datetime):
            raise OperationFailure(f"{operator} requires a datetime startDate")
        unit = evaluate_expression(document, spec["unit"], variables)
        if unit is None:
            return None
        unit = _require_date_unit(operator, unit)
        amount = evaluate_expression(document, spec["amount"], variables)
        if amount is None:
            return None
        amount = exact_bson_int64(amount)
        if amount is None:
            message = f"{operator} amount must be an integer"
            raise server_error(message, code=5166405)
        if (
            "timezone" in spec
            and evaluate_expression(document, spec["timezone"], variables) is None
        ):
            return None
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        localized = _localize_datetime(value, timezone)
        signed_amount = amount if operator == "$dateAdd" else -amount
        try:
            updated = _add_date_unit(localized, unit, signed_amount)
        except (OverflowError, ValueError) as error:
            message = f"{operator} date arithmetic exceeds the supported range"
            raise server_error(message, code=5976500) from error
        return _restore_datetime_timezone(updated, value)

    if operator == "$dateToString":
        if not isinstance(spec, dict) or "date" not in spec:
            raise OperationFailure("$dateToString requires date")
        timezone_value = (
            evaluate_expression(document, spec["timezone"], variables)
            if "timezone" in spec
            else "UTC"
        )
        timezone = (
            _resolve_timezone(operator, timezone_value)
            if timezone_value is not None
            else None
        )
        fmt = (
            evaluate_expression(document, spec["format"], variables)
            if "format" in spec
            else _default_date_to_string_format(timezone or datetime.UTC)
        )
        _validate_date_format(fmt)
        value = evaluate_expression_with_missing(document, spec["date"], variables)
        if value is missing_sentinel or value is None:
            return (
                evaluate_expression(document, spec["onNull"], variables)
                if "onNull" in spec
                else None
            )
        if timezone is None or fmt is None:
            return None
        value = _date_operand(value)
        if not isinstance(value, datetime.datetime):
            raise OperationFailure("$dateToString requires a date input")
        return _mongo_format_datetime(_localize_datetime(value, timezone), fmt)

    if operator == "$dateToParts":
        if not isinstance(spec, dict) or "date" not in spec:
            raise OperationFailure("$dateToParts requires date")
        value = evaluate_expression_with_missing(document, spec["date"], variables)
        if value is missing_sentinel or value is None:
            return None
        value = _date_operand(value)
        if not isinstance(value, datetime.datetime):
            raise OperationFailure("$dateToParts requires a date input")
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        localized = _localize_datetime(value, timezone)
        iso8601 = evaluate_expression(document, spec["iso8601"], variables) if "iso8601" in spec else False
        if not isinstance(iso8601, bool):
            raise OperationFailure("$dateToParts iso8601 must evaluate to a boolean")
        if iso8601:
            iso_parts = localized.isocalendar()
            return {
                "isoWeekYear": iso_parts.year,
                "isoWeek": iso_parts.week,
                "isoDayOfWeek": iso_parts.weekday,
                "hour": localized.hour,
                "minute": localized.minute,
                "second": localized.second,
                "millisecond": localized.microsecond // 1000,
            }
        return {
            "year": localized.year,
            "month": localized.month,
            "day": localized.day,
            "hour": localized.hour,
            "minute": localized.minute,
            "second": localized.second,
            "millisecond": localized.microsecond // 1000,
        }

    if operator == "$dateFromString":
        if not isinstance(spec, dict) or "dateString" not in spec:
            raise OperationFailure("$dateFromString requires dateString")
        raw_value = evaluate_expression_with_missing(document, spec["dateString"], variables)
        if raw_value is missing_sentinel or raw_value is None:
            return evaluate_expression(document, spec["onNull"], variables) if "onNull" in spec else None
        if not isinstance(raw_value, str):
            raise OperationFailure("$dateFromString dateString must evaluate to a string")
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        try:
            if "format" in spec:
                fmt = evaluate_expression(document, spec["format"], variables)
                if not isinstance(fmt, str):
                    raise OperationFailure("$dateFromString format must evaluate to a string")
                parsed = datetime.datetime.strptime(raw_value, _python_strptime_format(operator, fmt))
            else:
                parsed = datetime.datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except OperationFailure:
            raise
        except Exception as exc:
            if "onError" in spec:
                return evaluate_expression(document, spec["onError"], variables)
            raise OperationFailure("$dateFromString could not parse dateString") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone)
        return _to_utc_naive(parsed)

    if operator == "$dateFromParts":
        if not isinstance(spec, dict):
            raise OperationFailure("$dateFromParts requires a document specification")
        return _build_date_from_parts(
            operator,
            spec,
            document,
            variables,
            evaluate_expression=evaluate_expression,
        )

    if operator in {
        "$year",
        "$month",
        "$dayOfMonth",
        "$dayOfWeek",
        "$dayOfYear",
        "$hour",
        "$minute",
        "$second",
        "$millisecond",
        "$isoDayOfWeek",
    }:
        localized = _evaluate_localized_date_operand(
            operator,
            document,
            spec,
            variables,
            evaluate_expression=evaluate_expression,
            evaluate_expression_with_missing=evaluate_expression_with_missing,
            missing_sentinel=missing_sentinel,
        )
        if localized is None:
            return None
        if operator == "$year":
            return localized.year
        if operator == "$month":
            return localized.month
        if operator == "$dayOfMonth":
            return localized.day
        if operator == "$dayOfWeek":
            return ((localized.weekday() + 1) % 7) + 1
        if operator == "$dayOfYear":
            return int(localized.strftime("%j"))
        if operator == "$hour":
            return localized.hour
        if operator == "$minute":
            return localized.minute
        if operator == "$second":
            return localized.second
        if operator == "$millisecond":
            return localized.microsecond // 1000
        return localized.isoweekday()

    if operator in {"$week", "$isoWeek", "$isoWeekYear"}:
        localized = _evaluate_localized_date_operand(
            operator,
            document,
            spec,
            variables,
            evaluate_expression=evaluate_expression,
            evaluate_expression_with_missing=evaluate_expression_with_missing,
            missing_sentinel=missing_sentinel,
        )
        if localized is None:
            return None
        if operator == "$week":
            return int(localized.strftime("%U"))
        iso_parts = localized.isocalendar()
        if operator == "$isoWeek":
            return iso_parts.week
        return iso_parts.year

    if operator == "$dateDiff":
        if not isinstance(spec, dict) or not {"startDate", "endDate", "unit"} <= set(spec):
            raise OperationFailure("$dateDiff requires startDate, endDate and unit")
        start_date = evaluate_expression(document, spec["startDate"], variables)
        end_date = evaluate_expression(document, spec["endDate"], variables)
        if start_date is None or end_date is None:
            return None
        start_date, end_date = _date_operand(start_date), _date_operand(end_date)
        if not isinstance(start_date, datetime.datetime) or not isinstance(end_date, datetime.datetime):
            raise OperationFailure("$dateDiff requires datetime startDate and endDate")
        unit = evaluate_expression(document, spec["unit"], variables)
        if unit is None:
            return None
        unit = _require_date_unit(operator, unit)
        if (
            "timezone" in spec
            and evaluate_expression(document, spec["timezone"], variables) is None
        ):
            return None
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        start_of_week = evaluate_expression(document, spec["startOfWeek"], variables) if "startOfWeek" in spec else "sunday"
        if unit == "week":
            if start_of_week is None:
                return None
            _week_day(start_of_week)
        else:
            start_of_week = "sunday"
        localized_start = _localize_datetime(start_date, timezone)
        localized_end = _localize_datetime(end_date, timezone)
        return BsonInt64(_date_diff_units(
            localized_start, localized_end, unit, start_of_week=start_of_week
        ))

    raise OperationFailure(f"Unsupported date expression operator: {operator}")


def _date_operand(value):
    if isinstance(value, Timestamp):
        return bson_datetime_from_milliseconds(value.time * 1000)
    if is_object_id_like(value):
        return bson_datetime_from_milliseconds(
            int(str(normalize_object_id(value))[:8], 16) * 1000
        )
    return value


def _week_day(value):
    weekdays = {"sun": 6, "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5}
    if (
        isinstance(value, str)
        and value.lower()[:3] in weekdays
        and value.lower()
        in {
            "sun",
            "sunday",
            "mon",
            "monday",
            "tue",
            "tuesday",
            "wed",
            "wednesday",
            "thu",
            "thursday",
            "fri",
            "friday",
            "sat",
            "saturday",
        }
    ):
        return weekdays[value.lower()[:3]]
    message = "startOfWeek must be a valid day of the week"
    raise server_error(message, code=5439016)


def _truncate_datetime(value: datetime.datetime, unit: str, bin_size: int, start_of_week: str) -> datetime.datetime:
    if bin_size <= 0:
        raise OperationFailure("$dateTrunc binSize must be a positive integer")

    if unit == "millisecond":
        anchor = bson_datetime_milliseconds(
            datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)
        )
        offset = bson_datetime_milliseconds(value) - anchor
        result = bson_datetime_from_milliseconds(
            anchor + (offset // bin_size) * bin_size
        )
        return (
            result
            if value.tzinfo is None
            else result.replace(tzinfo=datetime.UTC).astimezone(value.tzinfo)
        )

    if unit in {"year", "quarter", "month"}:
        width = bin_size * {"year": 12, "quarter": 3, "month": 1}[unit]
        months = (value.year - 2000) * 12 + value.month - 1
        year_delta, month = divmod((months // width) * width, 12)
        return value.replace(
            year=2000 + year_delta,
            month=month + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if unit == "week":
        weekday_index = _week_day(start_of_week)
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = (day_start.weekday() - weekday_index) % 7
        week_start = day_start - datetime.timedelta(days=delta)
        anchor = datetime.datetime(2000, 1, 1, tzinfo=value.tzinfo)
        anchor += datetime.timedelta(days=(weekday_index - anchor.weekday()) % 7)
        weeks = (week_start - anchor).days // 7
        return anchor + datetime.timedelta(weeks=(weeks // bin_size) * bin_size)

    if unit == "day":
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = datetime.datetime(2000, 1, 1, tzinfo=value.tzinfo)
        days = (day_start - anchor).days
        return anchor + datetime.timedelta(days=(days // bin_size) * bin_size)

    if unit in {"hour", "minute", "second"}:
        width = {"hour": 3600000, "minute": 60000, "second": 1000}[unit] * bin_size
        anchor = bson_datetime_milliseconds(
            datetime.datetime(2000, 1, 1, tzinfo=value.tzinfo)
        )
        offset = bson_datetime_milliseconds(value) - anchor
        result = bson_datetime_from_milliseconds(anchor + (offset // width) * width)
        return (
            result
            if value.tzinfo is None
            else result.replace(tzinfo=datetime.UTC).astimezone(value.tzinfo)
        )

    raise OperationFailure(f"Unsupported $dateTrunc unit: {unit}")


def _format_timezone_offset(value: datetime.datetime) -> str:
    offset = value.utcoffset() or datetime.timedelta()
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"{sign}{hours:02d}{minutes:02d}"


_DATE_FORMATTERS = {
    "Y": lambda value: f"{value.year:04d}",
    "m": lambda value: f"{value.month:02d}",
    "d": lambda value: f"{value.day:02d}",
    "H": lambda value: f"{value.hour:02d}",
    "M": lambda value: f"{value.minute:02d}",
    "S": lambda value: f"{value.second:02d}",
    "L": lambda value: f"{value.microsecond // 1000:03d}",
    "z": _format_timezone_offset,
    "G": lambda value: f"{value.isocalendar().year:04d}",
    "V": lambda value: f"{value.isocalendar().week:02d}",
    "u": lambda value: str(value.isocalendar().weekday),
    "j": lambda value: value.strftime("%j"),
    "U": lambda value: value.strftime("%U"),
    "w": lambda value: str(((value.weekday() + 1) % 7) + 1),
    "Z": lambda value: str(
        int((value.utcoffset() or datetime.timedelta()).total_seconds() // 60)
    ),
    "%": lambda _value: "%",
}
_DATE_FORMAT_TOKEN = re.compile(r"%(.)|%$", re.DOTALL)


def _validate_date_format(fmt):
    if fmt is None:
        return
    if not is_bson_string(fmt):
        message = "$dateToString format must be a string"
        raise server_error(message, code=18533)
    for match in _DATE_FORMAT_TOKEN.finditer(fmt):
        token = match.group(1)
        if token is None:
            message = "Unmatched '%' at end of format string"
            raise server_error(message, code=18535)
        if token not in _DATE_FORMATTERS:
            message = f"Invalid format character '%{token}' in format string"
            raise server_error(message, code=18536)


def _mongo_format_datetime(value: datetime.datetime, fmt: str) -> str:
    _validate_date_format(fmt)
    return _DATE_FORMAT_TOKEN.sub(
        lambda match: _DATE_FORMATTERS[match.group(1)](value), fmt
    )


def validate_date_options(operator, spec, *, is_constant, evaluate_constant):
    if not isinstance(spec, dict):
        return
    if operator in {"$dateAdd", "$dateSubtract", "$dateTrunc", "$dateDiff"}:
        required = {
            "$dateAdd": ("startDate", "unit", "amount"),
            "$dateSubtract": ("startDate", "unit", "amount"),
            "$dateTrunc": ("date", "unit", "binSize"),
            "$dateDiff": ("startDate", "endDate", "unit"),
        }[operator]
        if any(
            name in spec
            and is_constant(spec[name])
            and evaluate_constant(spec[name]) is None
            for name in (*required, "timezone")
        ):
            return
    if "timezone" in spec and is_constant(spec["timezone"]):
        value = evaluate_constant(spec["timezone"])
        if value is not None:
            _resolve_timezone(operator, value)
    if "unit" in spec and is_constant(spec["unit"]):
        unit = evaluate_constant(spec["unit"])
        if unit is not None:
            _require_date_unit(operator, unit)
            if (
                unit == "week"
                and "startOfWeek" in spec
                and is_constant(spec["startOfWeek"])
            ):
                value = evaluate_constant(spec["startOfWeek"])
                if value is not None:
                    _week_day(value)
    if operator == "$dateToString" and "format" in spec and is_constant(spec["format"]):
        _validate_date_format(evaluate_constant(spec["format"]))


def _python_strptime_format(operator: str, fmt: str) -> str:
    supported_tokens = {
        "%Y": "%Y",
        "%m": "%m",
        "%d": "%d",
        "%H": "%H",
        "%M": "%M",
        "%S": "%S",
        "%L": "%f",
        "%z": "%z",
    }
    result = ""
    index = 0
    while index < len(fmt):
        if fmt[index] == "%" and index + 1 < len(fmt):
            token = fmt[index : index + 2]
            if token not in supported_tokens:
                raise OperationFailure(f"{operator} format token is not supported")
            result += supported_tokens[token]
            index += 2
            continue
        result += fmt[index]
        index += 1
    return result


def _default_date_to_string_format(timezone: datetime.tzinfo) -> str:
    return "%Y-%m-%dT%H:%M:%S.%LZ" if timezone == datetime.UTC else "%Y-%m-%dT%H:%M:%S.%L"


def _to_utc_naive(value: datetime.datetime) -> datetime.datetime:
    aware = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return aware.replace(tzinfo=None)


def _require_int_part(operator: str, name: str, value: Any) -> int:
    value = exact_bson_int64(value)
    if value is None:
        message = f"{operator} {name} must evaluate to an integer"
        raise server_error(message, code=40515)
    return value


def _require_int_part_in_range(
    operator: str,
    name: str,
    value: Any,
    *,
    minimum: int,
    maximum: int,
) -> int:
    integer = _require_int_part(operator, name, value)
    if integer < minimum or integer > maximum:
        raise OperationFailure(f"{operator} {name} must be in range [{minimum}, {maximum}]")
    return integer


def _build_date_from_parts(
    operator: str,
    spec: dict[str, Any],
    document: Document,
    variables: dict[str, Any] | None,
    *,
    evaluate_expression: ExpressionEvaluator,
) -> datetime.datetime:
    if (
        "timezone" in spec
        and evaluate_expression(document, spec["timezone"], variables) is None
    ):
        return None
    timezone = _resolve_timezone(
        operator,
        evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
    )

    standard_keys = {"year", "month", "day"}
    iso_keys = {"isoWeekYear", "isoWeek", "isoDayOfWeek"}
    has_standard = any(key in spec for key in standard_keys)
    has_iso = any(key in spec for key in iso_keys)
    if has_standard and has_iso:
        raise OperationFailure(f"{operator} cannot mix calendar and iso week date parts")
    if not has_standard and not has_iso:
        raise OperationFailure(f"{operator} requires either year or isoWeekYear")
    if has_standard and "year" not in spec:
        raise OperationFailure(f"{operator} year must be specified for calendar date parts")
    if has_iso and "isoWeekYear" not in spec:
        raise OperationFailure(f"{operator} isoWeekYear must be specified for iso week date parts")

    time_parts = []
    for name in ("hour", "minute", "second", "millisecond"):
        value = evaluate_expression(document, spec.get(name, 0), variables)
        if value is None:
            return None
        time_parts.append(_require_int_part(operator, name, value))
    hour, minute, second, millisecond = time_parts
    try:
        part_names = (
            ("year", "month", "day")
            if has_standard
            else ("isoWeekYear", "isoWeek", "isoDayOfWeek")
        )
        calendar_parts = []
        for name in part_names:
            value = evaluate_expression(document, spec.get(name, 1), variables)
            if value is None:
                return None
            calendar_parts.append(_require_int_part(operator, name, value))
        if has_standard:
            year, month, day = calendar_parts
            year_delta, month_index = divmod(month - 1, 12)
            localized = datetime.datetime(
                year + year_delta, month_index + 1, 1, tzinfo=timezone
            ) + datetime.timedelta(
                days=day - 1,
                hours=hour,
                minutes=minute,
                seconds=second,
                milliseconds=millisecond,
            )
        else:
            iso_year, iso_week, iso_day = calendar_parts
            base = datetime.date.fromisocalendar(iso_year, 1, 1)
            localized = datetime.datetime(
                base.year, base.month, base.day, tzinfo=timezone
            ) + datetime.timedelta(
                weeks=iso_week - 1,
                days=iso_day - 1,
                hours=hour,
                minutes=minute,
                seconds=second,
                milliseconds=millisecond,
            )
    except (ValueError, OverflowError) as exc:
        raise OperationFailure(f"{operator} produced an invalid date") from exc
    return _to_utc_naive(localized)


def _resolve_timezone(operator: str, timezone_value: Any) -> datetime.tzinfo:
    if timezone_value is None:
        return datetime.UTC
    if not isinstance(timezone_value, str):
        message = f"{operator} timezone must evaluate to a string"
        raise server_error(message, code=40517)
    normalized = timezone_value.strip()
    if normalized in {"UTC", "GMT", "Z"}:
        return datetime.UTC
    offset_match = re.fullmatch(r"([+-])(\d{2})(?::?(\d{2}))?", normalized)
    if offset_match:
        sign, hours_text, minutes_text = offset_match.groups()
        hours = int(hours_text)
        minutes = int(minutes_text or "0")
        delta = datetime.timedelta(hours=hours, minutes=minutes)
        if sign == "-":
            delta = -delta
        return datetime.timezone(delta)
    try:
        return ZoneInfo(normalized)
    except Exception as exc:
        message = f"{operator} timezone is invalid"
        raise server_error(message, code=40485) from exc


def _localize_datetime(value: datetime.datetime, timezone: datetime.tzinfo) -> datetime.datetime:
    aware_utc = value.astimezone(datetime.UTC) if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)
    return aware_utc.astimezone(timezone)


def _restore_datetime_timezone(value: datetime.datetime, original: datetime.datetime) -> datetime.datetime:
    utc_value = value.astimezone(datetime.UTC)
    if original.tzinfo is None:
        return utc_value.replace(tzinfo=None)
    return utc_value.astimezone(original.tzinfo)


def _evaluate_localized_date_operand(
    operator: str,
    document: Document,
    spec: object,
    variables: dict[str, Any] | None,
    *,
    evaluate_expression: ExpressionEvaluator,
    evaluate_expression_with_missing: ExpressionEvaluator,
    missing_sentinel: object,
) -> datetime.datetime | None:
    timezone_value = None
    date_expression: Any = spec
    if isinstance(spec, dict) and not any(key.startswith("$") for key in spec):
        if "date" not in spec:
            raise OperationFailure(f"{operator} requires a date expression")
        date_expression = spec["date"]
        timezone_value = evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None
    value = evaluate_expression_with_missing(document, date_expression, variables)
    if value is missing_sentinel or value is None:
        return None
    value = _date_operand(value)
    if not isinstance(value, datetime.datetime):
        raise OperationFailure(f"{operator} requires a date input")
    timezone = _resolve_timezone(operator, timezone_value)
    return _localize_datetime(value, timezone)


def _add_calendar_months(value: datetime.datetime, months: int) -> datetime.datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _add_date_unit(
    value: datetime.datetime, unit: str, amount: int
) -> datetime.datetime:
    durations = {"millisecond": 1, "second": 1000, "minute": 60000, "hour": 3600000}
    if unit in durations:
        result = bson_datetime_from_milliseconds(
            bson_datetime_milliseconds(value) + amount * durations[unit]
        )
        return (
            result
            if value.tzinfo is None
            else result.replace(tzinfo=datetime.UTC).astimezone(value.tzinfo)
        )
    if unit == "day":
        return value + datetime.timedelta(days=amount)
    if unit == "week":
        return value + datetime.timedelta(weeks=amount)
    if unit == "month":
        return _add_calendar_months(value, amount)
    if unit == "quarter":
        return _add_calendar_months(value, amount * 3)
    if unit == "year":
        return _add_calendar_months(value, amount * 12)
    raise OperationFailure(f"Unsupported date unit: {unit}")


def _require_date_unit(operator: str, unit: Any) -> str:
    if not isinstance(unit, str):
        raise OperationFailure(f"{operator} unit must evaluate to a string")
    if unit not in {"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond"}:
        message = f"{operator} unit is invalid"
        raise server_error(message, code=9)
    return unit


def _month_difference(start: datetime.datetime, end: datetime.datetime) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _date_diff_units(
    start: datetime.datetime,
    end: datetime.datetime,
    unit: str,
    *,
    start_of_week: str,
) -> int:
    if unit == "millisecond":
        return bson_datetime_milliseconds(end) - bson_datetime_milliseconds(start)
    if unit in {"second", "minute", "hour"}:

        def boundary(value):
            if unit == "hour":
                value = value.replace(minute=0, second=0, microsecond=0)
            elif unit == "minute":
                value = value.replace(second=0, microsecond=0)
            else:
                value = value.replace(microsecond=0)
            return bson_datetime_milliseconds(value)

        return (boundary(end) - boundary(start)) // {
            "second": 1000,
            "minute": 60000,
            "hour": 3600000,
        }[unit]
    if unit == "day":
        return (_truncate_datetime(end, "day", 1, start_of_week) - _truncate_datetime(start, "day", 1, start_of_week)).days
    if unit == "week":
        return (
            _truncate_datetime(end, "week", 1, start_of_week) - _truncate_datetime(start, "week", 1, start_of_week)
        ).days // 7
    if unit == "month":
        return _month_difference(
            _truncate_datetime(start, "month", 1, start_of_week),
            _truncate_datetime(end, "month", 1, start_of_week),
        )
    if unit == "quarter":
        return _month_difference(
            _truncate_datetime(start, "quarter", 1, start_of_week),
            _truncate_datetime(end, "quarter", 1, start_of_week),
        ) // 3
    if unit == "year":
        return _truncate_datetime(end, "year", 1, start_of_week).year - _truncate_datetime(start, "year", 1, start_of_week).year
    raise OperationFailure("$dateDiff unit is invalid")
