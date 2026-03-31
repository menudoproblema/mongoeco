import calendar
import datetime
import re
from collections.abc import Callable
from typing import Any
from zoneinfo import ZoneInfo

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.errors import OperationFailure
from mongoeco.types import Document


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
        if not isinstance(value, datetime.datetime):
            raise OperationFailure("$dateTrunc requires a datetime value")
        unit = evaluate_expression(document, spec["unit"], variables)
        if not isinstance(unit, str):
            raise OperationFailure("$dateTrunc unit must be a string")
        bin_size = evaluate_expression(document, spec["binSize"], variables) if "binSize" in spec else 1
        if not isinstance(bin_size, int) or isinstance(bin_size, bool):
            raise OperationFailure("$dateTrunc binSize must be an integer")
        timezone = evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None
        if timezone not in (None, "UTC"):
            raise OperationFailure("$dateTrunc timezone is not supported")
        start_of_week = evaluate_expression(document, spec["startOfWeek"], variables) if "startOfWeek" in spec else "sunday"
        if not isinstance(start_of_week, str):
            raise OperationFailure("$dateTrunc startOfWeek must be a string")
        return _truncate_datetime(value, unit, bin_size, start_of_week)

    if operator in {"$dateAdd", "$dateSubtract"}:
        if not isinstance(spec, dict) or not {"startDate", "unit", "amount"} <= set(spec):
            raise OperationFailure(f"{operator} requires startDate, unit and amount")
        value = evaluate_expression(document, spec["startDate"], variables)
        if value is None:
            return None
        if not isinstance(value, datetime.datetime):
            raise OperationFailure(f"{operator} requires a datetime startDate")
        unit = _require_date_unit(operator, evaluate_expression(document, spec["unit"], variables))
        amount = evaluate_expression(document, spec["amount"], variables)
        if amount is None:
            return None
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise OperationFailure(f"{operator} amount must evaluate to an integer")
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        localized = _localize_datetime(value, timezone)
        signed_amount = amount if operator == "$dateAdd" else -amount
        updated = _add_date_unit(localized, unit, signed_amount)
        return _restore_datetime_timezone(updated, value)

    if operator == "$dateToString":
        if not isinstance(spec, dict) or "date" not in spec:
            raise OperationFailure("$dateToString requires date")
        value = evaluate_expression_with_missing(document, spec["date"], variables)
        if value is missing_sentinel or value is None:
            return evaluate_expression(document, spec["onNull"], variables) if "onNull" in spec else None
        if not isinstance(value, datetime.datetime):
            raise OperationFailure("$dateToString requires a date input")
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        fmt = (
            evaluate_expression(document, spec["format"], variables)
            if "format" in spec
            else _default_date_to_string_format(timezone)
        )
        if not isinstance(fmt, str):
            raise OperationFailure("$dateToString format must evaluate to a string")
        localized = _localize_datetime(value, timezone)
        return _mongo_format_datetime(localized, fmt)

    if operator == "$dateToParts":
        if not isinstance(spec, dict) or "date" not in spec:
            raise OperationFailure("$dateToParts requires date")
        value = evaluate_expression_with_missing(document, spec["date"], variables)
        if value is missing_sentinel or value is None:
            return None
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
        if not isinstance(start_date, datetime.datetime) or not isinstance(end_date, datetime.datetime):
            raise OperationFailure("$dateDiff requires datetime startDate and endDate")
        unit = _require_date_unit(operator, evaluate_expression(document, spec["unit"], variables))
        timezone = _resolve_timezone(
            operator,
            evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None,
        )
        start_of_week = evaluate_expression(document, spec["startOfWeek"], variables) if "startOfWeek" in spec else "sunday"
        if not isinstance(start_of_week, str):
            raise OperationFailure("$dateDiff startOfWeek must be a string")
        localized_start = _localize_datetime(start_date, timezone)
        localized_end = _localize_datetime(end_date, timezone)
        return _date_diff_units(localized_start, localized_end, unit, start_of_week=start_of_week)

    raise OperationFailure(f"Unsupported date expression operator: {operator}")


def _truncate_datetime(value: datetime.datetime, unit: str, bin_size: int, start_of_week: str) -> datetime.datetime:
    if bin_size <= 0:
        raise OperationFailure("$dateTrunc binSize must be a positive integer")

    if unit == "year":
        year = ((value.year - 1) // bin_size) * bin_size + 1
        return value.replace(year=year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "quarter":
        quarter_index = (value.month - 1) // 3
        truncated_quarter = (quarter_index // bin_size) * bin_size
        month = truncated_quarter * 3 + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "month":
        month_index = value.month - 1
        month = (month_index // bin_size) * bin_size + 1
        return value.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

    if unit == "week":
        weekdays = {
            "sunday": 6,
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
        }
        normalized = start_of_week.lower()
        if normalized not in weekdays:
            raise OperationFailure("$dateTrunc startOfWeek is invalid")
        weekday_index = weekdays[normalized]
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = (day_start.weekday() - weekday_index) % 7
        week_start = day_start - datetime.timedelta(days=delta)
        anchor = datetime.datetime(1970, 1, 4, tzinfo=value.tzinfo)
        if normalized != "sunday":
            anchor_delta = (anchor.weekday() - weekday_index) % 7
            anchor -= datetime.timedelta(days=anchor_delta)
        weeks = (week_start - anchor).days // 7
        return anchor + datetime.timedelta(weeks=(weeks // bin_size) * bin_size)

    if unit == "day":
        day_start = value.replace(hour=0, minute=0, second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        days = (day_start - anchor).days
        return anchor + datetime.timedelta(days=(days // bin_size) * bin_size)

    if unit == "hour":
        hour_start = value.replace(minute=0, second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        hours = int((hour_start - anchor).total_seconds() // 3600)
        return anchor + datetime.timedelta(hours=(hours // bin_size) * bin_size)

    if unit == "minute":
        minute_start = value.replace(second=0, microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        minutes = int((minute_start - anchor).total_seconds() // 60)
        return anchor + datetime.timedelta(minutes=(minutes // bin_size) * bin_size)

    if unit == "second":
        second_start = value.replace(microsecond=0)
        anchor = datetime.datetime(1970, 1, 1, tzinfo=value.tzinfo)
        seconds = int((second_start - anchor).total_seconds())
        return anchor + datetime.timedelta(seconds=(seconds // bin_size) * bin_size)

    raise OperationFailure(f"Unsupported $dateTrunc unit: {unit}")


def _format_timezone_offset(value: datetime.datetime) -> str:
    offset = value.utcoffset() or datetime.timedelta()
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"{sign}{hours:02d}{minutes:02d}"


def _mongo_format_datetime(value: datetime.datetime, fmt: str) -> str:
    result = fmt
    replacements = {
        "%Y": f"{value.year:04d}",
        "%m": f"{value.month:02d}",
        "%d": f"{value.day:02d}",
        "%H": f"{value.hour:02d}",
        "%M": f"{value.minute:02d}",
        "%S": f"{value.second:02d}",
        "%L": f"{value.microsecond // 1000:03d}",
        "%z": _format_timezone_offset(value),
        "%G": f"{value.isocalendar().year:04d}",
        "%V": f"{value.isocalendar().week:02d}",
        "%u": str(value.isocalendar().weekday),
    }
    for token, replacement in replacements.items():
        result = result.replace(token, replacement)
    return result


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
    if not isinstance(value, int) or isinstance(value, bool):
        raise OperationFailure(f"{operator} {name} must evaluate to an integer")
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

    hour = _require_int_part_in_range(
        operator,
        "hour",
        evaluate_expression(document, spec.get("hour", 0), variables),
        minimum=0,
        maximum=23,
    )
    minute = _require_int_part_in_range(
        operator,
        "minute",
        evaluate_expression(document, spec.get("minute", 0), variables),
        minimum=0,
        maximum=59,
    )
    second = _require_int_part_in_range(
        operator,
        "second",
        evaluate_expression(document, spec.get("second", 0), variables),
        minimum=0,
        maximum=59,
    )
    millisecond = _require_int_part_in_range(
        operator,
        "millisecond",
        evaluate_expression(document, spec.get("millisecond", 0), variables),
        minimum=0,
        maximum=999,
    )

    try:
        if has_standard:
            year = _require_int_part(operator, "year", evaluate_expression(document, spec["year"], variables))
            month = _require_int_part(operator, "month", evaluate_expression(document, spec.get("month", 1), variables))
            day = _require_int_part(operator, "day", evaluate_expression(document, spec.get("day", 1), variables))
            localized = datetime.datetime(year, month, day, hour, minute, second, millisecond * 1000, tzinfo=timezone)
        else:
            iso_year = _require_int_part(operator, "isoWeekYear", evaluate_expression(document, spec["isoWeekYear"], variables))
            iso_week = _require_int_part(operator, "isoWeek", evaluate_expression(document, spec.get("isoWeek", 1), variables))
            iso_day = _require_int_part(
                operator,
                "isoDayOfWeek",
                evaluate_expression(document, spec.get("isoDayOfWeek", 1), variables),
            )
            base = datetime.date.fromisocalendar(iso_year, iso_week, iso_day)
            localized = datetime.datetime(
                base.year,
                base.month,
                base.day,
                hour,
                minute,
                second,
                millisecond * 1000,
                tzinfo=timezone,
            )
    except ValueError as exc:
        raise OperationFailure(f"{operator} produced an invalid date") from exc
    return _to_utc_naive(localized)


def _resolve_timezone(operator: str, timezone_value: Any) -> datetime.tzinfo:
    if timezone_value is None:
        return datetime.UTC
    if not isinstance(timezone_value, str):
        raise OperationFailure(f"{operator} timezone must evaluate to a string")
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
        raise OperationFailure(f"{operator} timezone is invalid") from exc


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
    if isinstance(spec, dict):
        if "date" not in spec:
            raise OperationFailure(f"{operator} requires a date expression")
        date_expression = spec["date"]
        timezone_value = evaluate_expression(document, spec["timezone"], variables) if "timezone" in spec else None
    value = evaluate_expression_with_missing(document, date_expression, variables)
    if value is missing_sentinel or value is None:
        return None
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


def _add_date_unit(value: datetime.datetime, unit: str, amount: int) -> datetime.datetime:
    if unit == "millisecond":
        return value + datetime.timedelta(milliseconds=amount)
    if unit == "second":
        return value + datetime.timedelta(seconds=amount)
    if unit == "minute":
        return value + datetime.timedelta(minutes=amount)
    if unit == "hour":
        return value + datetime.timedelta(hours=amount)
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
        raise OperationFailure(f"{operator} unit is invalid")
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
        return int((end - start).total_seconds() * 1000)
    if unit == "second":
        return int(
            (_truncate_datetime(end, "second", 1, start_of_week) - _truncate_datetime(start, "second", 1, start_of_week)).total_seconds()
        )
    if unit == "minute":
        return int(
            (_truncate_datetime(end, "minute", 1, start_of_week) - _truncate_datetime(start, "minute", 1, start_of_week)).total_seconds()
            // 60
        )
    if unit == "hour":
        return int(
            (_truncate_datetime(end, "hour", 1, start_of_week) - _truncate_datetime(start, "hour", 1, start_of_week)).total_seconds()
            // 3600
        )
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
