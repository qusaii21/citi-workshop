"""
Strict value parsers used by the schema contracts.

Each parser accepts the native type (as delivered in Bronze Parquet) or its
canonical string form (as delivered in JSONL), returns ``None`` for null or blank
input, and raises ``ParseError`` with a reason code for anything else. Nothing is
guessed: an unparseable date is an error, never "today" or the S3 partition date.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

_INT = re.compile(r"^[+-]?\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:\d{2})?$")


class ParseError(ValueError):
    """A value cannot be converted to its contract type."""

    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(detail)
        self.reason_code = reason_code
        self.detail = detail


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def parse_string(value: Any) -> str | None:
    """Return the value with surrounding whitespace removed; blank becomes None."""
    if _is_blank(value):
        return None
    if not isinstance(value, str):
        raise ParseError("type_conversion_failed", f"expected a string, got {type(value).__name__} {value!r}")
    return value.strip()


def parse_int(value: Any) -> int | None:
    """Parse an integer from int, an integral float, or a string of digits (e.g. JSON "989")."""
    if _is_blank(value):
        return None
    if isinstance(value, bool):
        raise ParseError("type_conversion_failed", f"expected an integer, got boolean {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    if isinstance(value, str) and _INT.match(value.strip()):
        return int(value.strip())
    raise ParseError("type_conversion_failed", f"expected an integer, got {value!r}")


def parse_float(value: Any) -> float | None:
    """Parse a finite number from int, float, Decimal or a numeric string."""
    if _is_blank(value):
        return None
    if isinstance(value, bool):
        raise ParseError("type_conversion_failed", f"expected a number, got boolean {value!r}")
    if isinstance(value, (int, float, Decimal)):
        result = float(value)
    elif isinstance(value, str) and _FLOAT.match(value.strip()):
        result = float(value.strip())
    else:
        raise ParseError("type_conversion_failed", f"expected a number, got {value!r}")
    if not math.isfinite(result):
        raise ParseError("type_conversion_failed", f"expected a finite number, got {value!r}")
    return result


def parse_decimal(value: Any, precision: int, scale: int) -> Decimal | None:
    """
    Parse an exact decimal that fits NUMERIC(precision, scale) without rounding.

    Raises:
        ParseError: ``type_conversion_failed`` for non-numbers, or when the value has
            more fractional digits than ``scale`` or more integer digits than allowed.
    """
    if _is_blank(value):
        return None
    if isinstance(value, bool):
        raise ParseError("type_conversion_failed", f"expected a decimal, got boolean {value!r}")
    try:
        number = Decimal(value.strip() if isinstance(value, str) else str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ParseError("type_conversion_failed", f"expected a decimal, got {value!r}") from exc
    if not number.is_finite():
        raise ParseError("type_conversion_failed", f"expected a finite decimal, got {value!r}")
    exponent = number.normalize().as_tuple().exponent
    if isinstance(exponent, int) and -exponent > scale:
        raise ParseError("type_conversion_failed", f"{value!r} has more than {scale} decimal places")
    if abs(number) >= Decimal(10) ** (precision - scale):
        raise ParseError("type_conversion_failed", f"{value!r} does not fit NUMERIC({precision},{scale})")
    return number.quantize(Decimal(1).scaleb(-scale))


def parse_date(value: Any) -> date | None:
    """Parse a calendar date from a ``date`` or a strict ``YYYY-MM-DD`` string."""
    if _is_blank(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str) and _DATE.match(value.strip()):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ParseError("invalid_date", f"impossible calendar date {value!r}") from exc
    raise ParseError("invalid_date", f"expected YYYY-MM-DD, got {value!r}")


def parse_timestamp(value: Any) -> datetime | None:
    """
    Parse an ISO 8601 timestamp (date and time required).

    Naive values are kept as-is (the source has no offset; treated as UTC by
    assumption). Offset-aware values are converted to UTC and made naive so every
    Silver timestamp is comparable.
    """
    if _is_blank(value):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and _TIMESTAMP.match(value.strip()):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ParseError("invalid_timestamp", f"impossible timestamp {value!r}") from exc
    else:
        raise ParseError("invalid_timestamp", f"expected YYYY-MM-DDTHH:MM[:SS], got {value!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed
