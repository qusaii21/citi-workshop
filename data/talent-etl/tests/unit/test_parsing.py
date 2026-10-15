"""Unit tests for the strict contract parsers."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from talent_etl.parsing import (
    ParseError,
    parse_date,
    parse_decimal,
    parse_float,
    parse_int,
    parse_string,
    parse_timestamp,
)


@pytest.mark.parametrize("parser", [parse_string, parse_int, parse_float, parse_date, parse_timestamp])
@pytest.mark.parametrize("blank", [None, "", "   "])
def test_blank_values_parse_to_none(parser, blank):
    assert parser(blank) is None


def test_parse_string_trims_and_rejects_non_strings():
    assert parse_string("  Sales ") == "Sales"
    with pytest.raises(ParseError) as err:
        parse_string(123)
    assert err.value.reason_code == "type_conversion_failed"


@pytest.mark.parametrize("value, expected", [(989, 989), ("989", 989), (" 42 ", 42), ("007", 7), (3.0, 3),
                                             (Decimal("5"), 5), ("-1", -1)])
def test_parse_int_accepts(value, expected):
    assert parse_int(value) == expected


@pytest.mark.parametrize("value", ["abc", "12.5", 12.5, True, "1e3", [1], Decimal("1.5")])
def test_parse_int_rejects(value):
    with pytest.raises(ParseError) as err:
        parse_int(value)
    assert err.value.reason_code == "type_conversion_failed"


@pytest.mark.parametrize("value, expected", [(46, 46.0), ("72.5", 72.5), (Decimal("1.25"), 1.25), (".5", 0.5)])
def test_parse_float_accepts(value, expected):
    assert parse_float(value) == expected


@pytest.mark.parametrize("value", ["abc", True, "nan", float("inf"), float("nan"), {}])
def test_parse_float_rejects(value):
    with pytest.raises(ParseError, match="number"):
        parse_float(value)


@pytest.mark.parametrize("value, expected", [
    (Decimal("56.87"), Decimal("56.87")), ("41.9", Decimal("41.90")), (94, Decimal("94.00")),
    (56.87, Decimal("56.87")), ("999.99", Decimal("999.99")), ("-999.99", Decimal("-999.99")),
])
def test_parse_decimal_accepts_values_that_fit(value, expected):
    result = parse_decimal(value, 5, 2)
    assert result == expected and result.as_tuple().exponent == -2


@pytest.mark.parametrize("value, fragment", [
    ("56.875", "more than 2 decimal places"), ("1000", r"does not fit NUMERIC\(5,2\)"),
    ("abc", "expected a decimal"), (True, "boolean"), ("NaN", "finite"), ("Infinity", "finite"),
])
def test_parse_decimal_rejects_without_rounding(value, fragment):
    with pytest.raises(ParseError, match=fragment) as err:
        parse_decimal(value, 5, 2)
    assert err.value.reason_code == "type_conversion_failed"


def test_parse_date():
    assert parse_date("2024-04-06") == date(2024, 4, 6)
    assert parse_date(date(2024, 4, 6)) == date(2024, 4, 6)


@pytest.mark.parametrize("value", ["2026-13-45", "2026-02-30", "06/04/2024", "2024-4-6", "yesterday",
                                   datetime(2024, 4, 6, 10, 0), 20240406])
def test_parse_date_rejects(value):
    with pytest.raises(ParseError) as err:
        parse_date(value)
    assert err.value.reason_code == "invalid_date"


@pytest.mark.parametrize("value, expected", [
    ("2023-10-13T14:49:00", datetime(2023, 10, 13, 14, 49)),
    ("2023-10-13 14:49", datetime(2023, 10, 13, 14, 49)),
    ("2023-10-13T14:49:00.123456", datetime(2023, 10, 13, 14, 49, 0, 123456)),
    ("2023-10-13T14:49:00Z", datetime(2023, 10, 13, 14, 49)),
    ("2023-10-13T16:49:00+02:00", datetime(2023, 10, 13, 14, 49)),
    (datetime(2023, 10, 13, 14, 49), datetime(2023, 10, 13, 14, 49)),
])
def test_parse_timestamp_accepts_iso_8601(value, expected):
    assert parse_timestamp(value) == expected


@pytest.mark.parametrize("value", ["yesterday", "N/A", "2026-13-45", "2023-10-13", "2023-02-30T10:00:00",
                                   "13/10/2023 14:49", 1697208540])
def test_parse_timestamp_rejects_known_bad_values(value):
    with pytest.raises(ParseError) as err:
        parse_timestamp(value)
    assert err.value.reason_code == "invalid_timestamp"
