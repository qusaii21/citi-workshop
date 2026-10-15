"""Unit tests for business definitions."""

from datetime import date
from fractions import Fraction

import pytest

from talent_etl.definitions import (
    CRITICAL_GAP_RATIO,
    competency_gap,
    is_critical_gap,
    is_pre_hire,
    rating_trend,
    review_period_bounds,
)


@pytest.mark.parametrize("period, bounds", [
    ("2024-H2", (date(2024, 7, 1), date(2024, 12, 31))),
    ("2025-H1", (date(2025, 1, 1), date(2025, 6, 30))),
])
def test_review_period_bounds(period, bounds):
    assert review_period_bounds(period) == bounds


@pytest.mark.parametrize("period", ["2024-H3", "2024H1", "24-H1", "2024-h1", ""])
def test_review_period_bounds_rejects_invalid(period):
    with pytest.raises(ValueError, match="invalid review period"):
        review_period_bounds(period)


@pytest.mark.parametrize("required, proficiency, gap, critical", [
    (2, 1, 1, True),    # missing 1 of 2 = 50%: critical (never flagged by the old gap >= 2 rule)
    (3, 1, 2, True),    # 2 of 3 = 67%
    (4, 2, 2, True),    # 2 of 4 = 50%: exactly on the boundary counts
    (3, 2, 1, False),   # 1 of 3 = 33%
    (4, 3, 1, False),   # 1 of 4 = 25%
    (5, 3, 2, False),   # 2 of 5 = 40%
    (5, 2, 3, True),    # 3 of 5 = 60%
    (3, 3, 0, False),
    (2, 3, -1, False),
    (1, 1, 0, False),
])
def test_competency_gap_and_proportional_critical_rule(required, proficiency, gap, critical):
    assert competency_gap(required, proficiency) == gap
    assert is_critical_gap(gap, required) is critical


def test_critical_ratio_is_one_half_and_overridable():
    assert CRITICAL_GAP_RATIO == Fraction(1, 2)
    assert is_critical_gap(1, 3, ratio=Fraction(1, 3)) is True
    assert is_critical_gap(1, 0) is False  # defensive: an impossible required level is never critical


def test_pre_hire_boundary_is_strictly_before_hire_date():
    hire = date(2025, 3, 1)
    assert is_pre_hire(date(2025, 2, 28), hire) is True
    assert is_pre_hire(date(2025, 3, 1), hire) is False   # same day counts as employed
    assert is_pre_hire(date(2025, 6, 30), hire) is False  # hired during the period: valid review


@pytest.mark.parametrize("previous, current, trend", [(3, 4, "improving"), (4, 2, "declining"), (3, 3, "stable")])
def test_rating_trend(previous, current, trend):
    assert rating_trend(previous, current) == trend
