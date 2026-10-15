"""
Business definitions shared by the Silver and Gold layers.

Values marked ASSUMPTION are not specified by the workshop requirements or the
source data; they are documented in docs/business_definitions.md and need
business approval before Gold metrics rely on them.
"""

from __future__ import annotations

import re
from datetime import date
from fractions import Fraction

# Learning event types in funnel order (observed in the source; enforced by the contract).
LEARNING_EVENT_TYPES = (
    "enrollment", "module_started", "module_completed", "quiz_attempted",
    "quiz_passed", "course_completed", "certification_earned",
)
QUIZ_EVENT_TYPES = frozenset({"quiz_attempted", "quiz_passed"})
ENROLLMENT_EVENT = "enrollment"
# APPROVED (A13): a course counts as completed on an explicit course_completed event, even when
# earlier funnel events are missing; inconsistent histories are flagged (BUS-010/011), not dropped.
COURSE_COMPLETION_EVENT = "course_completed"
CERTIFICATION_EVENT = "certification_earned"

# APPROVED (A1, option B): a gap is critical when the employee is missing at least half of
# the required level, i.e. gap / required_level >= 1/2. This is fair across required levels:
# 1 short of 2, 2 short of 3 and 2 short of 4 are all critical; 1 short of 3 or 4 is not.
# In the data this flags 682 of 4,000 assessments (17.05%). An exact Fraction avoids float rounding.
CRITICAL_GAP_RATIO = Fraction(1, 2)

# ASSUMPTION: an entity fails the pipeline when more than 5% of its records are quarantined.
# The worst observed rate is learning activity at about 0.5%.
MAX_QUARANTINE_RATE = 0.05

_PERIOD = re.compile(r"^(\d{4})-H([12])$")


def review_period_bounds(period: str) -> tuple[date, date]:
    """
    Return the first and last day of a half-year period such as ``2024-H2``.

    Raises:
        ValueError: If ``period`` is not ``YYYY-H1`` or ``YYYY-H2``.
    """
    match = _PERIOD.match(period)
    if not match:
        raise ValueError(f"invalid review period {period!r}; expected YYYY-H1 or YYYY-H2")
    year, half = int(match.group(1)), match.group(2)
    return (date(year, 1, 1), date(year, 6, 30)) if half == "1" else (date(year, 7, 1), date(year, 12, 31))


def competency_gap(required_level: int, proficiency_level: int) -> int:
    """Return required minus proficiency: positive means a shortfall, zero or negative means met."""
    return required_level - proficiency_level


def is_critical_gap(gap: int, required_level: int, ratio: Fraction = CRITICAL_GAP_RATIO) -> bool:
    """
    Return True when the shortfall is at least ``ratio`` of the required level.

    Args:
        gap: ``required_level - proficiency_level``; zero or negative never counts.
        required_level: The level the role requires (1-5 per the contract).
        ratio: Share of the required level that must be missing (default 1/2).
    """
    return gap > 0 and required_level > 0 and Fraction(gap, required_level) >= ratio


def is_pre_hire(record_end: date, hire_date: date) -> bool:
    """
    Return True when a record's effective end date is before the employee was hired.

    A review or talent cycle that ends before the hire date (or a learning event or
    assessment dated before it) cannot describe this employee's work at ACME.
    """
    return record_end < hire_date


def rating_trend(previous_rating: int, current_rating: int) -> str:
    """Classify the change between two consecutive valid ratings."""
    delta = current_rating - previous_rating
    if delta > 0:
        return "improving"
    if delta < 0:
        return "declining"
    return "stable"


# ---- Gold-level proposals (docs/gold_design.md, assumptions A19-A24) ---------------------------

# A24: performer flags on the latest valid rating.
HIGH_PERFORMER_MIN_RATING = 4
LOW_PERFORMER_MAX_RATING = 2

# A19: promotion-ready = High potential, Ready Now and a strong latest valid rating.
PROMOTION_POTENTIAL = "High"
PROMOTION_READINESS = "Ready Now"
PROMOTION_MIN_RATING = 4

# A20: explainable attrition score and bands.
ATTRITION_HR_POINTS = {"High": 2, "Medium": 1, "Low": 0}
ATTRITION_HIGH_BAND_MIN_POINTS = 3
ATTRITION_MEDIUM_BAND_MIN_POINTS = 2

# A21: learning counts as recent within this many days before the as-of date.
LEARNING_ACTIVITY_WINDOW_DAYS = 365

RATING_TRENDS = ("improving", "stable", "declining", "insufficient_history")
RISK_BANDS = ("High", "Medium", "Low")


def period_index(period: str) -> int:
    """Return a sortable index of a half-year period: consecutive periods differ by exactly 1."""
    start, _end = review_period_bounds(period)
    return start.year * 2 + (0 if start.month == 1 else 1)


def attrition_band(points: int) -> str:
    """Map attrition points to the High / Medium / Low band (A20)."""
    if points >= ATTRITION_HIGH_BAND_MIN_POINTS:
        return "High"
    if points >= ATTRITION_MEDIUM_BAND_MIN_POINTS:
        return "Medium"
    return "Low"
