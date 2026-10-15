"""
Serving layer: the web dashboard's data, built from the published Gold version.

The dashboard (``backend/talent-dashboard`` + ``frontend/``) shows the same answers as
``notebooks/talent_insights.ipynb``. Its Lambda has no pandas, so after every Gold run the
pipeline publishes the figures as two JSON documents:

- ``summary.json``: freshness, data quality and the aggregates behind the seven questions.
- ``employees.json``: one profile per employee (review history, skill gaps, learning,
  promotion and attrition signals) for the employee lists and the detail view.
- ``lake_index.json`` + ``lake/<layer>/<table>/chunk-NNNNN.json``: every Bronze, Silver
  (including quarantine) and Gold table, all rows and columns, in pages of LAKE_CHUNK_ROWS rows,
  for the dashboard's data-lake browser. Silver and Gold row counts are reconciled with their pointers.

Both are derived only from the published Gold version (plus the Silver and Gold manifests
for the data-quality section) and written under ``serving/runs/run_id=<gold run id>/``.
Reconciliation checks against Gold run before anything is published; the pointer
``serving/_current.json`` is replaced last, so readers never see a half-written version.
Publishing is idempotent: if the pointer already describes the current Gold version,
nothing is rewritten.
"""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd

from talent_etl import analysis, definitions, dq_report, dq_rules, explore, gold_tables
from talent_etl.config import Settings
from talent_etl.gold import GoldPointer, GoldSourceError, read_current_gold
from talent_etl.io_s3 import S3Client
from talent_etl.publish import Layer, collect_garbage, mark_started, put_json, read_json
from talent_etl.silver import read_current

logger = logging.getLogger(__name__)

SERVING_LAYER = Layer("serving")
SCHEMA_VERSION = 8                    # 2: lake catalog; 3: rules; 4: pipeline; 5: lineage; 6: Q6/Q7; 7: plain rules; 8: Gold checks
LAKE_CHUNK_ROWS = 500
LAKE_LAYERS = {
    "bronze": "Raw copies of the sources, unchanged: one snapshot per PostgreSQL table (latest ingest date) "
              "and every line of the learning-activity JSONL files (lines that are not valid JSON are kept in _raw).",
    "silver": "Validated, typed tables with lineage columns; records that failed a rule are in the "
              "<entity>.quarantine tables, with the rule and the original record.",
    "gold": "Curated business tables that answer the seven questions.",
}
TREND_ORDER = ["improving", "stable", "declining", "insufficient_history"]
BAND_ORDER = ["High", "Medium", "Low"]
GOLD_TABLES = ("dim_employee", "performance_history", "employee_performance", "employee_skill_gaps",
               "competency_gap_summary", "skill_distribution", "employee_learning", "promotion_pipeline",
               "attrition_risk", "team_performance")
# Two-sided 5% critical values of Student's t by degrees of freedom (for the Q6 correlation).
_T_CRITICAL = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262,
               10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110,
               18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
               26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042}


class ServingError(RuntimeError):
    """The dashboard data does not reconcile with Gold; nothing was published."""


@dataclass(frozen=True)
class ServingResult:
    """Outcome of one serving run."""

    status: str                       # published | up_to_date
    gold_run_id: str
    summary_key: str
    employees_key: str
    employees: int
    checks: int


def serving_key(gold_run_id: str, name: str) -> str:
    """Key of one serving document of a Gold version."""
    return f"{SERVING_LAYER.run_prefix(gold_run_id)}{name}.json"


def lake_chunk_key(gold_run_id: str, layer: str, table: str, chunk: int) -> str:
    """Key of one page of a lake table in the catalog."""
    return f"{SERVING_LAYER.run_prefix(gold_run_id)}lake/{layer}/{table}/chunk-{chunk:05d}.json"


def publish_lake_catalog(s3: S3Client, bucket: str, gold_run_id: str) -> tuple[dict[str, Any], list[dict]]:
    """
    Write every table of every layer as JSON pages; return the catalog index and its checks.

    Tables are read with ``talent_etl.explore`` (Bronze: latest ingest date; Silver and Gold:
    the published versions). The pages are written but nothing points to them until the
    caller publishes the serving pointer.
    """
    silver, gold = read_current(s3, bucket), read_current_gold(s3, bucket)
    expected = {("silver", entity): rows for entity, rows in (silver.rows if silver else {}).items()}
    expected |= {("gold", table): rows for table, rows in (gold.rows if gold else {}).items()}
    layers, checks = [], []
    for layer in explore.LAYERS:
        files = explore.table_files(s3, bucket, layer)
        entries = []
        for table, keys in files.items():
            df = explore.load(s3, bucket, layer, table)
            entries.append(_write_lake_table(s3, bucket, gold_run_id, layer, table, df, keys))
            if (layer, table) in expected:
                checks.append({"name": f"{layer}.{table} rows match the published pointer", "actual": len(df),
                               "expected": expected[(layer, table)], "passed": len(df) == expected[(layer, table)]})
        if layer == "silver" and silver:
            for entity, key in sorted(silver.quarantine.items()):
                rejected = explore.quarantine(s3, bucket, entity)
                if len(rejected):
                    entries.append(_write_lake_table(s3, bucket, gold_run_id, layer, f"{entity}.quarantine",
                                                     rejected, [key]))
        layers.append({"layer": layer, "description": LAKE_LAYERS[layer], "tables": entries})
    index = {"schema_version": SCHEMA_VERSION, "gold_run_id": gold_run_id, "chunk_rows": LAKE_CHUNK_ROWS,
             "silver_run_id": silver.run_id if silver else None, "layers": layers}
    return index, checks


def _write_lake_table(s3: S3Client, bucket: str, gold_run_id: str, layer: str, table: str, df: pd.DataFrame,
                      keys: list[str]) -> dict[str, Any]:
    rows = _records(_plain(df))
    chunks = math.ceil(len(rows) / LAKE_CHUNK_ROWS)
    for chunk in range(chunks):
        page = rows[chunk * LAKE_CHUNK_ROWS:(chunk + 1) * LAKE_CHUNK_ROWS]
        s3.put_bytes(bucket, lake_chunk_key(gold_run_id, layer, table, chunk),
                     json.dumps({"rows": page}, separators=(",", ":")).encode(), content_type="application/json")
    return {"table": table, "rows": len(rows), "chunks": chunks, "files": len(keys), "example_file": keys[0],
            "columns": [{"name": str(c), "type": str(t)} for c, t in df.dtypes.items()]}


def _plain(df: pd.DataFrame) -> pd.DataFrame:
    """Arrow list values (numpy arrays) as lists, so they serialise as JSON arrays."""
    nested = [c for c in df.columns if df[c].dtype == object and hasattr(_first(df[c]), "tolist")]
    return df.assign(**{c: df[c].map(lambda v: v.tolist() if hasattr(v, "tolist") else v) for c in nested}) if nested else df


# ---------------------------------------------------------------- building (pure functions)

def build_summary(t: dict[str, pd.DataFrame], context: dict[str, Any]) -> dict[str, Any]:
    """
    Aggregates for the dashboard, computed exactly as the notebook computes them.

    Args:
        t: The ten Gold tables as pandas.
        context: ``freshness`` (dict), ``quality`` / ``history_metrics`` / ``gold_checks``
            (record lists from the published manifests).
    """
    published = {k: v for k, v in context.items() if not k.startswith("_")}
    return {"schema_version": SCHEMA_VERSION, **published, "rules": rules(), "pipeline": pipeline(context),
            "q1": _q1(t), "q2": _q2(t), "q3": _q3(t), "q4": _q4(t), "q5": _q5(t), "q6": _q6(t), "q7": _q7(t)}


def rules() -> dict[str, Any]:
    """The business rules behind the figures (definitions.py), so the dashboard never restates them by hand."""
    return {
        "critical_gap_ratio": float(definitions.CRITICAL_GAP_RATIO),
        "high_performer_min_rating": definitions.HIGH_PERFORMER_MIN_RATING,
        "low_performer_max_rating": definitions.LOW_PERFORMER_MAX_RATING,
        "promotion": {"potential": definitions.PROMOTION_POTENTIAL, "readiness": definitions.PROMOTION_READINESS,
                      "min_rating": definitions.PROMOTION_MIN_RATING},
        "attrition": {"hr_points": dict(definitions.ATTRITION_HR_POINTS), "signal_points": 1,
                      "high_band_min_points": definitions.ATTRITION_HIGH_BAND_MIN_POINTS,
                      "medium_band_min_points": definitions.ATTRITION_MEDIUM_BAND_MIN_POINTS},
        "learning_window_days": definitions.LEARNING_ACTIVITY_WINDOW_DAYS,
        "rating_scale": [1, 5],
    }


def _lineage() -> dict[str, dict[str, Any]]:
    """What each Gold table reads directly and how it combines it (tests check the reads against the builders)."""
    D = definitions
    return {
        "dim_employee": {"silver": ["employees", "teams"], "gold": [],
                         "how": "employees LEFT JOIN teams ON team_id adds team_name and department; "
                                "tenure_years = (as-of date − hire_date) / 365.25."},
        "performance_history": {"silver": ["performance_reviews"], "gold": [],
                                "how": "Every review, labelled is_valid = not pre-hire. previous_valid_rating is the employee's "
                                       "preceding valid review (joined back on review_id); rating_change = rating − previous."},
        "employee_performance": {"silver": ["performance_reviews", "employees"], "gold": [],
                                 "how": "Valid reviews grouped by employee, for every employee: latest and previous valid review. "
                                        "The trend is set only when the two periods are consecutive half-years. "
                                        f"High performer: latest ≥ {D.HIGH_PERFORMER_MIN_RATING}; low: ≤ {D.LOW_PERFORMER_MAX_RATING}."},
        "employee_skill_gaps": {"silver": ["competency_assessments", "competencies"], "gold": ["dim_employee"],
                                "how": "Latest assessment per (employee, competency) JOIN competencies ON competency_id "
                                       "JOIN dim_employee ON employee_id (department, team, level). gap = required − proficiency; "
                                       f"critical when gap ÷ required ≥ {float(D.CRITICAL_GAP_RATIO)}."},
        "competency_gap_summary": {"silver": [], "gold": ["employee_skill_gaps"],
                                   "how": "employee_skill_gaps grouped by competency: employees assessed, average proficiency and "
                                          "required level, employees below required, critical-gap count and rate, rank."},
        "skill_distribution": {"silver": [], "gold": ["employee_skill_gaps"],
                               "how": "employee_skill_gaps grouped by competency × department × proficiency level; "
                                      "share_within_group = employees ÷ employees in that competency and department."},
        "employee_learning": {"silver": ["learning_activity", "employees"], "gold": [],
                              "how": "Learning events (pre-hire excluded) grouped by employee, for every employee: distinct courses "
                                     "enrolled / completed / certified, quiz counts, average score, last activity, and events in the "
                                     f"{D.LEARNING_ACTIVITY_WINDOW_DAYS} days before the as-of date."},
        "promotion_pipeline": {"silver": ["talent_review"],
                               "gold": ["dim_employee", "employee_performance", "employee_skill_gaps", "employee_learning"],
                               "how": "talent_review LEFT JOIN dim_employee, employee_performance, critical gaps per employee and "
                                      "employee_learning, all ON employee_id. is_promotion_ready = potential "
                                      f"{D.PROMOTION_POTENTIAL} AND readiness {D.PROMOTION_READINESS} AND latest valid rating "
                                      f"≥ {D.PROMOTION_MIN_RATING}."},
        "attrition_risk": {"silver": ["talent_review", "employees"], "gold": ["employee_performance", "employee_learning"],
                           "how": "For every employee: HR risk from talent_review, trend and latest rating from "
                                  "employee_performance, activity from employee_learning (ON employee_id). risk_points = HR points "
                                  "+ declining + low latest rating + inactive; the band follows from the points."},
        "team_performance": {"silver": ["teams", "projects"],
                             "gold": ["dim_employee", "employee_performance", "employee_skill_gaps"],
                             "how": "Team members (dim_employee) JOIN employee_performance and critical gaps ON employee_id, "
                                    "grouped by team_id; projects grouped by team_id. teams LEFT JOIN both: headcount, average "
                                    "latest rating, high-performer share, project count, average outcome, on-time share."},
    }


def _c(kind: str, how: str, *sources: str) -> dict[str, Any]:
    """One Gold column's origin: kind (copied | joined | computed | aggregated | pipeline), how, and source columns."""
    return {"kind": kind, "how": how, "from": list(sources)}


def column_lineage() -> dict[str, dict[str, dict[str, Any]]]:
    """Where every Gold column comes from (``layer.table.column``); tests check it against the contracts and reads."""
    D = definitions
    copied = lambda src: _c("copied", "copied as is", src)  # noqa: E731
    pr, ca, la, tr = ("silver.performance_reviews", "silver.competency_assessments", "silver.learning_activity",
                      "silver.talent_review")
    valid = "valid reviews only (is_pre_hire = false)"
    return {
        "dim_employee": {
            **{c: copied(f"silver.employees.{c}") for c in
               ("employee_id", "first_name", "last_name", "email", "job_level", "hire_date", "team_id")},
            "team_name": _c("joined", "teams.name, joined on team_id", "silver.teams.name", "silver.employees.team_id"),
            "department": _c("joined", "teams.department, joined on team_id", "silver.teams.department",
                             "silver.employees.team_id"),
            "tenure_years": _c("computed", "(as-of date − hire_date) ÷ 365.25 days", "silver.employees.hire_date"),
            "as_of": _c("pipeline", "the source snapshot date of the published Silver version"),
        },
        "performance_history": {
            **{c: copied(f"{pr}.{c}") for c in ("employee_id", "review_period", "review_id", "rating", "period_start",
                                                 "period_end", "submitted_date", "manager_comment", "is_pre_hire")},
            "is_valid": _c("computed", "NOT is_pre_hire", f"{pr}.is_pre_hire"),
            "previous_valid_rating": _c("computed", "rating of the employee's preceding valid review",
                                        f"{pr}.rating", f"{pr}.review_period", f"{pr}.is_pre_hire"),
            "rating_change": _c("computed", "rating − previous_valid_rating", f"{pr}.rating"),
        },
        "employee_performance": {
            "employee_id": _c("copied", "one row for every employee", "silver.employees.employee_id"),
            "valid_review_count": _c("aggregated", f"count of {valid}", f"{pr}.review_id", f"{pr}.is_pre_hire"),
            "pre_hire_review_count": _c("aggregated", "count of reviews with is_pre_hire = true", f"{pr}.is_pre_hire"),
            "latest_valid_period": _c("aggregated", f"latest review_period among {valid}", f"{pr}.review_period"),
            "latest_valid_rating": _c("aggregated", "rating of the latest valid review", f"{pr}.rating"),
            "previous_valid_rating": _c("aggregated", "rating of the valid review for the immediately preceding half-year",
                                        f"{pr}.rating", f"{pr}.review_period"),
            "rating_delta": _c("computed", "latest_valid_rating − previous_valid_rating", f"{pr}.rating"),
            "rating_trend": _c("computed", "improving if delta > 0, stable if 0, declining if < 0, "
                                           "else insufficient_history", f"{pr}.rating", f"{pr}.review_period"),
            "is_high_performer": _c("computed", f"latest_valid_rating ≥ {D.HIGH_PERFORMER_MIN_RATING}", f"{pr}.rating"),
            "is_low_performer": _c("computed", f"latest_valid_rating ≤ {D.LOW_PERFORMER_MAX_RATING}", f"{pr}.rating"),
        },
        "employee_skill_gaps": {
            **{c: _c("copied", "from the latest assessment per (employee, competency)", f"{ca}.{c}") for c in
               ("employee_id", "competency_id", "assessment_id", "assessed_date", "proficiency_level", "required_level",
                "gap", "is_critical_gap", "is_pre_hire")},
            "competency_name": _c("joined", "competencies.name, joined on competency_id", "silver.competencies.name"),
            "competency_category": _c("joined", "competencies.category, joined on competency_id",
                                      "silver.competencies.category"),
            **{c: _c("joined", f"dim_employee.{c}, joined on employee_id", f"gold.dim_employee.{c}")
               for c in ("department", "team_id", "job_level")},
            "gap_ratio": _c("computed", "gap ÷ required_level", f"{ca}.gap", f"{ca}.required_level"),
            "has_gap": _c("computed", "gap > 0", f"{ca}.gap"),
        },
        "competency_gap_summary": {
            **{c: _c("copied", "group key", f"gold.employee_skill_gaps.{c}")
               for c in ("competency_id", "competency_name", "competency_category")},
            "employees_assessed": _c("aggregated", "distinct employees", "gold.employee_skill_gaps.employee_id"),
            "avg_proficiency": _c("aggregated", "mean", "gold.employee_skill_gaps.proficiency_level"),
            "avg_required": _c("aggregated", "mean", "gold.employee_skill_gaps.required_level"),
            "employees_below_required": _c("aggregated", "count where has_gap", "gold.employee_skill_gaps.has_gap"),
            "critical_gap_count": _c("aggregated", "count where is_critical_gap", "gold.employee_skill_gaps.is_critical_gap"),
            "critical_gap_rate": _c("computed", "critical_gap_count ÷ employees_assessed",
                                    "gold.employee_skill_gaps.is_critical_gap", "gold.employee_skill_gaps.employee_id"),
            "rank_by_critical_rate": _c("computed", "rank by critical_gap_rate, highest = 1",
                                        "gold.employee_skill_gaps.is_critical_gap"),
        },
        "skill_distribution": {
            **{c: _c("copied", "group key", f"gold.employee_skill_gaps.{c}") for c in
               ("competency_id", "competency_name", "competency_category", "department", "proficiency_level")},
            "employees": _c("aggregated", "distinct employees in the group", "gold.employee_skill_gaps.employee_id"),
            "share_within_group": _c("computed", "employees ÷ all employees of that competency and department",
                                     "gold.employee_skill_gaps.employee_id"),
        },
        "employee_learning": {
            "employee_id": _c("copied", "one row for every employee", "silver.employees.employee_id"),
            "events_counted": _c("aggregated", "count of events with is_pre_hire = false", f"{la}.event_id", f"{la}.is_pre_hire"),
            "pre_hire_events_excluded": _c("aggregated", "count of events with is_pre_hire = true", f"{la}.is_pre_hire"),
            **{c: _c("aggregated", f"distinct course_name where event_type = {t}", f"{la}.course_name", f"{la}.event_type")
               for c, t in (("courses_enrolled", D.ENROLLMENT_EVENT), ("courses_completed", D.COURSE_COMPLETION_EVENT),
                            ("certifications_earned", D.CERTIFICATION_EVENT))},
            "quiz_attempts": _c("aggregated", "count where event_type = quiz_attempted", f"{la}.event_type"),
            "quiz_passes": _c("aggregated", "count where event_type = quiz_passed", f"{la}.event_type"),
            "avg_quiz_score": _c("aggregated", "mean score", f"{la}.score"),
            "last_activity_date": _c("aggregated", "latest event_date", f"{la}.event_date"),
            "events_last_12m": _c("aggregated", f"count of events in the {D.LEARNING_ACTIVITY_WINDOW_DAYS} days before the "
                                                "as-of date", f"{la}.event_date"),
            "is_learning_active_12m": _c("computed", "events_last_12m > 0", f"{la}.event_date"),
        },
        "promotion_pipeline": {
            **{c: copied(f"{tr}.{c}") for c in ("employee_id", "review_cycle", "potential_rating", "promotion_readiness")},
            "hr_attrition_risk": _c("copied", "talent_review.attrition_risk, renamed", f"{tr}.attrition_risk"),
            "talent_review_pre_hire": _c("copied", "talent_review.is_pre_hire, renamed", f"{tr}.is_pre_hire"),
            **{c: _c("joined", f"dim_employee.{c}, joined on employee_id", f"gold.dim_employee.{c}")
               for c in ("job_level", "department")},
            **{c: _c("joined", f"employee_performance.{c}, joined on employee_id", f"gold.employee_performance.{c}")
               for c in ("latest_valid_rating", "rating_trend")},
            "critical_gap_count": _c("aggregated", "sum of is_critical_gap per employee",
                                     "gold.employee_skill_gaps.is_critical_gap"),
            "courses_completed": _c("joined", "employee_learning.courses_completed, joined on employee_id",
                                    "gold.employee_learning.courses_completed"),
            "is_promotion_ready": _c("computed", f"potential_rating = {D.PROMOTION_POTENTIAL} AND promotion_readiness = "
                                                 f"{D.PROMOTION_READINESS} AND latest_valid_rating ≥ {D.PROMOTION_MIN_RATING}",
                                     f"{tr}.potential_rating", f"{tr}.promotion_readiness",
                                     "gold.employee_performance.latest_valid_rating"),
        },
        "attrition_risk": {
            "employee_id": _c("copied", "one row for every employee", "silver.employees.employee_id"),
            "hr_attrition_risk": _c("joined", "talent_review.attrition_risk, joined on employee_id", f"{tr}.attrition_risk"),
            "talent_review_pre_hire": _c("joined", "talent_review.is_pre_hire, joined on employee_id", f"{tr}.is_pre_hire"),
            **{c: _c("joined", f"employee_performance.{c}, joined on employee_id", f"gold.employee_performance.{c}")
               for c in ("latest_valid_rating", "rating_trend")},
            "hr_points": _c("computed", "HR risk mapped to points: " + ", ".join(
                f"{k} = {v}" for k, v in D.ATTRITION_HR_POINTS.items()), f"{tr}.attrition_risk"),
            "is_rating_declining": _c("computed", "rating_trend = declining", "gold.employee_performance.rating_trend"),
            "is_low_latest_rating": _c("computed", f"latest_valid_rating ≤ {D.LOW_PERFORMER_MAX_RATING}",
                                       "gold.employee_performance.latest_valid_rating"),
            "is_learning_inactive_12m": _c("computed", "NOT is_learning_active_12m",
                                           "gold.employee_learning.is_learning_active_12m"),
            "risk_points": _c("computed", "hr_points + 1 per flag (declining, low rating, inactive)", f"{tr}.attrition_risk",
                              "gold.employee_performance.rating_trend", "gold.employee_performance.latest_valid_rating",
                              "gold.employee_learning.is_learning_active_12m"),
            "risk_band": _c("computed", f"High if points ≥ {D.ATTRITION_HIGH_BAND_MIN_POINTS}, Medium if ≥ "
                                        f"{D.ATTRITION_MEDIUM_BAND_MIN_POINTS}, else Low", f"{tr}.attrition_risk"),
        },
        "team_performance": {
            "team_id": copied("silver.teams.team_id"),
            "team_name": _c("copied", "teams.name, renamed", "silver.teams.name"),
            "department": copied("silver.teams.department"),
            "headcount": _c("aggregated", "employees per team", "gold.dim_employee.team_id"),
            "employees_with_valid_rating": _c("aggregated", "members with a latest valid rating",
                                              "gold.employee_performance.latest_valid_rating"),
            "avg_latest_valid_rating": _c("aggregated", "mean of members' latest valid rating",
                                          "gold.employee_performance.latest_valid_rating"),
            "high_performer_share": _c("aggregated", "share of rated members who are high performers",
                                       "gold.employee_performance.is_high_performer"),
            "critical_gap_count": _c("aggregated", "sum of members' critical gaps", "gold.employee_skill_gaps.is_critical_gap"),
            "project_count": _c("aggregated", "projects per team", "silver.projects.project_id", "silver.projects.team_id"),
            "avg_project_outcome_score": _c("aggregated", "mean outcome_score of the team's projects",
                                            "silver.projects.outcome_score"),
            "on_time_project_share": _c("aggregated", "share of projects with outcome_status = on-time",
                                        "silver.projects.outcome_status"),
            "has_projects": _c("computed", "project_count > 0", "silver.projects.project_id"),
        },
    }


QUESTION_LINEAGE = [
    {"question": 1, "tables": {
        "performance_history": ["employee_id", "review_period", "rating", "is_pre_hire", "is_valid", "previous_valid_rating",
                                "rating_change"],
        "employee_performance": ["latest_valid_rating", "rating_trend", "valid_review_count", "is_high_performer",
                                 "is_low_performer"],
        "dim_employee": ["first_name", "last_name", "department", "job_level"]},
     "steps": ["List each employee's reviews from performance_history; pre-hire reviews stay visible but is_valid = false.",
               "Take the latest valid rating and its trend from employee_performance.",
               "Count employees per latest valid rating and per trend; flag high and low performers."]},
    {"question": 2, "tables": {
        "employee_skill_gaps": ["employee_id", "competency_name", "competency_category", "proficiency_level", "required_level",
                                "gap", "gap_ratio", "is_critical_gap"],
        "competency_gap_summary": ["employees_assessed", "critical_gap_count", "critical_gap_rate"]},
     "steps": ["gap = required_level − proficiency_level on each employee's latest assessment.",
               "Critical when gap ÷ required_level ≥ the approved ratio (A1).",
               "Per competency: critical_gap_rate = critical gaps ÷ employees assessed; list employees with a critical gap."]},
    {"question": 3, "tables": {
        "promotion_pipeline": ["potential_rating", "promotion_readiness", "latest_valid_rating", "is_promotion_ready",
                               "critical_gap_count", "courses_completed"]},
     "steps": ["Start from every talent review.", "Keep High potential, then Ready Now.",
               "Require a latest valid rating at or above the threshold (A19); show critical gaps and courses as context."]},
    {"question": 4, "tables": {
        "employee_learning": ["courses_enrolled", "courses_completed", "certifications_earned", "quiz_attempts", "quiz_passes",
                              "avg_quiz_score", "last_activity_date", "is_learning_active_12m", "pre_hire_events_excluded"]},
     "steps": ["Count distinct courses with an explicit completion event per employee (A13); certifications the same way.",
               "Exclude pre-hire events and count them separately (A4).",
               "Flag employees with no learning in the window before the as-of date (A21)."]},
    {"question": 5, "tables": {
        "attrition_risk": ["hr_attrition_risk", "hr_points", "is_rating_declining", "is_low_latest_rating",
                           "is_learning_inactive_12m", "risk_points", "risk_band"],
        "employee_performance": ["rating_trend"]},
     "steps": ["Give points: HR's own risk rating, a declining trend, a low latest rating, no recent learning.",
               "risk_points = the sum; the band follows from the points (A20).",
               "The direct answer to “performance trends”: employees whose rating declined."]},
    {"question": 6, "tables": {
        "team_performance": ["team_id", "avg_latest_valid_rating", "avg_project_outcome_score", "on_time_project_share",
                             "project_count", "has_projects"]},
     "steps": ["Keep teams with projects and at least one rated member.",
               "Correlate average latest rating with average project outcome score (Pearson r, Spearman ρ, t, p-value).",
               "Compare teams whose projects were all on time with teams that had a project not on time "
               "(difference in average rating, permutation test)."]},
    {"question": 7, "tables": {
        "employee_skill_gaps": ["competency_name", "competency_category", "department", "job_level", "proficiency_level",
                                "required_level"],
        "skill_distribution": ["competency_category", "department", "proficiency_level", "employees"]},
     "steps": ["Per competency: the share of people at each level, meeting the required level, and experts (level 4–5).",
               "Test whether department differences are bigger than chance (shuffle departments, permutation test).",
               "Compare job levels."]},
]


# The Gold reconciliation checks (gold_tables.py) in everyday words; the integration test requires all of them.
GOLD_CHECK_PLAIN = {
    "rows = Silver employees": "One row for every employee in the cleaned data",
    "rows = Silver reviews": "Every review from the cleaned data is included",
    "valid rows = Silver non-pre-hire reviews": "Reviews counted as valid match the reviews dated after each hire date",
    "sum valid_review_count = valid reviews": "Valid reviews counted per employee add up to all valid reviews",
    "trend counts add up to employees": "Every employee has exactly one rating trend",
    "trend present iff delta present": "A trend is given exactly when a rating change can be measured",
    "rows = distinct (employee, competency) in Silver": "One row for each employee and skill that was assessed",
    "critical gaps = recomputed from latest Silver assessments": "Critical gaps recounted from the assessments give the same total",
    "rows = competencies assessed": "One row for every skill that was assessed",
    "sum critical = employee_skill_gaps critical": "Critical gaps per skill add up to the total number of critical gaps",
    "sum employees = employee_skill_gaps rows": "People counted per skill level add up to all assessments",
    "shares add up to 1 per group": "The percentages in each group add up to 100%",
    "counted + excluded events = Silver learning rows": "Counted plus excluded learning activities equal all learning activities",
    "sum courses_completed = distinct counted completions (A13)": "Completed courses add up to the distinct course completions recorded",
    "rows = Silver talent reviews": "One row for every talent review",
    "every ready employee satisfies the rule (A19)": "Every promotion-ready employee really meets all three conditions",
    "risk_points = sum of signal points": "Each person's risk points equal the sum of their warning signs",
    "band consistent with points": "Each person's risk band matches their points",
    "rows = Silver teams": "One row for every team",
    "sum headcount = Silver employees": "Team sizes add up to all employees",
    "sum project_count = Silver projects": "Project counts add up to all projects",
}
_KEY_GRAIN = {"employee_id": "employee", "review_period": "review period", "competency_id": "skill", "department": "department",
              "proficiency_level": "proficiency level", "team_id": "team"}


def _words(column: str) -> str:
    return column.replace("_", " ")


def explain_gold_check(name: str) -> tuple[str, str]:
    """A Gold check's kind (structure | values | totals) and its plain-language description."""
    keys = re.findall(r"'([a-z_]+)'", name)
    if name == "schema matches contract":
        return "structure", "Has exactly the right columns, each with the right kind of value"
    if name.startswith("primary key") and name.endswith("not null"):
        return "structure", "Every row has its " + " and ".join(_words(k) for k in keys)
    if name.startswith("primary key") and "unique" in name:
        return "structure", "No duplicates: one row per " + " × ".join(_KEY_GRAIN.get(k, _words(k)) for k in keys)
    match = re.fullmatch(r"([a-z_]+) in \[(-?[\d.]+), (-?[\d.]+)\]", name)
    if match:
        return "values", f"{_words(match.group(1)).capitalize()} is between {match.group(2)} and {match.group(3)}"
    match = re.fullmatch(r"([a-z_]+) in \[(.+)\]", name)
    if match:
        options = ", ".join(o.strip(" '").replace("_", " ") for o in match.group(2).split(","))
        return "values", f"{_words(match.group(1)).capitalize()} is only ever one of: {options}"
    match = re.fullmatch(r"([a-z_]+) not null", name)
    if match:
        return "structure", f"{_words(match.group(1)).capitalize()} is always filled in"
    return "totals", GOLD_CHECK_PLAIN.get(name, name)


def gold_check_details(s3: S3Client, bucket: str) -> list[dict[str, Any]]:
    """Every check behind the published Gold version (read from the run that built each table), in plain words."""
    from talent_etl.gold import GOLD_LAYER   # local: gold imports this module's dependencies
    pointer = read_current_gold(s3, bucket)
    manifests: dict[str, Any] = {}
    out = []
    for table, key in pointer.files.items():
        run_id = GOLD_LAYER.run_id_of(key)
        if run_id not in manifests:
            manifests[run_id] = json.loads(s3.get_bytes(bucket, GOLD_LAYER.manifest_key(run_id)))
        built = next(t for t in manifests[run_id]["tables"] if t["table"] == table)
        checks = []
        for check in built["checks"]:
            kind, plain = explain_gold_check(check["name"])
            checks.append({"name": check["name"], "passed": bool(check["passed"]), "detail": check.get("detail", ""),
                           "kind": kind, "plain": plain})
        out.append({"table": table, "checks": checks})
    return out


def plain_rules() -> dict[str, str]:
    """Every data-quality rule in everyday words, for non-technical readers (tests require one per rule)."""
    D = definitions
    return {
        "SCH-001": "A whole column we expect is missing from an HR table. That table is not used until it is fixed.",
        "SCH-002": "The data has an extra column we don't expect. It is left out of the clean data (the raw copy keeps it).",
        "SCH-003": "A column arrived as a different kind of value than expected (for example text instead of a number). "
                   "Every value is still checked one by one.",
        "SCH-004": "Some learning records call the description field \"notes\" instead of \"text\". Both are treated as the same field.",
        "JSN-001": "A line in a learning file is broken or cut off, so it can't be read.",
        "JSN-002": "A line in a learning file can be read, but it isn't a single record (for example just a list or a number).",
        "JSN-003": "A line contains garbled characters that can't be read as text.",
        "JSN-004": "A learning file is completely empty. It is noted and skipped.",
        "JSN-005": "A learning file contains a blank line. It is skipped and counted.",
        "REQ-001": "A field that must always be filled in is missing or blank (for example a record with no employee id).",
        "TYP-001": "A value is the wrong kind: letters where a number should be, or a number with too many decimals.",
        "TYP-002": "A date isn't written as year-month-day, or is impossible (for example 31 February).",
        "TYP-003": "A learning activity's date and time isn't a real date (for example \"yesterday\" or \"N/A\").",
        "DOM-001": "A value isn't one of the allowed options (for example a potential rating other than Low, Medium or High).",
        "DOM-002": "A number is outside its allowed range (for example a rating outside 1–5, or a score outside 0–100).",
        "DOM-003": "A review period isn't written like \"2025-H1\" (a year and which half of it).",
        "DOM-004": "A department or job level appears that we haven't seen before. Kept, but flagged for a look.",
        "DOM-005": "An email address or activity id doesn't look like the usual format. Kept, but flagged.",
        "DOM-006": "A project's outcome score is outside the normal 0–100 range. Kept, but flagged.",
        "DAT-001": "A date is in the future (after the day the data was loaded). Kept, but flagged.",
        "KEY-001": "The exact same record appears twice. One copy is kept and the repeat is dropped.",
        "KEY-002": "Two records share the same id but say different things. We can't tell which is right, so both are set aside.",
        "KEY-003": "The same review or assessment was recorded twice under different ids. The most recent one is kept.",
        "KEY-004": "Two employees share the same email address. Kept, but flagged.",
        "REF-001": "A record points to something that doesn't exist, such as a review for an unknown employee or an unknown team.",
        "JSN-006": "Learning files write the employee id as text (\"989\"). It is turned into a number so it matches the HR data.",
        "BUS-001": "A performance review covers a period before the person was hired. Kept and marked, but not used in ratings.",
        "BUS-002": "A learning activity is dated before the person was hired. Kept and marked, but not counted in their learning.",
        "BUS-003": "A skill assessment is dated before the person was hired. Kept, marked, and still used for skills.",
        "BUS-004": "A talent review happened before the person was hired. Kept, and marked in the promotion and attrition results.",
        "BUS-005": "A review was submitted before its review period had even ended. Kept, but flagged.",
        "BUS-006": "A score appears on a learning activity that isn't a quiz. Kept, but flagged.",
        "BUS-007": "A quiz has no score. Kept, but flagged.",
        "BUS-008": "A learning activity's date doesn't match the day-folder its file is stored in. The activity's own date is trusted.",
        "BUS-009": "Works out each skill gap (required level minus actual level) and marks it critical when someone is missing "
                   f"at least {'half' if float(D.CRITICAL_GAP_RATIO) == 0.5 else format(float(D.CRITICAL_GAP_RATIO), '.0%')} "
                   "of the required level.",
        "BUS-010": "An overall check of how often a course is completed with no record of enrolling first. Reported for the "
                   "whole dataset, not record by record; completions still count.",
        "BUS-011": "An overall check of how often a certificate has no completed course before it. Reported for the whole "
                   "dataset, not record by record.",
        "VOL-001": "No usable records are left for a whole table. The pipeline stops for that table.",
        "VOL-002": f"More than {D.MAX_QUARANTINE_RATE:.0%} of a table's records had to be set aside. Something is probably "
                   "wrong with the source, so the clean data is not replaced.",
    }


def pipeline(context: dict[str, Any]) -> dict[str, Any]:
    """How the pipeline works: the data-quality rules (with how often each fired) and the Gold lineage."""
    fired: dict[str, int] = {}
    for entity in context.get("_reason_counts", []):
        for reason, count in entity.items():
            fired[reason] = fired.get(reason, 0) + count
    plain = plain_rules()
    rule_rows = [{"rule_id": r.rule_id, "reason_code": r.reason_code, "entity": r.entity, "fields": r.fields,
                  "category": str(r.category), "severity": str(r.severity), "action": str(r.action), "logic": r.logic,
                  "plain": plain[r.rule_id],
                  "fired": fired.get(r.reason_code, 0)} for r in dq_rules.RULES]
    lineage, columns = _lineage(), column_lineage()
    tables = [{"table": name, "question": spec.question, "primary_key": list(spec.primary_key),
               "column_lineage": {c: columns[name][c] for c in spec.schema.names},   # in table column order
               "uses_as_of": spec.uses_as_of, "columns": [{"name": f.name, "type": str(f.type)} for f in spec.schema],
               **lineage[name]} for name, spec in gold_tables.TABLES.items()]
    return {"rules": rule_rows, "actions": [str(a) for a in dq_rules.Action], "gold_tables": tables,
            "questions": QUESTION_LINEAGE}


def build_employees(t: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """One profile per employee (the notebook's per-employee helpers, for every employee)."""
    dim = t["dim_employee"]
    perf, learning, promo, risk = (_by_employee(t[name]) for name in (
        "employee_performance", "employee_learning", "promotion_pipeline", "attrition_risk"))
    history = _grouped(t["performance_history"].sort_values(["employee_id", "review_period"]),
                       ["review_period", "rating", "is_pre_hire", "is_valid", "previous_valid_rating", "rating_change"])
    gaps = _grouped(t["employee_skill_gaps"].sort_values(["employee_id", "competency_name"]),
                    ["competency_name", "competency_category", "required_level", "proficiency_level", "gap",
                     "is_critical_gap", "is_pre_hire", "assessed_date"])
    profiles = []
    for row in _records(dim.sort_values("employee_id")):
        eid = row["employee_id"]
        p, lr, pr, rk = (rows.get(eid, {}) for rows in (perf, learning, promo, risk))
        profiles.append({
            "employee_id": eid, "name": f"{row['first_name']} {row['last_name']}",
            "department": row["department"], "team": row["team_name"], "job_level": row["job_level"],
            "hire_date": row["hire_date"], "tenure_years": _round(row["tenure_years"], 1),
            "performance": {k: p.get(k) for k in ("latest_valid_rating", "latest_valid_period", "previous_valid_rating",
                                                  "rating_trend", "valid_review_count", "pre_hire_review_count",
                                                  "is_high_performer", "is_low_performer")},
            "review_history": history.get(eid, []),
            "skill_gaps": gaps.get(eid, []),
            "critical_gaps": sum(1 for g in gaps.get(eid, []) if g["is_critical_gap"]),
            "learning": {**{k: lr.get(k) for k in ("courses_completed", "certifications_earned", "courses_enrolled",
                                                   "quiz_attempts", "quiz_passes", "events_counted",
                                                   "pre_hire_events_excluded", "last_activity_date",
                                                   "events_last_12m", "is_learning_active_12m")},
                         "avg_quiz_score": _round(lr.get("avg_quiz_score"), 1)},
            "promotion": {k: pr.get(k) for k in ("potential_rating", "promotion_readiness", "review_cycle",
                                                 "talent_review_pre_hire", "is_promotion_ready")},
            "attrition": {k: rk.get(k) for k in ("risk_band", "risk_points", "hr_attrition_risk", "hr_points",
                                                 "is_rating_declining", "is_low_latest_rating",
                                                 "is_learning_inactive_12m")},
        })
    return profiles


def reconcile(summary: dict[str, Any], employees: list[dict[str, Any]], t: dict[str, pd.DataFrame]) -> list[dict]:
    """Checks that the dashboard data says the same as Gold; every check must pass to publish."""
    perf, gaps = t["employee_performance"], t["employee_skill_gaps"]
    by_id = {e["employee_id"]: e for e in employees}

    def check(name: str, actual: Any, expected: Any) -> dict:
        return {"name": name, "actual": actual, "expected": expected, "passed": actual == expected}

    q1, q2, q3, q4, q5 = (summary[k] for k in ("q1", "q2", "q3", "q4", "q5"))
    return [
        check("one profile per employee", len(by_id), len(t["dim_employee"])),
        check("profiles are unique", len(employees), len(by_id)),
        check("rating distribution sums to rated employees", sum(r["employees"] for r in q1["rating_distribution"]),
              int(perf["latest_valid_rating"].notna().sum())),
        check("trend distribution sums to employees", sum(r["employees"] for r in q1["trend_distribution"]), len(perf)),
        check("review histories hold every review", sum(len(e["review_history"]) for e in employees),
              len(t["performance_history"])),
        check("critical gaps match Gold", sum(e["critical_gaps"] for e in employees),
              int(gaps["is_critical_gap"].sum())),
        check("critical-gap total in summary", q2["critical_gaps"], int(gaps["is_critical_gap"].sum())),
        check("promotion-ready profiles match Gold", sum(bool(e["promotion"]["is_promotion_ready"]) for e in employees),
              int(t["promotion_pipeline"]["is_promotion_ready"].sum())),
        check("promotion-ready total in summary", q3["promotion_ready"],
              int(t["promotion_pipeline"]["is_promotion_ready"].sum())),
        check("completions match Gold", q4["totals"]["distinct_course_completions"],
              int(t["employee_learning"]["courses_completed"].sum())),
        check("risk bands sum to employees", sum(r["employees"] for r in q5["bands"]), len(t["attrition_risk"])),
        check("teams in scatter have projects", len(summary["q6"]["teams"]), summary["q6"]["teams_with_projects"]),
        check("delivery-status comparison covers the compared teams", summary["q6"]["delivery_status"]["teams"],
              summary["q6"]["teams_with_projects"]),
        check("competency readiness covers every assessment", sum(r["assessed"] for r in summary["q7"]["readiness"]),
              len(gaps)),
    ]


# ---------------------------------------------------------------- the seven questions

def _q1(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    perf, history = t["employee_performance"], t["performance_history"]
    rated = perf.dropna(subset=["latest_valid_rating"])
    ratings = rated["latest_valid_rating"].astype(int).value_counts().sort_index()
    trends = perf["rating_trend"].value_counts().reindex(TREND_ORDER, fill_value=0)
    return {
        "employees": len(perf), "rated_employees": len(rated), "reviews": len(history),
        "pre_hire_reviews": int(history["is_pre_hire"].sum()),
        "high_performers": int(perf["is_high_performer"].sum()), "low_performers": int(perf["is_low_performer"].sum()),
        "employees_with_trend": int((perf["rating_trend"] != "insufficient_history").sum()),
        "employees_without_valid_review": int((perf["valid_review_count"] == 0).sum()),
        "review_periods": sorted(history["review_period"].unique().tolist()),
        "rating_distribution": [{"rating": int(r), "employees": int(n), "share": n / len(rated)}
                                for r, n in ratings.items()],
        "trend_distribution": [{"trend": k, "employees": int(n), "share": n / len(perf)} for k, n in trends.items()],
    }


def _q2(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    gaps, summary = t["employee_skill_gaps"], t["competency_gap_summary"]
    critical = gaps[gaps["is_critical_gap"]]
    per_employee = critical.groupby("employee_id").size()
    by_category = gaps.groupby("competency_category").agg(assessments=("is_critical_gap", "size"),
                                                          critical_gaps=("is_critical_gap", "sum")).reset_index()
    by_category["critical_gap_rate"] = by_category["critical_gaps"] / by_category["assessments"]
    ranked = summary.sort_values(["critical_gap_rate", "critical_gap_count"], ascending=False)
    return {
        "assessments": len(gaps), "critical_gaps": len(critical), "critical_gap_rate": len(critical) / len(gaps),
        "employees_assessed": int(gaps["employee_id"].nunique()),
        "employees_with_critical_gap": int(len(per_employee)),
        "employees_with_one": int((per_employee == 1).sum()), "employees_with_two_or_more": int((per_employee >= 2).sum()),
        "pre_hire_assessments": int(gaps["is_pre_hire"].sum()),
        "critical_threshold_ratio": float(definitions.CRITICAL_GAP_RATIO),
        "competencies": _records(ranked[["competency_name", "competency_category", "employees_assessed",
                                         "avg_proficiency", "avg_required", "employees_below_required",
                                         "critical_gap_count", "critical_gap_rate"]]),
        "by_category": _records(by_category.sort_values("critical_gap_rate", ascending=False)),
    }


def _q3(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    pipe = t["promotion_pipeline"]
    high = pipe["potential_rating"] == definitions.PROMOTION_POTENTIAL
    ready_now = high & (pipe["promotion_readiness"] == definitions.PROMOTION_READINESS)
    ready = pipe[pipe["is_promotion_ready"]]
    no_rating = int((ready_now & pipe["latest_valid_rating"].isna()).sum())
    return {
        "rule": {"potential": definitions.PROMOTION_POTENTIAL, "readiness": definitions.PROMOTION_READINESS,
                 "min_rating": definitions.PROMOTION_MIN_RATING},
        "stages": [
            {"stage": "Employees with a talent review", "employees": len(pipe)},
            {"stage": f"… with {definitions.PROMOTION_POTENTIAL} potential", "employees": int(high.sum())},
            {"stage": f"… and {definitions.PROMOTION_READINESS}", "employees": int(ready_now.sum())},
            {"stage": f"… and latest valid rating ≥ {definitions.PROMOTION_MIN_RATING} (promotion-ready)",
             "employees": len(ready)},
        ],
        "high_and_ready_now": int(ready_now.sum()), "promotion_ready": len(ready),
        "ready_now_without_valid_rating": no_rating,
        "ready_now_failing_rating": int(ready_now.sum()) - len(ready) - no_rating,
        "ready_with_critical_gap": int((ready["critical_gap_count"] > 0).sum()),
        "pre_hire_talent_reviews": int(pipe["talent_review_pre_hire"].sum()),
        "by_department": [{"department": d, "employees": int(n)}
                          for d, n in ready.groupby("department").size().sort_values(ascending=False).items()],
    }


def _q4(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    learning = t["employee_learning"]
    completed = learning["courses_completed"].clip(upper=4).astype(int).value_counts().sort_index()
    return {
        "totals": {
            "distinct_course_completions": int(learning["courses_completed"].sum()),
            "certifications_earned": int(learning["certifications_earned"].sum()),
            "employees_with_completion": int((learning["courses_completed"] > 0).sum()),
            "employees_with_certification": int((learning["certifications_earned"] > 0).sum()),
            "learning_events_counted": int(learning["events_counted"].sum()),
            "pre_hire_events_excluded": int(learning["pre_hire_events_excluded"].sum()),
            "employees_inactive_12m": int((~learning["is_learning_active_12m"]).sum()),
        },
        "window_days": definitions.LEARNING_ACTIVITY_WINDOW_DAYS,
        "completion_distribution": [{"courses": "4+" if k == 4 else str(k), "employees": int(n)}
                                    for k, n in completed.items()],
    }


def _q5(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    risk = t["attrition_risk"]
    bands = risk["risk_band"].value_counts().reindex(BAND_ORDER, fill_value=0)
    cross = pd.crosstab(risk["rating_trend"], risk["risk_band"]).reindex(index=TREND_ORDER, columns=BAND_ORDER,
                                                                         fill_value=0)
    declining = risk[risk["is_rating_declining"]]
    return {
        "bands": [{"band": b, "employees": int(n)} for b, n in bands.items()],
        "trend_by_band": [{"trend": trend, **{b: int(cross.loc[trend, b]) for b in BAND_ORDER}}
                          for trend in TREND_ORDER],
        "declining": len(declining),
        "declining_hr_high": int((declining["hr_attrition_risk"] == "High").sum()),
        "declining_in_high_band": int((declining["risk_band"] == "High").sum()),
        "signals": {
            "hr_high": int((risk["hr_attrition_risk"] == "High").sum()),
            "hr_medium": int((risk["hr_attrition_risk"] == "Medium").sum()),
            "rating_declining": int(risk["is_rating_declining"].sum()),
            "low_latest_rating": int(risk["is_low_latest_rating"].sum()),
            "learning_inactive_12m": int(risk["is_learning_inactive_12m"].sum()),
        },
    }


def _q6(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    teams = t["team_performance"]
    x, y = "avg_latest_valid_rating", "avg_project_outcome_score"
    with_projects = teams[teams["has_projects"]].dropna(subset=[x, y])
    n = len(with_projects)
    r = float(with_projects[x].corr(with_projects[y])) if n > 2 else None
    rho = float(with_projects[x].rank().corr(with_projects[y].rank())) if n > 2 else None  # Spearman
    t_stat = r * math.sqrt((n - 2) / (1 - r ** 2)) if r is not None and abs(r) < 1 else None
    critical = _T_CRITICAL.get(n - 2, 1.96)
    delivery = with_projects.assign(delivery=with_projects["on_time_project_share"].map(
        lambda s: "all on time" if s == 1 else ("none on time" if s == 0 else "mixed")))
    groups = delivery.groupby("delivery").agg(teams=("team_id", "count"), avg_team_rating=(x, "mean"),
                                              avg_outcome_score=(y, "mean")).reset_index()
    rated = t["employee_performance"].merge(t["dim_employee"][["employee_id", "department"]], on="employee_id")
    rated = rated.dropna(subset=["latest_valid_rating"])
    departments = rated.groupby("department").agg(rated_employees=("employee_id", "count"),
                                                  avg_latest_rating=("latest_valid_rating", "mean"),
                                                  high_performer_share=("is_high_performer", "mean")).reset_index()
    return {
        "teams_total": len(teams), "teams_with_projects": n, "projects": int(teams["project_count"].sum()),
        "pearson_r": r, "spearman_rho": rho, "t_statistic": t_stat, "t_critical_5pct": critical,
        "significant_5pct": bool(t_stat is not None and abs(t_stat) >= critical),
        "teams": _records(with_projects[["team_id", "team_name", "department", "headcount", x, y,
                                         "on_time_project_share", "project_count"]].sort_values("team_id")),
        "delivery": _records(groups),
        "delivery_status": analysis.delivery_status_comparison(teams),
        "departments": _records(departments.sort_values("avg_latest_rating", ascending=False)),
    }


def _q7(t: dict[str, pd.DataFrame]) -> dict[str, Any]:
    dist, summary = t["skill_distribution"], t["competency_gap_summary"]
    weighted = dist.assign(points=dist["proficiency_level"] * dist["employees"])
    cells = weighted.groupby(["department", "competency_category"])[["points", "employees"]].sum().reset_index()
    cells["avg_proficiency"] = cells["points"] / cells["employees"]
    levels = dist.groupby(["competency_category", "proficiency_level"])["employees"].sum().reset_index()
    levels["share"] = levels["employees"] / levels.groupby("competency_category")["employees"].transform("sum")
    ranked = summary.sort_values("avg_proficiency")
    cols = ["competency_name", "competency_category", "employees_assessed", "avg_proficiency", "avg_required"]
    return {
        "departments": sorted(cells["department"].unique().tolist()),
        "categories": sorted(cells["competency_category"].unique().tolist()),
        "heatmap": _records(cells.rename(columns={"employees": "assessments"})[
            ["department", "competency_category", "avg_proficiency", "assessments"]]),
        "level_mix": _records(levels),
        "lowest_competencies": _records(ranked.head(5)[cols]),
        "highest_competencies": _records(ranked.tail(5).iloc[::-1][cols]),
        "readiness": _records(analysis.competency_readiness(t["employee_skill_gaps"])),
        "expert_min_level": analysis.EXPERT_MIN_LEVEL,
        "department_variation": analysis.department_variation(t["employee_skill_gaps"]),
        "by_job_level": _records(_by_job_level(t["employee_skill_gaps"])),
    }


def _by_job_level(gaps: pd.DataFrame) -> pd.DataFrame:
    out = gaps.assign(meets=gaps["proficiency_level"] >= gaps["required_level"]).groupby("job_level").agg(
        assessments=("employee_id", "size"), avg_proficiency=("proficiency_level", "mean"), meets_required=("meets", "mean"))
    return out.reset_index().sort_values("avg_proficiency")


# ---------------------------------------------------------------- publishing

def publish_dashboard(settings: Settings, s3: S3Client, clock: Callable[[], datetime] | None = None) -> ServingResult:
    """
    Build the dashboard documents from the published Gold version and publish them.

    Raises:
        GoldSourceError: If no Gold version is published.
        ServingError: If a reconciliation check fails (nothing is published).
    """
    now = (clock or (lambda: datetime.now(timezone.utc)))
    bucket = settings.s3.lake_bucket
    gold = read_current_gold(s3, bucket)
    if gold is None:
        raise GoldSourceError(f"no published Gold in s3://{bucket}; run --step gold")
    summary_key, employees_key = serving_key(gold.run_id, "summary"), serving_key(gold.run_id, "employees")
    current = read_json(s3, bucket, SERVING_LAYER.current_key())
    if current and current.get("gold_run_id") == gold.run_id and current.get("schema_version") == SCHEMA_VERSION:
        return ServingResult("up_to_date", gold.run_id, summary_key, employees_key, current.get("employees", 0), 0)

    started = now()
    mark_started(s3, bucket, SERVING_LAYER, gold.run_id, started)
    tables = {name: s3.read_arrow(bucket, gold.files[name]).to_pandas() for name in GOLD_TABLES}
    summary = build_summary(tables, _context(settings, s3, gold))
    employees = build_employees(tables)
    checks = reconcile(summary, employees, tables)
    failed = [c for c in checks if not c["passed"]]
    manifest = {"gold_run_id": gold.run_id, "started_at": started.isoformat(), "checks": checks,
                "status": "FAILED" if failed else "SUCCEEDED"}
    if failed:
        put_json(s3, bucket, SERVING_LAYER.manifest_key(gold.run_id), manifest)
        raise ServingError(f"dashboard data does not reconcile with Gold: {[c['name'] for c in failed]}")

    lake_index, lake_checks = publish_lake_catalog(s3, bucket, gold.run_id)
    checks += lake_checks
    if not all(c["passed"] for c in lake_checks):
        manifest.update(checks=checks, status="FAILED")
        put_json(s3, bucket, SERVING_LAYER.manifest_key(gold.run_id), manifest)
        raise ServingError(f"lake catalog does not reconcile: {[c['name'] for c in lake_checks if not c['passed']]}")
    lake_index_key = serving_key(gold.run_id, "lake_index")
    put_json(s3, bucket, lake_index_key, lake_index)

    manifest["checks"] = checks
    put_json(s3, bucket, summary_key, summary)
    s3.put_bytes(bucket, employees_key, json.dumps(  # compact: about half the size of indented JSON
        {"schema_version": SCHEMA_VERSION, "gold_run_id": gold.run_id, "employees": employees},
        separators=(",", ":")).encode(), content_type="application/json")
    put_json(s3, bucket, SERVING_LAYER.manifest_key(gold.run_id), manifest)
    pointer = {"schema_version": SCHEMA_VERSION, "gold_run_id": gold.run_id, "as_of": gold.as_of,
               "published_at": now().isoformat(), "summary_key": summary_key, "employees_key": employees_key,
               "lake_index_key": lake_index_key, "employees": len(employees), "previous_gold_run_id": current.get("gold_run_id") if current else None}
    put_json(s3, bucket, SERVING_LAYER.current_key(), pointer)
    runs = [gold.run_id] + ([current["gold_run_id"]] if current and current.get("gold_run_id") else [])
    retained = {o.key for run in runs for o in s3.list_objects(bucket, SERVING_LAYER.run_prefix(run))}  # + previous
    collect_garbage(s3, bucket, SERVING_LAYER, retained, now())
    return ServingResult("published", gold.run_id, summary_key, employees_key, len(employees), len(checks))


def _context(settings: Settings, s3: S3Client, gold: GoldPointer) -> dict[str, Any]:
    bucket = settings.s3.lake_bucket
    manifest = dq_report.published_manifest(s3, bucket)
    quality = dq_report.entity_quality_table(manifest)[
        ["read", "accepted", "accepted_with_warnings", "quarantined", "deduplicated", "quarantine_rate"]]
    history = dq_report.history_metrics_table(manifest)[
        ["metric", "events", "flagged", "rate", "random_baseline_rate", "vs_random"]]
    checks = dq_report.gold_checks_table(s3, bucket)[["rows", "built_by_run", "checks", "passed"]]
    return {
        "freshness": {"environment": "local (LocalStack)" if settings.is_local else "cloud (AWS)",
                      "lake_bucket": bucket, "as_of": gold.as_of, "gold_run_id": gold.run_id,
                      "gold_published_at": gold.published_at, "silver_run_id": gold.silver_run_id},
        "quality": _records(quality.reset_index()),
        "history_metrics": _records(history.reset_index()),
        "gold_checks": _records(checks.reset_index()),
        "_reason_counts": [e.get("reason_counts", {}) for e in manifest["entities"]],
        "gold_check_details": gold_check_details(s3, bucket),
    }


# ---------------------------------------------------------------- JSON helpers

def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Rows as JSON-ready dicts: Python scalars, None for missing values, ISO strings for dates."""
    dates = [c for c in df.columns if df[c].dtype == object and isinstance(_first(df[c]), date)]
    if dates:  # calendar dates stay YYYY-MM-DD (to_json would turn them into timestamps)
        df = df.assign(**{c: df[c].map(lambda v: v.isoformat() if isinstance(v, date) else None) for c in dates})
    return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))


def _first(values: pd.Series) -> Any:
    present = values.dropna()
    return present.iloc[0] if len(present) else None


def _grouped(df: pd.DataFrame, columns: list[str]) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = {}
    for row in _records(df[["employee_id", *columns]]):
        out.setdefault(row.pop("employee_id"), []).append(row)
    return out


def _by_employee(df: pd.DataFrame) -> dict[int, dict[str, Any]]:
    return {row["employee_id"]: row for row in _records(df)}


def _round(value: Any, digits: int) -> float | None:
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else round(float(value), digits)
