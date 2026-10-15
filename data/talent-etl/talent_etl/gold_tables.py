"""
Gold table definitions: contract, Silver inputs, build logic and reconciliation checks.

Every builder is a pure function from Silver data (pandas) to one Gold table. Builders may
use other Gold tables through ``GoldInputs.table(name)`` (computed once, cached). Every
table declares its grain (primary key) and the Silver entities it reads, which decide
when it must be rebuilt. Checks reconcile each table with Silver independently of the
builder; see docs/gold_design.md for the business logic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd
import pyarrow as pa

from talent_etl.definitions import (
    ATTRITION_HR_POINTS,
    CERTIFICATION_EVENT,
    COURSE_COMPLETION_EVENT,
    ENROLLMENT_EVENT,
    HIGH_PERFORMER_MIN_RATING,
    LEARNING_ACTIVITY_WINDOW_DAYS,
    LOW_PERFORMER_MAX_RATING,
    PROMOTION_MIN_RATING,
    PROMOTION_POTENTIAL,
    PROMOTION_READINESS,
    RATING_TRENDS,
    RISK_BANDS,
    attrition_band,
    period_index,
)


class GoldInputs:
    """Lazy access to Silver entities and to Gold tables already built in this run."""

    def __init__(self, load_silver: Callable[[str], pd.DataFrame], as_of: date) -> None:
        self._load_silver = load_silver
        self._silver: dict[str, pd.DataFrame] = {}
        self._gold: dict[str, pd.DataFrame] = {}
        self.as_of = as_of

    def silver(self, entity: str) -> pd.DataFrame:
        """A Silver entity (loaded once per run)."""
        if entity not in self._silver:
            self._silver[entity] = self._load_silver(entity)
        return self._silver[entity]

    def table(self, name: str) -> pd.DataFrame:
        """A Gold table computed in this run (built once, then cached)."""
        if name not in self._gold:
            self._gold[name] = TABLES[name].build(self)
        return self._gold[name]


@dataclass(frozen=True)
class Check:
    """Outcome of one validation check."""

    table: str
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class GoldTable:
    """Contract and logic of one Gold table."""

    name: str
    question: str
    primary_key: tuple[str, ...]
    silver_inputs: tuple[str, ...]
    uses_as_of: bool
    schema: pa.Schema
    build: Callable[[GoldInputs], pd.DataFrame]
    reconcile: Callable[[pd.DataFrame, GoldInputs], list[tuple[str, bool, str]]] = field(default=lambda df, i: [])
    domains: dict[str, tuple] = field(default_factory=dict)       # column -> allowed values
    ranges: dict[str, tuple[float, float]] = field(default_factory=dict)  # column -> inclusive range


def _f(name, typ, nullable=False):
    return pa.field(name, typ, nullable=nullable)


I32, F64, STR, DATE, BOOL = pa.int32(), pa.float64(), pa.string(), pa.date32(), pa.bool_()


# ---- helpers ---------------------------------------------------------------------------------

def _valid_reviews(i: GoldInputs) -> pd.DataFrame:
    reviews = i.silver("performance_reviews")
    valid = reviews[~reviews["is_pre_hire"]].copy()
    valid["period_index"] = valid["review_period"].map(period_index)
    return valid.sort_values(["employee_id", "period_index"])


def _latest_assessments(i: GoldInputs) -> pd.DataFrame:
    """Latest assessment per (employee, competency): latest date, then highest id (A15)."""
    assessments = i.silver("competency_assessments")
    ordered = assessments.sort_values(["employee_id", "competency_id", "assessed_date", "assessment_id"])
    return ordered.groupby(["employee_id", "competency_id"], as_index=False).tail(1)


def _counted_events(i: GoldInputs) -> pd.DataFrame:
    events = i.silver("learning_activity")
    return events[~events["is_pre_hire"]]


def _window_start(i: GoldInputs) -> pd.Timestamp:
    return pd.Timestamp(i.as_of) - pd.Timedelta(days=LEARNING_ACTIVITY_WINDOW_DAYS)


def _employee_ids(i: GoldInputs) -> pd.Index:
    return pd.Index(i.silver("employees")["employee_id"].sort_values(), name="employee_id")


# ---- 1. dim_employee ----------------------------------------------------------------------

def build_dim_employee(i: GoldInputs) -> pd.DataFrame:
    teams = i.silver("teams")[["team_id", "name", "department"]].rename(columns={"name": "team_name"})
    df = i.silver("employees")[["employee_id", "first_name", "last_name", "email", "job_level", "hire_date",
                                "team_id"]].merge(teams, on="team_id", how="left")
    df["tenure_years"] = (pd.Timestamp(i.as_of) - pd.to_datetime(df["hire_date"])).dt.days / 365.25
    df["as_of"] = i.as_of
    return df.sort_values("employee_id")


def reconcile_dim_employee(df, i):
    return [("rows = Silver employees", len(df) == len(i.silver("employees")), f"{len(df)} vs {len(i.silver('employees'))}")]


# ---- 2. performance_history ---------------------------------------------------------------

def build_performance_history(i: GoldInputs) -> pd.DataFrame:
    df = i.silver("performance_reviews")[["employee_id", "review_period", "review_id", "rating", "period_start",
                                          "period_end", "submitted_date", "manager_comment", "is_pre_hire"]].copy()
    df["is_valid"] = ~df["is_pre_hire"]
    valid = _valid_reviews(i)[["review_id", "employee_id", "rating"]].copy()
    valid["previous_valid_rating"] = valid.groupby("employee_id")["rating"].shift(1)
    df = df.merge(valid[["review_id", "previous_valid_rating"]], on="review_id", how="left")
    df["rating_change"] = df["rating"] - df["previous_valid_rating"]
    return df.sort_values(["employee_id", "review_period"])


def reconcile_performance_history(df, i):
    reviews = i.silver("performance_reviews")
    valid = int((~reviews["is_pre_hire"]).sum())
    return [("rows = Silver reviews", len(df) == len(reviews), f"{len(df)} vs {len(reviews)}"),
            ("valid rows = Silver non-pre-hire reviews", int(df["is_valid"].sum()) == valid,
             f"{int(df['is_valid'].sum())} vs {valid}")]


# ---- 3. employee_performance --------------------------------------------------------------

def build_employee_performance(i: GoldInputs) -> pd.DataFrame:
    valid = _valid_reviews(i)
    out = pd.DataFrame(index=_employee_ids(i))
    grouped = valid.groupby("employee_id")
    latest = grouped.tail(1).set_index("employee_id")
    previous = valid[grouped.cumcount(ascending=False) == 1].set_index("employee_id")
    out["valid_review_count"] = grouped.size()
    out["pre_hire_review_count"] = i.silver("performance_reviews").query("is_pre_hire").groupby("employee_id").size()
    out[["valid_review_count", "pre_hire_review_count"]] = out[["valid_review_count", "pre_hire_review_count"]].fillna(0)
    out["latest_valid_period"] = latest["review_period"]
    out["latest_valid_rating"] = latest["rating"]
    consecutive = (latest["period_index"] - previous["period_index"]).reindex(out.index) == 1
    out["previous_valid_rating"] = previous["rating"].where(consecutive)
    out["rating_delta"] = out["latest_valid_rating"] - out["previous_valid_rating"]
    out["rating_trend"] = np.select(
        [out["rating_delta"] > 0, out["rating_delta"] == 0, out["rating_delta"] < 0],
        ["improving", "stable", "declining"], default="insufficient_history")
    out["is_high_performer"] = out["latest_valid_rating"] >= HIGH_PERFORMER_MIN_RATING
    out["is_low_performer"] = out["latest_valid_rating"] <= LOW_PERFORMER_MAX_RATING
    return out.reset_index()


def reconcile_employee_performance(df, i):
    valid = int((~i.silver("performance_reviews")["is_pre_hire"]).sum())
    employees = len(i.silver("employees"))
    return [("rows = Silver employees", len(df) == employees, f"{len(df)} vs {employees}"),
            ("sum valid_review_count = valid reviews", int(df["valid_review_count"].sum()) == valid,
             f"{int(df['valid_review_count'].sum())} vs {valid}"),
            ("trend counts add up to employees", int(df["rating_trend"].value_counts().sum()) == employees, ""),
            ("trend present iff delta present",
             bool(((df["rating_trend"] == "insufficient_history") == df["rating_delta"].isna()).all()), "")]


# ---- 4. employee_skill_gaps ---------------------------------------------------------------

def build_employee_skill_gaps(i: GoldInputs) -> pd.DataFrame:
    latest = _latest_assessments(i)
    competencies = i.silver("competencies")[["competency_id", "name", "category"]].rename(
        columns={"name": "competency_name", "category": "competency_category"})
    people = i.table("dim_employee")[["employee_id", "department", "team_id", "job_level"]]
    df = latest.merge(competencies, on="competency_id").merge(people, on="employee_id")
    df["gap_ratio"] = df["gap"] / df["required_level"]
    df["has_gap"] = df["gap"] > 0
    return df[["employee_id", "competency_id", "competency_name", "competency_category", "department", "team_id",
               "job_level", "assessment_id", "assessed_date", "proficiency_level", "required_level", "gap",
               "gap_ratio", "has_gap", "is_critical_gap", "is_pre_hire"]].sort_values(["employee_id", "competency_id"])


def reconcile_employee_skill_gaps(df, i):
    assessments = i.silver("competency_assessments")
    pairs = assessments[["employee_id", "competency_id"]].drop_duplicates()
    latest = assessments.sort_values("assessed_date").drop_duplicates(["employee_id", "competency_id"], keep="last")
    critical = int(((latest["required_level"] - latest["proficiency_level"]) * 2 >= latest["required_level"])
                   .where(latest["required_level"] > latest["proficiency_level"], False).sum())
    return [("rows = distinct (employee, competency) in Silver", len(df) == len(pairs), f"{len(df)} vs {len(pairs)}"),
            ("critical gaps = recomputed from latest Silver assessments", int(df["is_critical_gap"].sum()) == critical,
             f"{int(df['is_critical_gap'].sum())} vs {critical}")]


# ---- 5. competency_gap_summary ------------------------------------------------------------

def build_competency_gap_summary(i: GoldInputs) -> pd.DataFrame:
    gaps = i.table("employee_skill_gaps")
    df = gaps.groupby(["competency_id", "competency_name", "competency_category"], as_index=False).agg(
        employees_assessed=("employee_id", "nunique"), avg_proficiency=("proficiency_level", "mean"),
        avg_required=("required_level", "mean"), employees_below_required=("has_gap", "sum"),
        critical_gap_count=("is_critical_gap", "sum"))
    df["critical_gap_rate"] = df["critical_gap_count"] / df["employees_assessed"]
    df["rank_by_critical_rate"] = df["critical_gap_rate"].rank(method="min", ascending=False).astype(int)
    return df.sort_values("competency_id")


def reconcile_competency_gap_summary(df, i):
    gaps = i.table("employee_skill_gaps")
    return [("rows = competencies assessed", len(df) == gaps["competency_id"].nunique(), ""),
            ("sum critical = employee_skill_gaps critical", int(df["critical_gap_count"].sum()) ==
             int(gaps["is_critical_gap"].sum()), f"{int(df['critical_gap_count'].sum())}")]


# ---- 6. skill_distribution ----------------------------------------------------------------

def build_skill_distribution(i: GoldInputs) -> pd.DataFrame:
    gaps = i.table("employee_skill_gaps")
    keys = ["competency_id", "competency_name", "competency_category", "department", "proficiency_level"]
    df = gaps.groupby(keys, as_index=False).agg(employees=("employee_id", "nunique"))
    group_total = df.groupby(["competency_id", "department"])["employees"].transform("sum")
    df["share_within_group"] = df["employees"] / group_total
    return df.sort_values(["competency_id", "department", "proficiency_level"])


def reconcile_skill_distribution(df, i):
    gaps = i.table("employee_skill_gaps")
    shares = df.groupby(["competency_id", "department"])["share_within_group"].sum()
    return [("sum employees = employee_skill_gaps rows", int(df["employees"].sum()) == len(gaps),
             f"{int(df['employees'].sum())} vs {len(gaps)}"),
            ("shares add up to 1 per group", bool(np.allclose(shares, 1.0)), "")]


# ---- 7. employee_learning -----------------------------------------------------------------

def build_employee_learning(i: GoldInputs) -> pd.DataFrame:
    events = i.silver("learning_activity")
    counted = _counted_events(i)
    out = pd.DataFrame(index=_employee_ids(i))
    by_employee = counted.groupby("employee_id")

    def distinct_courses(event_type):
        return counted[counted["event_type"] == event_type].groupby("employee_id")["course_name"].nunique()

    def count(event_type):
        return counted[counted["event_type"] == event_type].groupby("employee_id").size()

    out["events_counted"] = by_employee.size()
    out["pre_hire_events_excluded"] = events[events["is_pre_hire"]].groupby("employee_id").size()
    out["courses_enrolled"] = distinct_courses(ENROLLMENT_EVENT)
    out["courses_completed"] = distinct_courses(COURSE_COMPLETION_EVENT)
    out["certifications_earned"] = distinct_courses(CERTIFICATION_EVENT)
    out["quiz_attempts"] = count("quiz_attempted")
    out["quiz_passes"] = count("quiz_passed")
    counts = ["events_counted", "pre_hire_events_excluded", "courses_enrolled", "courses_completed",
              "certifications_earned", "quiz_attempts", "quiz_passes"]
    out[counts] = out[counts].fillna(0)
    out["avg_quiz_score"] = counted.dropna(subset=["score"]).groupby("employee_id")["score"].mean()
    out["last_activity_date"] = by_employee["event_date"].max().dt.date
    recent = counted[counted["event_date"] >= _window_start(i)]
    out["events_last_12m"] = recent.groupby("employee_id").size()
    out["events_last_12m"] = out["events_last_12m"].fillna(0)
    out["is_learning_active_12m"] = out["events_last_12m"] > 0
    return out.reset_index()


def reconcile_employee_learning(df, i):
    events = i.silver("learning_activity")
    counted = events[~events["is_pre_hire"]]
    completions = counted[counted["event_type"] == COURSE_COMPLETION_EVENT][["employee_id", "course_name"]]
    distinct = len(completions.drop_duplicates())
    total = int(df["events_counted"].sum() + df["pre_hire_events_excluded"].sum())
    return [("rows = Silver employees", len(df) == len(i.silver("employees")), ""),
            ("counted + excluded events = Silver learning rows", total == len(events), f"{total} vs {len(events)}"),
            ("sum courses_completed = distinct counted completions (A13)", int(df["courses_completed"].sum()) == distinct,
             f"{int(df['courses_completed'].sum())} vs {distinct}")]


# ---- 8. promotion_pipeline ----------------------------------------------------------------

def build_promotion_pipeline(i: GoldInputs) -> pd.DataFrame:
    talent = i.silver("talent_review").rename(columns={"attrition_risk": "hr_attrition_risk",
                                                        "is_pre_hire": "talent_review_pre_hire"})
    people = i.table("dim_employee")[["employee_id", "job_level", "department"]]
    performance = i.table("employee_performance")[["employee_id", "latest_valid_rating", "rating_trend"]]
    critical = i.table("employee_skill_gaps").groupby("employee_id")["is_critical_gap"].sum().rename("critical_gap_count")
    learning = i.table("employee_learning")[["employee_id", "courses_completed"]]
    df = (talent[["employee_id", "review_cycle", "potential_rating", "promotion_readiness", "hr_attrition_risk",
                  "talent_review_pre_hire"]]
          .merge(people, on="employee_id", how="left").merge(performance, on="employee_id", how="left")
          .merge(critical, on="employee_id", how="left").merge(learning, on="employee_id", how="left"))
    df["critical_gap_count"] = df["critical_gap_count"].fillna(0)
    df["is_promotion_ready"] = ((df["potential_rating"] == PROMOTION_POTENTIAL)
                                & (df["promotion_readiness"] == PROMOTION_READINESS)
                                & (df["latest_valid_rating"] >= PROMOTION_MIN_RATING)).fillna(False)
    return df.sort_values("employee_id")


def reconcile_promotion_pipeline(df, i):
    ready = df[df["is_promotion_ready"]]
    rule = ((ready["potential_rating"] == PROMOTION_POTENTIAL) & (ready["promotion_readiness"] == PROMOTION_READINESS)
            & (ready["latest_valid_rating"] >= PROMOTION_MIN_RATING)).all()
    return [("rows = Silver talent reviews", len(df) == len(i.silver("talent_review")), ""),
            ("every ready employee satisfies the rule (A19)", bool(rule), f"{len(ready)} ready")]


# ---- 9. attrition_risk --------------------------------------------------------------------

def build_attrition_risk(i: GoldInputs) -> pd.DataFrame:
    performance = i.table("employee_performance").set_index("employee_id")
    learning = i.table("employee_learning").set_index("employee_id")
    talent = i.silver("talent_review").set_index("employee_id")
    out = pd.DataFrame(index=_employee_ids(i))
    out["hr_attrition_risk"] = talent["attrition_risk"]
    out["talent_review_pre_hire"] = talent["is_pre_hire"]
    out["latest_valid_rating"] = performance["latest_valid_rating"]
    out["rating_trend"] = performance["rating_trend"]
    out["hr_points"] = out["hr_attrition_risk"].map(ATTRITION_HR_POINTS).fillna(0)
    out["is_rating_declining"] = out["rating_trend"] == "declining"
    out["is_low_latest_rating"] = (out["latest_valid_rating"] <= LOW_PERFORMER_MAX_RATING).fillna(False)
    out["is_learning_inactive_12m"] = ~learning["is_learning_active_12m"]
    out["risk_points"] = (out["hr_points"] + out["is_rating_declining"].astype(int)
                          + out["is_low_latest_rating"].astype(int) + out["is_learning_inactive_12m"].astype(int))
    out["risk_band"] = out["risk_points"].map(attrition_band)
    return out.reset_index()


def reconcile_attrition_risk(df, i):
    recomputed = (df["hr_attrition_risk"].map(ATTRITION_HR_POINTS).fillna(0) + df["is_rating_declining"].astype(int)
                  + df["is_low_latest_rating"].astype(int) + df["is_learning_inactive_12m"].astype(int))
    return [("rows = Silver employees", len(df) == len(i.silver("employees")), ""),
            ("risk_points = sum of signal points", bool((recomputed == df["risk_points"]).all()), ""),
            ("band consistent with points", bool((df["risk_points"].map(attrition_band) == df["risk_band"]).all()), "")]


# ---- 10. team_performance -----------------------------------------------------------------

def build_team_performance(i: GoldInputs) -> pd.DataFrame:
    people = i.table("dim_employee")[["employee_id", "team_id"]]
    performance = i.table("employee_performance")[["employee_id", "latest_valid_rating", "is_high_performer"]]
    critical = i.table("employee_skill_gaps").groupby("employee_id")["is_critical_gap"].sum().rename("critical_gaps")
    staff = people.merge(performance, on="employee_id").merge(critical, on="employee_id", how="left")
    rated = staff.dropna(subset=["latest_valid_rating"])
    teams = i.silver("teams")[["team_id", "name", "department"]].rename(columns={"name": "team_name"}).set_index("team_id")
    out = teams.copy()
    out["headcount"] = staff.groupby("team_id").size()
    out["employees_with_valid_rating"] = rated.groupby("team_id").size()
    out["avg_latest_valid_rating"] = rated.groupby("team_id")["latest_valid_rating"].mean()
    out["high_performer_share"] = rated.groupby("team_id")["is_high_performer"].mean()
    out["critical_gap_count"] = staff.groupby("team_id")["critical_gaps"].sum()
    projects = i.silver("projects").copy()
    projects["score"] = projects["outcome_score"].astype(float)
    out["project_count"] = projects.groupby("team_id").size()
    out["avg_project_outcome_score"] = projects.groupby("team_id")["score"].mean()
    out["on_time_project_share"] = projects.assign(on_time=projects["outcome_status"] == "on-time").groupby(
        "team_id")["on_time"].mean()
    for column in ("headcount", "employees_with_valid_rating", "critical_gap_count", "project_count"):
        out[column] = out[column].fillna(0)
    out["has_projects"] = out["project_count"] > 0
    return out.reset_index().sort_values("team_id")


def reconcile_team_performance(df, i):
    return [("rows = Silver teams", len(df) == len(i.silver("teams")), ""),
            ("sum headcount = Silver employees", int(df["headcount"].sum()) == len(i.silver("employees")),
             f"{int(df['headcount'].sum())}"),
            ("sum project_count = Silver projects", int(df["project_count"].sum()) == len(i.silver("projects")), "")]


# ---- registry -------------------------------------------------------------------------------

TABLES: dict[str, GoldTable] = {t.name: t for t in (
    GoldTable("dim_employee", "shared dimension", ("employee_id",), ("employees", "teams"), True, pa.schema([
        _f("employee_id", I32), _f("first_name", STR), _f("last_name", STR), _f("email", STR), _f("job_level", STR),
        _f("hire_date", DATE), _f("team_id", I32), _f("team_name", STR, True), _f("department", STR, True),
        _f("tenure_years", F64), _f("as_of", DATE)]), build_dim_employee, reconcile_dim_employee),
    GoldTable("performance_history", "Q1 rating history", ("employee_id", "review_period"),
              ("performance_reviews",), False, pa.schema([
        _f("employee_id", I32), _f("review_period", STR), _f("review_id", I32), _f("rating", I32),
        _f("period_start", DATE), _f("period_end", DATE), _f("submitted_date", DATE), _f("manager_comment", STR, True),
        _f("is_pre_hire", BOOL), _f("is_valid", BOOL), _f("previous_valid_rating", I32, True),
        _f("rating_change", I32, True)]), build_performance_history, reconcile_performance_history,
              ranges={"rating": (1, 5), "previous_valid_rating": (1, 5)}),
    GoldTable("employee_performance", "Q1 latest rating and trend", ("employee_id",),
              ("performance_reviews", "employees"), False, pa.schema([
        _f("employee_id", I32), _f("valid_review_count", I32), _f("pre_hire_review_count", I32),
        _f("latest_valid_period", STR, True), _f("latest_valid_rating", I32, True),
        _f("previous_valid_rating", I32, True), _f("rating_delta", I32, True), _f("rating_trend", STR),
        _f("is_high_performer", BOOL), _f("is_low_performer", BOOL)]),
              build_employee_performance, reconcile_employee_performance,
              domains={"rating_trend": RATING_TRENDS}, ranges={"latest_valid_rating": (1, 5)}),
    GoldTable("employee_skill_gaps", "Q2 skill gaps", ("employee_id", "competency_id"),
              ("competency_assessments", "competencies", "employees", "teams"), True, pa.schema([
        _f("employee_id", I32), _f("competency_id", I32), _f("competency_name", STR), _f("competency_category", STR),
        _f("department", STR, True), _f("team_id", I32), _f("job_level", STR), _f("assessment_id", I32),
        _f("assessed_date", DATE), _f("proficiency_level", I32), _f("required_level", I32), _f("gap", I32),
        _f("gap_ratio", F64), _f("has_gap", BOOL), _f("is_critical_gap", BOOL), _f("is_pre_hire", BOOL)]),
              build_employee_skill_gaps, reconcile_employee_skill_gaps,
              ranges={"proficiency_level": (1, 5), "required_level": (1, 5)}),
    GoldTable("competency_gap_summary", "Q2 org-wide gaps / training needs", ("competency_id",),
              ("competency_assessments", "competencies", "employees", "teams"), True, pa.schema([
        _f("competency_id", I32), _f("competency_name", STR), _f("competency_category", STR),
        _f("employees_assessed", I32), _f("avg_proficiency", F64), _f("avg_required", F64),
        _f("employees_below_required", I32), _f("critical_gap_count", I32), _f("critical_gap_rate", F64),
        _f("rank_by_critical_rate", I32)]), build_competency_gap_summary, reconcile_competency_gap_summary,
              ranges={"critical_gap_rate": (0, 1)}),
    GoldTable("skill_distribution", "Q7 skill distribution",
              ("competency_id", "department", "proficiency_level"),
              ("competency_assessments", "competencies", "employees", "teams"), True, pa.schema([
        _f("competency_id", I32), _f("competency_name", STR), _f("competency_category", STR), _f("department", STR),
        _f("proficiency_level", I32), _f("employees", I32), _f("share_within_group", F64)]),
              build_skill_distribution, reconcile_skill_distribution, ranges={"share_within_group": (0, 1)}),
    GoldTable("employee_learning", "Q4 training completed", ("employee_id",),
              ("learning_activity", "employees"), True, pa.schema([
        _f("employee_id", I32), _f("events_counted", I32), _f("pre_hire_events_excluded", I32),
        _f("courses_enrolled", I32), _f("courses_completed", I32), _f("certifications_earned", I32),
        _f("quiz_attempts", I32), _f("quiz_passes", I32), _f("avg_quiz_score", F64, True),
        _f("last_activity_date", DATE, True), _f("events_last_12m", I32), _f("is_learning_active_12m", BOOL)]),
              build_employee_learning, reconcile_employee_learning, ranges={"avg_quiz_score": (0, 100)}),
    GoldTable("promotion_pipeline", "Q3 promotion readiness", ("employee_id",),
              ("talent_review", "performance_reviews", "competency_assessments", "competencies", "employees",
               "teams", "learning_activity"), True, pa.schema([
        _f("employee_id", I32), _f("review_cycle", STR), _f("potential_rating", STR), _f("promotion_readiness", STR),
        _f("hr_attrition_risk", STR), _f("talent_review_pre_hire", BOOL), _f("job_level", STR, True),
        _f("department", STR, True), _f("latest_valid_rating", I32, True), _f("rating_trend", STR, True),
        _f("critical_gap_count", I32), _f("courses_completed", I32, True), _f("is_promotion_ready", BOOL)]),
              build_promotion_pipeline, reconcile_promotion_pipeline),
    GoldTable("attrition_risk", "Q5 attrition risk", ("employee_id",),
              ("talent_review", "performance_reviews", "learning_activity", "employees"), True, pa.schema([
        _f("employee_id", I32), _f("hr_attrition_risk", STR, True), _f("talent_review_pre_hire", BOOL, True),
        _f("latest_valid_rating", I32, True), _f("rating_trend", STR), _f("hr_points", I32),
        _f("is_rating_declining", BOOL), _f("is_low_latest_rating", BOOL), _f("is_learning_inactive_12m", BOOL),
        _f("risk_points", I32), _f("risk_band", STR)]), build_attrition_risk, reconcile_attrition_risk,
              domains={"risk_band": RISK_BANDS, "rating_trend": RATING_TRENDS}, ranges={"risk_points": (0, 5)}),
    GoldTable("team_performance", "Q6 performance vs team and project outcomes", ("team_id",),
              ("teams", "employees", "performance_reviews", "competency_assessments", "competencies", "projects"),
              True, pa.schema([
        _f("team_id", I32), _f("team_name", STR), _f("department", STR), _f("headcount", I32),
        _f("employees_with_valid_rating", I32), _f("avg_latest_valid_rating", F64, True),
        _f("high_performer_share", F64, True), _f("critical_gap_count", I32), _f("project_count", I32),
        _f("avg_project_outcome_score", F64, True), _f("on_time_project_share", F64, True), _f("has_projects", BOOL)]),
              build_team_performance, reconcile_team_performance,
              ranges={"avg_latest_valid_rating": (1, 5), "high_performer_share": (0, 1), "on_time_project_share": (0, 1)}),
)}


def to_arrow(table: GoldTable, df: pd.DataFrame) -> pa.Table:
    """Convert a built DataFrame to the table's exact Arrow schema (columns in contract order)."""
    columns = {}
    for f in table.schema:
        series = df[f.name]
        values = [None if v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NaT or v is pd.NA else v
                  for v in series.tolist()]
        if pa.types.is_date32(f.type):
            values = [v.date() if isinstance(v, pd.Timestamp) else v for v in values]
        if pa.types.is_integer(f.type):
            values = [int(v) if v is not None else None for v in values]
        if pa.types.is_boolean(f.type):
            values = [bool(v) if v is not None else None for v in values]
        columns[f.name] = pa.array(values, type=f.type)
    return pa.table(columns, schema=table.schema)


def check_table(table: GoldTable, arrow: pa.Table, df: pd.DataFrame, inputs: GoldInputs) -> list[Check]:
    """Run the generic contract checks and the table's reconciliation with Silver."""
    checks = [Check(table.name, "schema matches contract", arrow.schema.equals(table.schema), "")]
    key = arrow.select(list(table.primary_key)).to_pandas()
    checks.append(Check(table.name, f"primary key {table.primary_key} not null",
                        not key.isna().any().any(), ""))
    duplicates = int(key.duplicated().sum())
    checks.append(Check(table.name, f"primary key {table.primary_key} unique (grain)", duplicates == 0,
                        f"{duplicates} duplicates"))
    nullable = {f.name for f in table.schema if f.nullable}
    for f in table.schema:
        if f.name not in nullable and arrow.column(f.name).null_count:
            checks.append(Check(table.name, f"{f.name} not null", False, f"{arrow.column(f.name).null_count} nulls"))
    for column, allowed in table.domains.items():
        values = set(arrow.column(column).drop_null().to_pylist())
        checks.append(Check(table.name, f"{column} in {list(allowed)}", values <= set(allowed),
                            f"unexpected {sorted(values - set(allowed))}"))
    for column, (low, high) in table.ranges.items():
        values = arrow.column(column).drop_null().to_pylist()
        bad = [v for v in values if not low <= v <= high]
        checks.append(Check(table.name, f"{column} in [{low}, {high}]", not bad, f"{len(bad)} out of range"))
    for name, passed, detail in table.reconcile(df, inputs):
        checks.append(Check(table.name, name, bool(passed), detail))
    return checks
