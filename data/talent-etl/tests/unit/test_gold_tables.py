"""
Unit tests for the Gold table builders and checks, on a small hand-built Silver dataset.

Every expected value below was worked out by hand from the fixture; the comments say why.
"""

from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pytest

from talent_etl.gold_tables import TABLES, GoldInputs, check_table, to_arrow

AS_OF = date(2026, 10, 14)


def silver_frames():
    """Four employees, three teams, two competencies; see the test docstrings for the story."""
    d, ts = date, pd.Timestamp
    return {
        "teams": pd.DataFrame({"team_id": [10, 20, 30], "name": ["A", "B", "C"],
                               "department": ["Sales", "Design", "Finance"]}),
        "employees": pd.DataFrame({
            "employee_id": [1, 2, 3, 4], "first_name": ["Ann", "Bo", "Cy", "Di"], "last_name": ["X"] * 4,
            "email": ["a@x.io", "b@x.io", "c@x.io", "d@x.io"], "team_id": [10, 10, 20, 20],
            "job_level": ["IC1", "IC2", "Manager", "Director"],
            "hire_date": [d(2020, 1, 1), d(2025, 3, 1), d(2019, 6, 1), d(2018, 1, 1)]}),
        "competencies": pd.DataFrame({"competency_id": [1, 2], "name": ["Python", "Coaching"],
                                      "category": ["Technical", "Leadership"]}),
        "projects": pd.DataFrame({"project_id": [1, 2], "name": ["P1", "P2"], "team_id": [10, 10],
                                  "outcome_status": ["on-time", "delayed"],
                                  "outcome_score": [Decimal("80.00"), Decimal("60.00")]}),
        "performance_reviews": pd.DataFrame({
            "review_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "employee_id": [1, 1, 2, 2, 3, 3, 4, 4],
            "review_period": ["2024-H2", "2025-H1", "2024-H2", "2025-H1", "2024-H1", "2025-H1", "2024-H2", "2025-H1"],
            "rating": [3, 4, 5, 2, 4, 2, 4, 3],
            "manager_comment": [None] * 8, "submitted_date": [d(2025, 8, 1)] * 8,
            "period_start": [d(2024, 7, 1), d(2025, 1, 1), d(2024, 7, 1), d(2025, 1, 1), d(2024, 1, 1),
                             d(2025, 1, 1), d(2024, 7, 1), d(2025, 1, 1)],
            "period_end": [d(2024, 12, 31), d(2025, 6, 30), d(2024, 12, 31), d(2025, 6, 30), d(2024, 6, 30),
                           d(2025, 6, 30), d(2024, 12, 31), d(2025, 6, 30)],
            "is_pre_hire": [False, False, True, False, False, False, False, False]}),
        "competency_assessments": pd.DataFrame({
            "assessment_id": [1, 2, 3, 4, 5], "employee_id": [1, 1, 2, 3, 3], "competency_id": [1, 2, 1, 1, 1],
            "proficiency_level": [2, 1, 2, 1, 4], "required_level": [4, 2, 3, 4, 4],
            "assessed_date": [d(2025, 6, 1), d(2025, 6, 1), d(2025, 6, 1), d(2025, 1, 1), d(2026, 1, 1)],
            "gap": [2, 1, 1, 3, 0], "is_critical_gap": [True, True, False, True, False],
            "is_pre_hire": [False] * 5}),
        "talent_review": pd.DataFrame({
            "employee_id": [1, 2, 3, 4], "review_cycle": ["2025-H1"] * 4,
            "potential_rating": ["High", "High", "Medium", "High"],
            "promotion_readiness": ["Ready Now", "Ready Now", "1-2 Years", "Ready Now"],
            "attrition_risk": ["Low", "High", "Medium", "Medium"], "is_pre_hire": [False, True, False, False]}),
        "learning_activity": pd.DataFrame({
            "event_id": [f"E{i}" for i in range(1, 11)],
            "employee_id": [1, 1, 1, 1, 1, 1, 2, 2, 4, 1],
            "event_type": ["enrollment", "course_completed", "course_completed", "certification_earned",
                           "quiz_attempted", "quiz_passed", "course_completed", "enrollment", "module_started",
                           "module_started"],
            "course_name": ["SQL", "SQL", "SQL", "SQL", "SQL", "SQL", "Go", "Go", "Python", "Go"],
            "event_date": [ts("2026-01-01"), ts("2026-02-01"), ts("2026-03-01"), ts("2026-03-02"), ts("2026-02-10"),
                           ts("2026-02-11"), ts("2025-01-01"), ts("2025-06-01"), ts("2025-11-01"), ts("2019-01-01")],
            "score": [None, None, None, None, 50.0, 80.0, None, None, None, None],
            "is_pre_hire": [False, False, False, False, False, False, True, False, False, True]}),
    }


@pytest.fixture
def inputs():
    frames = silver_frames()
    return GoldInputs(lambda entity: frames[entity].copy(), AS_OF)


def rows(inputs, name, key="employee_id"):
    return inputs.table(name).set_index(key).to_dict("index")


def test_every_table_builds_converts_and_passes_its_checks(inputs):
    for name, table in TABLES.items():
        df = inputs.table(name)
        arrow = to_arrow(table, df)
        failed = [c for c in check_table(table, arrow, df, inputs) if not c.passed]
        assert failed == [], name


def test_declared_silver_inputs_cover_everything_a_table_reads():
    """Fingerprints rely on these declarations, so a missing input would leave a table stale."""
    frames = silver_frames()
    for name, table in TABLES.items():
        read: set[str] = set()

        def loader(entity, read=read):
            read.add(entity)
            return frames[entity].copy()
        GoldInputs(loader, AS_OF).table(name)
        assert read <= set(table.silver_inputs), (name, read - set(table.silver_inputs))


def test_dim_employee(inputs):
    dim = rows(inputs, "dim_employee")
    assert (dim[1]["team_name"], dim[1]["department"]) == ("A", "Sales")
    assert dim[2]["tenure_years"] == pytest.approx((date(2026, 10, 14) - date(2025, 3, 1)).days / 365.25)


def test_performance_history_labels_pre_hire_and_links_previous_valid_review(inputs):
    history = inputs.table("performance_history").set_index("review_id")
    assert history.loc[3, "is_valid"] == False  # noqa: E712 - pre-hire review of employee 2
    assert pd.isna(history.loc[4, "previous_valid_rating"])   # the pre-hire review is not a previous valid one
    assert (history.loc[2, "previous_valid_rating"], history.loc[2, "rating_change"]) == (3, 1)


def test_employee_performance_trend_needs_consecutive_valid_periods(inputs):
    perf = rows(inputs, "employee_performance")
    assert (perf[1]["rating_trend"], perf[1]["rating_delta"], perf[1]["latest_valid_rating"]) == ("improving", 1, 4)
    assert perf[2]["rating_trend"] == "insufficient_history"      # its only other review is pre-hire (A7)
    assert (perf[2]["valid_review_count"], perf[2]["pre_hire_review_count"]) == (1, 1)
    assert perf[3]["rating_trend"] == "insufficient_history"      # 2024-H1 -> 2025-H1 is not consecutive
    assert perf[4]["rating_trend"] == "declining"
    assert (perf[1]["is_high_performer"], perf[2]["is_low_performer"], perf[4]["is_high_performer"]) == (True, True, False)
    assert "avg_valid_rating" not in inputs.table("employee_performance").columns   # removed: unused


def test_skill_gaps_use_the_latest_assessment_and_the_approved_critical_rule(inputs):
    gaps = inputs.table("employee_skill_gaps").set_index(["employee_id", "competency_id"])
    assert len(gaps) == 4                                           # employee 3 has two assessments of competency 1
    assert gaps.loc[(3, 1), "assessment_id"] == 5 and not gaps.loc[(3, 1), "is_critical_gap"]   # latest wins
    assert gaps.loc[(1, 2), "is_critical_gap"] and gaps.loc[(1, 2), "gap_ratio"] == 0.5         # 1 of 2 missing
    assert not gaps.loc[(2, 1), "is_critical_gap"]                  # 1 of 3 missing
    assert gaps.loc[(1, 1), "department"] == "Sales"


def test_competency_gap_summary_and_ranking(inputs):
    summary = rows(inputs, "competency_gap_summary", "competency_id")
    assert (summary[1]["employees_assessed"], summary[1]["critical_gap_count"]) == (3, 1)
    assert summary[1]["critical_gap_rate"] == pytest.approx(1 / 3)
    assert (summary[2]["critical_gap_rate"], summary[2]["rank_by_critical_rate"]) == (1.0, 1)
    assert summary[1]["employees_below_required"] == 2


def test_skill_distribution_counts_and_shares(inputs):
    dist = inputs.table("skill_distribution").set_index(["competency_id", "department", "proficiency_level"])
    assert dist.loc[(1, "Sales", 2), "employees"] == 2 and dist.loc[(1, "Sales", 2), "share_within_group"] == 1.0
    assert dist.loc[(1, "Design", 4), "employees"] == 1
    assert int(dist["employees"].sum()) == 4


def test_employee_learning_counts_completions_once_and_excludes_pre_hire(inputs):
    learning = rows(inputs, "employee_learning")
    one = learning[1]
    assert (one["events_counted"], one["pre_hire_events_excluded"]) == (6, 1)
    assert (one["courses_completed"], one["certifications_earned"], one["courses_enrolled"]) == (1, 1, 1)  # A13
    assert (one["quiz_attempts"], one["quiz_passes"], one["avg_quiz_score"]) == (1, 1, 65.0)
    assert one["last_activity_date"] == date(2026, 3, 2) and one["is_learning_active_12m"]
    assert (learning[2]["courses_completed"], learning[2]["pre_hire_events_excluded"]) == (0, 1)  # A4
    assert not learning[2]["is_learning_active_12m"]               # 2025-06-01 is outside the 365-day window
    assert learning[3]["events_counted"] == 0 and pd.isna(learning[3]["last_activity_date"])
    assert "quiz_pass_rate" not in inputs.table("employee_learning").columns   # A23: no funnel rates


def test_promotion_pipeline_rule(inputs):
    pipeline = rows(inputs, "promotion_pipeline")
    assert pipeline[1]["is_promotion_ready"]                        # High, Ready Now, latest 4
    assert not pipeline[2]["is_promotion_ready"]                    # High, Ready Now, but latest valid rating 2
    assert not pipeline[4]["is_promotion_ready"]                    # High, Ready Now, latest 3
    assert not pipeline[3]["is_promotion_ready"]                    # Medium potential
    assert (pipeline[1]["critical_gap_count"], pipeline[1]["courses_completed"]) == (2, 1)
    assert pipeline[2]["talent_review_pre_hire"]


def test_attrition_points_and_bands(inputs):
    risk = rows(inputs, "attrition_risk")
    assert (risk[1]["risk_points"], risk[1]["risk_band"]) == (0, "Low")
    assert (risk[2]["risk_points"], risk[2]["risk_band"]) == (4, "High")    # HR High 2 + low rating + inactive
    assert (risk[3]["risk_points"], risk[3]["risk_band"]) == (3, "High")    # HR Medium 1 + low rating + inactive
    assert (risk[4]["risk_points"], risk[4]["risk_band"]) == (2, "Medium")  # HR Medium 1 + declining
    assert risk[4]["is_rating_declining"] and not risk[4]["is_low_latest_rating"]


def test_team_performance(inputs):
    teams = rows(inputs, "team_performance", "team_id")
    a = teams[10]
    assert (a["headcount"], a["avg_latest_valid_rating"], a["high_performer_share"], a["critical_gap_count"]) == (2, 3.0, 0.5, 2)
    assert (a["project_count"], a["avg_project_outcome_score"], a["on_time_project_share"]) == (2, 70.0, 0.5)
    assert (teams[20]["avg_latest_valid_rating"], teams[20]["has_projects"]) == (2.5, False)
    assert teams[30]["headcount"] == 0 and pd.isna(teams[30]["avg_latest_valid_rating"])


def test_to_arrow_matches_contract_and_keeps_nulls(inputs):
    arrow = to_arrow(TABLES["employee_performance"], inputs.table("employee_performance"))
    assert arrow.schema.equals(TABLES["employee_performance"].schema)
    assert arrow.column("previous_valid_rating").to_pylist() == [3, None, None, 4]


# ---- the checks themselves catch broken tables ------------------------------------------------

def failing(inputs, name, df):
    table = TABLES[name]
    arrow = to_arrow(table, df)
    return {c.name for c in check_table(table, arrow, df, inputs) if not c.passed}


def test_duplicate_primary_key_fails_the_grain_check(inputs):
    df = inputs.table("dim_employee")
    assert "primary key ('employee_id',) unique (grain)" in failing(inputs, "dim_employee", pd.concat([df, df.head(1)]))


def test_domain_and_range_violations_fail(inputs):
    df = inputs.table("attrition_risk").copy()
    df.loc[0, "risk_band"] = "Extreme"
    assert "risk_band in ['High', 'Medium', 'Low']" in failing(inputs, "attrition_risk", df)
    perf = inputs.table("employee_performance").copy()
    perf.loc[0, "latest_valid_rating"] = 7
    assert "latest_valid_rating in [1, 5]" in failing(inputs, "employee_performance", perf)


def test_reconciliation_failure_is_reported(inputs):
    df = inputs.table("employee_learning").copy()
    df.loc[0, "courses_completed"] = 5
    assert "sum courses_completed = distinct counted completions (A13)" in failing(inputs, "employee_learning", df)


def test_schema_mismatch_and_nulls_fail(inputs):
    table = TABLES["dim_employee"]
    df = inputs.table("dim_employee")
    arrow = to_arrow(table, df).set_column(0, pa.field("employee_id", pa.int64()), pa.array([1, 2, 3, None]))
    failed = {c.name for c in check_table(table, arrow, df, inputs) if not c.passed}
    assert {"schema matches contract", "primary key ('employee_id',) not null"} <= failed
