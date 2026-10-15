"""Integration: seeded sources -> Bronze -> Silver -> Gold in a throwaway LocalStack bucket.

The expected values were measured independently with pandas on the published Silver data
(before the Gold code existed) and are documented in docs/gold_design.md.
"""

from dataclasses import replace
from datetime import date

import pytest

import job
from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor
from talent_etl.gold import GoldBuilder, load_gold_table, read_current_gold
from talent_etl.silver import SilverBuilder

pytestmark = pytest.mark.integration


@pytest.fixture
def lake(settings, pg, s3, s3_bucket):
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    lake_settings = replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket))
    BronzeIngestor(lake_settings, pg, s3, run_id="it-bronze", ingest_date=date(2026, 10, 14)).run()
    SilverBuilder(lake_settings, s3, run_id="it-silver").run()
    return lake_settings


def test_gold_on_real_data_matches_independent_measurements(lake, s3, s3_bucket):
    result = GoldBuilder(lake, s3, run_id="it-gold").run()
    assert (result.status, result.published) == ("SUCCEEDED", True)
    assert all(c["passed"] for t in result.tables for c in t.checks)
    g = lambda name: load_gold_table(s3, s3_bucket, name).to_pandas()  # noqa: E731
    assert read_current_gold(s3, s3_bucket).rows == {
        "dim_employee": 2000, "performance_history": 4000, "employee_performance": 2000, "employee_skill_gaps": 4000,
        "competency_gap_summary": 40, "skill_distribution": 1595, "employee_learning": 2000,
        "promotion_pipeline": 2000, "attrition_risk": 2000, "team_performance": 100}

    perf = g("employee_performance")
    assert perf["rating_trend"].value_counts().to_dict() == {
        "improving": 721, "stable": 692, "insufficient_history": 396, "declining": 191}
    assert (int(perf["is_high_performer"].sum()), int(perf["is_low_performer"].sum())) == (879, 402)
    pipeline = g("promotion_pipeline")
    assert int(pipeline["is_promotion_ready"].sum()) == 148
    learning = g("employee_learning")
    assert (int(learning["courses_completed"].sum()), int(learning["certifications_earned"].sum())) == (2824, 1465)
    assert int((~learning["is_learning_active_12m"]).sum()) == 56
    assert int(learning["events_counted"].sum() + learning["pre_hire_events_excluded"].sum()) == 29574
    assert g("attrition_risk")["risk_band"].value_counts().to_dict() == {"Low": 1536, "Medium": 271, "High": 193}
    assert int(g("employee_skill_gaps")["is_critical_gap"].sum()) == 682              # approved A1
    teams = g("team_performance")
    with_projects = teams[teams["has_projects"]]
    assert len(with_projects) == 27
    assert round(with_projects[["avg_latest_valid_rating", "avg_project_outcome_score"]].corr().iloc[0, 1], 3) == 0.136


def test_gold_rerun_reuses_tables(lake, s3, s3_bucket):
    GoldBuilder(lake, s3, run_id="it-gold-1").run()
    second = GoldBuilder(lake, s3, run_id="it-gold-2").run()
    assert {t.mode for t in second.tables} == {"reused"}
    assert read_current_gold(s3, s3_bucket).previous_run_id == "it-gold-1"


def test_job_gold_and_all_steps(lake, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "gold"]) == 0
    assert job.main(["--step", "gold", "--full-refresh"]) == 0
    assert job.main(["--step", "all", "--ingest-date", "2026-10-14"]) == 0


def test_job_gold_fails_clearly_without_silver(settings, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "gold"]) == 1
