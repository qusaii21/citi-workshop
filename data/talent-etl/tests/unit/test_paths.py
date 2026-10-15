"""Lake path conventions are stable: Silver depends on these exact keys."""

from datetime import date

from talent_etl import paths
from talent_etl.contracts import CONTRACTS


def test_bronze_paths():
    assert paths.bronze_postgres_key("employees", date(2026, 10, 14)) == \
        "bronze/postgres/employees/ingest_date=2026-10-14/part-00000.parquet"
    assert paths.bronze_learning_prefix() == "bronze/learning_activity/"


def test_silver_paths():
    assert paths.silver_run_prefix("r1") == "silver/runs/run_id=r1/"
    assert paths.silver_key("employees", "r1") == "silver/runs/run_id=r1/employees/part-00000.parquet"
    assert paths.silver_month_key("learning_activity", 2024, 2, "r1") == \
        "silver/runs/run_id=r1/learning_activity/event_year=2024/event_month=02/part-00000.parquet"
    assert paths.silver_quarantine_key("learning_activity", "r1") == \
        "silver/runs/run_id=r1/_quarantine/learning_activity/part-00000.parquet"
    assert paths.silver_started_key("r1") == "silver/runs/run_id=r1/_STARTED.json"
    assert paths.silver_dataset_prefix("teams", "r1") == "silver/runs/run_id=r1/teams/"
    assert paths.silver_current_key() == "silver/_current.json"
    assert paths.silver_manifest_key("r1") == "silver/_manifests/run_id=r1.json"


def test_partition_columns_match_contract():
    assert CONTRACTS["learning_activity"].partition_by == ("event_year", "event_month")
    assert all(not c.partition_by for n, c in CONTRACTS.items() if n != "learning_activity")
    assert CONTRACTS["performance_reviews"].required_columns == (
        "review_id", "employee_id", "review_period", "rating", "submitted_date")
