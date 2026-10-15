"""Integration: seeded sources -> Bronze -> Silver (incremental, atomic publish) in a throwaway LocalStack bucket."""

import json
from dataclasses import replace
from datetime import date

import pandas as pd
import pyarrow as pa
import pytest

import job
from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor
from talent_etl.contracts import CONTRACTS
from talent_etl.io_s3 import S3Client
from talent_etl.silver import SilverBuilder, current_files, read_current

pytestmark = pytest.mark.integration

INGEST_DATE = date(2026, 10, 14)
EXPECTED_SILVER_ROWS = {
    "teams": 100, "competencies": 40, "employees": 2000, "projects": 33, "performance_reviews": 4000,
    "competency_assessments": 4000, "talent_review": 2000, "learning_activity": 29574,
}
LINEAGE = ["_run_id", "_processed_at", "_bronze_ingest_date"]


@pytest.fixture
def lake(settings, pg, s3, s3_bucket):
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    lake_settings = replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket))
    BronzeIngestor(lake_settings, pg, s3, run_id="it-bronze", ingest_date=INGEST_DATE).run()
    return lake_settings


def published(s3, bucket, entity):
    return pa.concat_tables([s3.read_arrow(bucket, key) for key in current_files(s3, bucket, entity)])


def learning_content(s3, bucket):
    table = published(s3, bucket, "learning_activity").drop_columns(LINEAGE)
    return table.sort_by([("event_id", "ascending")])


def test_first_build_reconciles_with_bronze_and_is_queryable(lake, s3, s3_bucket, tmp_path):
    result = SilverBuilder(lake, s3, run_id="it-silver-1").run()
    assert (result.status, result.published, result.full_refresh) == ("SUCCEEDED", True, True)
    pointer = read_current(s3, s3_bucket)
    assert pointer.rows == EXPECTED_SILVER_ROWS
    for entity in result.entities:
        counts = entity.counts
        if CONTRACTS[entity.entity].source_system == "postgres":
            bronze_rows = s3.read_arrow(s3_bucket, f"bronze/postgres/{entity.entity}/ingest_date=2026-10-14/"
                                                   "part-00000.parquet").num_rows
            assert counts["read"] == bronze_rows
        assert counts["read"] == counts["accepted"] + counts["quarantined"] + counts["deduplicated"]
        assert s3.read_arrow(s3_bucket, pointer.quarantine[entity.entity]).num_rows == counts["quarantined"]
    learning = next(e for e in result.entities if e.entity == "learning_activity")
    # BUS-010/011 are dataset-level metrics, not row warnings (A13 reassessment).
    assert "completion_without_enrollment" not in learning.reason_counts
    assert learning.counts["accepted_with_warnings"] == 5185
    completion = learning.metrics["history"]["completion_without_enrollment"]
    certification = learning.metrics["history"]["certification_without_completion"]
    assert (completion["events"], completion["flagged"]) == (3490, 3406)            # measured independently
    assert (certification["events"], certification["flagged"]) == (1764, 1731)
    # The exact baseline lies inside the 95% range of an independent 200-shuffle simulation.
    assert 0.9705 <= completion["random_baseline_rate"] <= 0.9794
    assert 0.9717 <= certification["random_baseline_rate"] <= 0.9853
    assert completion["random_baseline_approx_95"][0] <= completion["rate"] <= completion["random_baseline_approx_95"][1]
    assert len(pointer.files["learning_activity"]) == 37

    # Queryable: materialise the published files as a hive tree and read it with pandas.
    for entity, keys in pointer.files.items():
        for key in keys:
            path = tmp_path / entity / key.split(f"/{entity}/", 1)[1]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(s3.get_bytes(s3_bucket, key))
    events = pd.read_parquet(tmp_path / "learning_activity")
    assert len(events) == 29574 and events["event_id"].is_unique
    assert {"event_year", "event_month"} <= set(events.columns)
    assert int(events["is_pre_hire"].sum()) == 5161
    assessments = pd.read_parquet(tmp_path / "competency_assessments")
    assert int(assessments["is_critical_gap"].sum()) == 682


def test_new_day_updates_incrementally_and_matches_a_full_rebuild(lake, s3, s3_bucket):
    SilverBuilder(lake, s3, run_id="it-silver-1").run()
    first = read_current(s3, s3_bucket)

    # A new day lands in Bronze: two events in a month Silver has never seen, one of them a
    # completion for a pair that already has an enrollment, one a brand-new pair.
    new_key = "bronze/learning_activity/year=2026/month=10/day=01/events-000.jsonl"
    enrolled = published(s3, s3_bucket, "learning_activity").filter(
        pa.compute.equal(published(s3, s3_bucket, "learning_activity")["event_type"], "enrollment")).slice(0, 1)
    employee_id, course = enrolled["employee_id"][0].as_py(), enrolled["course_name"][0].as_py()
    body = "".join(json.dumps(r) + "\n" for r in [
        {"event_id": "EV9000000001", "employee_id": str(employee_id), "event_date": "2026-10-01T09:00:00",
         "event_type": "course_completed", "course_name": course, "text": "Completed.", "score": None},
        {"event_id": "EV9000000002", "employee_id": "1", "event_date": "2026-10-01T10:00:00",
         "event_type": "enrollment", "course_name": "Brand New Course", "text": "Enrolled.", "score": None},
    ])
    s3.put_bytes(s3_bucket, new_key, body.encode())

    result = SilverBuilder(lake, s3, run_id="it-silver-2").run()
    assert result.full_refresh is False
    modes = {e.entity: e.mode for e in result.entities}
    assert all(modes[n] == "reused" for n in CONTRACTS if n != "learning_activity")
    learning = next(e for e in result.entities if e.entity == "learning_activity")
    assert learning.stats["files_new_or_changed"] == 1 and learning.stats["files_reprocessed"] == 1
    assert learning.stats["partitions_affected"] == 1 and learning.stats["rows_added"] == 2
    assert len(learning.written) == 4   # 1 partition + quarantine + 2 state tables, out of 37 partitions
    second = read_current(s3, s3_bucket)
    assert second.rows["learning_activity"] == 29576
    reused = set(first.files["learning_activity"]) & set(second.files["learning_activity"])
    assert len(reused) == 37                         # every existing partition reused without copying
    completion = next(r for r in published(s3, s3_bucket, "learning_activity").to_pylist()
                      if r["event_id"] == "EV9000000001")
    assert completion["_dq_warnings"] == []                                     # never a row-level warning

    incremental = learning_content(s3, s3_bucket)
    incremental_counts = learning.counts, learning.reason_counts, learning.metrics
    full = SilverBuilder(lake, s3, run_id="it-silver-full", full_refresh=True).run()
    full_learning = next(e for e in full.entities if e.entity == "learning_activity")
    assert learning_content(s3, s3_bucket).equals(incremental)
    assert (full_learning.counts, full_learning.reason_counts, full_learning.metrics) == incremental_counts


def test_crash_mid_write_on_real_s3_keeps_published_silver(lake, s3, s3_bucket, monkeypatch):
    SilverBuilder(lake, s3, run_id="it-good").run()
    pointer_bytes = s3.get_bytes(s3_bucket, "silver/_current.json")
    original, writes = S3Client.write_arrow, {"n": 0}

    def crashing_write(self, table, bucket, key):
        writes["n"] += 1
        if writes["n"] == 6:
            raise RuntimeError("simulated crash")
        return original(self, table, bucket, key)
    monkeypatch.setattr(S3Client, "write_arrow", crashing_write)
    with pytest.raises(RuntimeError, match="simulated crash"):
        SilverBuilder(lake, s3, run_id="it-crash", full_refresh=True).run()
    monkeypatch.undo()
    assert s3.get_bytes(s3_bucket, "silver/_current.json") == pointer_bytes
    assert read_current(s3, s3_bucket).rows == EXPECTED_SILVER_ROWS
    SilverBuilder(lake, s3, run_id="it-recovered").run()
    assert not s3.list_objects(s3_bucket, "silver/runs/run_id=it-crash/")


def test_job_silver_step(lake, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "silver"]) == 0
    assert job.main(["--step", "silver", "--ingest-date", "2026-10-14"]) == 0
    assert job.main(["--step", "silver", "--full-refresh"]) == 0
    assert job.main(["--step", "silver", "--ingest-date", "2026-01-01"]) == 1  # no such snapshot


def test_job_silver_fails_clearly_without_bronze(settings, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "silver"]) == 1
