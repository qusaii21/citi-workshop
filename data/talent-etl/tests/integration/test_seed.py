"""Integration: seed a throwaway schema and bucket from the real sample zip, then run the cloud entry path."""

import uuid
from dataclasses import replace
from pathlib import Path

import pytest
from psycopg import sql

import job
from talent_etl.seed import seed_sources, sources_missing

pytestmark = pytest.mark.integration

ZIP = Path(__file__).resolve().parents[4] / "sampleData" / "data-engineering-sample-performance-scenario.zip"
EXPECTED = {"teams": 100, "competencies": 40, "employees": 2000, "projects": 33,
            "performance_reviews": 4000, "competency_assessments": 4000, "talent_review": 2000}


@pytest.fixture
def scratch(settings, pg, s3, s3_bucket):
    schema = f"it_seed_{uuid.uuid4().hex[:8]}"
    seeded = replace(settings, postgres=replace(settings.postgres, source_schema=schema),
                     s3=replace(settings.s3, source_bucket=s3_bucket))
    yield seeded
    pg.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def test_seed_loads_both_sources_idempotently_from_s3(scratch, pg, s3, s3_bucket, policy):
    assert sources_missing(scratch, pg, s3)                      # nothing there yet
    s3.put_bytes(s3_bucket, "seed/sample.zip", ZIP.read_bytes())
    settings = replace(scratch, seed_zip_uri=f"s3://{s3_bucket}/seed/sample.zip")   # as in the cloud
    first = seed_sources(settings, s3, policy)
    assert first.table_rows == EXPECTED and first.learning_files == 783
    second = seed_sources(settings, s3, policy)                  # rerun: same result, nothing stale
    assert (second.table_rows, second.learning_removed) == (EXPECTED, 0)
    assert sources_missing(settings, pg, s3) == []
    assert pg.count_rows(settings.postgres.source_schema, "employees") == 2000
    assert len([o for o in s3.list_objects(s3_bucket, "learning_activity/")]) == 783


def test_cloud_entry_path_seeds_then_runs_the_whole_pipeline(scratch, s3, s3_bucket, monkeypatch):
    lake = f"{s3_bucket}-lake"
    s3.ensure_bucket(lake)
    try:
        for name, value in {"POSTGRES_SOURCE_SCHEMA": scratch.postgres.source_schema, "SOURCE_BUCKET": s3_bucket,
                            "LAKE_BUCKET": lake, "PIPELINE_STEP": "all", "SEED_IF_MISSING": "true",
                            "SEED_ZIP_URI": str(ZIP)}.items():
            monkeypatch.setenv(name, value)
        assert job.main([]) == 0                                 # no arguments, exactly like the Helm job
        assert {o.key.split("/")[0] for o in s3.list_objects(lake, "")} == {"bronze", "silver", "gold", "serving"}
        assert job.main([]) == 0                                 # second run: sources present, no reseed
    finally:
        s3.delete_prefix(lake, "")
        s3._client.delete_bucket(Bucket=lake)
