"""Integration tests: Bronze ingestion from the seeded local PostgreSQL and LocalStack sources."""

import hashlib
from dataclasses import replace
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest
from psycopg import sql

import job
from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor

pytestmark = pytest.mark.integration

EXPECTED_ROWS = {
    "teams": 100, "competencies": 40, "employees": 2000, "projects": 33,
    "performance_reviews": 4000, "competency_assessments": 4000, "talent_review": 2000,
}
EXPECTED_FILES = 783
EMPTY_FILE = "learning_activity/year=2025/month=09/day=03/events-000.jsonl"
INGEST_DATE = date(2026, 10, 14)


@pytest.fixture
def seeded(settings, pg, s3):
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    if not s3.list_objects(settings.s3.source_bucket, f"{settings.s3.source_prefix}/"):
        pytest.skip("S3 source not seeded; run scripts/seed_sources.sh")


@pytest.fixture
def lake_settings(settings, s3_bucket):
    return replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket))


def ingest(lake_settings, pg, s3, run_id):
    return BronzeIngestor(lake_settings, pg, s3, run_id=run_id, ingest_date=INGEST_DATE).run()


def test_bronze_lands_all_sources_and_rerun_is_idempotent(seeded, lake_settings, pg, s3, s3_bucket):
    first = ingest(lake_settings, pg, s3, "it-run-1")

    # PostgreSQL: one Parquet object per table with exactly the source row count.
    assert {t.table: t.rows for t in first.tables} == EXPECTED_ROWS
    for name, rows in EXPECTED_ROWS.items():
        key = f"bronze/postgres/{name}/ingest_date=2026-10-14/part-00000.parquet"
        assert s3.read_arrow(s3_bucket, key).num_rows == rows
        assert pg.count_rows(lake_settings.postgres.source_schema, name) == rows

    # Learning activity: same keys, sizes and MD5 ETags as the source, empty file included.
    source = {o.key: o for o in s3.list_objects(lake_settings.s3.source_bucket, "learning_activity/")}
    landed = {o.key.removeprefix("bronze/"): o for o in s3.list_objects(s3_bucket, "bronze/learning_activity/")}
    assert len(source) == len(landed) == EXPECTED_FILES
    assert {k: (o.size, o.etag) for k, o in landed.items()} == {k: (o.size, o.etag) for k, o in source.items()}
    assert landed[EMPTY_FILE].size == 0
    assert first.learning_activity.empty_files == [EMPTY_FILE]
    assert (first.learning_activity.copied, first.learning_activity.skipped_unchanged) == (EXPECTED_FILES, 0)

    # Rerun: no new objects, no duplicate rows, nothing re-copied.
    keys_before = [o.key for o in s3.list_objects(s3_bucket, "bronze/") if "_manifests" not in o.key]
    second = ingest(lake_settings, pg, s3, "it-run-2")
    keys_after = [o.key for o in s3.list_objects(s3_bucket, "bronze/") if "_manifests" not in o.key]
    assert keys_after == keys_before
    assert {t.table: t.rows for t in second.tables} == EXPECTED_ROWS
    assert (second.learning_activity.copied, second.learning_activity.skipped_unchanged) == (0, EXPECTED_FILES)
    assert len(s3.list_objects(s3_bucket, "bronze/_manifests/")) == 2


def test_bronze_preserves_source_values_and_types(seeded, lake_settings, pg, s3, s3_bucket):
    ingest(lake_settings, pg, s3, "it-run-types")
    projects = s3.read_arrow(s3_bucket, "bronze/postgres/projects/ingest_date=2026-10-14/part-00000.parquet")
    assert projects.schema.field("outcome_score").type == pa.decimal128(5, 2)
    assert projects.schema.field("project_id").type == pa.int32()
    first = projects.sort_by("project_id").slice(0, 1).to_pylist()[0]
    assert (first["project_id"], first["name"], first["outcome_score"]) == (1, "DimGray Launch", Decimal("56.87"))
    assert first["_source_system"] == "postgres" and first["_source_object"] == "hr_source.projects"

    employees = s3.read_arrow(s3_bucket, "bronze/postgres/employees/ingest_date=2026-10-14/part-00000.parquet")
    assert employees.schema.field("hire_date").type == pa.date32()
    source = pg.query_df(sql.SQL("SELECT * FROM {}.employees ORDER BY employee_id").format(
        sql.Identifier(lake_settings.postgres.source_schema)))
    landed = employees.select(list(source.columns)).sort_by("employee_id")
    source_rows = [tuple(row) for row in source.astype(object).itertuples(index=False)]
    assert [tuple(row.values()) for row in landed.to_pylist()] == source_rows  # values identical, row for row

    reviews = s3.read_arrow(s3_bucket, "bronze/postgres/performance_reviews/ingest_date=2026-10-14/part-00000.parquet")
    assert reviews.column("_run_id").unique().to_pylist() == ["it-run-types"]


def test_bronze_file_bodies_are_byte_identical(seeded, lake_settings, pg, s3, s3_bucket):
    ingest(lake_settings, pg, s3, "it-run-bytes")
    src_bucket = lake_settings.s3.source_bucket
    for key in [EMPTY_FILE, "learning_activity/year=2023/month=11/day=22/events-000.jsonl"]:
        body = s3.get_bytes(s3_bucket, f"bronze/{key}")
        assert body == s3.get_bytes(src_bucket, key)
        assert hashlib.md5(body).hexdigest() == s3.list_objects(src_bucket, key)[0].etag  # nosec B324
    head = s3._client.head_object(Bucket=s3_bucket, Key=f"bronze/{EMPTY_FILE}")
    assert head["Metadata"]["run_id"] == "it-run-bytes"
    assert head["Metadata"]["source_uri"] == f"s3://{src_bucket}/{EMPTY_FILE}"


def test_read_table_arrow_against_real_postgres(pg, pg_schema):
    pg.execute(sql.SQL(
        "CREATE TABLE {}.t (id INTEGER, score NUMERIC(5,2), day DATE, note TEXT)"
    ).format(sql.Identifier(pg_schema)))
    pg.execute(sql.SQL("INSERT INTO {}.t VALUES (1, 56.87, '2025-01-02', 'x'), (NULL, NULL, NULL, NULL)").format(
        sql.Identifier(pg_schema)))
    table = pg.read_table_arrow(pg_schema, "t")
    assert table.schema.types == [pa.int32(), pa.decimal128(5, 2), pa.date32(), pa.string()]
    assert table.column("id").to_pylist() == [1, None]


def test_job_bronze_step_end_to_end(seeded, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "bronze", "--ingest-date", "2026-10-14"]) == 0
    assert job.main(["--step", "bronze", "--ingest-date", "2026-10-14"]) == 0


def test_job_bronze_fails_clearly_when_source_missing(seeded, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    monkeypatch.setenv("SOURCE_BUCKET", s3_bucket)  # the empty lake bucket has no learning_activity/
    assert job.main(["--step", "bronze", "--ingest-date", "2026-10-14"]) == 1
