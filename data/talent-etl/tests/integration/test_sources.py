"""Integration tests: read/write against local PostgreSQL and LocalStack S3."""

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest
from psycopg import sql

import job

pytestmark = pytest.mark.integration


def test_postgres_read_table_preserves_columns_and_types(pg, pg_schema):
    pg.execute(sql.SQL(
        "CREATE TABLE {}.projects (project_id INTEGER PRIMARY KEY, outcome_score NUMERIC(5,2), start DATE)"
    ).format(sql.Identifier(pg_schema)))
    pg.execute(
        sql.SQL("INSERT INTO {}.projects VALUES (1, 56.87, '2025-01-02'), (2, NULL, NULL)").format(
            sql.Identifier(pg_schema)
        )
    )
    df = pg.read_table(pg_schema, "projects")
    assert list(df.columns) == ["project_id", "outcome_score", "start"]
    assert df.loc[0, "outcome_score"] == Decimal("56.87")
    assert df.loc[0, "start"] == date(2025, 1, 2)
    assert pg.count_rows(pg_schema, "projects") == 2
    assert pg.list_tables(pg_schema) == ["projects"]


def test_postgres_empty_table_keeps_columns(pg, pg_schema):
    pg.execute(sql.SQL("CREATE TABLE {}.t (a INT, b TEXT)").format(sql.Identifier(pg_schema)))
    df = pg.read_table(pg_schema, "t")
    assert df.empty and list(df.columns) == ["a", "b"]


def test_s3_bytes_and_parquet_round_trip(s3, s3_bucket):
    s3.put_bytes(s3_bucket, "raw/year=2025/a.jsonl", b'{"x": 1}\n')
    s3.put_bytes(s3_bucket, "raw/year=2025/empty.jsonl", b"")
    assert s3.get_bytes(s3_bucket, "raw/year=2025/a.jsonl") == b'{"x": 1}\n'
    assert [(o.key, o.size) for o in s3.list_objects(s3_bucket, "raw/")] == [
        ("raw/year=2025/a.jsonl", 9), ("raw/year=2025/empty.jsonl", 0),
    ]
    df = pd.DataFrame({"employee_id": [1, 2], "hire_date": pd.to_datetime(["2020-01-01", "2021-06-30"])})
    s3.write_parquet(df, s3_bucket, "silver/employees/part-0.parquet")
    pd.testing.assert_frame_equal(s3.read_parquet(s3_bucket, "silver/employees/part-0.parquet"), df)
    assert s3.object_exists(s3_bucket, "silver/employees/part-0.parquet")
    assert not s3.object_exists(s3_bucket, "silver/missing.parquet")


def test_seeded_sources_pass_job_check(settings, pg, s3):
    """Smoke test: requires scripts/seed_sources.sh to have been run."""
    if set(job.EXPECTED_SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    assert pg.count_rows(settings.postgres.source_schema, "employees") == 2000
    assert len(s3.list_objects(settings.s3.source_bucket, f"{settings.s3.source_prefix}/")) == 783
    assert job.main(["--step", "check"]) == 0
