"""Unit tests for talent_etl.bronze using in-memory S3 and PostgreSQL fakes."""

import io
import json
import logging
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from talent_etl.bronze import (
    SOURCE_TABLES,
    BronzeIngestor,
    BronzeIntegrityError,
    BronzeSourceError,
    add_metadata_columns,
)
from talent_etl.config import Settings
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy
from tests.fakes import FakePostgres, FakeS3, client_error

INGEST_DATE = date(2026, 10, 14)
NOW = datetime(2026, 10, 14, 10, 0, tzinfo=timezone.utc)
SRC, LAKE = "acme-talent-source", "acme-talent-lake"

SOURCE_FILES = {
    "learning_activity/year=2023/month=10/day=13/events-000.jsonl": b'{"event_id": "EV1"}\n{"event_id": "EV2"}\n',
    "learning_activity/year=2024/month=02/day=28/events-000.jsonl": b'{"event_id": "EV3", "employee_id": nul\n',
    "learning_activity/year=2025/month=09/day=03/events-000.jsonl": b"",
    "learning_activity/year=2026/month=01/day=05/events-000.jsonl": b'{"event_id": "EV4"}\n',
    "learning_activity/stray.jsonl": b'{"event_id": "EV5"}\n',
}


def source_tables(empty=()):
    """Small tables shaped like the real sources; names in ``empty`` have zero rows."""
    tables = {
        name: pa.table({f"{name}_id": pa.array([1, 2], pa.int32()), "name": pa.array(["a", None])})
        for name in SOURCE_TABLES
    }
    tables["projects"] = pa.table({
        "project_id": pa.array([1, 2], pa.int32()),
        "outcome_score": pa.array([Decimal("56.87"), Decimal("41.93")], pa.decimal128(5, 2)),
        "start": pa.array([date(2025, 1, 2), None], pa.date32()),
    })
    for name in empty:
        tables[name] = tables[name].slice(0, 0)
    return tables


@pytest.fixture
def fake_s3():
    fake = FakeS3(page_size=2)  # forces pagination of every listing
    fake.buckets.add(SRC)
    for key, body in SOURCE_FILES.items():
        fake.objects[(SRC, key)] = body
    return fake


@pytest.fixture
def settings():
    return Settings.from_env({})


def make_ingestor(settings, fake_s3, pg=None, run_id="run-1", ingest_date=INGEST_DATE):
    policy = RetryPolicy(attempts=3, base_delay=0, jitter=False, sleep=lambda _s: None)
    return BronzeIngestor(
        settings,
        pg or FakePostgres("hr_source", source_tables()),
        S3Client(settings.s3, policy, client=fake_s3),
        run_id=run_id, ingest_date=ingest_date, clock=lambda: NOW,
    )


def read_parquet(fake_s3, key):
    return pq.read_table(io.BytesIO(fake_s3.objects[(LAKE, key)]))


# --- metadata -------------------------------------------------------------------

def test_add_metadata_columns_appends_prefixed_columns_and_keeps_source():
    source = source_tables()["projects"]
    landed = add_metadata_columns(source, run_id="r", ingest_date=INGEST_DATE, ingested_at=NOW,
                                  source_system="postgres", source_object="hr_source.projects")
    assert landed.column_names[:3] == source.column_names
    assert landed.select(source.column_names).equals(source)
    assert landed.column_names[3:] == ["_run_id", "_ingest_date", "_ingested_at", "_source_system", "_source_object"]
    assert landed.column("_source_object").to_pylist() == ["hr_source.projects"] * 2
    assert landed.column("_ingested_at").to_pylist() == [NOW, NOW]


def test_add_metadata_columns_rejects_reserved_prefix():
    with pytest.raises(BronzeSourceError, match="reserved"):
        add_metadata_columns(pa.table({"_secret": [1]}), run_id="r", ingest_date=INGEST_DATE,
                             ingested_at=NOW, source_system="postgres", source_object="s.t")


# --- PostgreSQL snapshots --------------------------------------------------------

def test_run_writes_one_parquet_per_table_with_lossless_types(settings, fake_s3):
    result = make_ingestor(settings, fake_s3).run()
    assert [t.table for t in result.tables] == list(SOURCE_TABLES)
    for name in SOURCE_TABLES:
        key = f"bronze/postgres/{name}/ingest_date=2026-10-14/part-00000.parquet"
        assert fake_s3.keys(LAKE, f"bronze/postgres/{name}/") == [key]
    projects = read_parquet(fake_s3, "bronze/postgres/projects/ingest_date=2026-10-14/part-00000.parquet")
    assert projects.schema.field("outcome_score").type == pa.decimal128(5, 2)
    assert projects.column("outcome_score").to_pylist() == [Decimal("56.87"), Decimal("41.93")]
    assert projects.column("_run_id").to_pylist() == ["run-1", "run-1"]
    assert projects.column("_ingest_date").to_pylist() == [INGEST_DATE] * 2
    assert {t.table: t.rows for t in result.tables} == dict.fromkeys(SOURCE_TABLES, 2)


def test_empty_source_table_writes_empty_snapshot_with_schema_and_warns(settings, fake_s3, caplog):
    pg = FakePostgres("hr_source", source_tables(empty=("talent_review",)))
    with caplog.at_level(logging.WARNING):
        result = make_ingestor(settings, fake_s3, pg=pg).run()
    landed = read_parquet(fake_s3, "bronze/postgres/talent_review/ingest_date=2026-10-14/part-00000.parquet")
    assert landed.num_rows == 0
    assert landed.column_names == ["talent_review_id", "name", "_run_id", "_ingest_date",
                                   "_ingested_at", "_source_system", "_source_object"]
    assert next(t for t in result.tables if t.table == "talent_review").rows == 0
    assert any(r.message == "source table is empty; writing an empty snapshot" for r in caplog.records)


def test_rerun_same_date_overwrites_without_duplicates(settings, fake_s3):
    make_ingestor(settings, fake_s3, run_id="run-1").run()
    first_keys = fake_s3.keys(LAKE, "bronze/postgres/")
    make_ingestor(settings, fake_s3, run_id="run-2").run()
    assert fake_s3.keys(LAKE, "bronze/postgres/") == first_keys
    teams = read_parquet(fake_s3, "bronze/postgres/teams/ingest_date=2026-10-14/part-00000.parquet")
    assert teams.num_rows == 2
    assert teams.column("_run_id").to_pylist() == ["run-2", "run-2"]


def test_rerun_removes_stale_objects_in_the_partition(settings, fake_s3):
    stale = "bronze/postgres/teams/ingest_date=2026-10-14/part-00001.parquet"
    fake_s3.buckets.add(LAKE)
    fake_s3.objects[(LAKE, stale)] = b"left over from an older layout"
    result = make_ingestor(settings, fake_s3).run()
    assert (LAKE, stale) not in fake_s3.objects
    assert next(t for t in result.tables if t.table == "teams").stale_objects_removed == 1


def test_new_ingest_date_adds_a_partition_and_keeps_the_old_one(settings, fake_s3):
    make_ingestor(settings, fake_s3, ingest_date=date(2026, 10, 13)).run()
    make_ingestor(settings, fake_s3, ingest_date=INGEST_DATE).run()
    assert fake_s3.keys(LAKE, "bronze/postgres/teams/") == [
        "bronze/postgres/teams/ingest_date=2026-10-13/part-00000.parquet",
        "bronze/postgres/teams/ingest_date=2026-10-14/part-00000.parquet",
    ]


# --- learning activity mirror ------------------------------------------------------

def test_learning_files_are_mirrored_byte_for_byte_with_partitions(settings, fake_s3):
    result = make_ingestor(settings, fake_s3).run()
    for key, body in SOURCE_FILES.items():
        assert fake_s3.objects[(LAKE, f"bronze/{key}")] == body
    meta = fake_s3.metadata[(LAKE, "bronze/learning_activity/year=2023/month=10/day=13/events-000.jsonl")]
    assert meta == {
        "run_id": "run-1", "ingest_date": "2026-10-14", "ingested_at": NOW.isoformat(),
        "source_uri": f"s3://{SRC}/learning_activity/year=2023/month=10/day=13/events-000.jsonl",
    }
    learning = result.learning_activity
    assert (learning.source_files, learning.copied, learning.skipped_unchanged) == (5, 5, 0)
    assert learning.source_bytes == sum(len(b) for b in SOURCE_FILES.values())


def test_empty_and_unpartitioned_files_are_copied_and_reported_not_fatal(settings, fake_s3, caplog):
    with caplog.at_level(logging.WARNING):
        learning = make_ingestor(settings, fake_s3).run().learning_activity
    empty_key = "learning_activity/year=2025/month=09/day=03/events-000.jsonl"
    assert learning.empty_files == [empty_key]
    assert fake_s3.objects[(LAKE, f"bronze/{empty_key}")] == b""
    assert learning.unpartitioned_files == ["learning_activity/stray.jsonl"]
    messages = {r.message for r in caplog.records}
    assert "source file is empty; copied as-is" in messages
    assert "source file is outside a year=/month=/day= partition; copied as-is" in messages


def test_rerun_skips_unchanged_files_and_keeps_original_lineage(settings, fake_s3):
    make_ingestor(settings, fake_s3, run_id="run-1").run()
    before = fake_s3.keys(LAKE, "bronze/learning_activity/")
    learning = make_ingestor(settings, fake_s3, run_id="run-2").run().learning_activity
    assert (learning.copied, learning.skipped_unchanged) == (0, 5)
    assert fake_s3.keys(LAKE, "bronze/learning_activity/") == before
    meta = fake_s3.metadata[(LAKE, "bronze/learning_activity/year=2023/month=10/day=13/events-000.jsonl")]
    assert meta["run_id"] == "run-1"


def test_rerun_copies_only_new_and_changed_files(settings, fake_s3):
    make_ingestor(settings, fake_s3, run_id="run-1").run()
    changed = "learning_activity/year=2026/month=01/day=05/events-000.jsonl"
    added = "learning_activity/year=2026/month=01/day=06/events-000.jsonl"
    fake_s3.objects[(SRC, changed)] = b'{"event_id": "EV4", "fixed": true}\n'
    fake_s3.objects[(SRC, added)] = b'{"event_id": "EV6"}\n'
    learning = make_ingestor(settings, fake_s3, run_id="run-2").run().learning_activity
    assert (learning.copied, learning.skipped_unchanged) == (2, 4)
    assert fake_s3.objects[(LAKE, f"bronze/{changed}")] == b'{"event_id": "EV4", "fixed": true}\n'
    assert fake_s3.metadata[(LAKE, f"bronze/{added}")]["run_id"] == "run-2"


def test_transient_s3_errors_are_retried(settings, fake_s3):
    ingestor = make_ingestor(settings, fake_s3)
    source_files = ingestor.preflight()
    fake_s3.buckets.add(LAKE)
    fake_s3.failures = [client_error("SlowDown"), client_error("503")]
    learning = ingestor.ingest_learning_activity(source_files)
    assert learning.copied == 5
    assert fake_s3.failures == []


def test_copy_mismatch_raises_integrity_error(settings, fake_s3):
    ingestor = make_ingestor(settings, fake_s3)
    source_files = ingestor.preflight()
    fake_s3.corrupt_copies = True
    with pytest.raises(BronzeIntegrityError, match="missing or different"):
        ingestor.ingest_learning_activity(source_files)


def test_parquet_readback_mismatch_raises_integrity_error(settings, fake_s3, monkeypatch):
    ingestor = make_ingestor(settings, fake_s3)
    fake_s3.buckets.add(LAKE)
    original = S3Client.read_arrow
    monkeypatch.setattr(S3Client, "read_arrow", lambda self, b, k: original(self, b, k).slice(0, 1))
    with pytest.raises(BronzeIntegrityError, match="wrote 1 rows"):
        ingestor.ingest_table("teams")


# --- failure handling -------------------------------------------------------------

def test_missing_sources_fail_before_anything_is_written(settings, fake_s3):
    tables = source_tables()
    del tables["employees"], tables["teams"]
    for key in list(SOURCE_FILES):
        del fake_s3.objects[(SRC, key)]
    with pytest.raises(BronzeSourceError) as err:
        make_ingestor(settings, fake_s3, pg=FakePostgres("hr_source", tables)).run()
    assert "['employees', 'teams']" in str(err.value)
    assert f"no objects under s3://{SRC}/learning_activity/" in str(err.value)
    assert LAKE not in fake_s3.buckets
    assert fake_s3.keys(LAKE) == []


def test_directory_markers_are_not_treated_as_files(settings, fake_s3):
    fake_s3.objects[(SRC, "learning_activity/year=2023/")] = b""
    assert len(make_ingestor(settings, fake_s3).preflight()) == len(SOURCE_FILES)


def test_source_prefix_trailing_slash_is_normalised(fake_s3):
    settings = Settings.from_env({"SOURCE_PREFIX": "learning_activity/"})
    learning = make_ingestor(settings, fake_s3).run().learning_activity
    assert learning.source_uri == f"s3://{SRC}/learning_activity/"
    assert (LAKE, "bronze/learning_activity/year=2023/month=10/day=13/events-000.jsonl") in fake_s3.objects


# --- manifest ---------------------------------------------------------------------

def test_manifest_records_every_output(settings, fake_s3):
    result = make_ingestor(settings, fake_s3).run()
    assert result.manifest_key == "bronze/_manifests/ingest_date=2026-10-14/run_id=run-1.json"
    manifest = json.loads(fake_s3.objects[(LAKE, result.manifest_key)])
    assert manifest["run_id"] == "run-1"
    assert manifest["ingested_at"] == NOW.isoformat()
    assert {t["table"]: t["rows"] for t in manifest["tables"]} == dict.fromkeys(SOURCE_TABLES, 2)
    assert manifest["learning_activity"]["empty_files"] == [
        "learning_activity/year=2025/month=09/day=03/events-000.jsonl"
    ]


def test_manifests_accumulate_one_per_run(settings, fake_s3):
    make_ingestor(settings, fake_s3, run_id="run-1").run()
    make_ingestor(settings, fake_s3, run_id="run-2").run()
    assert fake_s3.keys(LAKE, "bronze/_manifests/") == [
        "bronze/_manifests/ingest_date=2026-10-14/run_id=run-1.json",
        "bronze/_manifests/ingest_date=2026-10-14/run_id=run-2.json",
    ]


def test_settings_bucket_override_is_respected(fake_s3):
    settings = Settings.from_env({})
    settings = replace(settings, s3=replace(settings.s3, lake_bucket="other-lake"))
    make_ingestor(settings, fake_s3).run()
    assert fake_s3.keys("other-lake", "bronze/postgres/teams/")
