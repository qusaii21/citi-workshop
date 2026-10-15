"""Unit tests for talent_etl.explore (read-only browsing of the lake), on a fake S3."""

import io
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from talent_etl import explore
from talent_etl.config import Settings
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy
from tests.fakes import FakeS3


def parquet(**columns):
    buffer = io.BytesIO()
    pq.write_table(pa.table(columns), buffer)
    return buffer.getvalue()


@pytest.fixture
def lake():
    fake = FakeS3()
    put = lambda key, body: fake.objects.__setitem__(("lake", key), body)  # noqa: E731
    put("bronze/postgres/teams/ingest_date=2026-10-13/part-00000.parquet", parquet(team_id=[1]))
    put("bronze/postgres/teams/ingest_date=2026-10-14/part-00000.parquet", parquet(team_id=[1, 2]))
    put("bronze/learning_activity/year=2023/month=09/day=04/events-000.jsonl",
        b'{"event_id": "E1", "score": 5}\n\n{broken\n["not", "an", "object"]\n')
    put("bronze/_manifests/ingest_date=2026-10-14/run_id=b1.json", b"{}")
    put("silver/_current.json", json.dumps({
        "run_id": "s1", "run_prefix": "silver/runs/run_id=s1/", "bronze_ingest_date": "2026-10-14", "started_at": "x",
        "published_at": "x", "manifest_key": "m", "previous_run_id": None, "rows": {"learning_activity": 2, "teams": 2},
        "files": {"learning_activity": ["silver/l1.parquet", "silver/l2.parquet"], "teams": ["silver/t.parquet"]},
        "quarantine": {"learning_activity": "silver/q.parquet"}}).encode())
    put("silver/l1.parquet", parquet(event_id=["E1"]))
    put("silver/l2.parquet", parquet(event_id=["E2"]))
    put("silver/t.parquet", parquet(team_id=[1, 2]))
    put("silver/q.parquet", parquet(reason_codes=["malformed_json"]))
    put("gold/_current.json", json.dumps({
        "run_id": "g1", "run_prefix": "p", "silver_run_id": "s1", "as_of": "2026-10-14", "started_at": "x",
        "published_at": "x", "manifest_key": "m", "previous_run_id": None, "rows": {"dim_employee": 1}, "fingerprints": {},
        "files": {"dim_employee": "gold/d.parquet"}}).encode())
    put("gold/d.parquet", parquet(employee_id=[7]))
    return S3Client(Settings.from_env({}).s3, RetryPolicy(attempts=1), client=fake)


def test_overview_lists_every_layer(lake):
    table = explore.overview(lake, "lake")
    assert table[["layer", "table", "files"]].values.tolist() == [
        ["bronze", "teams", 1], ["bronze", "learning_activity", 1],
        ["silver", "learning_activity", 2], ["silver", "teams", 1], ["gold", "dim_employee", 1]]


def test_bronze_uses_the_latest_ingest_date(lake):
    assert explore.load(lake, "lake", "bronze", "teams")["team_id"].tolist() == [1, 2]


def test_bronze_learning_keeps_bad_lines_raw(lake):
    df = explore.load(lake, "lake", "bronze", "learning_activity")
    assert df["event_id"].tolist()[0] == "E1" and df["_source_line"].tolist() == [1, 3, 4]
    assert df["_raw"].tolist()[1:] == ["{broken", '["not", "an", "object"]']


def test_silver_and_gold_follow_the_published_pointer(lake):
    assert explore.load(lake, "lake", "silver", "learning_activity")["event_id"].tolist() == ["E1", "E2"]
    assert explore.load(lake, "lake", "silver", "learning_activity", max_files=1)["event_id"].tolist() == ["E1"]
    assert explore.load(lake, "lake", "gold", "dim_employee")["employee_id"].tolist() == [7]
    assert explore.quarantine(lake, "lake", "learning_activity")["reason_codes"].tolist() == ["malformed_json"]
    assert explore.quarantine(lake, "lake", "teams").empty


def test_errors_and_empty_lake(lake):
    with pytest.raises(KeyError, match="available"):
        explore.load(lake, "lake", "gold", "nope")
    with pytest.raises(ValueError, match="layer must be one of"):
        explore.table_files(lake, "lake", "platinum")
    empty = S3Client(Settings.from_env({}).s3, RetryPolicy(attempts=1), client=FakeS3())
    assert explore.overview(empty, "lake").empty and explore.quarantine(empty, "lake", "teams").empty
    assert isinstance(explore.overview(empty, "lake"), pd.DataFrame)


def test_read_text(lake):
    assert json.loads(explore.read_text(lake, "lake", "gold/_current.json"))["run_id"] == "g1"
