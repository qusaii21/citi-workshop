"""Unit tests for talent_etl.seed (S3 side and sample lookup) and the job's environment defaults."""

import io
import zipfile
from dataclasses import replace

import pytest

import job
from talent_etl.config import Settings
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy
from talent_etl.seed import SeedError, open_sample, upload_learning
from tests.fakes import FakeS3

PREFIX = "data-engineering-performance-talent-management/"


def sample_zip(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(PREFIX + "schema.sql", "CREATE TABLE teams (team_id INTEGER);")
        for name, body in files.items():
            archive.writestr(PREFIX + name, body)
    buffer.seek(0)
    return zipfile.ZipFile(buffer)


def client(fake):
    return S3Client(Settings.from_env({}).s3, RetryPolicy(attempts=1), client=fake)


def test_upload_learning_syncs_files_byte_for_byte_and_removes_stale_keys():
    fake = FakeS3()
    fake.buckets.add("acme-talent-source")
    fake.objects[("acme-talent-source", "learning_activity/year=2020/month=01/day=01/old.jsonl")] = b"stale"
    archive = sample_zip({"learning_activity/year=2023/month=10/day=13/events-000.jsonl": '{"a": 1}\n',
                          "learning_activity/year=2025/month=09/day=03/events-000.jsonl": ""})
    uploaded, removed = upload_learning(Settings.from_env({}), archive, client(fake))
    assert (uploaded, removed) == (2, 1)
    assert fake.objects[("acme-talent-source", "learning_activity/year=2023/month=10/day=13/events-000.jsonl")] == b'{"a": 1}\n'
    assert fake.objects[("acme-talent-source", "learning_activity/year=2025/month=09/day=03/events-000.jsonl")] == b""
    assert ("acme-talent-source", "learning_activity/year=2020/month=01/day=01/old.jsonl") not in fake.objects


def test_open_sample_from_s3_uri_local_path_and_default(tmp_path):
    fake = FakeS3()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(PREFIX + "schema.sql", "--")
    fake.objects[("src", "seed/sample.zip")] = buffer.getvalue()
    settings = replace(Settings.from_env({}), seed_zip_uri="s3://src/seed/sample.zip")
    assert open_sample(settings, client(fake)).namelist() == [PREFIX + "schema.sql"]
    path = tmp_path / "sample.zip"
    path.write_bytes(buffer.getvalue())
    assert open_sample(replace(settings, seed_zip_uri=str(path)), client(fake)).namelist() == [PREFIX + "schema.sql"]
    assert open_sample(Settings.from_env({}), client(fake)).namelist()  # default: the repository's sampleData/
    with pytest.raises(SeedError, match="sample zip not found"):
        open_sample(replace(settings, seed_zip_uri=str(tmp_path / "missing.zip")), client(fake))


def test_sample_without_schema_is_rejected():
    from talent_etl.seed import _member_prefix
    buffer = io.BytesIO()
    zipfile.ZipFile(buffer, "w").close()
    with pytest.raises(SeedError, match="no schema.sql"):
        _member_prefix(zipfile.ZipFile(io.BytesIO(buffer.getvalue())))


def test_seed_zip_uri_is_read_from_the_environment():
    assert Settings.from_env({"SEED_ZIP_URI": "s3://b/k.zip"}).seed_zip_uri == "s3://b/k.zip"
    assert Settings.from_env({}).seed_zip_uri is None


def test_step_and_seed_flag_default_from_the_environment(monkeypatch):
    monkeypatch.delenv("PIPELINE_STEP", raising=False)
    monkeypatch.delenv("SEED_IF_MISSING", raising=False)
    args = job.parse_args([])
    assert (args.step, args.seed_if_missing) == ("check", False)
    monkeypatch.setenv("PIPELINE_STEP", "all")
    monkeypatch.setenv("SEED_IF_MISSING", "true")
    args = job.parse_args([])
    assert (args.step, args.seed_if_missing) == ("all", True)
    assert job.parse_args(["--step", "silver"]).step == "silver"   # an explicit flag wins
    monkeypatch.setenv("PIPELINE_STEP", "bogus")
    with pytest.raises(SystemExit) as exit_info:
        job.parse_args([])
    assert exit_info.value.code == 2
