"""Unit tests for talent_etl.dq_report (the notebook's data-quality section)."""

import json

import pytest

from talent_etl.config import Settings
from talent_etl.dq_report import entity_quality_table, history_metrics_table, published_manifest
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy
from talent_etl.silver import SilverSourceError
from tests.fakes import FakeS3

MANIFEST = {"entities": [
    {"entity": "teams", "mode": "reused",
     "counts": {"read": 100, "accepted": 100, "accepted_with_warnings": 0, "quarantined": 0, "deduplicated": 0}},
    {"entity": "learning_activity", "mode": "incremental",
     "counts": {"read": 200, "accepted": 190, "accepted_with_warnings": 5, "quarantined": 4, "deduplicated": 6},
     "metrics": {"history": {
         "completion_without_enrollment": {"rule_id": "BUS-010", "events": 100, "flagged": 97, "rate": 0.97,
                                           "random_baseline_rate": 0.975, "random_baseline_approx_95": [0.94, 1.0]},
         "certification_without_completion": {"rule_id": "BUS-011", "events": 50, "flagged": 10, "rate": 0.2,
                                              "random_baseline_rate": 0.98, "random_baseline_approx_95": [0.94, 1.0]},
     }}},
]}


def test_entity_quality_table():
    table = entity_quality_table(MANIFEST)
    assert table.loc["learning_activity", "quarantine_rate"] == pytest.approx(0.02)
    assert table.loc["teams", "mode"] == "reused"


def test_history_metrics_table_compares_with_random_order():
    table = history_metrics_table(MANIFEST)
    assert table.loc["BUS-010", "vs_random"] == "consistent with random order"
    assert table.loc["BUS-011", "vs_random"] == "differs from random order"
    assert table.loc["BUS-010", "flagged"] == 97


def test_published_manifest_reads_the_pointer_target():
    fake = FakeS3()
    s3 = S3Client(Settings.from_env({}).s3, RetryPolicy(attempts=1), client=fake)
    with pytest.raises(SilverSourceError, match="no published Silver"):
        published_manifest(s3, "lake")
    fake.objects[("lake", "silver/_current.json")] = json.dumps({
        "run_id": "r1", "run_prefix": "p", "bronze_ingest_date": "2026-10-14", "started_at": "x", "published_at": "x",
        "manifest_key": "silver/_manifests/run_id=r1.json", "previous_run_id": None, "files": {}, "quarantine": {},
        "rows": {}}).encode()
    fake.objects[("lake", "silver/_manifests/run_id=r1.json")] = json.dumps(MANIFEST).encode()
    assert published_manifest(s3, "lake") == MANIFEST


def test_gold_checks_come_from_the_run_that_built_each_table():
    from talent_etl.dq_report import gold_checks_table
    fake = FakeS3()
    s3 = S3Client(Settings.from_env({}).s3, RetryPolicy(attempts=1), client=fake)
    with pytest.raises(SilverSourceError, match="no published Gold"):
        gold_checks_table(s3, "lake")
    fake.objects[("lake", "gold/_current.json")] = json.dumps({
        "run_id": "g2", "run_prefix": "gold/runs/run_id=g2/", "silver_run_id": "s1", "as_of": "2026-10-14",
        "started_at": "x", "published_at": "x", "manifest_key": "gold/_manifests/run_id=g2.json",
        "previous_run_id": "g1", "rows": {"a": 3, "b": 5}, "fingerprints": {},
        "files": {"a": "gold/runs/run_id=g1/a/part-00000.parquet", "b": "gold/runs/run_id=g2/b/part-00000.parquet"},
    }).encode()
    ok = {"passed": True, "name": "x"}
    fake.objects[("lake", "gold/_manifests/run_id=g1.json")] = json.dumps(
        {"tables": [{"table": "a", "checks": [ok, ok]}]}).encode()
    fake.objects[("lake", "gold/_manifests/run_id=g2.json")] = json.dumps(
        {"tables": [{"table": "a", "checks": []}, {"table": "b", "checks": [ok, {"passed": False, "name": "bad"}]}]}).encode()
    table = gold_checks_table(s3, "lake")
    assert (table.loc["a", "built_by_run"], table.loc["a", "passed"]) == ("g1", 2)   # reused table: checks from g1
    assert (table.loc["b", "checks"], table.loc["b", "failed"]) == (2, ["bad"])
