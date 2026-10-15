"""Integration: seeded sources -> Bronze -> Silver -> Gold -> serving in a throwaway LocalStack bucket.

The expected figures are the ones the notebook quotes and verifies against Gold
(notebooks/talent_insights.ipynb, last cell): the dashboard must say exactly the same.
"""

import json
from dataclasses import replace
from datetime import date

import pytest

import job
from talent_etl import serving
from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor
from talent_etl.gold import GoldBuilder, GoldSourceError
from talent_etl.serving import SERVING_LAYER, ServingError, publish_dashboard
from talent_etl.silver import SilverBuilder

pytestmark = pytest.mark.integration


@pytest.fixture
def lake(settings, pg, s3, s3_bucket):
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    lake_settings = replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket))
    BronzeIngestor(lake_settings, pg, s3, run_id="it-bronze", ingest_date=date(2026, 10, 14)).run()
    SilverBuilder(lake_settings, s3, run_id="it-silver").run()
    GoldBuilder(lake_settings, s3, run_id="it-gold").run()
    return lake_settings


def read(s3, bucket, key):
    return json.loads(s3.get_bytes(bucket, key))


def test_dashboard_data_matches_the_notebook(lake, s3, s3_bucket):
    result = publish_dashboard(lake, s3)
    assert (result.status, result.gold_run_id, result.employees, result.checks) == ("published", "it-gold", 2000, 32)
    pointer = read(s3, s3_bucket, SERVING_LAYER.current_key())
    assert (pointer["gold_run_id"], pointer["as_of"], pointer["previous_gold_run_id"]) == ("it-gold", "2026-10-14", None)
    s = read(s3, s3_bucket, pointer["summary_key"])
    assert s["freshness"]["environment"] == "local (LocalStack)" and s["freshness"]["lake_bucket"] == s3_bucket
    assert sum(t["checks"] for t in s["gold_checks"]) == sum(t["passed"] for t in s["gold_checks"]) == 69
    details = [c for t in s["gold_check_details"] for c in t["checks"]]
    assert len(details) == 69 and all(c["passed"] for c in details)
    assert {k: sum(c["kind"] == k for c in details) for k in ("structure", "values", "totals")} == {
        "structure": 30, "values": 15, "totals": 24}
    assert all(c["plain"] != c["name"] for c in details)          # every real check has plain wording
    assert (s["q1"]["rated_employees"], s["q1"]["high_performers"], s["q1"]["low_performers"]) == (1730, 879, 402)
    assert [r["employees"] for r in s["q1"]["trend_distribution"]] == [721, 692, 191, 396]
    assert (s["q2"]["critical_gaps"], s["q2"]["employees_with_critical_gap"], s["q2"]["employees_with_two_or_more"]) \
        == (682, 633, 49)
    assert s["q2"]["competencies"][0]["competency_name"] == "Stakeholder Management"
    assert (s["q3"]["high_and_ready_now"], s["q3"]["promotion_ready"], s["q3"]["ready_now_without_valid_rating"],
            s["q3"]["ready_with_critical_gap"]) == (357, 148, 45, 61)
    assert s["q4"]["totals"]["distinct_course_completions"] == 2824
    assert s["q4"]["totals"]["employees_inactive_12m"] == 56
    assert [b["employees"] for b in s["q5"]["bands"]] == [193, 271, 1536]
    assert (s["q5"]["declining"], s["q5"]["declining_hr_high"], s["q5"]["declining_in_high_band"]) == (191, 116, 161)
    assert (s["q6"]["teams_with_projects"], round(s["q6"]["pearson_r"], 2), s["q6"]["significant_5pct"]) == (27, 0.14, False)
    heat = [c["avg_proficiency"] for c in s["q7"]["heatmap"]]
    assert (round(min(heat), 2), round(max(heat), 2)) == (2.85, 3.29)

    lake_index = read(s3, s3_bucket, pointer["lake_index_key"])
    rows = {(layer["layer"], t["table"]): t["rows"] for layer in lake_index["layers"] for t in layer["tables"]}
    assert (rows[("bronze", "learning_activity")], rows[("silver", "learning_activity")],
            rows[("silver", "learning_activity.quarantine")], rows[("gold", "skill_distribution")]) == (29868, 29574, 142, 1595)
    assert len(rows) == 27                                           # 8 Bronze + 9 Silver (with quarantine) + 10 Gold

    employees = read(s3, s3_bucket, pointer["employees_key"])["employees"]
    assert sum(e["promotion"]["is_promotion_ready"] is True for e in employees) == 148
    assert sum(len(e["review_history"]) for e in employees) == 4000


def test_publishing_is_idempotent_and_keeps_the_previous_version(lake, s3, s3_bucket):
    first = publish_dashboard(lake, s3)
    assert publish_dashboard(lake, s3).status == "up_to_date"            # same Gold: nothing rewritten

    GoldBuilder(lake, s3, run_id="it-gold-2", full_refresh=True).run()
    second = publish_dashboard(lake, s3)
    assert (second.status, second.gold_run_id) == ("published", "it-gold-2")
    pointer = read(s3, s3_bucket, SERVING_LAYER.current_key())
    assert pointer["previous_gold_run_id"] == "it-gold"
    keys = {o.key for o in s3.list_objects(s3_bucket, "serving/runs/")}
    assert {first.summary_key, first.employees_key, second.summary_key, second.employees_key} <= keys
    assert any(k.startswith("serving/runs/run_id=it-gold/lake/") for k in keys)   # previous catalog kept too


def test_failed_reconciliation_publishes_nothing(lake, s3, s3_bucket, monkeypatch):
    monkeypatch.setattr(serving, "reconcile", lambda *a: [{"name": "forced", "passed": False}])
    with pytest.raises(ServingError, match="forced"):
        publish_dashboard(lake, s3)
    assert not s3.object_exists(s3_bucket, SERVING_LAYER.current_key())
    assert read(s3, s3_bucket, SERVING_LAYER.manifest_key("it-gold"))["status"] == "FAILED"


def test_serving_needs_gold(settings, s3, s3_bucket):
    with pytest.raises(GoldSourceError):
        publish_dashboard(replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket)), s3)


def test_job_serve_step(lake, s3, s3_bucket, monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)
    assert job.main(["--step", "serve"]) == 0
    assert read(s3, s3_bucket, SERVING_LAYER.current_key())["gold_run_id"] == "it-gold"


def test_lake_catalog_mismatch_publishes_nothing(lake, s3, s3_bucket, monkeypatch):
    monkeypatch.setattr(serving, "publish_lake_catalog",
                        lambda *a: ({}, [{"name": "gold.x rows match the published pointer", "passed": False}]))
    with pytest.raises(ServingError, match="lake catalog does not reconcile"):
        publish_dashboard(lake, s3)
    assert not s3.object_exists(s3_bucket, SERVING_LAYER.current_key())
    assert read(s3, s3_bucket, SERVING_LAYER.manifest_key("it-gold"))["status"] == "FAILED"
