"""Unit tests for talent_etl.gold: Bronze -> Silver -> Gold in memory, incremental and atomic."""

import json
from datetime import date, timedelta

import pytest

import talent_etl.gold as gold_module
from talent_etl.gold import (
    GoldBuilder,
    GoldSourceError,
    GoldValidationError,
    load_gold_table,
    read_current_gold,
)
from talent_etl.gold_tables import TABLES
from talent_etl.io_s3 import S3Client
from tests.unit.test_silver import (  # noqa: F401 - fixtures are used by name
    LAKE,
    NOW,
    add_source_file,
    build_bronze,
    build_silver,
    client,
    employee,
    event,
    fake,
    lines,
    settings,
    source_tables,
)

LEARNING_TABLES = {"employee_learning", "promotion_pipeline", "attrition_risk"}


def build_gold(settings, fake, run_id="gold-run", full_refresh=False, now=NOW):
    return GoldBuilder(settings, client(settings, fake), run_id=run_id, full_refresh=full_refresh,
                       clock=lambda: now).run()


def modes(result):
    return {t.table: t.mode for t in result.tables}


def gold_content(settings, fake):
    return {name: load_gold_table(client(settings, fake), LAKE, name).to_pylist() for name in TABLES}


@pytest.fixture
def silver_built(settings, fake):
    build_bronze(settings, fake)
    build_silver(settings, fake)


@pytest.fixture
def gold_built(silver_built, settings, fake):
    return build_gold(settings, fake)


def test_first_run_builds_validates_and_publishes_every_table(gold_built, settings, fake):
    assert (gold_built.status, gold_built.published) == ("SUCCEEDED", True)
    assert set(modes(gold_built).values()) == {"built"}
    pointer = read_current_gold(client(settings, fake), LAKE)
    assert pointer.silver_run_id == "silver-run" and pointer.as_of == "2026-10-14"
    assert set(pointer.files) == set(TABLES)
    assert all(key.startswith("gold/runs/run_id=gold-run/") for key in pointer.files.values())
    manifest = json.loads(fake.objects[(LAKE, "gold/_manifests/run_id=gold-run.json")])
    checks = [c for t in manifest["tables"] for c in t["checks"]]
    assert checks and all(c["passed"] for c in checks)
    assert pointer.rows["dim_employee"] == 2 and pointer.rows["team_performance"] == 2


def test_rerun_without_changes_reuses_every_table(gold_built, settings, fake):
    before = read_current_gold(client(settings, fake), LAKE)
    result = build_gold(settings, fake, run_id="gold-run-2")
    assert set(modes(result).values()) == {"reused"}
    after = read_current_gold(client(settings, fake), LAKE)
    assert after.files == before.files and after.previous_run_id == "gold-run"


def test_new_learning_data_rebuilds_only_learning_dependent_tables(gold_built, settings, fake):
    add_source_file(fake, "learning_activity/year=2026/month=05/day=01/events-000.jsonl", lines(
        event("EV0000007001", event_date="2026-05-01T09:00:00", event_type="course_completed", course_name="SQL")))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    result = build_gold(settings, fake, run_id="gold-run-2")
    assert {t for t, m in modes(result).items() if m == "built"} == LEARNING_TABLES
    learning = {r["employee_id"]: r for r in load_gold_table(client(settings, fake), LAKE, "employee_learning").to_pylist()}
    assert learning[989]["courses_completed"] == 1


def test_new_snapshot_date_rebuilds_only_tables_that_use_the_as_of_date(gold_built, settings, fake):
    build_bronze(settings, fake, ingest_date=date(2026, 10, 20))   # same data, later snapshot
    build_silver(settings, fake, run_id="silver-run-2")
    result = build_gold(settings, fake, run_id="gold-run-2")
    built = {t for t, m in modes(result).items() if m == "built"}
    assert built == {name for name, table in TABLES.items() if table.uses_as_of}
    assert {"performance_history", "employee_performance"}.isdisjoint(built)


def test_master_data_change_rebuilds_dependent_tables_and_matches_a_full_rebuild(gold_built, settings, fake):
    later = source_tables(employees=[employee(989, date(2024, 1, 1)), employee(462, date(2025, 3, 1), team_id=2)])
    build_bronze(settings, fake, tables=later, ingest_date=date(2026, 10, 14))
    build_silver(settings, fake, run_id="silver-run-2")
    incremental = build_gold(settings, fake, run_id="gold-run-2")
    assert modes(incremental)["dim_employee"] == "built"
    content = gold_content(settings, fake)
    build_gold(settings, fake, run_id="gold-full", full_refresh=True)
    assert gold_content(settings, fake) == content


def test_incremental_learning_update_matches_a_full_rebuild(gold_built, settings, fake):
    add_source_file(fake, "learning_activity/year=2026/month=06/day=01/events-000.jsonl", lines(
        event("EV0000007101", employee_id="462", event_date="2026-06-01T09:00:00", event_type="certification_earned")))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    build_gold(settings, fake, run_id="gold-run-2")
    content = gold_content(settings, fake)
    build_gold(settings, fake, run_id="gold-full", full_refresh=True)
    assert gold_content(settings, fake) == content


def test_logic_version_change_rebuilds_everything(gold_built, settings, fake, monkeypatch):
    monkeypatch.setattr(gold_module, "GOLD_LOGIC_VERSION", gold_module.GOLD_LOGIC_VERSION + 1)
    assert set(modes(build_gold(settings, fake, run_id="gold-run-2")).values()) == {"built"}


def test_full_refresh_rebuilds_everything(gold_built, settings, fake):
    result = build_gold(settings, fake, run_id="gold-run-2", full_refresh=True)
    assert set(modes(result).values()) == {"built"} and result.full_refresh


def test_failed_check_blocks_publishing_and_is_recorded(gold_built, settings, fake, monkeypatch):
    pointer_before = fake.objects[(LAKE, "gold/_current.json")]
    broken = TABLES["team_performance"].__class__(**{
        **TABLES["team_performance"].__dict__, "reconcile": lambda df, i: [("forced failure", False, "test")]})
    monkeypatch.setitem(TABLES, "team_performance", broken)
    with pytest.raises(GoldValidationError, match="team_performance: forced failure"):
        build_gold(settings, fake, run_id="gold-bad", full_refresh=True)
    assert fake.objects[(LAKE, "gold/_current.json")] == pointer_before
    manifest = json.loads(fake.objects[(LAKE, "gold/_manifests/run_id=gold-bad.json")])
    assert manifest["status"] == "FAILED" and not fake.keys(LAKE, "gold/runs/run_id=gold-bad/")


def test_crash_mid_write_keeps_the_published_version(gold_built, settings, fake, monkeypatch):
    pointer_before = fake.objects[(LAKE, "gold/_current.json")]
    content = gold_content(settings, fake)
    original, calls = S3Client.write_arrow, {"n": 0}

    def crash(self, table, bucket, key):
        if key.startswith("gold/"):
            calls["n"] += 1
            if calls["n"] == 4:
                raise RuntimeError("simulated crash")
        return original(self, table, bucket, key)
    monkeypatch.setattr(S3Client, "write_arrow", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        build_gold(settings, fake, run_id="gold-crash", full_refresh=True)
    monkeypatch.undo()
    assert fake.objects[(LAKE, "gold/_current.json")] == pointer_before and gold_content(settings, fake) == content
    assert json.loads(fake.objects[(LAKE, "gold/_manifests/run_id=gold-crash.json")])["status"] == "FAILED"
    build_gold(settings, fake, run_id="gold-run-3", full_refresh=True)
    assert not fake.keys(LAKE, "gold/runs/run_id=gold-crash/")


def test_unreferenced_gold_files_are_cleaned_up(gold_built, settings, fake):
    first_keys = set(read_current_gold(client(settings, fake), LAKE).files.values())
    build_gold(settings, fake, run_id="gold-run-2", full_refresh=True)
    build_gold(settings, fake, run_id="gold-run-3", full_refresh=True)
    assert not any((LAKE, k) in fake.objects for k in first_keys)       # neither current nor previous


def test_older_run_does_not_overwrite_a_newer_gold_version(gold_built, settings, fake):
    build_gold(settings, fake, run_id="gold-newer", full_refresh=True, now=NOW + timedelta(hours=1))
    result = build_gold(settings, fake, run_id="gold-older", full_refresh=True, now=NOW)
    assert (result.status, result.published) == ("SUPERSEDED", False)
    assert read_current_gold(client(settings, fake), LAKE).run_id == "gold-newer"


def test_gold_needs_published_silver(settings, fake):
    fake.buckets.add(LAKE)
    with pytest.raises(GoldSourceError, match="no published Silver"):
        build_gold(settings, fake)
    with pytest.raises(GoldSourceError, match="no published Gold"):
        load_gold_table(client(settings, fake), LAKE, "dim_employee")
