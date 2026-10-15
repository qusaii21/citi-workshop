"""
Unit tests for talent_etl.silver (incremental, atomically published Silver).

Bronze is built in memory with BronzeIngestor. The central guarantee is tested per scenario:
after any incremental update, the published Silver equals a full rebuild of the same Bronze
(ignoring lineage columns), while only the affected files are rewritten.
"""

import io
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import talent_etl.silver as silver_module
from talent_etl.bronze import BronzeIngestor
from talent_etl.config import Settings
from talent_etl.contracts import CONTRACTS, QUARANTINE_SCHEMA
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy
from talent_etl.silver import (
    ABANDONED_RUN_AGE,
    SilverBuilder,
    SilverIntegrityError,
    SilverSourceError,
    SilverValidationError,
    _check_not_null,
    current_files,
    latest_complete_snapshot,
    read_current,
)
from tests.fakes import FakePostgres, FakeS3, client_error

SNAPSHOT = date(2026, 10, 14)
NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
SRC, LAKE = "acme-talent-source", "acme-talent-lake"
LINEAGE = ["_run_id", "_processed_at", "_bronze_ingest_date", "_quarantined_at"]


def event(event_id, employee_id="989", event_date="2023-10-13T14:49:00", event_type="enrollment", **extra):
    record = {"event_id": event_id, "employee_id": employee_id, "event_date": event_date,
              "event_type": event_type, "course_name": "Coaching for Managers", "text": "Enrolled.", "score": None}
    record.update(extra)
    return json.dumps({k: v for k, v in record.items() if v is not ...})


def lines(*records):
    return "".join(r + "\n" for r in records)


LEARNING_FILES = {
    "learning_activity/year=2023/month=10/day=13/events-000.jsonl": lines(
        event("EV0000000001"),
        event("EV0000000001"),                                            # exact duplicate -> deduplicated
        '{"event_id": "EV0000000002", "employee_id": "989", "event_da',   # truncated -> quarantined
        event("EV0000000003", employee_id=None),                          # null employee -> quarantined
        event("EV0000000004", event_date="yesterday"),                    # bad date -> quarantined
        event("EV0000000005", event_type="quiz_attempted", score=46),
    ),
    "learning_activity/year=2024/month=01/day=05/events-000.jsonl":
        lines(event("EV0000000006", event_date="2024-01-05T09:00:00", text=..., notes="Started.")),  # alias
    "learning_activity/year=2025/month=02/day=28/events-000.jsonl":
        lines(event("EV0000000007", employee_id="462", event_date="2025-02-28T10:00:00")),          # pre-hire
    "learning_activity/year=2025/month=09/day=03/events-000.jsonl": "",                             # empty file
    # 100 clean events keep the quarantine rate (3/108 = 2.8%) well under the 5% fail-safe, as in real data.
    "learning_activity/year=2023/month=12/day=05/events-000.jsonl": lines(
        *(event(f"EV00000010{i:02d}", event_date="2023-12-05T09:00:00") for i in range(100))),
}


def table(entity, rows):
    contract = CONTRACTS[entity]
    return pa.Table.from_pylist(rows, schema=pa.schema([pa.field(c.name, c.silver_type) for c in contract.columns]))


def employee(employee_id, hire_date, team_id=1):
    return {"employee_id": employee_id, "first_name": "A", "last_name": "B", "email": f"e{employee_id}@acme.example",
            "team_id": team_id, "job_level": "IC1", "hire_date": hire_date}


def source_tables(employees=None, **overrides):
    tables = {
        "teams": table("teams", [{"team_id": 1, "name": "Team A", "department": "Sales"},
                                 {"team_id": 2, "name": "Team B", "department": "Design"}]),
        "competencies": table("competencies", [{"competency_id": 1, "name": "Python", "category": "Technical"}]),
        "employees": table("employees", employees or [employee(989, date(2020, 1, 1)),
                                                      employee(462, date(2025, 3, 1), team_id=2)]),
        "projects": table("projects", [{"project_id": 1, "name": "Launch", "team_id": 1,
                                        "outcome_status": "on-time", "outcome_score": Decimal("56.87")}]),
        "performance_reviews": table("performance_reviews", [
            {"review_id": 1, "employee_id": 989, "review_period": "2024-H2", "rating": 3,
             "manager_comment": None, "submitted_date": date(2025, 8, 31)},
            {"review_id": 2, "employee_id": 462, "review_period": "2024-H2", "rating": 4,
             "manager_comment": "ok", "submitted_date": date(2025, 8, 31)},
        ]),
        "competency_assessments": table("competency_assessments", [
            {"assessment_id": 1, "employee_id": 989, "competency_id": 1, "proficiency_level": 2,
             "required_level": 4, "assessed_date": date(2025, 12, 28)},
        ]),
        "talent_review": table("talent_review", [
            {"employee_id": 989, "review_cycle": "2025-H1", "potential_rating": "High",
             "promotion_readiness": "Ready Now", "attrition_risk": "Low"},
        ]),
    }
    return tables | overrides


@pytest.fixture
def fake():
    s3 = FakeS3(page_size=3)
    s3.buckets.add(SRC)
    for key, body in LEARNING_FILES.items():
        s3.objects[(SRC, key)] = body.encode()
    return s3


@pytest.fixture
def settings():
    return Settings.from_env({})


def client(settings, fake):
    return S3Client(settings.s3, RetryPolicy(attempts=2, base_delay=0, jitter=False, sleep=lambda _s: None),
                    client=fake)


def build_bronze(settings, fake, tables=None, ingest_date=SNAPSHOT):
    BronzeIngestor(settings, FakePostgres("hr_source", tables or source_tables()), client(settings, fake),
                   run_id="bronze-run", ingest_date=ingest_date, clock=lambda: NOW).run()


def build_silver(settings, fake, run_id="silver-run", ingest_date=None, now=NOW, full_refresh=False):
    return SilverBuilder(settings, client(settings, fake), run_id=run_id, ingest_date=ingest_date,
                         full_refresh=full_refresh, clock=lambda: now).run()


def add_source_file(fake, key, body):
    fake.objects[(SRC, key)] = body.encode()


def read(fake, key):
    return pq.read_table(io.BytesIO(fake.objects[(LAKE, key)]))


def silver_keys(fake):
    return fake.keys(LAKE, "silver/")


def pointer(settings, fake):
    return read_current(client(settings, fake), LAKE)


def published(settings, fake, entity):
    """Read an entity exactly as a downstream reader would: through the pointer."""
    keys = current_files(client(settings, fake), LAKE, entity)
    return pa.concat_tables([read(fake, k) for k in keys])


def manifest(fake, run_id):
    return json.loads(fake.objects[(LAKE, f"silver/_manifests/run_id={run_id}.json")])


def entity_result(result, name):
    return next(e for e in result.entities if e.entity == name)


def content(settings, fake):
    """Published data and quarantine without lineage columns, in a canonical order."""
    current = pointer(settings, fake)
    out = {}
    for name, contract in CONTRACTS.items():
        data = published(settings, fake, name)
        data = data.drop_columns([c for c in LINEAGE if c in data.column_names])
        order = ["event_id"] if name == "learning_activity" else list(contract.primary_key)
        out[name] = data.sort_by([(c, "ascending") for c in order]).to_pylist()
        quarantine = read(fake, current.quarantine[name]).drop_columns(
            ["_run_id", "_bronze_ingest_date", "_quarantined_at"])
        out[f"{name} quarantine"] = quarantine.sort_by(
            [("source_object", "ascending"), ("source_line", "ascending")]).to_pylist()
    return out


def assert_equivalent_to_full_rebuild(settings, fake):
    """The central guarantee: incremental Silver == full rebuild of the same Bronze."""
    incremental = content(settings, fake)
    incremental_counts = manifest(fake, pointer(settings, fake).run_id)["entities"]
    build_silver(settings, fake, run_id="full-check", full_refresh=True)
    assert content(settings, fake) == incremental
    full_counts = manifest(fake, "full-check")["entities"]
    for inc, full in zip(incremental_counts, full_counts):
        assert (inc["entity"], inc["counts"], inc["reason_counts"], inc["metrics"]) == (
            full["entity"], full["counts"], full["reason_counts"], full["metrics"])


@pytest.fixture
def built(settings, fake):
    build_bronze(settings, fake)
    return build_silver(settings, fake)


# --- first build ---------------------------------------------------------------------

def test_first_run_is_a_full_build_published_through_the_pointer(built, settings, fake):
    assert (built.status, built.published, built.full_refresh) == ("SUCCEEDED", True, True)
    assert built.full_refresh_reason == "nothing published yet"
    current = pointer(settings, fake)
    assert current.run_id == "silver-run" and current.previous_run_id is None and current.format_version == 2
    assert current.files["employees"] == ["silver/runs/run_id=silver-run/employees/part-00000.parquet"]
    assert current.rows["learning_activity"] == 104
    assert set(current.state) == {"postgres", "learning"}


def test_every_entity_is_written_with_its_contract_file_schema(built, settings, fake):
    for name, contract in CONTRACTS.items():
        assert published(settings, fake, name).schema.equals(contract.silver_file_schema()), name
    assert published(settings, fake, "employees").column("employee_id").to_pylist() == [462, 989]


def test_learning_is_partitioned_by_event_month_without_partition_columns_in_files(built, settings, fake):
    prefix = "silver/runs/run_id=silver-run/learning_activity/"
    assert current_files(client(settings, fake), LAKE, "learning_activity") == [
        f"{prefix}event_year=2023/event_month=10/part-00000.parquet",
        f"{prefix}event_year=2023/event_month=12/part-00000.parquet",
        f"{prefix}event_year=2024/event_month=01/part-00000.parquet",
        f"{prefix}event_year=2025/event_month=02/part-00000.parquet",
    ]
    october = read(fake, f"{prefix}event_year=2023/event_month=10/part-00000.parquet")
    assert "event_year" not in october.column_names and "event_month" not in october.column_names
    assert october.column("event_id").to_pylist() == ["EV0000000001", "EV0000000005"]
    assert october.column("_source_line").to_pylist() == [1, 6]


def test_flags_warnings_and_lineage_are_materialised(built, settings, fake):
    reviews = published(settings, fake, "performance_reviews").to_pylist()
    pre_hire = next(r for r in reviews if r["employee_id"] == 462)
    assert pre_hire["is_pre_hire"] is True and pre_hire["_dq_warnings"] == ["pre_hire_review"]
    assert pre_hire["_run_id"] == "silver-run" and pre_hire["_bronze_ingest_date"] == SNAPSHOT
    (assessment,) = published(settings, fake, "competency_assessments").to_pylist()
    assert (assessment["gap"], assessment["is_critical_gap"]) == (2, True)
    events = {r["event_id"]: r for r in published(settings, fake, "learning_activity").to_pylist()}
    assert events["EV0000000006"]["text"] == "Started."
    assert events["EV0000000006"]["_dq_warnings"] == ["field_alias_applied"]
    assert events["EV0000000007"]["_dq_warnings"] == ["pre_hire_learning_event"]


def test_quarantine_is_written_for_every_entity_with_reasons_and_raw_records(built, settings, fake):
    current = pointer(settings, fake)
    for name in CONTRACTS:
        assert read(fake, current.quarantine[name]).schema.equals(QUARANTINE_SCHEMA)
    rows = read(fake, current.quarantine["learning_activity"]).to_pylist()
    assert [(r["record_key"], r["reason_codes"][0]) for r in rows] == [
        ("EV0000000002", "malformed_json"), ("EV0000000003", "missing_required_field"),
        ("EV0000000004", "invalid_timestamp"),
    ]
    assert rows[0]["raw_record"] == '{"event_id": "EV0000000002", "employee_id": "989", "event_da'


def test_manifest_reconciles_counts(built, fake):
    m = manifest(fake, "silver-run")
    assert (m["status"], m["full_refresh"]) == ("SUCCEEDED", True)
    learning = next(e for e in m["entities"] if e["entity"] == "learning_activity")
    assert learning["counts"] == {"read": 108, "accepted": 104, "accepted_with_warnings": 2,
                                  "quarantined": 3, "deduplicated": 1}
    assert learning["reason_counts"]["empty_file"] == 1
    for entity in m["entities"]:
        counts = entity["counts"]
        assert counts["read"] == counts["accepted"] + counts["quarantined"] + counts["deduplicated"]
        assert sum(entity["outputs"].values()) == counts["accepted"]


# --- incremental updates (each compared with a full rebuild) -------------------------

def test_rerun_without_changes_reuses_everything_and_writes_no_data(built, settings, fake):
    before = pointer(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.full_refresh is False
    assert {e.entity: e.mode for e in result.entities} == {
        **{n: "reused" for n in CONTRACTS if n != "learning_activity"}, "learning_activity": "incremental"}
    assert all(e.written == [] for e in result.entities)
    after = pointer(settings, fake)
    assert (after.run_id, after.previous_run_id) == ("silver-run-2", "silver-run")
    assert after.files == before.files and after.quarantine == before.quarantine
    assert entity_result(result, "learning_activity").stats["files_reprocessed"] == 0


def test_new_day_file_rewrites_only_its_partition(built, settings, fake):
    before = pointer(settings, fake)
    add_source_file(fake, "learning_activity/year=2024/month=03/day=02/events-000.jsonl", lines(
        event("EV0000002001", event_date="2024-03-02T10:00:00"),
        event("EV0000002002", event_date="2024-03-02T11:00:00", event_type="module_started")))
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    learning = entity_result(result, "learning_activity")
    assert learning.stats["files_new_or_changed"] == 1 and learning.stats["files_reprocessed"] == 1
    assert learning.stats["partitions_affected"] == 1 and learning.stats["rows_added"] == 2
    new_partition = "silver/runs/run_id=silver-run-2/learning_activity/event_year=2024/event_month=03/part-00000.parquet"
    assert sorted(learning.written) == sorted([
        new_partition, "silver/runs/run_id=silver-run-2/_quarantine/learning_activity/part-00000.parquet",
        "silver/runs/run_id=silver-run-2/_state/learning_files.parquet",
        "silver/runs/run_id=silver-run-2/_state/learning_index.parquet"])
    after = pointer(settings, fake)
    assert set(after.files["learning_activity"]) == set(before.files["learning_activity"]) | {new_partition}
    assert after.rows["learning_activity"] == 106
    assert_equivalent_to_full_rebuild(settings, fake)


def test_changed_bronze_file_replaces_its_rows(built, settings, fake):
    fixed = LEARNING_FILES["learning_activity/year=2023/month=10/day=13/events-000.jsonl"].replace(
        '{"event_id": "EV0000000002", "employee_id": "989", "event_da',
        event("EV0000000002", event_date="2023-10-13T15:00:00"))
    add_source_file(fake, "learning_activity/year=2023/month=10/day=13/events-000.jsonl", fixed)
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    learning = entity_result(result, "learning_activity")
    assert learning.stats["files_new_or_changed"] == 1 and learning.counts["quarantined"] == 2
    assert "EV0000000002" in published(settings, fake, "learning_activity").column("event_id").to_pylist()
    assert_equivalent_to_full_rebuild(settings, fake)


def test_cross_file_exact_duplicate_keeps_the_copy_first_in_source_order(built, settings, fake):
    original = [line for line in LEARNING_FILES["learning_activity/year=2023/month=10/day=13/events-000.jsonl"]
                .splitlines() if "EV0000000005" in line][0]
    add_source_file(fake, "learning_activity/year=2023/month=10/day=01/events-000.jsonl", lines(original))
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    learning = entity_result(result, "learning_activity")
    assert learning.stats["files_reprocessed"] == 2  # the new file pulled in the file sharing the event_id
    row = next(r for r in published(settings, fake, "learning_activity").to_pylist() if r["event_id"] == "EV0000000005")
    assert row["_source_object"] == "learning_activity/year=2023/month=10/day=01/events-000.jsonl"
    assert learning.counts["deduplicated"] == 2
    assert_equivalent_to_full_rebuild(settings, fake)


def test_cross_file_conflicting_duplicate_quarantines_every_copy(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=05/day=01/events-000.jsonl",
                    lines(event("EV0000000005", event_type="quiz_attempted", score=99)))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    assert "EV0000000005" not in published(settings, fake, "learning_activity").column("event_id").to_pylist()
    quarantine = read(fake, pointer(settings, fake).quarantine["learning_activity"]).to_pylist()
    assert sum(r["record_key"] == "EV0000000005" for r in quarantine) == 2
    assert_equivalent_to_full_rebuild(settings, fake)


def test_hire_date_change_recomputes_pre_hire_flags_in_old_partitions(built, settings, fake):
    later = source_tables(employees=[employee(989, date(2024, 1, 1)), employee(462, date(2025, 3, 1), team_id=2)])
    build_bronze(settings, fake, tables=later, ingest_date=date(2026, 10, 15))
    result = build_silver(settings, fake, run_id="silver-run-2")
    modes = {e.entity: e.mode for e in result.entities}
    assert modes["teams"] == "reused" and modes["employees"] == "processed"
    learning = entity_result(result, "learning_activity")
    assert learning.stats["files_new_or_changed"] == 0 and learning.stats["files_reprocessed"] > 0
    events = {r["event_id"]: r for r in published(settings, fake, "learning_activity").to_pylist()}
    assert events["EV0000000001"]["is_pre_hire"] is True  # 2023 event, employee now hired 2024-01-01
    assert events["EV0000000007"]["is_pre_hire"] is True  # employee 462 unaffected
    assert_equivalent_to_full_rebuild(settings, fake)


def test_late_arriving_employee_releases_quarantined_events(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=06/day=01/events-000.jsonl",
                    lines(event("EV0000003001", employee_id="777", event_date="2024-06-01T09:00:00")))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    quarantine = read(fake, pointer(settings, fake).quarantine["learning_activity"]).to_pylist()
    assert any(r["record_key"] == "EV0000003001" and r["reason_codes"] == ["unknown_reference"] for r in quarantine)
    with_777 = source_tables(employees=[employee(989, date(2020, 1, 1)), employee(462, date(2025, 3, 1), team_id=2),
                                        employee(777, date(2024, 1, 1))])
    build_bronze(settings, fake, tables=with_777, ingest_date=date(2026, 10, 15))
    build_silver(settings, fake, run_id="silver-run-3")
    assert "EV0000003001" in published(settings, fake, "learning_activity").column("event_id").to_pylist()
    assert_equivalent_to_full_rebuild(settings, fake)


def history(result_or_manifest_entity, code):
    metrics = result_or_manifest_entity.metrics if hasattr(result_or_manifest_entity, "metrics") \
        else result_or_manifest_entity["metrics"]
    return metrics["history"][code]


def test_completion_without_enrollment_counts_and_is_never_a_row_warning(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=07/day=01/events-000.jsonl", lines(
        event("EV0000004001", event_type="course_completed", event_date="2024-07-01T09:00:00", course_name="SQL"),
        event("EV0000004002", event_type="certification_earned", event_date="2024-07-01T10:00:00", course_name="Go")))
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    rows = {r["event_id"]: r for r in published(settings, fake, "learning_activity").to_pylist()}
    assert rows["EV0000004001"]["event_type"] == "course_completed"      # A13: the completion counts
    assert rows["EV0000004001"]["_dq_warnings"] == [] and rows["EV0000004002"]["_dq_warnings"] == []
    learning = entity_result(result, "learning_activity")
    assert learning.counts["accepted_with_warnings"] == 2                 # unchanged by BUS-010/011
    assert "completion_without_enrollment" not in learning.reason_counts
    assert history(learning, "completion_without_enrollment") | {} == {
        **history(learning, "completion_without_enrollment"), "events": 1, "flagged": 1, "rate": 1.0}
    assert history(learning, "certification_without_completion")["flagged"] == 1
    m = manifest(fake, "silver-run-2")["entities"][-1]
    assert history(m, "completion_without_enrollment")["rule_id"] == "BUS-010"


def test_late_enrollment_updates_the_metric_without_rewriting_old_partitions(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=07/day=01/events-000.jsonl", lines(
        event("EV0000004001", event_type="course_completed", event_date="2024-07-01T09:00:00", course_name="SQL")))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    july = next(k for k in pointer(settings, fake).files["learning_activity"] if "event_month=07" in k)
    add_source_file(fake, "learning_activity/year=2024/month=06/day=15/events-000.jsonl", lines(
        event("EV0000004000", event_date="2024-06-15T09:00:00", course_name="SQL")))
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-3")
    learning = entity_result(result, "learning_activity")
    assert learning.stats["partitions_affected"] == 1 and learning.stats["files_reprocessed"] == 1
    assert july in pointer(settings, fake).files["learning_activity"]   # the July file is reused as-is
    assert history(learning, "completion_without_enrollment")["flagged"] == 0
    assert learning.stats["history_metrics_recomputed"] == 1
    assert_equivalent_to_full_rebuild(settings, fake)


def test_history_metrics_are_reused_when_nothing_changed(built, settings, fake):
    first = entity_result(build_silver(settings, fake, run_id="silver-run-2"), "learning_activity")
    assert first.stats["history_metrics_recomputed"] == 0
    assert first.metrics == manifest(fake, "silver-run")["entities"][-1]["metrics"]


def test_deleted_bronze_file_removes_its_rows(built, settings, fake):
    del fake.objects[(LAKE, "bronze/learning_activity/year=2024/month=01/day=05/events-000.jsonl")]
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert entity_result(result, "learning_activity").stats["files_deleted"] == 1
    assert not any("event_year=2024/event_month=01" in k
                   for k in current_files(client(settings, fake), LAKE, "learning_activity"))
    assert_equivalent_to_full_rebuild(settings, fake)


def test_unchanged_postgres_snapshot_on_a_new_date_is_reused(built, settings, fake):
    build_bronze(settings, fake, ingest_date=date(2026, 10, 15))
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.bronze_ingest_date == "2026-10-15"
    assert all(e.mode == "reused" for e in result.entities if e.entity != "learning_activity")


def test_rules_change_triggers_an_automatic_full_refresh(built, settings, fake, monkeypatch):
    monkeypatch.setattr(silver_module, "LOGIC_VERSION", silver_module.LOGIC_VERSION + 1)
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.full_refresh is True and result.full_refresh_reason.startswith("rules changed")
    assert pointer(settings, fake).rules_fingerprint == silver_module.rules_fingerprint()


def test_full_refresh_flag_rebuilds_everything(built, settings, fake):
    result = build_silver(settings, fake, run_id="silver-run-2", full_refresh=True)
    assert (result.full_refresh, result.full_refresh_reason) == (True, "requested (--full-refresh)")
    assert all(e.mode in ("processed", "full") for e in result.entities)


def test_previous_version_without_state_is_rebuilt_in_full(built, settings, fake):
    raw = json.loads(fake.objects[(LAKE, "silver/_current.json")])
    raw.pop("state"), raw.pop("format_version")
    fake.objects[(LAKE, "silver/_current.json")] = json.dumps(raw).encode()
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.full_refresh_reason == "published version has no incremental state"


# --- retention and cleanup -----------------------------------------------------------

def test_files_referenced_by_the_current_or_previous_version_are_kept(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=03/day=02/events-000.jsonl",
                    lines(event("EV0000002001", event_date="2024-03-02T10:00:00")))
    build_bronze(settings, fake)
    build_silver(settings, fake, run_id="silver-run-2")
    build_silver(settings, fake, run_id="silver-run-3")
    current = pointer(settings, fake)
    present = set(fake.keys(LAKE, "silver/runs/"))
    assert current.referenced_keys() <= present
    # Unchanged data still lives in the first run's folder; nothing was copied.
    assert all(k.startswith(("silver/runs/run_id=silver-run/", "silver/runs/run_id=silver-run-2/")) for k in present)
    assert len(fake.keys(LAKE, "silver/_manifests/")) == 3


def test_unreferenced_files_from_older_versions_are_deleted(built, settings, fake):
    old_ledger = pointer(settings, fake).state["learning"]["ledger_key"]
    for n, day in enumerate(("02", "03", "04"), start=1):
        add_source_file(fake, f"learning_activity/year=2024/month=03/day={day}/events-000.jsonl",
                        lines(event(f"EV000000500{n}", event_date=f"2024-03-{day}T10:00:00")))
        build_bronze(settings, fake)
        build_silver(settings, fake, run_id=f"silver-run-{n + 1}")
    assert (LAKE, old_ledger) not in fake.objects   # superseded twice: no retained version references it


def test_old_fixed_path_layout_is_cleaned_up_on_publish(settings, fake):
    fake.buckets.add(LAKE)
    fake.objects[(LAKE, "silver/teams/part-00000.parquet")] = b"old layout"
    build_bronze(settings, fake)
    build_silver(settings, fake)
    assert (LAKE, "silver/teams/part-00000.parquet") not in fake.objects


def test_hard_crash_without_manifest_is_cleaned_up_only_when_abandoned(built, settings, fake):
    def leave_partial(run_id, started):
        fake.objects[(LAKE, f"silver/runs/run_id={run_id}/_STARTED.json")] = json.dumps(
            {"run_id": run_id, "started_at": started.isoformat()}).encode()
        fake.objects[(LAKE, f"silver/runs/run_id={run_id}/teams/part-00000.parquet")] = b"partial"
    leave_partial("killed-long-ago", NOW - ABANDONED_RUN_AGE - timedelta(minutes=1))
    leave_partial("maybe-still-running", NOW - timedelta(minutes=5))
    build_silver(settings, fake, run_id="silver-run-2")
    remaining = {k.split("/")[2] for k in fake.keys(LAKE, "silver/runs/")}
    assert "run_id=killed-long-ago" not in remaining and "run_id=maybe-still-running" in remaining


def test_cleanup_failure_does_not_fail_a_published_run(built, settings, fake, monkeypatch):
    monkeypatch.setattr(S3Client, "delete_keys", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.published and pointer(settings, fake).run_id == "silver-run-2"


# --- crash safety ----------------------------------------------------------------------

def crash_on_put(fake, after):
    """Make the fake S3 fail permanently on the Nth Silver data write from now on (a crash mid-run)."""
    original, calls = fake.put_object, {"n": 0}

    def put_object(Bucket, Key, Body, ContentType):
        if Key.startswith("silver/runs/") and not Key.endswith("_STARTED.json"):
            calls["n"] += 1
            if calls["n"] == after:
                raise client_error("AccessDenied")
        return original(Bucket=Bucket, Key=Key, Body=Body, ContentType=ContentType)
    fake.put_object = put_object
    return lambda: setattr(fake, "put_object", original)


def test_crash_mid_write_leaves_published_silver_untouched(built, settings, fake):
    pointer_before = fake.objects[(LAKE, "silver/_current.json")]
    published_before = content(settings, fake)
    restore = crash_on_put(fake, after=5)
    with pytest.raises(Exception, match="AccessDenied"):
        build_silver(settings, fake, run_id="silver-crash", full_refresh=True)
    restore()
    assert fake.objects[(LAKE, "silver/_current.json")] == pointer_before
    assert content(settings, fake) == published_before
    crashed = manifest(fake, "silver-crash")
    assert (crashed["status"], crashed["published"]) == ("FAILED", False) and "AccessDenied" in crashed["error"]


def test_next_run_after_a_crash_publishes_and_removes_the_partial_files(built, settings, fake):
    restore = crash_on_put(fake, after=5)
    with pytest.raises(Exception):
        build_silver(settings, fake, run_id="silver-crash", full_refresh=True)
    restore()
    build_silver(settings, fake, run_id="silver-run-3")
    assert pointer(settings, fake).previous_run_id == "silver-run"
    assert not fake.keys(LAKE, "silver/runs/run_id=silver-crash/")


def test_crash_before_pointer_flip_keeps_old_pointer(built, settings, fake):
    pointer_before = fake.objects[(LAKE, "silver/_current.json")]
    original = fake.put_object

    def no_pointer(Bucket, Key, Body, ContentType):
        if Key == "silver/_current.json":
            raise RuntimeError("killed")
        return original(Bucket=Bucket, Key=Key, Body=Body, ContentType=ContentType)
    fake.put_object = no_pointer
    with pytest.raises(RuntimeError, match="killed"):
        build_silver(settings, fake, run_id="silver-unpublished", full_refresh=True)
    fake.put_object = original
    assert fake.objects[(LAKE, "silver/_current.json")] == pointer_before
    assert manifest(fake, "silver-unpublished")["status"] == "SUCCEEDED"  # complete, but never published


def test_older_run_does_not_overwrite_a_newer_published_run(built, settings, fake):
    build_silver(settings, fake, run_id="silver-newer", now=NOW + timedelta(hours=1))
    result = build_silver(settings, fake, run_id="silver-older", now=NOW)
    assert (result.status, result.published) == ("SUPERSEDED", False)
    assert pointer(settings, fake).run_id == "silver-newer"


def test_missing_referenced_object_blocks_publishing(settings, fake, monkeypatch):
    build_bronze(settings, fake)
    original = SilverBuilder._write_postgres

    def drop_teams_after_write(self, contract, plan, snapshot, base):
        original(self, contract, plan, snapshot, base)
        if contract.name == "teams":
            del fake.objects[(LAKE, "silver/runs/run_id=silver-run/teams/part-00000.parquet")]
    monkeypatch.setattr(SilverBuilder, "_write_postgres", drop_teams_after_write)
    with pytest.raises(SilverIntegrityError, match="would reference missing objects"):
        build_silver(settings, fake)
    assert pointer(settings, fake) is None


# --- snapshot selection and readers --------------------------------------------------

def test_latest_complete_snapshot_ignores_incomplete_dates(settings, fake):
    build_bronze(settings, fake, ingest_date=date(2026, 10, 13))
    build_bronze(settings, fake, ingest_date=date(2026, 10, 14))
    del fake.objects[(LAKE, "bronze/postgres/teams/ingest_date=2026-10-14/part-00000.parquet")]
    assert latest_complete_snapshot(client(settings, fake), LAKE) == date(2026, 10, 13)


def test_no_snapshot_or_missing_requested_snapshot_raises(settings, fake):
    fake.buckets.add(LAKE)
    with pytest.raises(SilverSourceError, match="no complete Bronze snapshot"):
        build_silver(settings, fake)
    build_bronze(settings, fake)
    with pytest.raises(SilverSourceError, match=r"2026-10-01 is missing tables \['teams'"):
        build_silver(settings, fake, ingest_date=date(2026, 10, 1))
    assert build_silver(settings, fake, ingest_date=SNAPSHOT).bronze_ingest_date == "2026-10-14"


def test_missing_learning_files_raise(settings, fake):
    build_bronze(settings, fake)
    fake.delete_objects(LAKE, {"Objects": [{"Key": k} for k in fake.keys(LAKE, "bronze/learning_activity/")]})
    with pytest.raises(SilverSourceError, match="no files under"):
        build_silver(settings, fake)


def test_readers_before_anything_is_published(settings, fake):
    fake.buckets.add(LAKE)
    assert read_current(client(settings, fake), LAKE) is None
    with pytest.raises(SilverSourceError, match="no published Silver"):
        current_files(client(settings, fake), LAKE, "teams")


# --- failures: all-or-nothing --------------------------------------------------------

def test_failed_entity_blocks_every_write_and_skips_children(built, settings, fake):
    silver_before = {k: fake.objects[(LAKE, k)] for k in silver_keys(fake) if "_manifests" not in k}
    broken = source_tables()
    broken["employees"] = broken["employees"].drop_columns(["hire_date"])
    build_bronze(settings, fake, tables=broken, ingest_date=date(2026, 10, 15))
    with pytest.raises(SilverValidationError, match="employees: FAILED"):
        build_silver(settings, fake, run_id="silver-bad")
    assert {k: fake.objects[(LAKE, k)] for k in silver_keys(fake) if "_manifests" not in k} == silver_before
    m = manifest(fake, "silver-bad")
    assert (m["status"], m["published"]) == ("FAILED", False)
    assert {e["entity"]: e["status"] for e in m["entities"]} == {
        "teams": "SUCCEEDED", "competencies": "SUCCEEDED", "employees": "FAILED", "projects": "SUCCEEDED",
        "performance_reviews": "SKIPPED", "competency_assessments": "SKIPPED", "talent_review": "SKIPPED",
        "learning_activity": "SKIPPED"}


def test_quarantine_rate_is_checked_on_entity_totals_not_on_the_increment(built, settings, fake):
    # The decision uses the entity total: 3 + 4 new bad of 112 records = 6.25% > 5% fails.
    add_source_file(fake, "learning_activity/year=2024/month=08/day=01/events-000.jsonl", lines(
        *(event(f"EV000000600{i}", employee_id=None) for i in range(4))))
    build_bronze(settings, fake)
    with pytest.raises(SilverValidationError, match=r"learning_activity: FAILED .*7/112 quarantined"):
        build_silver(settings, fake, run_id="silver-run-2")
    assert pointer(settings, fake).run_id == "silver-run"


def test_small_bad_increment_passes_when_the_entity_total_is_healthy(built, settings, fake):
    add_source_file(fake, "learning_activity/year=2024/month=08/day=01/events-000.jsonl",
                    lines(event("EV0000006001", employee_id=None)))  # 100% bad increment, 4/109 overall
    build_bronze(settings, fake)
    result = build_silver(settings, fake, run_id="silver-run-2")
    assert result.published and entity_result(result, "learning_activity").counts["quarantined"] == 4


def test_not_null_check_rejects_nulls_in_required_columns():
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False)])
    _check_not_null(pa.table({"id": pa.array([1], pa.int32())}, schema=schema), "ok")
    with pytest.raises(SilverIntegrityError, match=r"nulls in non-nullable columns \['id'\]"):
        _check_not_null(pa.table({"id": pa.array([None], pa.int32())}, schema=schema), "bad")


def test_readback_mismatch_raises_and_nothing_is_published(settings, fake, monkeypatch):
    build_bronze(settings, fake)
    original = S3Client.read_arrow
    monkeypatch.setattr(S3Client, "read_arrow", lambda self, b, k: original(self, b, k).slice(0, 0)
                        if "/teams/" in k and k.startswith("silver/") else original(self, b, k))
    with pytest.raises(SilverIntegrityError, match="teams/part-00000.parquet: read back 0 rows"):
        build_silver(settings, fake)
    assert pointer(settings, fake) is None


def test_lake_bucket_from_settings_is_used(fake):
    settings = Settings.from_env({})
    other = replace(settings, s3=replace(settings.s3, lake_bucket="other-lake"))
    build_bronze(other, fake)
    build_silver(other, fake)
    assert read_current(client(other, fake), "other-lake").run_id == "silver-run"
