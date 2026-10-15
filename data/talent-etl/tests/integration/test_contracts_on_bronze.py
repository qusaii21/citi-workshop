"""
Integration: run the schema contracts and data quality rules over real Bronze data.

Bronze is produced from the seeded local sources into a throwaway LocalStack bucket,
then read back exactly as Silver will read it (Parquet + JSONL from S3). The
expected counts were measured independently with pandas on the sample data and
are documented in docs/data_quality.md.
"""

from datetime import date

import pytest

from talent_etl.bronze import BronzeIngestor
from talent_etl.contracts import CONTRACTS
from talent_etl.dq_rules import Action
from talent_etl.paths import bronze_learning_prefix, bronze_postgres_key
from talent_etl.validation import (
    RawRecord,
    ValidationContext,
    hire_dates_from,
    parse_jsonl,
    validate_entity,
)

pytestmark = pytest.mark.integration

INGEST_DATE = date(2026, 10, 14)

EXPECTED_COUNTS = {
    "teams": (100, 100, 0, 0, 0),
    "competencies": (40, 40, 0, 0, 0),
    "employees": (2000, 2000, 0, 0, 0),
    "projects": (33, 33, 0, 0, 0),
    "performance_reviews": (4000, 4000, 666, 0, 0),
    "competency_assessments": (4000, 4000, 234, 0, 0),
    "talent_review": (2000, 2000, 270, 0, 0),
    "learning_activity": (29868, 29574, 5185, 142, 152),
}  # read, accepted, accepted_with_warnings, quarantined, deduplicated


@pytest.fixture(scope="module")
def reports(request):
    """Validate every entity from a fresh Bronze build (module-scoped: built once)."""
    settings = request.getfixturevalue("lake_settings")
    pg, s3 = request.getfixturevalue("pg"), request.getfixturevalue("s3")
    BronzeIngestor(settings, pg, s3, run_id="it-contracts", ingest_date=INGEST_DATE).run()
    lake = settings.s3.lake_bucket
    context = ValidationContext(ingest_date=INGEST_DATE)
    results = {}
    for name, contract in CONTRACTS.items():
        if contract.source_system == "postgres":
            table = s3.read_arrow(lake, bronze_postgres_key(name, INGEST_DATE))
            records = [RawRecord(row, contract.source_object) for row in table.to_pylist()]
            report = validate_entity(
                contract, records, context, source_columns=table.column_names,
                source_types={f.name: f.type for f in table.schema},
            )
        else:
            records, rejects, file_issues = [], [], []
            prefix = bronze_learning_prefix()
            for obj in s3.list_objects(lake, prefix):
                parsed = parse_jsonl(s3.get_bytes(lake, obj.key), obj.key.removeprefix("bronze/"))
                records += parsed.records
                rejects += parsed.rejects
                file_issues += parsed.file_issues
            report = validate_entity(contract, records, context, parse_rejects=rejects, file_issues=file_issues)
        results[name] = report
        context.reference_keys[name] = report.accepted_keys()
        if name == "employees":
            context.hire_dates = hire_dates_from(report)
    return results


# Module-scoped wrappers around the function-scoped integration fixtures.
@pytest.fixture(scope="module")
def lake_settings(settings, request):
    import uuid
    from dataclasses import replace

    from talent_etl.io_s3 import S3Client
    from talent_etl.retry import RetryPolicy

    pg = request.getfixturevalue("pg")
    from talent_etl.bronze import SOURCE_TABLES
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    bucket = f"it-{uuid.uuid4().hex[:8]}"
    s3 = S3Client(settings.s3, RetryPolicy(attempts=2, base_delay=0.1, jitter=False))
    s3.ensure_bucket(bucket)
    yield replace(settings, s3=replace(settings.s3, lake_bucket=bucket))
    s3.delete_prefix(bucket, "")
    s3._client.delete_bucket(Bucket=bucket)


@pytest.fixture(scope="module")
def pg(settings):
    from talent_etl.io_pg import PostgresClient
    from talent_etl.retry import RetryPolicy
    with PostgresClient(settings.postgres, RetryPolicy(attempts=2, base_delay=0.1, jitter=False)) as client:
        try:
            client.count_rows("pg_catalog", "pg_class")
        except Exception as exc:  # noqa: BLE001 - any connection problem means skip
            pytest.skip(f"local PostgreSQL unreachable: {exc}")
        yield client


@pytest.fixture(scope="module")
def s3(settings):
    from talent_etl.io_s3 import S3Client
    from talent_etl.retry import RetryPolicy
    return S3Client(settings.s3, RetryPolicy(attempts=2, base_delay=0.1, jitter=False))


@pytest.mark.parametrize("entity", list(CONTRACTS))
def test_entity_counts_match_independent_profile(reports, entity):
    report = reports[entity]
    counts = report.counts()
    read, accepted, warned, quarantined, deduplicated = EXPECTED_COUNTS[entity]
    assert (counts["read"], counts["accepted"], counts["accepted_with_warnings"],
            counts["quarantined"], counts["deduplicated"]) == (read, accepted, warned, quarantined, deduplicated)
    assert counts["read"] == counts["accepted"] + counts["quarantined"] + counts["deduplicated"]
    assert not report.failed


def test_bronze_postgres_schemas_match_contracts_without_drift(reports):
    for name, report in reports.items():
        if CONTRACTS[name].source_system == "postgres":
            assert report.dataset_issues == [], name


def test_known_learning_defects_are_classified_exactly(reports):
    report = reports["learning_activity"]
    reasons = report.reason_counts()
    assert reasons["malformed_json"] == 13
    assert reasons["empty_file"] == 1
    assert reasons["duplicate_record"] == 152
    assert reasons["missing_required_field"] == 90   # 76 null employee_id + 14 blank event_date
    assert reasons["invalid_timestamp"] == 39        # yesterday x15, N/A x13, 2026-13-45 x11
    assert reasons["field_alias_applied"] == 38
    assert reasons["pre_hire_learning_event"] == 5161
    assert reasons["unknown_reference"] == 0
    quarantine = report.quarantine_records()
    assert len(quarantine) == 142
    assert all(row["record_key"] for row in quarantine)  # every rejected line is traceable to an event_id
    assert all(row["raw_record"] and row["reason_codes"] and row["rule_ids"] for row in quarantine)
    missing_employee = [r for r in quarantine if "employee_id is null" in r["reason_detail"]]
    assert len(missing_employee) == 76
    assert sum("event_date is null" in r["reason_detail"] for r in quarantine) == 14
    bad_dates = {r["raw_record"].split('"event_date": ')[1].split(",")[0] for r in quarantine
                 if "invalid_timestamp" in r["reason_codes"]}
    assert bad_dates == {'"yesterday"', '"N/A"', '"2026-13-45"'}


def test_business_flags_on_real_data(reports):
    reviews = reports["performance_reviews"]
    assert sum(o.derived["is_pre_hire"] for o in reviews.accepted) == 666
    assert len({o.values["employee_id"] for o in reviews.accepted if o.derived["is_pre_hire"]}) == 396
    assessments = reports["competency_assessments"]
    assert sum(o.derived["is_critical_gap"] for o in assessments.accepted) == 682  # 325 at gap 2 + 357 at 1-of-2
    assert sorted({o.derived["gap"] for o in assessments.accepted}) == [-1, 0, 1, 2]
    assert sum(o.derived["is_pre_hire"] for o in assessments.accepted) == 234
    assert sum(o.derived["is_pre_hire"] for o in reports["talent_review"].accepted) == 270
    learning = reports["learning_activity"]
    assert all(o.action is not Action.QUARANTINE for o in learning.accepted)
    assert sum(o.derived["is_pre_hire"] for o in learning.accepted) == 5161
