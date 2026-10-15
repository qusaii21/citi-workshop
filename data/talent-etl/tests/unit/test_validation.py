"""
Every data quality rule, exercised on small fixtures modelled on the real defects.

Each test asserts the final action and the reason code / rule id, not just that
validation ran.
"""

import json
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pytest

from talent_etl.contracts import CONTRACTS
from talent_etl.dq_rules import Action
from talent_etl.validation import (
    RawRecord,
    ValidationContext,
    check_dataset_schema,
    hire_dates_from,
    parse_jsonl,
    validate_entity,
)

PARTITION = "learning_activity/year=2023/month=10/day=13/events-000.jsonl"
HIRE_DATES = {989: date(2020, 1, 1), 462: date(2025, 3, 1)}


def ctx(**overrides):
    base = dict(
        ingest_date=date(2026, 10, 14),
        reference_keys={"teams": {1, 2}, "competencies": {1, 12}, "employees": set(HIRE_DATES)},
        hire_dates=dict(HIRE_DATES),
    )
    return ValidationContext(**(base | overrides))


def event(**overrides):
    record = {"event_id": "EV0000001103", "employee_id": "989", "event_date": "2023-10-13T14:49:00",
              "event_type": "quiz_attempted", "course_name": "Time Management for Professionals",
              "text": "Took the Time Management for Professionals quiz.", "score": 46}
    record.update(overrides)
    return {k: v for k, v in record.items() if v is not ...}  # ... removes a key


def jsonl(*records):
    return "".join((r if isinstance(r, str) else json.dumps(r)) + "\n" for r in records).encode()


def validate_events(body, source=PARTITION, extra_records=(), **context):
    parsed = parse_jsonl(body, source)
    return validate_entity(CONTRACTS["learning_activity"], list(parsed.records) + list(extra_records),
                           ctx(**context), parse_rejects=parsed.rejects, file_issues=parsed.file_issues)


def only(report):
    (outcome,) = report.outcomes
    return outcome


def codes(outcome):
    return [issue.reason_code for issue in outcome.issues]


# ===================================================================================
# Learning activity JSONL: the known defects
# ===================================================================================

def test_valid_event_is_accepted_with_typed_values():
    outcome = only(validate_events(jsonl(event())))
    assert outcome.action is Action.ACCEPT
    assert codes(outcome) == ["employee_id_string_cast"]  # informational, ACCEPT
    assert outcome.values["employee_id"] == 989
    assert outcome.values["score"] == 46.0
    assert outcome.derived == {"event_year": 2023, "event_month": 10, "is_pre_hire": False}


def test_malformed_truncated_line_is_quarantined_with_raw_line_and_location():
    truncated = '{"event_id": "EV0000002169", "employee_id": "1020", "event_date": "2023-11-22T08:32:00", "event_type": "module_started'
    report = validate_events(jsonl(event(), truncated))
    (row,) = report.quarantine_records()
    assert row["reason_codes"] == ["malformed_json"]
    assert row["rule_ids"] == ["JSN-001"]
    assert row["raw_record"] == truncated
    assert (row["source_object"], row["source_line"], row["record_key"]) == (PARTITION, 2, "EV0000002169")
    assert "line 2" in row["reason_detail"]
    assert report.counts() == {"read": 2, "accepted": 1, "accepted_with_warnings": 0,
                               "quarantined": 1, "deduplicated": 0}


def test_non_object_json_and_bad_encoding_are_quarantined():
    body = b'["not", "an", "object"]\n' + b'{"event_id": "EV1", "text": "caf\xe9"}\n' + jsonl(event())
    report = validate_events(body)
    rows = report.quarantine_records()
    assert [r["reason_codes"] for r in rows] == [["not_a_json_object"], ["invalid_encoding"]]
    assert "�" in rows[1]["raw_record"]  # undecodable byte preserved as U+FFFD
    assert len(report.accepted) == 1


def test_empty_file_is_a_warning_not_a_failure():
    parsed = parse_jsonl(b"", "learning_activity/year=2025/month=09/day=03/events-000.jsonl")
    assert parsed.records == [] and parsed.rejects == [] and parsed.lines_read == 0
    (issue,) = parsed.file_issues
    assert (issue.reason_code, issue.action) == ("empty_file", Action.WARN)
    other = parse_jsonl(jsonl(event()), PARTITION)
    report = validate_entity(CONTRACTS["learning_activity"], other.records, ctx(),
                             file_issues=parsed.file_issues + other.file_issues)
    assert not report.failed
    assert report.reason_counts()["empty_file"] == 1


def test_blank_lines_and_crlf_are_handled():
    body = jsonl(event()).replace(b"\n", b"\r\n") + b"   \n" + jsonl(event(event_id="EV0000001104"))
    parsed = parse_jsonl(body, PARTITION)
    assert len(parsed.records) == 2 and parsed.lines_read == 3
    assert [i.reason_code for i in parsed.file_issues] == ["blank_line"]


def test_exact_duplicate_event_is_deduplicated_keeping_the_first_copy():
    report = validate_events(jsonl(event(), event()))
    first, second = report.outcomes
    assert first.action is Action.ACCEPT
    assert second.action is Action.DEDUPLICATE
    assert codes(second) == ["duplicate_record"]
    assert f"{PARTITION}:1" in second.issues[0].detail
    assert report.counts()["deduplicated"] == 1 and report.quarantine_records() == []


def test_conflicting_duplicate_event_quarantines_every_copy():
    report = validate_events(jsonl(event(), event(score=99)))
    assert [o.action for o in report.outcomes] == [Action.QUARANTINE, Action.QUARANTINE]
    assert all("conflicting_duplicate" in codes(o) for o in report.outcomes)
    assert [r["rule_ids"] for r in report.quarantine_records()] == [["KEY-002"], ["KEY-002"]]


def test_duplicate_of_an_invalid_event_is_quarantined_once():
    bad = event(employee_id=None)
    report = validate_events(jsonl(bad, bad))
    assert [o.action for o in report.outcomes] == [Action.QUARANTINE, Action.DEDUPLICATE]
    assert len(report.quarantine_records()) == 1


@pytest.mark.parametrize("employee_id", [None, ...])  # null value, or key absent
def test_null_or_missing_employee_id_is_quarantined(employee_id):
    record = event(employee_id=employee_id)
    report = validate_events(jsonl(record))
    outcome = only(report)
    assert outcome.action is Action.QUARANTINE
    assert codes(outcome) == ["missing_required_field"]
    (row,) = report.quarantine_records()
    assert row["record_key"] == "EV0000001103"
    assert row["rule_ids"] == ["REQ-001"]
    assert json.loads(row["raw_record"]) == record  # original line kept verbatim
    assert row["source_line"] == 1


@pytest.mark.parametrize("bad_date, reason", [
    ("yesterday", "invalid_timestamp"), ("N/A", "invalid_timestamp"), ("2026-13-45", "invalid_timestamp"),
    ("", "missing_required_field"), (None, "missing_required_field"),
])
def test_unparseable_event_date_is_quarantined_never_inferred_from_partition(bad_date, reason):
    outcome = only(validate_events(jsonl(event(event_date=bad_date))))
    assert outcome.action is Action.QUARANTINE
    assert codes(outcome)[-1] == reason
    assert "event_date" not in outcome.derived and "event_year" not in outcome.derived


def test_notes_instead_of_text_is_mapped_with_a_warning():
    outcome = only(validate_events(jsonl(event(text=..., notes="Enrolled in Strategic Thinking Workshop."))))
    assert outcome.action is Action.WARN
    assert "field_alias_applied" in codes(outcome)
    assert outcome.values["text"] == "Enrolled in Strategic Thinking Workshop."
    assert outcome.warnings == ["field_alias_applied"]


def test_text_and_notes_together_keep_text_and_drop_notes():
    outcome = only(validate_events(jsonl(event(notes="other"))))
    assert outcome.values["text"] == "Took the Time Management for Professionals quiz."
    assert [(i.reason_code, i.field) for i in outcome.issues if i.action is Action.WARN] == [
        ("unexpected_column", "notes")]


@pytest.mark.parametrize("employee_id, action, reason", [
    ("989", Action.ACCEPT, "employee_id_string_cast"),
    (989, Action.ACCEPT, None),
    ("abc", Action.QUARANTINE, "type_conversion_failed"),
    ("12.5", Action.QUARANTINE, "type_conversion_failed"),
    ("0", Action.QUARANTINE, "out_of_range"),
    ("99999", Action.QUARANTINE, "unknown_reference"),
])
def test_employee_id_string_cast_and_failures(employee_id, action, reason):
    outcome = only(validate_events(jsonl(event(employee_id=employee_id))))
    assert outcome.action is action
    if reason:
        assert reason in codes(outcome)
    else:
        assert codes(outcome) == []


def test_unknown_employee_is_quarantined_as_foreign_key_violation():
    report = validate_events(jsonl(event(employee_id="5")))
    (row,) = report.quarantine_records()
    assert row["reason_codes"] == ["unknown_reference"] and row["rule_ids"] == ["REF-001"]
    assert "employee_id=5 not found in employees" in row["reason_detail"]


def test_event_before_hire_is_flagged_not_quarantined():
    before = only(validate_events(jsonl(event(employee_id="462", event_date="2025-02-28T23:59:00")),
                                  source="learning_activity/year=2025/month=02/day=28/events-000.jsonl"))
    assert before.action is Action.WARN
    assert before.warnings == ["pre_hire_learning_event"]
    assert before.derived["is_pre_hire"] is True
    on_day = only(validate_events(jsonl(event(employee_id="462", event_date="2025-03-01T08:00:00")),
                                  source="learning_activity/year=2025/month=03/day=01/events-000.jsonl"))
    assert on_day.derived["is_pre_hire"] is False and on_day.action is Action.ACCEPT


@pytest.mark.parametrize("overrides, action, reason", [
    ({"event_type": "course_viewed"}, Action.QUARANTINE, "invalid_domain_value"),
    ({"course_name": "  "}, Action.QUARANTINE, "missing_required_field"),
    ({"course_name": 42}, Action.QUARANTINE, "type_conversion_failed"),
    ({"score": 101}, Action.QUARANTINE, "out_of_range"),
    ({"score": -1}, Action.QUARANTINE, "out_of_range"),
    ({"score": "abc"}, Action.QUARANTINE, "type_conversion_failed"),
    ({"score": True}, Action.QUARANTINE, "type_conversion_failed"),
    ({"event_type": "enrollment", "score": 50}, Action.WARN, "unexpected_score"),
    ({"score": None}, Action.WARN, "missing_quiz_score"),
    ({"event_id": "evt-1"}, Action.WARN, "unexpected_format"),
    ({"device": "mobile"}, Action.WARN, "unexpected_column"),
    ({"event_date": "2026-12-01T09:00:00"}, Action.WARN, "future_date"),
    ({"event_id": None}, Action.QUARANTINE, "missing_required_field"),
])
def test_learning_field_rules(overrides, action, reason):
    outcome = only(validate_events(jsonl(event(**overrides))))
    assert outcome.action is action
    assert reason in codes(outcome)


def test_partition_mismatch_warns_and_event_date_wins():
    outcome = only(validate_events(jsonl(event()), source="learning_activity/year=2023/month=10/day=14/x.jsonl"))
    assert outcome.action is Action.WARN
    assert codes(outcome)[-1] == "partition_date_mismatch"
    assert outcome.derived["event_month"] == 10 and outcome.values["event_date"].day == 13


def test_offset_timestamps_are_normalised_to_utc():
    outcome = only(validate_events(jsonl(event(event_date="2023-10-13T16:49:00+02:00"))))
    assert outcome.values["event_date"].hour == 14 and outcome.action is Action.ACCEPT


def test_counts_reconcile_read_with_dispositions():
    body = jsonl(event(), event(), event(event_id="EV0000000002", employee_id=None), "{bad",
                 event(event_id="EV0000000003"))
    counts = validate_events(body).counts()
    assert counts == {"read": 5, "accepted": 2, "accepted_with_warnings": 0, "quarantined": 2, "deduplicated": 1}
    assert counts["read"] == counts["accepted"] + counts["quarantined"] + counts["deduplicated"]


# ===================================================================================
# PostgreSQL entities
# ===================================================================================

def row(entity, **values):
    defaults = {
        "teams": {"team_id": 1, "name": "Team LightYellow-1", "department": "Sales"},
        "competencies": {"competency_id": 1, "name": "Python", "category": "Technical"},
        "employees": {"employee_id": 989, "first_name": "Heather", "last_name": "Davis",
                      "email": "heather.davis.1@acme-corp.example", "team_id": 1, "job_level": "Director",
                      "hire_date": date(2020, 1, 1)},
        "projects": {"project_id": 1, "name": "DimGray Launch", "team_id": 1, "outcome_status": "on-time",
                     "outcome_score": Decimal("56.87")},
        "performance_reviews": {"review_id": 1, "employee_id": 989, "review_period": "2024-H2", "rating": 3,
                                "manager_comment": "Solid, consistent contributor this cycle.",
                                "submitted_date": date(2025, 8, 31)},
        "competency_assessments": {"assessment_id": 1, "employee_id": 989, "competency_id": 1,
                                   "proficiency_level": 3, "required_level": 3, "assessed_date": date(2025, 12, 28)},
        "talent_review": {"employee_id": 989, "review_cycle": "2025-H1", "potential_rating": "High",
                          "promotion_readiness": "Ready Now", "attrition_risk": "High"},
    }[entity]
    return RawRecord(defaults | values, f"hr_source.{entity}")


def validate_rows(entity, *records, columns=None, **context):
    contract = CONTRACTS[entity]
    return validate_entity(contract, records, ctx(**context),
                           source_columns=columns if columns is not None else list(contract.column_names))


def test_clean_rows_are_accepted_for_every_postgres_entity():
    for entity in ("teams", "competencies", "employees", "projects", "performance_reviews",
                   "competency_assessments", "talent_review"):
        outcome = only(validate_rows(entity, row(entity)))
        assert outcome.action is Action.ACCEPT, (entity, codes(outcome))


def test_missing_column_fails_the_entity_before_any_record_is_validated():
    columns = [c for c in CONTRACTS["employees"].column_names if c != "hire_date"]
    report = validate_rows("employees", row("employees"), columns=columns)
    assert report.failed and report.outcomes == []
    assert [(i.reason_code, i.field, i.rule.rule_id) for i in report.failures] == [
        ("missing_column", "hire_date", "SCH-001")]


def test_extra_column_and_metadata_columns():
    columns = list(CONTRACTS["teams"].column_names) + ["cost_center", "_run_id", "_ingest_date"]
    report = validate_rows("teams", row("teams", cost_center="X1"), columns=columns)
    assert [(i.reason_code, i.field) for i in report.dataset_issues] == [("unexpected_column", "cost_center")]
    assert not report.failed and only(report).action is Action.ACCEPT


def test_source_type_drift_warns_and_values_are_still_parsed():
    contract = CONTRACTS["performance_reviews"]
    issues = check_dataset_schema(contract, contract.column_names, {"rating": pa.string(), "review_id": pa.int32()})
    assert [(i.reason_code, i.field, i.action) for i in issues] == [("source_type_drift", "rating", Action.WARN)]
    assert only(validate_rows("performance_reviews", row("performance_reviews", rating="4"))).values["rating"] == 4


@pytest.mark.parametrize("entity, values, reason", [
    ("employees", {"first_name": None}, "missing_required_field"),
    ("employees", {"last_name": "   "}, "missing_required_field"),
    ("employees", {"hire_date": "2026-02-30"}, "invalid_date"),
    ("employees", {"hire_date": "06/04/2024"}, "invalid_date"),
    ("employees", {"team_id": "abc"}, "type_conversion_failed"),
    ("competencies", {"category": "Soft Skills"}, "invalid_domain_value"),
    ("projects", {"outcome_status": "done"}, "invalid_domain_value"),
    ("projects", {"outcome_score": Decimal("56.875")}, "type_conversion_failed"),
    ("projects", {"outcome_score": "1000"}, "type_conversion_failed"),
    ("performance_reviews", {"rating": 6}, "out_of_range"),
    ("performance_reviews", {"rating": 0}, "out_of_range"),
    ("performance_reviews", {"rating": "four"}, "type_conversion_failed"),
    ("performance_reviews", {"review_period": "2024-H3"}, "invalid_format"),
    ("competency_assessments", {"proficiency_level": 7}, "out_of_range"),
    ("competency_assessments", {"required_level": None}, "missing_required_field"),
    ("talent_review", {"promotion_readiness": "Soon"}, "invalid_domain_value"),
    ("talent_review", {"review_cycle": "FY2025"}, "invalid_format"),
    ("teams", {"team_id": -3}, "out_of_range"),
])
def test_postgres_record_rules_quarantine(entity, values, reason):
    report = validate_rows(entity, row(entity, **values))
    outcome = only(report)
    assert outcome.action is Action.QUARANTINE
    assert reason in codes(outcome)
    (quarantined,) = report.quarantine_records()
    assert reason in quarantined["reason_codes"]
    assert quarantined["source_object"] == f"hr_source.{entity}" and quarantined["source_line"] is None
    assert json.loads(quarantined["raw_record"])  # the full source row is preserved


@pytest.mark.parametrize("entity, values, reason", [
    ("teams", {"department": "Legal"}, "unexpected_value"),
    ("employees", {"job_level": "VP"}, "unexpected_value"),
    ("employees", {"email": "not-an-email"}, "unexpected_format"),
    ("employees", {"hire_date": date(2027, 1, 1)}, "future_date"),
    ("projects", {"outcome_score": Decimal("120.00")}, "outside_expected_range"),
    ("performance_reviews", {"submitted_date": date(2024, 12, 31)}, "review_submitted_early"),
])
def test_postgres_record_rules_warn_and_keep(entity, values, reason):
    outcome = only(validate_rows(entity, row(entity, **values)))
    assert outcome.action is Action.WARN
    assert reason in outcome.warnings


@pytest.mark.parametrize("entity, values", [
    ("employees", {"team_id": 99}),
    ("projects", {"team_id": 99}),
    ("performance_reviews", {"employee_id": 5}),
    ("competency_assessments", {"competency_id": 40}),
    ("talent_review", {"employee_id": 5}),
])
def test_foreign_key_violations_are_quarantined(entity, values):
    outcome = only(validate_rows(entity, row(entity, **values)))
    assert outcome.action is Action.QUARANTINE
    assert codes(outcome) == ["unknown_reference"]


def test_missing_parent_keys_in_context_is_a_programming_error():
    with pytest.raises(ValueError, match="accepted keys of 'teams'"):
        validate_rows("employees", row("employees"), reference_keys={})


def test_duplicate_email_warns_on_every_copy():
    report = validate_rows("employees", row("employees"), row("employees", employee_id=462))
    assert [o.warnings for o in report.outcomes] == [["duplicate_unique_value"], ["duplicate_unique_value"]]


def test_conflicting_primary_key_quarantines_both_rows():
    report = validate_rows("teams", row("teams"), row("teams", name="Renamed"))
    assert [o.action for o in report.outcomes] == [Action.QUARANTINE, Action.QUARANTINE]


def test_review_natural_key_duplicate_keeps_latest_submission():
    older = row("performance_reviews", review_id=1, submitted_date=date(2025, 8, 1))
    newer = row("performance_reviews", review_id=2, submitted_date=date(2025, 9, 1))
    report = validate_rows("performance_reviews", older, newer)
    assert [o.action for o in report.outcomes] == [Action.DEDUPLICATE, Action.ACCEPT]
    assert "superseded by ('review_id',)=2" in report.outcomes[0].issues[0].detail


def test_assessment_history_is_not_deduplicated_across_dates():
    report = validate_rows("competency_assessments",
                           row("competency_assessments", assessment_id=1, assessed_date=date(2025, 1, 1)),
                           row("competency_assessments", assessment_id=2, assessed_date=date(2026, 1, 1)))
    assert [o.action for o in report.outcomes] == [Action.ACCEPT, Action.ACCEPT]
    same_day = validate_rows("competency_assessments", row("competency_assessments", assessment_id=1),
                             row("competency_assessments", assessment_id=2))
    assert [o.action for o in same_day.outcomes] == [Action.DEDUPLICATE, Action.ACCEPT]


def test_pre_hire_review_is_flagged_and_mid_period_hire_is_not():
    pre = only(validate_rows("performance_reviews", row("performance_reviews", employee_id=462)))
    assert pre.action is Action.WARN and pre.warnings == ["pre_hire_review"]
    assert pre.derived == {"period_start": date(2024, 7, 1), "period_end": date(2024, 12, 31), "is_pre_hire": True}
    mid = only(validate_rows("performance_reviews",
                             row("performance_reviews", employee_id=462, review_period="2025-H1")))
    assert mid.action is Action.ACCEPT and mid.derived["is_pre_hire"] is False


@pytest.mark.parametrize("required, proficiency, gap, critical, action", [
    (4, 2, 2, True, Action.ACCEPT), (2, 1, 1, True, Action.ACCEPT), (3, 2, 1, False, Action.ACCEPT),
    (2, 3, -1, False, Action.ACCEPT),
])
def test_competency_gap_is_derived_and_critical_gap_is_informational(required, proficiency, gap, critical, action):
    outcome = only(validate_rows("competency_assessments",
                                 row("competency_assessments", required_level=required, proficiency_level=proficiency)))
    assert outcome.action is action
    assert (outcome.derived["gap"], outcome.derived["is_critical_gap"]) == (gap, critical)
    assert ("critical_gap" in codes(outcome)) is critical


def test_pre_hire_assessment_and_talent_review_are_flagged():
    assessment = only(validate_rows("competency_assessments", row("competency_assessments", employee_id=462,
                                                                  assessed_date=date(2025, 2, 1))))
    assert assessment.warnings == ["pre_hire_assessment"] and assessment.derived["is_pre_hire"] is True
    talent = only(validate_rows("talent_review", row("talent_review", employee_id=462, review_cycle="2024-H2")))
    assert talent.warnings == ["pre_hire_talent_review"] and talent.derived["is_pre_hire"] is True


def test_quarantine_rate_above_threshold_fails_the_entity():
    rows = [row("teams", team_id=i) for i in range(1, 20)] + [row("teams", team_id=99, name=None)]
    assert not validate_rows("teams", *rows).failed  # 1/20 = 5% is allowed
    report = validate_rows("teams", *rows, row("teams", team_id=100, name=None))
    assert [f.reason_code for f in report.failures] == ["quarantine_rate_exceeded"]
    assert report.failures[0].rule.action is Action.FAIL


def test_no_valid_records_fails_the_entity():
    report = validate_rows("teams", row("teams", name=None))
    assert {f.reason_code for f in report.failures} == {"no_valid_records", "quarantine_rate_exceeded"}
    assert validate_rows("teams").failures[0].reason_code == "no_valid_records"


def test_hire_dates_and_accepted_keys_come_from_accepted_rows_only():
    report = validate_rows("employees", row("employees"), row("employees", employee_id=462, email="b@x.io",
                                                              hire_date="bad"))
    assert hire_dates_from(report) == {989: date(2020, 1, 1)}
    assert report.accepted_keys() == {989}


# ===================================================================================
# Learning history (A13): completions count, inconsistent histories are flagged
# ===================================================================================

def history(*events):
    from datetime import datetime as dt

    from talent_etl.validation import HistoryEvent, learning_history_warnings
    return learning_history_warnings(
        HistoryEvent(eid, emp, course, etype, dt.fromisoformat(day)) for eid, emp, course, etype, day in events)


def test_completion_with_earlier_enrollment_is_consistent():
    assert history(("E1", 1, "SQL", "enrollment", "2024-01-01T09:00"),
                   ("E2", 1, "SQL", "course_completed", "2024-02-01T09:00")) == {}


def test_completion_without_enrollment_is_flagged_but_only_flagged():
    assert history(("E2", 1, "SQL", "course_completed", "2024-02-01T09:00")) == {
        "E2": ("completion_without_enrollment",)}


def test_enrollment_after_completion_still_flags_but_same_timestamp_does_not():
    assert history(("E1", 1, "SQL", "enrollment", "2024-03-01T09:00"),
                   ("E2", 1, "SQL", "course_completed", "2024-02-01T09:00")) == {
        "E2": ("completion_without_enrollment",)}
    assert history(("E1", 1, "SQL", "enrollment", "2024-02-01T09:00"),
                   ("E2", 1, "SQL", "course_completed", "2024-02-01T09:00")) == {}


def test_certification_needs_an_earlier_completion():
    assert history(("C1", 1, "SQL", "certification_earned", "2024-02-01T09:00")) == {
        "C1": ("certification_without_completion",)}
    assert history(("E1", 1, "SQL", "enrollment", "2024-01-01T09:00"),
                   ("E2", 1, "SQL", "course_completed", "2024-01-15T09:00"),
                   ("C1", 1, "SQL", "certification_earned", "2024-02-01T09:00")) == {}


def test_history_is_per_employee_and_course():
    flagged = history(("E1", 1, "SQL", "enrollment", "2024-01-01T09:00"),
                      ("E2", 2, "SQL", "course_completed", "2024-02-01T09:00"),      # other employee
                      ("E3", 1, "Python", "course_completed", "2024-02-01T09:00"))   # other course
    assert flagged == {"E2": ("completion_without_enrollment",), "E3": ("completion_without_enrollment",)}


def test_other_event_types_are_never_flagged():
    assert history(("Q1", 1, "SQL", "quiz_passed", "2024-02-01T09:00"),
                   ("M1", 1, "SQL", "module_completed", "2024-02-01T09:00")) == {}



def test_history_metrics_count_rate_and_exact_random_baseline():
    from datetime import datetime as dt
    from itertools import permutations

    from talent_etl.validation import HistoryEvent, learning_history_metrics

    raw = [("E1", 1, "SQL", "enrollment", "2024-01-01"), ("E2", 1, "SQL", "course_completed", "2024-02-01"),
           ("E3", 2, "SQL", "course_completed", "2024-02-01"), ("E4", 2, "SQL", "module_started", "2024-01-01"),
           ("E5", 1, "Go", "enrollment", "2024-03-01"), ("E6", 1, "Go", "certification_earned", "2024-01-01")]
    events = [HistoryEvent(i, e, c, t, dt.fromisoformat(d)) for i, e, c, t, d in raw]
    metrics = learning_history_metrics(events)
    completion = metrics["completion_without_enrollment"]
    assert (completion["events"], completion["flagged"], completion["rate"]) == (2, 1, 0.5)
    assert metrics["certification_without_completion"]["flagged"] == 1

    # Brute force: average the flagged rate over every distinct ordering of the same type labels.
    def flagged_rate(types, step, prerequisite):
        shuffled = [HistoryEvent(e.event_id, e.employee_id, e.course_name, t, e.event_date) for e, t in zip(events, types)]
        m = learning_history_metrics(shuffled)
        code = "completion_without_enrollment" if step == "course_completed" else "certification_without_completion"
        return m[code]["rate"]
    labelings = set(permutations([e.event_type for e in events]))
    for code, step, prerequisite in (("completion_without_enrollment", "course_completed", "enrollment"),
                                     ("certification_without_completion", "certification_earned", "course_completed")):
        expected = sum(flagged_rate(types, step, prerequisite) for types in labelings) / len(labelings)
        assert metrics[code]["random_baseline_rate"] == pytest.approx(expected, abs=1e-6)


def test_history_metrics_with_no_step_events_have_no_rate():
    from datetime import datetime as dt

    from talent_etl.validation import HistoryEvent, learning_history_metrics
    metrics = learning_history_metrics([HistoryEvent("E1", 1, "SQL", "enrollment", dt(2024, 1, 1))])
    assert metrics["completion_without_enrollment"] | {} == {
        **metrics["completion_without_enrollment"], "events": 0, "flagged": 0, "rate": None,
        "random_baseline_rate": None, "random_baseline_approx_95": None}
    assert learning_history_metrics([])["completion_without_enrollment"]["events"] == 0
