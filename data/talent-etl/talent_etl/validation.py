"""
Contract validation engine.

Classifies every source record as accepted, accepted with warnings, deduplicated
or quarantined, and decides whether an entity fails the pipeline. Each decision
carries the rule id and reason code from ``talent_etl.dq_rules``. Nothing is
discarded silently: rejected records keep their source object, line number,
key and raw content for the quarantine dataset.

This module validates; it does not build or write Silver datasets.

Order of evaluation for one entity:
    1. dataset schema (PostgreSQL column set and types)      -> FAIL / WARN
    2. exact duplicates on the primary key (raw content)       -> DEDUPLICATE
    3. per-record fields: presence, type, domain, range        -> QUARANTINE / WARN
    4. conflicting duplicates on the primary key               -> QUARANTINE
    5. duplicates on the natural key                           -> DEDUPLICATE
    6. foreign keys against accepted parent keys               -> QUARANTINE
    7. soft-unique columns and business rules                  -> WARN / ACCEPT (flags)
    8. volume thresholds                                       -> FAIL
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pyarrow as pa

from talent_etl.contracts import ColumnContract, EntityContract, Kind
from talent_etl.definitions import (
    MAX_QUARANTINE_RATE,
    CERTIFICATION_EVENT,
    COURSE_COMPLETION_EVENT,
    ENROLLMENT_EVENT,
    QUIZ_EVENT_TYPES,
    competency_gap,
    is_critical_gap,
    is_pre_hire,
    review_period_bounds,
)
from talent_etl.dq_rules import ACTION_PRECEDENCE, Action, Rule, rule_for
from talent_etl.parsing import (
    ParseError,
    parse_date,
    parse_decimal,
    parse_float,
    parse_int,
    parse_string,
    parse_timestamp,
)

_EVENT_ID_HINT = re.compile(r'"event_id"\s*:\s*"([^"]+)"')
_DAY_PARTITION = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")


@dataclass(frozen=True)
class RawRecord:
    """A source record before validation, with where it came from."""

    values: dict[str, Any]
    source_object: str
    source_line: int | None = None
    raw_text: str | None = None  # the original JSONL line; PostgreSQL rows are serialised on demand


@dataclass(frozen=True)
class Issue:
    """One rule firing on a record, a file or a dataset."""

    reason_code: str
    detail: str
    field: str | None = None

    @property
    def rule(self) -> Rule:
        """The registered rule for this reason code."""
        return rule_for(self.reason_code)

    @property
    def action(self) -> Action:
        """The action the rule prescribes."""
        return self.rule.action


@dataclass
class RecordOutcome:
    """Validation result for one record."""

    record: RawRecord
    record_key: str | None
    issues: list[Issue] = field(default_factory=list)
    values: dict[str, Any] = field(default_factory=dict)
    derived: dict[str, Any] = field(default_factory=dict)

    @property
    def action(self) -> Action:
        """Final disposition: the most severe action among the issues (ACCEPT when none)."""
        actions = {issue.action for issue in self.issues}
        return next((action for action in ACTION_PRECEDENCE if action in actions), Action.ACCEPT)

    @property
    def is_live(self) -> bool:
        """True while the record is still headed for Silver."""
        return self.action in (Action.ACCEPT, Action.WARN)

    @property
    def warnings(self) -> list[str]:
        """Reason codes of WARN issues (what Silver stores in ``_dq_warnings``)."""
        return [issue.reason_code for issue in self.issues if issue.action is Action.WARN]

    def add(self, reason_code: str, detail: str, field_name: str | None = None) -> None:
        """Record an issue (validates the reason code eagerly)."""
        rule_for(reason_code)
        self.issues.append(Issue(reason_code, detail, field_name))


@dataclass
class ValidationContext:
    """Run-wide inputs needed by cross-entity rules."""

    ingest_date: date
    reference_keys: dict[str, set[Any]] = field(default_factory=dict)  # entity -> accepted primary keys
    hire_dates: dict[int, date] = field(default_factory=dict)          # employee_id -> hire_date
    max_quarantine_rate: float = MAX_QUARANTINE_RATE


@dataclass
class JsonlParseResult:
    """Records and line-level rejects parsed from one JSONL object."""

    source_object: str
    records: list[RawRecord] = field(default_factory=list)
    rejects: list[dict[str, Any]] = field(default_factory=list)
    file_issues: list[Issue] = field(default_factory=list)
    lines_read: int = 0


@dataclass
class EntityReport:
    """Validation result for one entity."""

    contract: EntityContract
    dataset_issues: list[Issue] = field(default_factory=list)
    outcomes: list[RecordOutcome] = field(default_factory=list)
    parse_rejects: list[dict[str, Any]] = field(default_factory=list)
    failures: list[Issue] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        """True when a FAIL rule fired: Silver must not be written for this entity."""
        return bool(self.failures)

    def by_action(self, action: Action) -> list[RecordOutcome]:
        """Outcomes whose final disposition is ``action``."""
        return [outcome for outcome in self.outcomes if outcome.action is action]

    @property
    def accepted(self) -> list[RecordOutcome]:
        """Outcomes headed for Silver (accepted, with or without warnings)."""
        return [outcome for outcome in self.outcomes if outcome.is_live]

    def counts(self) -> dict[str, int]:
        """Record counts for logging, lineage and reconciliation (read = accepted + quarantined + deduplicated)."""
        quarantined = len(self.by_action(Action.QUARANTINE)) + len(self.parse_rejects)
        return {
            "read": len(self.outcomes) + len(self.parse_rejects),
            "accepted": len(self.accepted),
            "accepted_with_warnings": len(self.by_action(Action.WARN)),
            "quarantined": quarantined,
            "deduplicated": len(self.by_action(Action.DEDUPLICATE)),
        }

    def reason_counts(self) -> Counter[str]:
        """How often each reason code fired (records, file/dataset issues and parse rejects)."""
        counts: Counter[str] = Counter(issue.reason_code for issue in self.dataset_issues)
        for outcome in self.outcomes:
            counts.update(issue.reason_code for issue in outcome.issues)
        for reject in self.parse_rejects:
            counts.update(reject["reason_codes"])
        return counts

    def accepted_keys(self) -> set[Any]:
        """Primary key values of accepted records (single-column keys only)."""
        (pk,) = self.contract.primary_key
        return {outcome.values[pk] for outcome in self.accepted}

    def quarantine_records(self) -> list[dict[str, Any]]:
        """Rows for the quarantine dataset (QUARANTINE_SCHEMA without the run metadata columns)."""
        rows = list(self.parse_rejects)
        for outcome in self.by_action(Action.QUARANTINE):
            causes = [issue for issue in outcome.issues if issue.action is Action.QUARANTINE]
            record = outcome.record
            rows.append(_quarantine_row(
                self.contract.name, record.source_object, record.source_line, outcome.record_key,
                record.raw_text if record.raw_text is not None else json.dumps(record.values, default=str),
                causes,
            ))
        return rows


# --------------------------------------------------------------------------------------
# JSONL parsing
# --------------------------------------------------------------------------------------

def parse_jsonl(body: bytes, source_object: str, entity: str = "learning_activity") -> JsonlParseResult:
    """
    Split a JSONL object into records, quarantining lines that are not JSON objects.

    An empty object produces a JSN-004 file issue (warning) and no records.
    """
    result = JsonlParseResult(source_object=source_object)
    if not body:
        result.file_issues.append(Issue("empty_file", f"{source_object} has 0 bytes"))
        return result
    lines = body.split(b"\n")
    if lines[-1] == b"":
        lines.pop()  # trailing newline terminates the last line; it is not an extra line
    for number, raw_bytes in enumerate(lines, start=1):
        result.lines_read += 1
        raw_bytes = raw_bytes.rstrip(b"\r")
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            text = raw_bytes.decode("utf-8", errors="replace")
            result.rejects.append(_line_reject(entity, source_object, number, text, "invalid_encoding", str(exc)))
            continue
        if not text.strip():
            result.file_issues.append(Issue("blank_line", f"{source_object}:{number} is blank"))
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            result.rejects.append(_line_reject(entity, source_object, number, text, "malformed_json", exc.msg))
            continue
        if not isinstance(value, dict):
            result.rejects.append(_line_reject(
                entity, source_object, number, text, "not_a_json_object", f"got {type(value).__name__}"))
            continue
        result.records.append(RawRecord(value, source_object, number, text))
    return result


def _line_reject(entity: str, source_object: str, line: int, text: str, reason: str, detail: str) -> dict:
    hint = _EVENT_ID_HINT.search(text)
    return _quarantine_row(entity, source_object, line, hint.group(1) if hint else None, text,
                           [Issue(reason, f"line {line}: {detail}")])


def _quarantine_row(entity: str, source_object: str, line: int | None, key: str | None,
                    raw: str, issues: Sequence[Issue]) -> dict[str, Any]:
    return {
        "entity": entity,
        "source_object": source_object,
        "source_line": line,
        "record_key": key,
        "raw_record": raw,
        "rule_ids": [issue.rule.rule_id for issue in issues],
        "reason_codes": [issue.reason_code for issue in issues],
        "reason_detail": "; ".join(issue.detail for issue in issues),
    }


# --------------------------------------------------------------------------------------
# Schema checks
# --------------------------------------------------------------------------------------

def check_dataset_schema(
    contract: EntityContract, columns: Sequence[str], types: dict[str, pa.DataType] | None = None
) -> list[Issue]:
    """
    Compare a dataset's columns with the contract.

    Bronze metadata columns (``_`` prefix) are ignored. Any contract column that is
    absent fails the entity (SCH-001); extra columns warn (SCH-002); a type that
    differs from the contract warns (SCH-003) and values are still parsed.
    """
    source_columns = [name for name in columns if not name.startswith("_")]
    issues = [
        Issue("missing_column", f"{contract.name}: column {name!r} is missing", name)
        for name in contract.column_names if name not in source_columns
    ]
    issues += [
        Issue("unexpected_column", f"{contract.name}: column {name!r} is not in the contract; dropped", name)
        for name in source_columns if name not in contract.column_names
    ]
    for name, arrow_type in (types or {}).items():
        if name in contract.column_names and arrow_type != contract.column(name).silver_type:
            issues.append(Issue(
                "source_type_drift",
                f"{contract.name}.{name}: source type {arrow_type}, contract {contract.column(name).silver_type}",
                name,
            ))
    return issues


# --------------------------------------------------------------------------------------
# Entity validation
# --------------------------------------------------------------------------------------

def validate_entity(
    contract: EntityContract,
    records: Iterable[RawRecord],
    context: ValidationContext,
    *,
    source_columns: Sequence[str] | None = None,
    source_types: dict[str, pa.DataType] | None = None,
    parse_rejects: Sequence[dict[str, Any]] = (),
    file_issues: Sequence[Issue] = (),
    check_volume: bool = True,
) -> EntityReport:
    """
    Validate all records of one entity against its contract.

    Args:
        contract: The entity contract.
        records: Source records in source order (file, line).
        context: Ingest date, accepted parent keys and hire dates.
        source_columns: Dataset column names (PostgreSQL). When omitted, unknown keys
            are checked per record (JSONL).
        source_types: Dataset Arrow types, to detect type drift.
        parse_rejects: Line-level quarantine rows from ``parse_jsonl``.
        file_issues: File-level issues from ``parse_jsonl`` (empty file, blank lines).
        check_volume: Apply VOL-001/002 to these records. Incremental Silver passes False
            because a batch is only part of the entity; it applies them to entity totals.

    Raises:
        ValueError: If a foreign key's parent keys are missing from ``context``.
    """
    report = EntityReport(contract, dataset_issues=list(file_issues), parse_rejects=list(parse_rejects))
    if source_columns is not None:
        report.dataset_issues += check_dataset_schema(contract, source_columns, source_types)
        report.failures += [i for i in report.dataset_issues if i.action is Action.FAIL]
        if report.failed:
            return report

    report.outcomes = [RecordOutcome(record, _record_key(contract, record.values)) for record in records]
    _mark_exact_duplicates(contract, report.outcomes)
    for outcome in report.outcomes:
        if outcome.is_live:
            _validate_fields(contract, outcome, context, check_extra_keys=source_columns is None)
    _mark_conflicting_duplicates(contract, report.outcomes)
    _dedupe_natural_key(contract, report.outcomes)
    _check_foreign_keys(contract, report.outcomes, context)
    _check_unique_warn(contract, report.outcomes)
    business_rule = _BUSINESS_RULES.get(contract.name)
    if business_rule:
        for outcome in report.outcomes:
            if outcome.is_live:
                business_rule(outcome, context)
    if check_volume:
        report.failures += volume_failures(contract.name, report.counts(), context.max_quarantine_rate)
    return report


def _record_key(contract: EntityContract, values: dict[str, Any]) -> str | None:
    parts = [values.get(name) for name in contract.primary_key]
    if any(part is None or (isinstance(part, str) and not part.strip()) for part in parts):
        return None
    return "|".join(str(part).strip() if isinstance(part, str) else str(part) for part in parts)


def _mark_exact_duplicates(contract: EntityContract, outcomes: list[RecordOutcome]) -> None:
    first_seen: dict[tuple[str, str], RecordOutcome] = {}
    for outcome in outcomes:
        if outcome.record_key is None:
            continue  # no key to compare; REQ-001 will quarantine it
        content = json.dumps(outcome.record.values, sort_keys=True, default=str)
        original = first_seen.setdefault((outcome.record_key, content), outcome)
        if original is not outcome:
            location = f"{original.record.source_object}" + (
                f":{original.record.source_line}" if original.record.source_line else "")
            outcome.add("duplicate_record", f"{contract.primary_key}={outcome.record_key} duplicates {location}")


_PARSERS: dict[Kind, Callable[[Any, ColumnContract], Any]] = {
    Kind.INT: lambda value, _col: parse_int(value),
    Kind.FLOAT: lambda value, _col: parse_float(value),
    Kind.DECIMAL: lambda value, col: parse_decimal(value, col.silver_type.precision, col.silver_type.scale),
    Kind.STRING: lambda value, _col: parse_string(value),
    Kind.DATE: lambda value, _col: parse_date(value),
    Kind.TIMESTAMP: lambda value, _col: parse_timestamp(value),
}


def _validate_fields(
    contract: EntityContract, outcome: RecordOutcome, context: ValidationContext, check_extra_keys: bool
) -> None:
    values = outcome.record.values
    if check_extra_keys:
        known = set(contract.column_names) | {alias for col in contract.columns for alias in col.aliases}
        for name in sorted(set(values) - known):
            outcome.add("unexpected_column", f"unexpected key {name!r}; dropped", name)
    for column in contract.columns:
        raw = values.get(column.name)
        for alias in column.aliases:
            if alias not in values:
                continue
            if column.name in values:
                outcome.add("unexpected_column", f"both {column.name!r} and alias {alias!r} present; "
                                                 f"{alias!r} dropped", alias)
            else:
                raw = values[alias]
                outcome.add("field_alias_applied", f"{alias!r} mapped to {column.name!r}", column.name)
        try:
            parsed = _PARSERS[column.kind](raw, column)
        except ParseError as exc:
            outcome.add(exc.reason_code, f"{column.name}: {exc.detail}", column.name)
            continue
        outcome.values[column.name] = parsed
        if parsed is None:
            if column.required:
                outcome.add("missing_required_field", f"{column.name} is null, blank or absent", column.name)
            continue
        if column.name == "employee_id" and contract.source_system == "s3_jsonl" and isinstance(raw, str):
            outcome.add("employee_id_string_cast", f"employee_id {raw!r} cast to {parsed}", column.name)
        _check_constraints(column, parsed, outcome, context)


def _check_constraints(column: ColumnContract, value: Any, outcome: RecordOutcome, context: ValidationContext) -> None:
    name = column.name
    if column.allowed_values and value not in column.allowed_values:
        outcome.add("invalid_domain_value", f"{name}={value!r} not in {list(column.allowed_values)}", name)
    if column.expected_values and value not in column.expected_values:
        outcome.add("unexpected_value", f"{name}={value!r} not in observed values", name)
    if (column.min_value is not None and value < column.min_value) or \
            (column.max_value is not None and value > column.max_value):
        outcome.add("out_of_range", f"{name}={value} outside [{column.min_value}, {column.max_value}]", name)
    if (column.expected_min is not None and value < column.expected_min) or \
            (column.expected_max is not None and value > column.expected_max):
        outcome.add("outside_expected_range",
                    f"{name}={value} outside expected [{column.expected_min}, {column.expected_max}]", name)
    if column.pattern and not re.fullmatch(column.pattern, value):
        outcome.add("invalid_format", f"{name}={value!r} does not match {column.pattern}", name)
    if column.expected_pattern and not re.fullmatch(column.expected_pattern, value):
        outcome.add("unexpected_format", f"{name}={value!r} does not match {column.expected_pattern}", name)
    if column.not_after_ingest_date:
        day = value.date() if isinstance(value, datetime) else value
        if day > context.ingest_date:
            outcome.add("future_date", f"{name}={value} is after ingest date {context.ingest_date}", name)


def _pk_value(contract: EntityContract, outcome: RecordOutcome) -> tuple:
    return tuple(outcome.values.get(name) for name in contract.primary_key)


def _mark_conflicting_duplicates(contract: EntityContract, outcomes: list[RecordOutcome]) -> None:
    groups: dict[tuple, list[RecordOutcome]] = defaultdict(list)
    for outcome in outcomes:
        if outcome.is_live:
            groups[_pk_value(contract, outcome)].append(outcome)
    for key, group in groups.items():
        if len(group) > 1:
            for outcome in group:
                outcome.add("conflicting_duplicate",
                            f"{len(group)} records share {contract.primary_key}={key} with different content")


def _dedupe_natural_key(contract: EntityContract, outcomes: list[RecordOutcome]) -> None:
    if not contract.natural_key:
        return
    groups: dict[tuple, list[RecordOutcome]] = defaultdict(list)
    for outcome in outcomes:
        if outcome.is_live:
            groups[tuple(outcome.values[name] for name in contract.natural_key)].append(outcome)
    for key, group in groups.items():
        if len(group) < 2:
            continue
        group.sort(key=lambda o: tuple(o.values[name] for name in contract.dedup_order), reverse=True)
        survivor = group[0]
        for outcome in group[1:]:
            outcome.add("duplicate_natural_key",
                        f"{contract.natural_key}={key} superseded by {contract.primary_key}={survivor.record_key}")


def _check_foreign_keys(contract: EntityContract, outcomes: list[RecordOutcome], context: ValidationContext) -> None:
    for fk in contract.foreign_keys:
        if fk.references not in context.reference_keys:
            raise ValueError(f"{contract.name}: accepted keys of {fk.references!r} are required to check {fk.column}")
        parents = context.reference_keys[fk.references]
        for outcome in outcomes:
            if outcome.is_live and outcome.values[fk.column] not in parents:
                outcome.add("unknown_reference",
                            f"{fk.column}={outcome.values[fk.column]} not found in {fk.references}", fk.column)


def _check_unique_warn(contract: EntityContract, outcomes: list[RecordOutcome]) -> None:
    for name in contract.unique_warn:
        counts = Counter(o.values[name] for o in outcomes if o.is_live)
        for outcome in outcomes:
            if outcome.is_live and counts[outcome.values[name]] > 1:
                outcome.add("duplicate_unique_value", f"{name}={outcome.values[name]!r} is not unique", name)


# --------------------------------------------------------------------------------------
# Business rules (records stay in Silver, flagged)
# --------------------------------------------------------------------------------------

def _flag_pre_hire(outcome: RecordOutcome, context: ValidationContext, end: date, reason: str, label: str) -> None:
    hire = context.hire_dates.get(outcome.values["employee_id"])
    outcome.derived["is_pre_hire"] = hire is not None and is_pre_hire(end, hire)
    if outcome.derived["is_pre_hire"]:
        outcome.add(reason, f"{label} {end} is before hire_date {hire}")


def _performance_review_rules(outcome: RecordOutcome, context: ValidationContext) -> None:
    start, end = review_period_bounds(outcome.values["review_period"])
    outcome.derived.update(period_start=start, period_end=end)
    _flag_pre_hire(outcome, context, end, "pre_hire_review", "period end")
    if outcome.values["submitted_date"] <= end:
        outcome.add("review_submitted_early",
                    f"submitted {outcome.values['submitted_date']} on/before period end {end}", "submitted_date")


def _competency_assessment_rules(outcome: RecordOutcome, context: ValidationContext) -> None:
    gap = competency_gap(outcome.values["required_level"], outcome.values["proficiency_level"])
    required = outcome.values["required_level"]
    outcome.derived.update(gap=gap, is_critical_gap=is_critical_gap(gap, required))
    if outcome.derived["is_critical_gap"]:
        outcome.add("critical_gap", f"gap {gap} is at least half of required level {required}", "proficiency_level")
    _flag_pre_hire(outcome, context, outcome.values["assessed_date"], "pre_hire_assessment", "assessed_date")


def _talent_review_rules(outcome: RecordOutcome, context: ValidationContext) -> None:
    start, end = review_period_bounds(outcome.values["review_cycle"])
    outcome.derived.update(cycle_start=start, cycle_end=end)
    _flag_pre_hire(outcome, context, end, "pre_hire_talent_review", "cycle end")


def _learning_activity_rules(outcome: RecordOutcome, context: ValidationContext) -> None:
    event_date: datetime = outcome.values["event_date"]
    outcome.derived.update(event_year=event_date.year, event_month=event_date.month)
    _flag_pre_hire(outcome, context, event_date.date(), "pre_hire_learning_event", "event date")
    is_quiz = outcome.values["event_type"] in QUIZ_EVENT_TYPES
    if outcome.values["score"] is not None and not is_quiz:
        outcome.add("unexpected_score", f"score on {outcome.values['event_type']} event", "score")
    if outcome.values["score"] is None and is_quiz:
        outcome.add("missing_quiz_score", f"{outcome.values['event_type']} without score", "score")
    partition = _DAY_PARTITION.search(outcome.record.source_object)
    if partition:
        partition_day = date(*(int(part) for part in partition.groups()))
        if partition_day != event_date.date():
            outcome.add("partition_date_mismatch",
                        f"event_date {event_date.date()} vs partition {partition_day}", "event_date")


_BUSINESS_RULES: dict[str, Callable[[RecordOutcome, ValidationContext], None]] = {
    "performance_reviews": _performance_review_rules,
    "competency_assessments": _competency_assessment_rules,
    "talent_review": _talent_review_rules,
    "learning_activity": _learning_activity_rules,
}


def volume_failures(entity: str, counts: dict[str, int], max_quarantine_rate: float) -> list[Issue]:
    """Apply VOL-001 (no valid records) and VOL-002 (quarantine rate) to an entity's total counts."""
    failures = []
    if counts["accepted"] == 0:
        failures.append(Issue("no_valid_records", f"{entity}: 0 of {counts['read']} records accepted"))
    if counts["read"] and counts["quarantined"] / counts["read"] > max_quarantine_rate:
        failures.append(Issue(
            "quarantine_rate_exceeded",
            f"{entity}: {counts['quarantined']}/{counts['read']} quarantined "
            f"({counts['quarantined'] / counts['read']:.1%} > {max_quarantine_rate:.0%})",
        ))
    return failures


# Learning history metrics (A13, BUS-010/011). They compare events of the same (employee,
# course) pair and are reported per dataset in the Silver manifest; they are never attached to rows.
HISTORY_RULES = (
    # (reason_code, rule step, prerequisite step)
    ("completion_without_enrollment", COURSE_COMPLETION_EVENT, ENROLLMENT_EVENT),
    ("certification_without_completion", CERTIFICATION_EVENT, COURSE_COMPLETION_EVENT),
)


@dataclass(frozen=True)
class HistoryEvent:
    """The fields of an accepted learning event that the history rules need."""

    event_id: str
    employee_id: int
    course_name: str
    event_type: str
    event_date: datetime


def learning_history_warnings(events: Iterable[HistoryEvent]) -> dict[str, tuple[str, ...]]:
    """
    Find completions and certifications whose earlier funnel step is missing (BUS-010, BUS-011).

    A ``course_completed`` event is flagged when the same employee has no ``enrollment`` for the
    same course at or before it; a ``certification_earned`` event when there is no
    ``course_completed`` at or before it. The completion still counts (A13). The result feeds the
    dataset-level metric (``learning_history_metrics``); it is not attached to rows. Pass every
    accepted event of each (employee, course) pair being evaluated.

    Returns:
        event_id -> reason codes, for flagged events only.
    """
    events = list(events)
    first: dict[tuple[int, str, str], datetime] = {}
    for event in events:
        key = (event.employee_id, event.course_name, event.event_type)
        if key not in first or event.event_date < first[key]:
            first[key] = event.event_date
    flagged: dict[str, tuple[str, ...]] = {}
    for event in events:
        codes = []
        pair = (event.employee_id, event.course_name)
        if event.event_type == COURSE_COMPLETION_EVENT:
            enrolled = first.get((*pair, ENROLLMENT_EVENT))
            if enrolled is None or enrolled > event.event_date:
                codes.append("completion_without_enrollment")
        if event.event_type == CERTIFICATION_EVENT:
            completed = first.get((*pair, COURSE_COMPLETION_EVENT))
            if completed is None or completed > event.event_date:
                codes.append("certification_without_completion")
        if codes:
            flagged[event.event_id] = tuple(codes)
    return flagged


def learning_history_metrics(events: Iterable[HistoryEvent]) -> dict[str, dict[str, Any]]:
    """
    Dataset-level BUS-010/011 metrics with the exact rate expected under random event order.

    For each rule: how many step events exist, how many lack the prerequisite at or before
    them, and the rate. ``random_baseline_rate`` is the expected rate if the same event types
    were shuffled at random over the same employees, courses and dates (destroying any
    sequence). It is computed exactly, not simulated: with N events of which K carry the
    prerequisite type, an event that has m other events in its pair dated at or before it
    lacks the prerequisite with probability C(N-1-m, K) / C(N-1, K); averaging over all N
    positions gives the expected rate. Cost is O(N log N), deterministic, no sampling noise.
    ``random_baseline_approx_95`` adds a normal approximation of binomial spread around it,
    only for context.

    Pass every accepted learning event (the metric needs complete pairs).
    """
    events = list(events)
    flagged = learning_history_warnings(events)
    dates_by_pair: dict[tuple[int, str], list[datetime]] = defaultdict(list)
    for event in events:
        dates_by_pair[(event.employee_id, event.course_name)].append(event.event_date)
    for dates in dates_by_pair.values():
        dates.sort()
    # m = other events in the same pair dated at or before this one (ties count, as in the rule).
    from bisect import bisect_right
    earlier = [bisect_right(dates_by_pair[(e.employee_id, e.course_name)], e.event_date) - 1 for e in events]
    total = len(events)
    type_counts = Counter(e.event_type for e in events)
    metrics: dict[str, dict[str, Any]] = {}
    for code, step, prerequisite in HISTORY_RULES:
        step_events = [e for e in events if e.event_type == step]
        n = len(step_events)
        hits = sum(code in flagged.get(e.event_id, ()) for e in step_events)
        baseline = _random_order_rate(earlier, total, type_counts[prerequisite]) if n else None
        spread = 1.96 * (baseline * (1 - baseline) / n) ** 0.5 if n else None
        metrics[code] = {
            "rule_id": rule_for(code).rule_id, "step": step, "prerequisite": prerequisite,
            "events": n, "flagged": hits, "rate": round(hits / n, 6) if n else None,
            "random_baseline_rate": round(baseline, 6) if n else None,
            "random_baseline_approx_95": [round(max(0.0, baseline - spread), 6), round(min(1.0, baseline + spread), 6)]
            if n else None,
        }
    return metrics


def _random_order_rate(earlier: list[int], total: int, prerequisites: int) -> float:
    """Expected share of step events lacking the prerequisite when event types are randomly permuted."""
    others = total - 1
    cache: dict[int, float] = {}
    def none_of(m: int) -> float:
        if m not in cache:
            probability = 1.0
            for i in range(m):
                if others - prerequisites - i <= 0:
                    probability = 0.0
                    break
                probability *= (others - prerequisites - i) / (others - i)
            cache[m] = probability
        return cache[m]
    return sum(none_of(m) for m in earlier) / total if total else 0.0


def hire_dates_from(report: EntityReport) -> dict[int, date]:
    """Map accepted employee ids to hire dates, for the pre-hire business rules."""
    return {outcome.values["employee_id"]: outcome.values["hire_date"] for outcome in report.accepted}
