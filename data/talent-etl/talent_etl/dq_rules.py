"""
Data quality rule registry: the single source of truth for every validation rule.

Each rule has a stable id, the reason code written to quarantine records and
logs, a category, a severity and the action taken. docs/data_quality.md renders
this registry (see ``render_matrix_markdown``); a unit test keeps them in sync.

Categories:
    schema     the shape or type of the data is wrong (record or dataset unusable as-is)
    integrity  keys: duplicates and references to unknown parents
    business   well-formed data that is implausible for the business; kept and flagged
    volume     dataset-level thresholds that stop the pipeline
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Action(StrEnum):
    """What the pipeline does with a record (or the run) when a rule fires."""

    ACCEPT = "accept"            # keep the record unchanged; informational
    WARN = "warn"                # keep the record, log it and record the reason in _dq_warnings
    DEDUPLICATE = "deduplicate"  # drop this copy; a surviving copy is kept
    QUARANTINE = "quarantine"    # exclude from Silver and write it to the quarantine dataset
    FAIL = "fail"                # stop the pipeline for this entity


class Severity(StrEnum):
    """How serious a rule violation is."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Category(StrEnum):
    """What kind of problem a rule detects."""

    SCHEMA = "schema"
    INTEGRITY = "integrity"
    BUSINESS = "business"
    VOLUME = "volume"


# Record-level precedence when several rules fire: the most severe action wins.
ACTION_PRECEDENCE = (Action.QUARANTINE, Action.DEDUPLICATE, Action.WARN, Action.ACCEPT)


@dataclass(frozen=True)
class Rule:
    """One data quality rule."""

    rule_id: str
    reason_code: str
    entity: str          # an entity name, or "*" for every entity
    fields: str          # affected fields, free text
    category: Category
    severity: Severity
    action: Action
    logic: str           # how the rule is evaluated


RULES: tuple[Rule, ...] = (
    # ---- schema: dataset level -------------------------------------------------------
    Rule("SCH-001", "missing_column", "postgres entities", "any contract column", Category.SCHEMA,
         Severity.CRITICAL, Action.FAIL,
         "A contract column is absent from the Bronze Parquet schema. No record of the entity is validated. "
         "(JSONL keys are checked per record by REQ-001.)"),
    Rule("SCH-002", "unexpected_column", "*", "any non-contract column", Category.SCHEMA, Severity.WARNING,
         Action.WARN, "A column/key not in the contract (and not a known alias) is present. It is dropped from "
                      "Silver; Bronze keeps it."),
    Rule("SCH-003", "source_type_drift", "postgres entities", "any column", Category.SCHEMA, Severity.WARNING,
         Action.WARN, "The Bronze Arrow type differs from the contract's source type. Values are still parsed "
                      "with the contract parser; failures are quarantined by TYP rules."),
    Rule("SCH-004", "field_alias_applied", "learning_activity", "text <- notes", Category.SCHEMA, Severity.WARNING,
         Action.WARN, "A record uses the drifted key `notes` instead of `text`; its value is mapped to `text`."),
    # ---- schema: JSONL parsing ---------------------------------------------------------
    Rule("JSN-001", "malformed_json", "learning_activity", "whole line", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "json.loads fails (e.g. truncated line). The raw line, file key and line number are "
                            "quarantined."),
    Rule("JSN-002", "not_a_json_object", "learning_activity", "whole line", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "The line is valid JSON but not an object (array, string, number)."),
    Rule("JSN-003", "invalid_encoding", "learning_activity", "whole line", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "The line is not valid UTF-8; quarantined with undecodable bytes replaced (U+FFFD)."),
    Rule("JSN-004", "empty_file", "learning_activity", "whole file", Category.SCHEMA, Severity.WARNING,
         Action.WARN, "The source file has zero bytes. Logged and counted; not fatal; produces no records."),
    Rule("JSN-005", "blank_line", "learning_activity", "whole line", Category.SCHEMA, Severity.WARNING,
         Action.WARN, "A whitespace-only line inside a file is skipped and counted."),
    # ---- schema: record level ----------------------------------------------------------
    Rule("REQ-001", "missing_required_field", "*", "required columns", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "A required field is absent, null, or blank after trimming."),
    Rule("TYP-001", "type_conversion_failed", "*", "int/float/decimal/string columns", Category.SCHEMA,
         Severity.ERROR, Action.QUARANTINE,
         "The value cannot be converted to the contract type without loss (e.g. 'abc' as integer, "
         "56.875 as NUMERIC(5,2), a number where a string is expected)."),
    Rule("TYP-002", "invalid_date", "*", "date columns", Category.SCHEMA, Severity.ERROR, Action.QUARANTINE,
         "Not a strict YYYY-MM-DD date or an impossible calendar date."),
    Rule("TYP-003", "invalid_timestamp", "learning_activity", "event_date", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "Not an ISO 8601 date-time (e.g. 'yesterday', 'N/A', '', '2026-13-45'). Never "
                            "inferred from the S3 partition path."),
    Rule("DOM-001", "invalid_domain_value", "*", "enumerated columns", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "Value outside an enforced set (source CHECK constraint or event_type list)."),
    Rule("DOM-002", "out_of_range", "*", "bounded numeric columns", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "Numeric value outside an enforced range (e.g. rating 1-5, ids >= 1, score 0-100)."),
    Rule("DOM-003", "invalid_format", "*", "patterned columns", Category.SCHEMA, Severity.ERROR,
         Action.QUARANTINE, "Value does not match an enforced pattern (review_period / review_cycle YYYY-H1|H2)."),
    Rule("DOM-004", "unexpected_value", "*", "observed-domain columns", Category.BUSINESS, Severity.WARNING,
         Action.WARN, "Value outside the observed set for a column the source does not constrain "
                      "(department, job_level)."),
    Rule("DOM-005", "unexpected_format", "*", "email, event_id", Category.BUSINESS, Severity.WARNING,
         Action.WARN, "Value does not match the expected (unenforced) pattern."),
    Rule("DOM-006", "outside_expected_range", "projects", "outcome_score", Category.BUSINESS, Severity.WARNING,
         Action.WARN, "Value inside the type range but outside the expected business range (0-100)."),
    Rule("DAT-001", "future_date", "*", "hire_date, submitted_date, assessed_date, event_date",
         Category.BUSINESS, Severity.WARNING, Action.WARN, "Date is later than the run's ingest date."),
    # ---- integrity -----------------------------------------------------------------------
    Rule("KEY-001", "duplicate_record", "*", "primary key", Category.INTEGRITY, Severity.WARNING,
         Action.DEDUPLICATE, "Same primary key and identical content as an earlier record (source order: file, "
                             "line). The first copy is kept; later copies are dropped and counted."),
    Rule("KEY-002", "conflicting_duplicate", "*", "primary key", Category.INTEGRITY, Severity.ERROR,
         Action.QUARANTINE, "Same primary key but different content. No copy can be trusted, so all copies "
                            "are quarantined."),
    Rule("KEY-003", "duplicate_natural_key", "performance_reviews, competency_assessments",
         "natural key", Category.INTEGRITY, Severity.WARNING, Action.DEDUPLICATE,
         "Different primary keys share a natural key. The survivor is chosen by the contract's dedup order "
         "(latest date, then highest id); the others are dropped."),
    Rule("KEY-004", "duplicate_unique_value", "employees", "email", Category.INTEGRITY, Severity.WARNING,
         Action.WARN, "A value expected to be unique (not enforced by the source) appears more than once."),
    Rule("REF-001", "unknown_reference", "*", "foreign keys", Category.INTEGRITY, Severity.ERROR,
         Action.QUARANTINE, "Foreign key value not found among the parent entity's accepted keys "
                            "(e.g. unknown employee, team or competency)."),
    Rule("JSN-006", "employee_id_string_cast", "learning_activity", "employee_id", Category.SCHEMA,
         Severity.INFO, Action.ACCEPT, "employee_id arrives as a JSON string of digits ('989'); it is cast to "
                                       "int32 to join PostgreSQL ids. Non-digit strings fail TYP-001."),
    # ---- business ------------------------------------------------------------------------
    Rule("BUS-001", "pre_hire_review", "performance_reviews", "review_period, employees.hire_date",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "Review period ends before the employee's hire date. Kept in Silver with is_pre_hire=true; excluded "
         "from rating metrics and trends."),
    Rule("BUS-002", "pre_hire_learning_event", "learning_activity", "event_date, employees.hire_date",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "Event date (calendar day) is before the hire date. Kept with is_pre_hire=true; excluded from "
         "per-employee learning metrics by default (assumption A4)."),
    Rule("BUS-003", "pre_hire_assessment", "competency_assessments", "assessed_date, employees.hire_date",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "Assessment dated before the hire date. Kept with is_pre_hire=true; still used for skill metrics "
         "(assumption A5)."),
    Rule("BUS-004", "pre_hire_talent_review", "talent_review", "review_cycle, employees.hire_date",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "Talent review cycle ends before the hire date. Kept with is_pre_hire=true; flagged in promotion "
         "and attrition outputs (assumption A6)."),
    Rule("BUS-005", "review_submitted_early", "performance_reviews", "submitted_date, review_period",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "submitted_date is on or before the last day of the review period."),
    Rule("BUS-006", "unexpected_score", "learning_activity", "score, event_type", Category.BUSINESS,
         Severity.WARNING, Action.WARN, "A score is present on a non-quiz event. The value is kept."),
    Rule("BUS-007", "missing_quiz_score", "learning_activity", "score, event_type", Category.BUSINESS,
         Severity.WARNING, Action.WARN, "A quiz event has no score."),
    Rule("BUS-008", "partition_date_mismatch", "learning_activity", "event_date, S3 partition",
         Category.BUSINESS, Severity.WARNING, Action.WARN,
         "event_date's day differs from the file's year=/month=/day= partition. event_date wins."),
    Rule("BUS-010", "completion_without_enrollment", "learning_activity", "event_type, event_date (per employee+course)",
         Category.BUSINESS, Severity.INFO, Action.ACCEPT,
         "Dataset-level metric, not attached to rows: share of course_completed events with no enrollment for the "
         "same employee and course at or before them, reported in the Silver manifest next to the rate expected "
         "if event types were in random order. Completions always count (A13). Downgraded from warn: in the "
         "sample data the rate (97.6%) equals random ordering (97.5%), so it carries no row-level signal."),
    Rule("BUS-011", "certification_without_completion", "learning_activity",
         "event_type, event_date (per employee+course)", Category.BUSINESS, Severity.INFO, Action.ACCEPT,
         "Dataset-level metric, not attached to rows: share of certification_earned events with no "
         "course_completed for the same employee and course at or before them, with the random-order baseline. "
         "Downgraded from warn for the same reason as BUS-010 (98.1% observed, 98.0% random)."),
    Rule("BUS-009", "critical_gap", "competency_assessments", "required_level, proficiency_level",
         Category.BUSINESS, Severity.INFO, Action.ACCEPT,
         "gap = required_level - proficiency_level, and gap / required_level >= 1/2 (missing at least half "
         "of the required level; approved A1). Valid data: sets is_critical_gap=true for Gold."),
    # ---- volume --------------------------------------------------------------------------
    Rule("VOL-001", "no_valid_records", "*", "dataset", Category.VOLUME, Severity.CRITICAL, Action.FAIL,
         "Zero records survive validation for an entity."),
    Rule("VOL-002", "quarantine_rate_exceeded", "*", "dataset", Category.VOLUME, Severity.CRITICAL,
         Action.FAIL, "More than MAX_QUARANTINE_RATE (5%) of an entity's records are quarantined: the source "
                      "is likely broken, so Silver is not overwritten."),
)

RULES_BY_REASON: dict[str, Rule] = {rule.reason_code: rule for rule in RULES}


def rule_for(reason_code: str) -> Rule:
    """
    Return the rule registered for a reason code.

    Raises:
        KeyError: If the reason code is not registered (a programming error).
    """
    return RULES_BY_REASON[reason_code]


def render_matrix_markdown() -> str:
    """Render the registry as the Markdown table embedded in docs/data_quality.md."""
    lines = [
        "| Rule | Reason code | Entity | Field(s) | Category | Severity | Action | Validation logic |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for rule in RULES:
        lines.append(
            f"| {rule.rule_id} | `{rule.reason_code}` | {rule.entity} | {rule.fields} | {rule.category} | "
            f"{rule.severity} | **{rule.action}** | {rule.logic} |"
        )
    return "\n".join(lines)
