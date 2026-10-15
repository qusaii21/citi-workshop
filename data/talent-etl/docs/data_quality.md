# Data quality specification

This document specifies every validation rule, which action it takes, and how each known defect in the dataset is handled.

The rules are defined in [`talent_etl/dq_rules.py`](../talent_etl/dq_rules.py) and evaluated by [`talent_etl/validation.py`](../talent_etl/validation.py). The matrix below is rendered from that code (`render_matrix_markdown()`). `tests/unit/test_dq_rules.py` fails if the matrix and the code ever differ, and it also fails if a rule exists in the registry but the engine never emits it.

## Actions

| Action | Effect on the record | Where it is recorded |
|---|---|---|
| **accept** | Kept unchanged. Informational only; it may set a flag column such as `is_critical_gap`. | Validation report counts |
| **warn** | Kept in Silver. The reason code is added to `_dq_warnings`. | `_dq_warnings`, report counts, logs |
| **deduplicate** | This copy is dropped and a surviving copy is kept. | Report counts (`deduplicated`), logs |
| **quarantine** | Excluded from Silver and written to the run's `_quarantine/<entity>/` dataset, with its source object, line, key, raw content, rule ids and reasons. | Quarantine dataset, report counts |
| **fail** | **Nothing** is written or published this run (all-or-nothing, assumption A17): the pointer still names the previous run, child entities are skipped, and the job exits with code 1. | Logs (ERROR), Silver manifest (`status: FAILED`) |

### Precedence

When several rules fire on one record, the most severe action wins: **quarantine > deduplicate > warn > accept**. The record still keeps every reason code that fired.

### Reconciliation

For each entity, the counts must reconcile:

`read = accepted + quarantined + deduplicated`

`read` covers every JSONL line (including malformed ones) or every PostgreSQL row, so no record can disappear without being counted.

### Schema violations versus business anomalies

- A **schema** or **integrity** violation means the record cannot be trusted as data: it is malformed, mistyped, outside an enforced domain, unkeyed or orphaned. These records are quarantined.
- A **business** anomaly is well-formed data that is implausible for the business, such as a review dated before the hire date. These records are **kept in Silver and flagged**. Gold then decides how to treat them; see [business_definitions.md](business_definitions.md).

Pre-hire records are never classified as malformed.

## Validation matrix

| Rule | Reason code | Entity | Field(s) | Category | Severity | Action | Validation logic |
|---|---|---|---|---|---|---|---|
| SCH-001 | `missing_column` | postgres entities | any contract column | schema | critical | **fail** | A contract column is absent from the Bronze Parquet schema. No record of the entity is validated. (JSONL keys are checked per record by REQ-001.) |
| SCH-002 | `unexpected_column` | * | any non-contract column | schema | warning | **warn** | A column/key not in the contract (and not a known alias) is present. It is dropped from Silver; Bronze keeps it. |
| SCH-003 | `source_type_drift` | postgres entities | any column | schema | warning | **warn** | The Bronze Arrow type differs from the contract's source type. Values are still parsed with the contract parser; failures are quarantined by TYP rules. |
| SCH-004 | `field_alias_applied` | learning_activity | text <- notes | schema | warning | **warn** | A record uses the drifted key `notes` instead of `text`; its value is mapped to `text`. |
| JSN-001 | `malformed_json` | learning_activity | whole line | schema | error | **quarantine** | json.loads fails (e.g. truncated line). The raw line, file key and line number are quarantined. |
| JSN-002 | `not_a_json_object` | learning_activity | whole line | schema | error | **quarantine** | The line is valid JSON but not an object (array, string, number). |
| JSN-003 | `invalid_encoding` | learning_activity | whole line | schema | error | **quarantine** | The line is not valid UTF-8; quarantined with undecodable bytes replaced (U+FFFD). |
| JSN-004 | `empty_file` | learning_activity | whole file | schema | warning | **warn** | The source file has zero bytes. Logged and counted; not fatal; produces no records. |
| JSN-005 | `blank_line` | learning_activity | whole line | schema | warning | **warn** | A whitespace-only line inside a file is skipped and counted. |
| REQ-001 | `missing_required_field` | * | required columns | schema | error | **quarantine** | A required field is absent, null, or blank after trimming. |
| TYP-001 | `type_conversion_failed` | * | int/float/decimal/string columns | schema | error | **quarantine** | The value cannot be converted to the contract type without loss (e.g. 'abc' as integer, 56.875 as NUMERIC(5,2), a number where a string is expected). |
| TYP-002 | `invalid_date` | * | date columns | schema | error | **quarantine** | Not a strict YYYY-MM-DD date or an impossible calendar date. |
| TYP-003 | `invalid_timestamp` | learning_activity | event_date | schema | error | **quarantine** | Not an ISO 8601 date-time (e.g. 'yesterday', 'N/A', '', '2026-13-45'). Never inferred from the S3 partition path. |
| DOM-001 | `invalid_domain_value` | * | enumerated columns | schema | error | **quarantine** | Value outside an enforced set (source CHECK constraint or event_type list). |
| DOM-002 | `out_of_range` | * | bounded numeric columns | schema | error | **quarantine** | Numeric value outside an enforced range (e.g. rating 1-5, ids >= 1, score 0-100). |
| DOM-003 | `invalid_format` | * | patterned columns | schema | error | **quarantine** | Value does not match an enforced pattern (review_period / review_cycle YYYY-H1|H2). |
| DOM-004 | `unexpected_value` | * | observed-domain columns | business | warning | **warn** | Value outside the observed set for a column the source does not constrain (department, job_level). |
| DOM-005 | `unexpected_format` | * | email, event_id | business | warning | **warn** | Value does not match the expected (unenforced) pattern. |
| DOM-006 | `outside_expected_range` | projects | outcome_score | business | warning | **warn** | Value inside the type range but outside the expected business range (0-100). |
| DAT-001 | `future_date` | * | hire_date, submitted_date, assessed_date, event_date | business | warning | **warn** | Date is later than the run's ingest date. |
| KEY-001 | `duplicate_record` | * | primary key | integrity | warning | **deduplicate** | Same primary key and identical content as an earlier record (source order: file, line). The first copy is kept; later copies are dropped and counted. |
| KEY-002 | `conflicting_duplicate` | * | primary key | integrity | error | **quarantine** | Same primary key but different content. No copy can be trusted, so all copies are quarantined. |
| KEY-003 | `duplicate_natural_key` | performance_reviews, competency_assessments | natural key | integrity | warning | **deduplicate** | Different primary keys share a natural key. The survivor is chosen by the contract's dedup order (latest date, then highest id); the others are dropped. |
| KEY-004 | `duplicate_unique_value` | employees | email | integrity | warning | **warn** | A value expected to be unique (not enforced by the source) appears more than once. |
| REF-001 | `unknown_reference` | * | foreign keys | integrity | error | **quarantine** | Foreign key value not found among the parent entity's accepted keys (e.g. unknown employee, team or competency). |
| JSN-006 | `employee_id_string_cast` | learning_activity | employee_id | schema | info | **accept** | employee_id arrives as a JSON string of digits ('989'); it is cast to int32 to join PostgreSQL ids. Non-digit strings fail TYP-001. |
| BUS-001 | `pre_hire_review` | performance_reviews | review_period, employees.hire_date | business | warning | **warn** | Review period ends before the employee's hire date. Kept in Silver with is_pre_hire=true; excluded from rating metrics and trends. |
| BUS-002 | `pre_hire_learning_event` | learning_activity | event_date, employees.hire_date | business | warning | **warn** | Event date (calendar day) is before the hire date. Kept with is_pre_hire=true; excluded from per-employee learning metrics by default (assumption A4). |
| BUS-003 | `pre_hire_assessment` | competency_assessments | assessed_date, employees.hire_date | business | warning | **warn** | Assessment dated before the hire date. Kept with is_pre_hire=true; still used for skill metrics (assumption A5). |
| BUS-004 | `pre_hire_talent_review` | talent_review | review_cycle, employees.hire_date | business | warning | **warn** | Talent review cycle ends before the hire date. Kept with is_pre_hire=true; flagged in promotion and attrition outputs (assumption A6). |
| BUS-005 | `review_submitted_early` | performance_reviews | submitted_date, review_period | business | warning | **warn** | submitted_date is on or before the last day of the review period. |
| BUS-006 | `unexpected_score` | learning_activity | score, event_type | business | warning | **warn** | A score is present on a non-quiz event. The value is kept. |
| BUS-007 | `missing_quiz_score` | learning_activity | score, event_type | business | warning | **warn** | A quiz event has no score. |
| BUS-008 | `partition_date_mismatch` | learning_activity | event_date, S3 partition | business | warning | **warn** | event_date's day differs from the file's year=/month=/day= partition. event_date wins. |
| BUS-010 | `completion_without_enrollment` | learning_activity | event_type, event_date (per employee+course) | business | info | **accept** | Dataset-level metric, not attached to rows: share of course_completed events with no enrollment for the same employee and course at or before them, reported in the Silver manifest next to the rate expected if event types were in random order. Completions always count (A13). Downgraded from warn: in the sample data the rate (97.6%) equals random ordering (97.5%), so it carries no row-level signal. |
| BUS-011 | `certification_without_completion` | learning_activity | event_type, event_date (per employee+course) | business | info | **accept** | Dataset-level metric, not attached to rows: share of certification_earned events with no course_completed for the same employee and course at or before them, with the random-order baseline. Downgraded from warn for the same reason as BUS-010 (98.1% observed, 98.0% random). |
| BUS-009 | `critical_gap` | competency_assessments | required_level, proficiency_level | business | info | **accept** | gap = required_level - proficiency_level, and gap / required_level >= 1/2 (missing at least half of the required level; approved A1). Valid data: sets is_critical_gap=true for Gold. |
| VOL-001 | `no_valid_records` | * | dataset | volume | critical | **fail** | Zero records survive validation for an entity. |
| VOL-002 | `quarantine_rate_exceeded` | * | dataset | volume | critical | **fail** | More than MAX_QUARANTINE_RATE (5%) of an entity's records are quarantined: the source is likely broken, so Silver is not overwritten. |

## Known dataset defects and how they are handled

The counts below come from running the engine on the real sample data (the Bronze output of 2026-10-14). They were cross-checked with an independent pandas profile, and `tests/integration/test_contracts_on_bronze.py` asserts them.

| Known issue | Occurrences | Rule(s) | Action | Detail |
|---|---|---|---|---|
| Malformed or truncated JSONL lines | 13 lines (mid-file) | JSN-001 | quarantine | Raw line, S3 key and line number are kept. `record_key` holds the `event_id` extracted from the text (all 13 are recoverable). |
| Empty learning-activity file | 1 (`year=2025/month=09/day=03`) | JSN-004 | warn | Not fatal; produces no records. |
| Duplicate events by `event_id` | 152 extra copies (152 pairs, both copies in the same file, identical content) | KEY-001 | deduplicate | The first copy in file and line order is kept. Duplicates are removed before field checks, so the one duplicated *invalid* line (`EV0000010344`, blank `event_date`) is quarantined once, not twice. |
| Null `employee_id` | 76 lines (none duplicated) | REQ-001 | quarantine | Quarantined together with the full raw line. |
| Invalid or unparseable `event_date` | 54 lines: `yesterday` ×15, `N/A` ×13, `2026-13-45` ×11, `""` ×15 (one blank line is an exact duplicate, so 53 are quarantined) | TYP-003 (39), REQ-001 (14 blank) | quarantine | **Never inferred from the S3 partition path.** A blank value counts as missing (REQ-001); a non-blank unparseable value counts as invalid (TYP-003). |
| `notes` instead of `text` | 39 lines (38 accepted; 1 was a dropped duplicate copy) | SCH-004 | warn | Value mapped to `text`; `_dq_warnings` contains `field_alias_applied`. |
| String employee ids in JSONL | 29,779 lines | JSN-006 | accept | Cast to int32 so they join to PostgreSQL. Non-digit strings would fail TYP-001. |
| Foreign-key violations / unknown employees | 0 in this data | REF-001 | quarantine | Checked against the *accepted* parent keys: teams, then competencies, then employees, then the children. |
| Reviews for periods ending before the hire date | 666 reviews (396 employees: 270 with both reviews, 126 with one) | BUS-001 | warn | Kept with `is_pre_hire = true`; excluded from rating metrics. |
| Learning events before the hire date | 5,161 accepted events (17.5%) | BUS-002 | warn | Kept with `is_pre_hire = true`; excluded from per-employee learning metrics by default (assumption A4). |
| Assessments before the hire date | 234 | BUS-003 | warn | Kept and flagged (assumption A5). |
| Talent reviews for a cycle ending before the hire date | 270 (all employees hired after 2025-06-30) | BUS-004 | warn | Kept and flagged (assumption A6). |
| Competency values and gaps | 4,000 valid; gap −1: 1,480, 0: 1,527, +1: 668, +2: 325 | DOM-002, BUS-009 | accept | Levels must be 1–5. `gap` and `is_critical_gap` are derived. 682 assessments have a critical gap (missing at least half the required level): 325 with gap 2 at required 3 or 4, plus 357 with gap 1 at required 2. |
| Missing required fields / invalid domains / type failures | 0 in the PostgreSQL tables | REQ-001, DOM-*, TYP-* | quarantine | The rules run on every table regardless, so a future bad extract is caught. |

### Expected outcome per entity (sample data, ingest date 2026-10-14)

| Entity | Read | Accepted | of which with warnings | Quarantined | Deduplicated | Fail? |
|---|---|---|---|---|---|---|
| teams | 100 | 100 | 0 | 0 | 0 | no |
| competencies | 40 | 40 | 0 | 0 | 0 | no |
| employees | 2,000 | 2,000 | 0 | 0 | 0 | no |
| projects | 33 | 33 | 0 | 0 | 0 | no |
| performance_reviews | 4,000 | 4,000 | 666 | 0 | 0 | no |
| competency_assessments | 4,000 | 4,000 | 234 | 0 | 0 | no |
| talent_review | 2,000 | 2,000 | 270 | 0 | 0 | no |
| learning_activity | 29,868 | 29,574 | 5,185 (BUS-010/011 are dataset metrics, not row warnings: 3,406 of 3,490 completions and 1,731 of 1,764 certifications lack the earlier step, equal to the random-order baseline) | 142 (13 malformed + 76 null employee + 14 blank date + 39 bad date) | 152 | no (0.48% quarantined; threshold 5%) |

## Quarantine record format

Rejected records are never discarded silently. Each one becomes a row in its entity's quarantine dataset. The schema is listed in [schema_contracts.md](schema_contracts.md#quarantine-record); it includes `source_object`, `source_line`, `record_key`, the exact `raw_record`, `rule_ids`, `reason_codes` and `reason_detail`. Quarantine data is written into each run's folder together with the Silver datasets and published with them through the same pointer, so a rerun never duplicates quarantine rows.
