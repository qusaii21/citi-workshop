# Schema contracts

Each contract below has two schemas:

- **source**: what the source system delivers, and so what Bronze stores unchanged.
- **Silver**: what the Silver layer will hold.

The contracts are defined in code in [`talent_etl/contracts.py`](../talent_etl/contracts.py). Every contract is derived from the provided `schema.sql` and the observed JSONL. `tests/unit/test_contracts.py` parses `schema.sql` out of the sample zip and checks that every column name, order, type, nullability, primary key, foreign key and CHECK constraint matches. It also checks that the contract covers every key that appears in the JSONL sample.

Rule ids such as `REQ-001` refer to [data_quality.md](data_quality.md). Business terms are defined in [business_definitions.md](business_definitions.md).

## Conventions (all entities)

### Allowed transformations from source to Silver

Only the following transformations are allowed:

1. **Type conversion.** A value is converted to the Silver type using the strict parsers in [`talent_etl/parsing.py`](../talent_etl/parsing.py), and only when no information is lost:
   - integers: an int, an integral float, or a string of digits
   - decimals: no rounding, and the value must fit `NUMERIC(p,s)`
   - dates: `YYYY-MM-DD`
   - timestamps: ISO 8601 with a time
2. **Trimming.** Surrounding whitespace is trimmed from strings. A blank string becomes null.
3. **Known drift.** One drifted field name is mapped: `notes` becomes `text` (learning activity only).
4. **Timestamp normalisation.** Offset-aware timestamps are converted to naive UTC.
5. **Derived columns.** The derived columns listed per entity are added.
6. **Lineage and data-quality columns.** These are appended (see below).
7. **Record removal.** Records are removed only by the deduplicate or quarantine rules. A removed record is always counted, and a quarantined record is always written to quarantine.

The following are never done:

- Values are not changed in any other way: no case folding, no fixing of typos, no default values.
- Missing data is not inferred. In particular, `event_date` is never taken from the S3 partition path.
- Columns are not renamed, apart from the `notes` alias.

### Enforcement levels

| Constraint | Enforcement | Rule |
|---|---|---|
| Required (`NOT NULL` / primary key) | Quarantine | REQ-001 |
| Type | Quarantine | TYP-001/002/003 |
| `CHECK (... IN ...)` in `schema.sql`, or the `event_type` list | Quarantine | DOM-001 |
| `CHECK (... BETWEEN ...)`, ids ≥ 1, score 0–100 | Quarantine | DOM-002 |
| Enforced pattern (`review_period`, `review_cycle`) | Quarantine | DOM-003 |
| Observed domain the source does **not** constrain (`department`, `job_level`) | Warn | DOM-004 |
| Expected pattern (`email`, `event_id`) | Warn | DOM-005 |
| Expected business range (`outcome_score` 0–100) | Warn | DOM-006 |
| Date after the run's ingest date | Warn | DAT-001 |

The principle: if the source system enforces a constraint, a violation means the data is broken. If we only *observed* a pattern, a violation is suspicious but may be legitimate, so the record is kept and flagged.

### Schema drift

| Situation | PostgreSQL entities (checked on the Bronze Parquet schema) | Learning activity (checked per JSON record) |
|---|---|---|
| A contract column is missing | **Fail** the entity (SCH-001); no record is validated | Required key missing: **quarantine** the record (REQ-001). Optional key missing: null. |
| Extra column or key | **Warn** (SCH-002); dropped from Silver, kept in Bronze | **Warn** (SCH-002); dropped from Silver, kept in Bronze |
| Known alias (`notes`) | n/a | **Warn** (SCH-004); value mapped to `text`. If both keys are present, `text` wins and `notes` is dropped with SCH-002. |
| Type differs from contract | **Warn** (SCH-003); values are still parsed, and failures are quarantined | Parsed per value; failures are quarantined |
| Bronze metadata columns (`_*`) | Ignored | n/a |

### Silver lineage and data-quality columns (appended to every entity)

| Column | Type | Content |
|---|---|---|
| `_run_id` | string | Silver run that wrote the row |
| `_bronze_ingest_date` | date | Bronze snapshot / ingest date the row was read from |
| `_source_object` | string | `hr_source.<table>`, or the JSONL S3 key |
| `_source_line` | int32 | 1-based line in the JSONL file (learning activity only) |
| `_processed_at` | timestamp (UTC) | Silver processing time |
| `_dq_warnings` | list\<string\> | Reason codes of every WARN rule that fired on the row (empty when clean) |

## PostgreSQL entities

Source types are as declared in `schema.sql`. Bronze stores them losslessly: `INTEGER` → `int32`, `TEXT` → `string`, `DATE` → `date32`, `NUMERIC(5,2)` → `decimal128(5,2)`. Silver keeps the same types.

### teams

Grain: one row per team (100 rows).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| team_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 (DOM-002) |
| name | TEXT NOT NULL | string | yes | |
| department | TEXT NOT NULL | string | yes | Observed: Customer Support, Data & Analytics, Design, Engineering, Finance, Human Resources, Marketing, Operations, Product, Sales. Anything else warns (DOM-004). |

- **Keys:** PK `team_id`.
- **Duplicates:** an identical row with the same PK is deduplicated (KEY-001); the same PK with different content quarantines every copy (KEY-002).

### competencies

Grain: one row per competency (40 rows).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| competency_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 |
| name | TEXT NOT NULL | string | yes | |
| category | TEXT NOT NULL CHECK | string | yes | `Technical`, `Leadership`, `Domain` (DOM-001) |

- **Keys:** PK `competency_id`.

### employees

Grain: one row per employee (2,000 rows).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| employee_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 |
| first_name | TEXT NOT NULL | string | yes | |
| last_name | TEXT NOT NULL | string | yes | |
| email | TEXT NOT NULL | string | yes | Expected `^[^@\s]+@[^@\s]+\.[^@\s]+$` (DOM-005); expected unique (KEY-004, warn) |
| team_id | INTEGER NOT NULL REFERENCES teams | int32 | yes | ≥ 1; must exist in accepted teams (REF-001) |
| job_level | TEXT NOT NULL | string | yes | Observed: IC1, IC2, IC3, Senior IC, Manager, Senior Manager, Director (DOM-004) |
| hire_date | DATE NOT NULL | date32 | yes | `YYYY-MM-DD` (TYP-002); not after ingest date (DAT-001, warn) |

- **Keys:** PK `employee_id`; FK `team_id` → `teams`.

### projects

Grain: one row per project (33 rows).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| project_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 |
| name | TEXT NOT NULL | string | yes | **Not unique.** "MediumSeaGreen Rollout" appears for ids 11 and 26, so always join on `project_id`. |
| team_id | INTEGER NOT NULL REFERENCES teams | int32 | yes | FK → teams |
| outcome_status | TEXT NOT NULL CHECK | string | yes | `on-time`, `delayed`, `at-risk`, `cancelled` (DOM-001). Only the first two occur. |
| outcome_score | NUMERIC(5,2) NOT NULL | decimal128(5,2) | yes | Must fit NUMERIC(5,2) with no rounding (TYP-001). Expected 0–100 (DOM-006, warn); observed 41.52–94.76. |

- **Keys:** PK `project_id`; FK `team_id` → `teams`.
- **Linking:** projects link to teams only. There is no employee-to-project link.

### performance_reviews

Grain: one row per employee per half-year period (4,000 rows: 2 per employee).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| review_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 |
| employee_id | INTEGER NOT NULL REFERENCES employees | int32 | yes | FK → employees |
| review_period | TEXT NOT NULL | string | yes | Must match `^\d{4}-H[12]$` (DOM-003). Observed: 2024-H2, 2025-H1. |
| rating | INTEGER NOT NULL CHECK 1–5 | int32 | yes | 1–5 (DOM-002) |
| manager_comment | TEXT | string | no | Free text (9 distinct phrases observed) |
| submitted_date | DATE NOT NULL | date32 | yes | Not after ingest date (DAT-001); on or before period end warns (BUS-005) |

- **Derived columns:**
  - `period_start` and `period_end` (date32), from `review_period`.
  - `is_pre_hire` (bool): `period_end < hire_date` (BUS-001).
- **Keys:** PK `review_id`; natural key `(employee_id, review_period)`; FK `employee_id` → `employees`.
- **Duplicates on the natural key:** keep the row with the latest `submitted_date`, then the highest `review_id`; the others are deduplicated (KEY-003).

### competency_assessments

Grain: one row per employee, competency and assessment date (4,000 rows: 2 per employee).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| assessment_id | INTEGER PRIMARY KEY | int32 | yes | ≥ 1 |
| employee_id | INTEGER NOT NULL REFERENCES employees | int32 | yes | FK → employees |
| competency_id | INTEGER NOT NULL REFERENCES competencies | int32 | yes | FK → competencies |
| proficiency_level | INTEGER NOT NULL CHECK 1–5 | int32 | yes | 1–5 |
| required_level | INTEGER NOT NULL CHECK 1–5 | int32 | yes | 1–5 (observed 2–4 only) |
| assessed_date | DATE NOT NULL | date32 | yes | Not after ingest date |

- **Derived columns:**
  - `gap` (int32) = `required_level − proficiency_level`.
  - `is_critical_gap` (bool) = `gap ÷ required_level ≥ 1/2`, i.e. the employee is missing at least half of the required level (BUS-009; approved A1).
  - `is_pre_hire` (bool) = `assessed_date < hire_date` (BUS-003).
- **Keys:** PK `assessment_id`; natural key `(employee_id, competency_id, assessed_date)`; FKs `employee_id` → `employees` and `competency_id` → `competencies`.
- **History is kept.** Assessments of the same competency on different dates are all kept. Only same-day duplicates are deduplicated, keeping the highest `assessment_id`.

### talent_review

Grain: one row per employee (2,000 rows, all from cycle 2025-H1).

| Column | Source type | Silver type | Required | Constraints |
|---|---|---|---|---|
| employee_id | INTEGER PRIMARY KEY REFERENCES employees | int32 | yes | FK → employees |
| review_cycle | TEXT NOT NULL | string | yes | Must match `^\d{4}-H[12]$` (DOM-003) |
| potential_rating | TEXT NOT NULL CHECK | string | yes | `Low`, `Medium`, `High` |
| promotion_readiness | TEXT NOT NULL CHECK | string | yes | `Ready Now`, `1-2 Years`, `Not Ready` |
| attrition_risk | TEXT NOT NULL CHECK | string | yes | `Low`, `Medium`, `High` |

- **Derived columns:** `cycle_start` and `cycle_end` (date32); `is_pre_hire` (bool) = `cycle_end < hire_date` (BUS-004).
- **Keys:** PK and FK `employee_id`.

## Learning activity (S3 JSONL)

- **Source:** `s3://$SOURCE_BUCKET/learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl`, with one JSON object per line.
- **Size:** 783 files, 29,868 lines, events dated 2023-09-04 to 2026-09-02.
- **Grain:** one row per learning event.

| Key / column | Source (JSON) | Silver type | Required | Constraints / parsing |
|---|---|---|---|---|
| event_id | string, e.g. `EV0000001103` | string | yes | Expected `^EV\d{10}$` (DOM-005, warn) |
| employee_id | **string of digits** (`"989"`) or null | int32 | yes | Cast to int (JSN-006); non-digits are quarantined (TYP-001); ≥ 1; FK → employees (REF-001) |
| event_date | string, `YYYY-MM-DDTHH:MM:SS`, no offset | timestamp[us] (naive, treated as UTC) | yes | ISO 8601 with a time. Offsets are converted to UTC. Anything else (`yesterday`, `N/A`, `2026-13-45`) is quarantined (TYP-003). Blank is quarantined (REQ-001). **Never inferred from the partition.** |
| event_type | string | string | yes | One of `enrollment`, `module_started`, `module_completed`, `quiz_attempted`, `quiz_passed`, `course_completed`, `certification_earned` (DOM-001) |
| course_name | string | string | yes | Non-blank string; 42 distinct titles. There is no course master, so the value is not checked against a list. |
| text | string; **drift alias `notes`** | string | no | `notes` is mapped to `text` (SCH-004) |
| score | integer or null | float64 | no | Number in 0–100 (DOM-002). Expected only on quiz events (BUS-006/007). |

- **Derived columns:**
  - `event_year` and `event_month` (int32), from the parsed `event_date`. These are the partition columns. They are stored **only in the hive path** (`event_year=2024/event_month=02/`), not inside the Parquet files, because pandas and pyarrow refuse to read a dataset whose files repeat a partition column (`Unable to merge: Field event_year has incompatible types`). Readers get them back from the path. `EntityContract.silver_file_schema()` is the in-file schema.
  - `is_pre_hire` (bool): the event's calendar day is before `hire_date` (BUS-002).
- **Keys:** PK `event_id`; FK `employee_id` → `employees`.
- **Duplicates:** an identical copy with the same `event_id` is deduplicated, keeping the first in file and line order (KEY-001). The same `event_id` with different content quarantines every copy (KEY-002). Exact duplicates are removed **before** field validation, so a duplicated invalid record is quarantined once.
- **File and line level:** a malformed line (JSN-001), a non-object line (JSN-002) or invalid UTF-8 (JSN-003) is quarantined with its raw text and line number. An empty file (JSN-004) or a blank line (JSN-005) is a warning.

## Quarantine record

Each entity has a quarantine dataset, and every rejected record lands in it. The schema is `QUARANTINE_SCHEMA` in `contracts.py`:

| Column | Type | Content |
|---|---|---|
| entity | string | e.g. `learning_activity` |
| source_object | string | `hr_source.<table>`, or the S3 key of the JSONL file |
| source_line | int32 | 1-based line number (JSONL); null for PostgreSQL |
| record_key | string | Primary key value(s). For malformed JSON, the `event_id` is extracted from the raw text where possible. |
| raw_record | string | The original JSONL line exactly as read (undecodable bytes become U+FFFD), or the PostgreSQL row as JSON |
| rule_ids | list\<string\> | e.g. `["REQ-001"]` |
| reason_codes | list\<string\> | e.g. `["missing_required_field"]` |
| reason_detail | string | Human-readable reasons, e.g. `employee_id is null, blank or absent` |
| _run_id, _bronze_ingest_date, _quarantined_at | | Run lineage |
