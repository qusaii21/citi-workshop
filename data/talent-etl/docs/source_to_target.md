# Source-to-target mapping

This document traces every entity from its source through Bronze to Silver. All paths come from [`talent_etl/paths.py`](../talent_etl/paths.py) and are relative to the lake bucket (`LAKE_BUCKET`). The column-level contracts and parsing rules are in [schema_contracts.md](schema_contracts.md), and the rules are in [data_quality.md](data_quality.md).

> Status: Bronze, Silver and Gold are implemented (`python job.py --step bronze|silver|gold|all`). Gold tables, their Silver inputs and checks are specified in [gold_design.md](gold_design.md).

## How Silver runs (applies to every entity)

- **Input:**
  - PostgreSQL entities are read from one Bronze snapshot, `bronze/postgres/<table>/ingest_date=D/`. `D` is the `--ingest-date` argument, or by default the latest available date (assumption A14).
  - Learning activity is read from every file under `bronze/learning_activity/`.
- **Order:** teams, competencies, employees, projects, performance_reviews, competency_assessments, talent_review, learning_activity. Parents are validated first so that foreign keys resolve against *accepted* parent keys, and so that employees' hire dates are available for the pre-hire rules.
- **Processing: incremental (approved A14).** A run only does the work that changed since the published version.
  - **Learning activity:** the unit of work is a **Bronze file**. Two state tables published with each version remember the past: a **file ledger** (ETag, counts and partitions per file) and an **event index** (file, status, employee, course, type and date per event).
    - A run reprocesses only new or changed files (ETag differs) and deleted files, plus every file that must be re-decided with them. That means files sharing an `event_id` (so the duplicate rules again see every copy in source order) and files with events of employees whose master data changed (added, removed, or hire date changed), expanded until nothing more is added.
    - Only the month partitions that gain or lose rows are rewritten. The dataset-level history metrics (BUS-010/011) are recomputed from the event index when it changes (about 30 ms) and reused otherwise. They are not attached to rows, so they never cause a partition to be rewritten.
  - **PostgreSQL entities:** they arrive in Bronze as full snapshots, so the unit of change is the table.
    - An unchanged snapshot (same content hash) whose parents did not change is **reused** without validation.
    - Otherwise it is revalidated, and rewritten only if its Silver output changed (`unchanged` otherwise).
    - A row-level merge would not save I/O here, because the whole snapshot must be read anyway. Row-level CDC belongs in Bronze ingestion.
  - **Full rebuild only when needed:** on `--full-refresh`, when nothing is published yet, when the published version has no incremental state, or automatically when the **rules fingerprint** changes (rules, contracts, definitions or `LOGIC_VERSION`). Otherwise rows validated under old rules could linger.
  - **Correctness guarantee:** an incremental run publishes exactly the rows, quarantine, counts and reason counts that a full rebuild of the same Bronze would, apart from lineage columns. The tests assert this for new, changed and deleted files, cross-file exact and conflicting duplicates, hire-date changes, late-arriving employees and late enrollments.
  - **Lineage under incremental processing:** `_run_id`, `_processed_at` and `_bronze_ingest_date` on a row identify the run that last wrote it.
- **Write strategy: new files + atomic pointer.** New and changed files are written into a **new, never-modified folder** `silver/runs/run_id=<run_id>/`. Unchanged datasets and partitions are **not copied**: the new pointer keeps referencing the files of earlier runs. When everything has been written and verified, the run **publishes** by replacing one small object, `silver/_current.json`. That one PUT is the commit.
  - A single S3 PUT is atomic: a reader sees either the whole old pointer or the whole new one. Readers always resolve Silver through the pointer (`talent_etl.silver.read_current` / `current_files`), so they see one complete run, never a mix.
  - If a run crashes at any point before the pointer is replaced, published Silver is unchanged and the half-written folder is never read.
  - Rerunning with no new Bronze data writes no data files at all; it only publishes a new pointer that references the same files.
- **Failure is all-or-nothing (assumption A17).** Every entity is validated before anything is written. If any entity fails (SCH-001, VOL-001 or VOL-002), nothing is written, the pointer is not moved, a manifest with `status: FAILED` is written, and the job exits with code 1. A write error or failed check after validation has the same effect: the run is recorded as `FAILED` and never published.
- **Ordering guard.** If a run that *started later* has already published, an older run does not publish over it. It is recorded as `SUPERSEDED`, so Silver is never rolled back to older data.
- **Retention and cleanup.** After publishing, the run deletes Silver objects that no reader can reach. Because files are shared between versions, retention is **by reference**: every file that the **current or the previous** pointer references is kept (rollback), along with every manifest (the audit trail). It deletes:
  - files in finished run folders (folders with a manifest) that neither retained version references
  - run folders with no manifest whose `_STARTED.json` marker is more than 24 hours old (hard crashes)
  - any object outside `runs/`, `_manifests/` and the pointer, such as the earlier fixed-path layout

  A folder with no manifest that is younger than 24 hours is kept, because another run may still be writing it. Cleanup errors are logged as warnings and never fail an already-published run.
- **Children of a failed parent are skipped (assumption A18).** They are not validated, because every row would otherwise be quarantined as an orphan. They appear as `SKIPPED` in the manifest.
- **Read-back checks:** every written object is read back, and its row count and schema must match. Non-nullable columns must contain no nulls. Rows written per entity must equal rows accepted.
- **Audit:** each run writes a manifest to `silver/_manifests/run_id=<run_id>.json` with its status (`SUCCEEDED`, `FAILED` or `SUPERSEDED`), read, accepted, warned, quarantined and deduplicated counts, and reason counts for every entity. Whether a run is *published* is decided only by the pointer. A `SUCCEEDED` manifest whose run is not current means the run was superseded later, or crashed between writing its manifest and replacing the pointer.

## Entity map

`R` is the published run, read from `silver/_current.json`, which also lists every file of the run per entity.

| Entity | Source | Bronze path | Silver path | Quarantine path | Partitioning |
|---|---|---|---|---|---|
| teams | `hr_source.teams` | `bronze/postgres/teams/ingest_date=D/part-00000.parquet` | `silver/runs/run_id=R/teams/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/teams/part-00000.parquet` | none |
| competencies | `hr_source.competencies` | `bronze/postgres/competencies/ingest_date=D/…` | `silver/runs/run_id=R/competencies/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/competencies/…` | none |
| employees | `hr_source.employees` | `bronze/postgres/employees/ingest_date=D/…` | `silver/runs/run_id=R/employees/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/employees/…` | none |
| projects | `hr_source.projects` | `bronze/postgres/projects/ingest_date=D/…` | `silver/runs/run_id=R/projects/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/projects/…` | none |
| performance_reviews | `hr_source.performance_reviews` | `bronze/postgres/performance_reviews/ingest_date=D/…` | `silver/runs/run_id=R/performance_reviews/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/performance_reviews/…` | none |
| competency_assessments | `hr_source.competency_assessments` | `bronze/postgres/competency_assessments/ingest_date=D/…` | `silver/runs/run_id=R/competency_assessments/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/competency_assessments/…` | none |
| talent_review | `hr_source.talent_review` | `bronze/postgres/talent_review/ingest_date=D/…` | `silver/runs/run_id=R/talent_review/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/talent_review/…` | none |
| learning_activity | `s3://$SOURCE_BUCKET/learning_activity/year=/month=/day=/*.jsonl` | `bronze/learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl` (byte copy) | `silver/runs/run_id=R/learning_activity/event_year=YYYY/event_month=MM/part-00000.parquet` | `silver/runs/run_id=R/_quarantine/learning_activity/part-00000.parquet` | `event_year`, `event_month` |

### Partitioning decisions

- **PostgreSQL entities: unpartitioned, one Parquet file each.** The largest has 4,000 rows, about 70 KB as Parquet. Splitting them would only create many tiny files and add listing cost. Every consumer reads the whole table (dimensions, or facts with 2 rows per employee). If volumes grow, the natural next step is partitioning `performance_reviews` by `review_period`.
- **Learning activity: partitioned by `event_year` / `event_month`, derived from the *parsed* `event_date`.**
  - It is the only time-series fact, and the analyses are time-bounded (training completed per period, learning trends).
  - Month partitions let a reader prune by time.
  - At 37 months and about 800 rows per month, partitions are small but not tiny. Day partitions, like the source layout, would give 783 files of about 38 rows, which is the classic small-files problem.
  - Year partitions would give only 4 coarse files.
  - The partition value comes from `event_date`, never from the source path. A record whose date cannot be parsed is quarantined, so it can never land in a wrong partition.
- **Quarantine: one file per entity.** It holds hundreds of rows at most and is read for troubleshooting, not analytics.

## Column mappings

**Legend:**
- **=** copied unchanged, same type.
- **trim** surrounding whitespace removed; a blank value becomes null.
- **cast** type conversion with a strict parser.
- **derived** computed in Silver.

Bronze metadata columns (`_run_id`, `_ingest_date`, `_ingested_at`, `_source_system`, `_source_object`) are not carried over as-is. Silver writes its own lineage columns (see the last table).

### teams, competencies, employees, projects

| Source column | Bronze type | Silver column | Silver type | Mapping | Checks |
|---|---|---|---|---|---|
| teams.team_id | int32 | team_id | int32 | = | REQ-001, TYP-001, DOM-002, KEY-001/002 |
| teams.name | string | name | string | trim | REQ-001 |
| teams.department | string | department | string | trim | REQ-001, DOM-004 |
| competencies.competency_id | int32 | competency_id | int32 | = | REQ-001, DOM-002, KEY-001/002 |
| competencies.name | string | name | string | trim | REQ-001 |
| competencies.category | string | category | string | trim | REQ-001, DOM-001 |
| employees.employee_id | int32 | employee_id | int32 | = | REQ-001, DOM-002, KEY-001/002 |
| employees.first_name / last_name | string | first_name / last_name | string | trim | REQ-001 |
| employees.email | string | email | string | trim | REQ-001, DOM-005, KEY-004 |
| employees.team_id | int32 | team_id | int32 | = | REQ-001, REF-001 → teams |
| employees.job_level | string | job_level | string | trim | REQ-001, DOM-004 |
| employees.hire_date | date32 | hire_date | date32 | = | REQ-001, TYP-002, DAT-001 |
| projects.project_id | int32 | project_id | int32 | = | REQ-001, DOM-002, KEY-001/002 |
| projects.name | string | name | string | trim (not unique) | REQ-001 |
| projects.team_id | int32 | team_id | int32 | = | REF-001 → teams |
| projects.outcome_status | string | outcome_status | string | trim | DOM-001 |
| projects.outcome_score | decimal128(5,2) | outcome_score | decimal128(5,2) | = (no rounding) | TYP-001, DOM-006 |

### performance_reviews

| Source column | Bronze type | Silver column | Silver type | Mapping | Checks |
|---|---|---|---|---|---|
| review_id | int32 | review_id | int32 | = | REQ-001, KEY-001/002 |
| employee_id | int32 | employee_id | int32 | = | REF-001 → employees |
| review_period | string | review_period | string | trim | DOM-003, KEY-003 (with employee_id) |
| rating | int32 | rating | int32 | = | DOM-002 (1–5) |
| manager_comment | string | manager_comment | string | trim (nullable) | none |
| submitted_date | date32 | submitted_date | date32 | = | TYP-002, DAT-001, BUS-005 |
| (review_period) | | period_start, period_end | date32 | derived: H1 = Jan 1 – Jun 30, H2 = Jul 1 – Dec 31 | |
| (period_end, employees.hire_date) | | is_pre_hire | bool | derived: `period_end < hire_date` | BUS-001 |

**Deduplication:** on `(employee_id, review_period)`, keep the latest `submitted_date`, then the highest `review_id`.

### competency_assessments

| Source column | Bronze type | Silver column | Silver type | Mapping | Checks |
|---|---|---|---|---|---|
| assessment_id | int32 | assessment_id | int32 | = | KEY-001/002 |
| employee_id | int32 | employee_id | int32 | = | REF-001 → employees |
| competency_id | int32 | competency_id | int32 | = | REF-001 → competencies |
| proficiency_level | int32 | proficiency_level | int32 | = | DOM-002 (1–5) |
| required_level | int32 | required_level | int32 | = | DOM-002 (1–5) |
| assessed_date | date32 | assessed_date | date32 | = | TYP-002, DAT-001 |
| (required_level, proficiency_level) | | gap | int32 | derived: `required_level − proficiency_level` | |
| (gap, required_level) | | is_critical_gap | bool | derived: `gap ÷ required_level ≥ 1/2` | BUS-009 |
| (assessed_date, employees.hire_date) | | is_pre_hire | bool | derived: `assessed_date < hire_date` | BUS-003 |

**Deduplication:** on `(employee_id, competency_id, assessed_date)`, keep the highest `assessment_id`. History across dates is kept.

### talent_review

| Source column | Bronze type | Silver column | Silver type | Mapping | Checks |
|---|---|---|---|---|---|
| employee_id | int32 | employee_id | int32 | = | KEY-001/002, REF-001 → employees |
| review_cycle | string | review_cycle | string | trim | DOM-003 |
| potential_rating | string | potential_rating | string | trim | DOM-001 |
| promotion_readiness | string | promotion_readiness | string | trim | DOM-001 |
| attrition_risk | string | attrition_risk | string | trim | DOM-001 |
| (review_cycle) | | cycle_start, cycle_end | date32 | derived | |
| (cycle_end, employees.hire_date) | | is_pre_hire | bool | derived: `cycle_end < hire_date` | BUS-004 |

### learning_activity

| Source key | JSON type | Silver column | Silver type | Mapping | Checks |
|---|---|---|---|---|---|
| event_id | string | event_id | string | trim | REQ-001, DOM-005, KEY-001/002 |
| employee_id | string of digits | employee_id | int32 | **cast** `"989"` → 989 | REQ-001, TYP-001, DOM-002, JSN-006, REF-001 → employees |
| event_date | string | event_date | timestamp[us] | **cast** ISO 8601; offset converted to UTC | REQ-001, TYP-003, DAT-001, BUS-008 |
| event_type | string | event_type | string | trim | DOM-001 |
| course_name | string | course_name | string | trim | REQ-001 |
| text, **or `notes`** | string | text | string | trim; **rename `notes` → `text`** | SCH-004, SCH-002 |
| score | integer / null | score | float64 | **cast** | DOM-002 (0–100), BUS-006/007 |
| (event_date) | | event_year, event_month | int32 | derived; partition columns, stored in the hive path only | |
| (event_date, employees.hire_date) | | is_pre_hire | bool | derived: `event_date::date < hire_date` | BUS-002 |
| any other key | | | | dropped | SCH-002 |

**Deduplication:** exact copies on `event_id` keep the first in file and line order (KEY-001). Conflicting copies are all quarantined (KEY-002).

**Line-level checks:** JSN-001 to JSN-005.

### Silver lineage columns (every entity)

| Silver column | Source of value |
|---|---|
| `_run_id` | Current Silver run id |
| `_bronze_ingest_date` | PostgreSQL: the Bronze snapshot's `ingest_date`. Learning activity: the run's ingest date. |
| `_source_object` | PostgreSQL: Bronze `_source_object` (e.g. `hr_source.employees`). Learning activity: the source S3 key relative to the source bucket. |
| `_source_line` | Learning activity only: 1-based line number in the JSONL file |
| `_processed_at` | Silver processing timestamp (UTC) |
| `_dq_warnings` | Reason codes of WARN rules that fired on the row |
