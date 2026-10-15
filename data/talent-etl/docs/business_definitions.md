# Business definitions

This document defines the business rules that Silver flags and Gold will compute. The definitions are implemented in [`talent_etl/definitions.py`](../talent_etl/definitions.py) and tested in `tests/unit/test_definitions.py`.

Anything the workshop requirements or the data do not settle is marked **ASSUMPTION Ax**. Each assumption is listed at the end and needs approval before Gold relies on it.

## Competency gap

- **gap = `required_level − proficiency_level`**
  - A positive gap is a shortfall: the employee is below the level their role requires.
  - A gap of zero or less means the requirement is met or exceeded.
- **Critical gap: the employee is missing at least half of the required level**, i.e. `gap ÷ required_level ≥ 1/2` (`CRITICAL_GAP_RATIO`). **APPROVED (A1, option B, 2026-10-14).**

  | Required level | Critical when actual level is | Example |
  |---|---|---|
  | 1 | never critical (lowest proficiency is 1, so the gap can't be positive) | |
  | 2 | 1 | 1 of 2 missing = 50% |
  | 3 | 1 | 2 of 3 missing = 67% (1 of 3 = 33% is not critical) |
  | 4 | 2 or lower | 2 of 4 missing = 50% (1 of 4 = 25% is not critical) |
  | 5 | 2 or lower | 3 of 5 missing = 60% (2 of 5 = 40% is not critical) |

  - **Why proportional.** The earlier proposal, `gap ≥ 2`, could never flag a role that requires level 2, because the lowest proficiency is 1. That left out 357 employees at level 1 in a level-2 requirement, the lowest possible score.
  - **What it flags in the data:** **682 of 4,000 assessments (17.05%)**.
    - 325 have gap 2: 148 at required 3, and 177 at required 4.
    - 357 have gap 1 at required 2.
    - The old rule flagged 325.
  - **No per-topic difficulty adjustment.** This was considered and rejected. There's no difficulty data in any source, and critical rates by category (all about 8% under the old rule) and by competency (2–15%, with about 100 assessments each) are mostly within random variation.
  - **Topic-level view instead.** Gold will report which competencies have the most people falling short, as a separate training-needs view. It doesn't change the definition.
- **Which assessment counts.** Gap metrics use the most recent assessment per `(employee_id, competency_id)`. Silver keeps the history.
- **Pre-hire assessments** (234) count in skill metrics (**ASSUMPTION A5**). An assessment describes a skill level whenever it was measured, for example during recruitment. They stay flagged with `is_pre_hire`.

## Performance reviews and rating trends

- **Valid review:** a review that passes the contract (rating 1–5, a well-formed `review_period`, a known employee) **and** has `is_pre_hire = false`.
  - `is_pre_hire` means the review period ended before the hire date, so the review cannot describe this employee's work at ACME.
  - If an employee was hired *during* the period, their review for that period is valid. 243 reviews fall in this case.
- **Pre-hire reviews** (666 reviews across 396 employees) stay in Silver but are **excluded from all rating metrics, distributions and trends** (**ASSUMPTION A7**).
- **Rating trend:** defined only for an employee with **two consecutive valid reviews**: the latest valid period and the one immediately before it.
  - With the current periods that means 2024-H2 → 2025-H1.
  - The trend is `improving` (delta > 0), `stable` (delta = 0) or `declining` (delta < 0), where delta = current rating − previous rating.
  - In this data, 1,604 employees have a trend.
  - 396 have no trend: 270 have no valid review at all, and 126 have only one.
  - Employees without a trend are reported as `insufficient history`, never as `stable`.
- **Review history (business question 1):** all valid reviews per employee in period order. Pre-hire reviews are shown separately and labelled.

## Pre-hire records (summary)

| Entity | Pre-hire definition | Count | In Silver | Downstream treatment |
|---|---|---|---|---|
| performance_reviews | `period_end < hire_date` | 666 | yes, flagged | Excluded from ratings and trends (A7) |
| learning_activity | `event_date` (day) `< hire_date` | 5,161 | yes, flagged | Excluded from per-employee learning metrics by default and reported as a separate count (**ASSUMPTION A4**) |
| competency_assessments | `assessed_date < hire_date` | 234 | yes, flagged | Included in skill metrics (A5) |
| talent_review | `cycle_end < hire_date` | 270 | yes, flagged | Included in the promotion and attrition outputs, with `talent_review_pre_hire = true` so readers can filter them (**ASSUMPTION A6**). These are the only talent data for 270 recent hires. |

**Pre-hire boundary.** A record on the hire date itself is **not** pre-hire.

**Possible data-generation artefact.** Every pre-hire review (666), assessment (234) and talent review (270) belongs to an employee hired on or after 2025-01-01. The 270 employees hired after 2025-06-30 have *every* review and their talent review dated before their hire. This suggests the HR records were generated without regard to hire date, rather than backdated by HR.

Learning events are spread more widely. 4,197 of the 5,161 pre-hire events (81%) belong to employees hired after 2024-12-31, but 964 belong to employees hired as early as 2023-09-17. That pattern could also be genuine pre-boarding activity.

Both are worth confirming with the data owner.

## Learning completion and certification

- **Course completed: an accepted, explicit `course_completed` event counts. APPROVED (A13).** An employee's "courses completed" is the number of **distinct `course_name` values** with a `course_completed` event, so repeated completion events for the same course count once.
- **Certification earned:** an accepted `certification_earned` event, counted as distinct `course_name` values in the same way.
- **Prior events are not required; inconsistent histories are reported as dataset-level metrics (A13, revised 2026-10-15).** A missing enrollment record does not make a completion false, so the completion still counts. Two informational metrics measure how often the expected earlier step is missing. **They are not attached to rows** (not in `_dq_warnings`).
  - **BUS-010 `completion_without_enrollment`:** the share of `course_completed` events with no `enrollment` for the same employee and course at or before them.
  - **BUS-011 `certification_without_completion`:** the share of `certification_earned` events with no `course_completed` for the same employee and course at or before them.
  - Each is reported in the Silver manifest (`entities[learning_activity].metrics.history`) with the **random-order baseline**: the rate expected if the same event types were shuffled at random over the same employees, courses and dates.
    - It is computed exactly, not simulated, in O(N log N): about 30 ms for 29,574 events.
    - An approximate 95% range is shown for context.
    - `talent_etl.dq_report.history_metrics_table` turns this into the notebook's data-quality table.
  - **Why they were downgraded from row warnings.** On the sample data:
    - The observed rates (97.59% and 98.13%) sit inside the random-order baseline (97.49% and 98.00%). That matches a 200-shuffle simulation.
    - When both steps exist for a pair, the prerequisite comes first in only 50% (169 pairs) and 49% (68 pairs) of cases.
    - 83.6% of employee-course pairs have a single event.
    - The decline in the flag rate over the years also matches random order, so enrollments before the data starts don't explain the gaps.
    - The events therefore carry no sequence, and a row-level flag on about 97% of completions gave no actionable signal while hiding genuine warnings.
  - **When it would matter again:** a rate clearly *below* the random baseline (`vs_random = differs from random order`) would mean the source has started recording real learning journeys. At that point the decision should be revisited.
- **Quiz pass rate:** `quiz_passed` events ÷ `quiz_attempted` events, per employee or course. Scores are averaged only over quiz events that have a score.
- **Training metrics exclude:**
  - quarantined events
  - deduplicated copies
  - pre-hire events (A4)
- **Courses and competencies:** course names are not linked to competencies in any source. Linking training to skill gaps needs a reference mapping of the 42 courses to the 40 competencies. It was **not built in Gold**, because the mapping would be invented reference data. It is deferred until the business supplies one (see [gold_design.md](gold_design.md)).
- **Gold-level definitions** (promotion readiness, attrition score, performer flags, learning window, as-of date, no funnel rates) are proposals A19–A24 in [gold_design.md](gold_design.md).

## What is excluded from downstream analytics, and why

| Excluded | Why | Where to find it |
|---|---|---|
| Quarantined records (all entities) | Malformed, mistyped, outside an enforced domain, unkeyed or orphaned; they cannot be trusted | The published run's `_quarantine/<entity>/` (listed in `silver/_current.json`) |
| Deduplicated copies | They would double-count events or reviews | Counted in the Silver manifest |
| Pre-hire reviews | They cannot describe performance at ACME | Silver, `is_pre_hire = true` |
| Pre-hire learning events | Training before employment would inflate per-employee metrics | Silver, `is_pre_hire = true` |
| Entities that FAIL validation | A broken source (missing columns, more than 5% quarantined (**ASSUMPTION A2**), or zero valid rows) must not overwrite good Silver | Logs and manifest; the previous Silver is kept |

Records that are **flagged but still included**:
- pre-hire assessments (A5)
- pre-hire talent reviews, labelled (A6)
- every row with a `_dq_warnings` entry, such as the `notes` → `text` alias

## Assumptions requiring approval

| # | Assumption | Default chosen | Alternatives | Impact on this data |
|---|---|---|---|---|
| A1 | Critical competency gap | **APPROVED:** `gap ÷ required_level ≥ 1/2` (option B) | `gap ≥ 2` (option A); peer-relative; difficulty-weighted | 682 critical, compared with 325 under option A |
| A2 | Quarantine-rate failure threshold | 5% per entity | Stricter (1%) or none | Learning activity is at 0.48%, so nothing fails |
| A3 | `notes` key | Mapped to `text`, with a warning | Quarantine those records | 38 events kept, not 38 quarantined |
| A4 | Pre-hire learning events | Kept in Silver, excluded from per-employee learning metrics, reported separately | Include them | 5,161 events (17.5% of accepted) |
| A5 | Pre-hire assessments | Included in skill metrics | Exclude them | 234 assessments |
| A6 | Pre-hire talent reviews | Included, labelled | Exclude, which leaves 270 employees with no talent data | 270 employees |
| A7 | Pre-hire reviews | Excluded from ratings and trends; a mid-period hire's review is valid | Also exclude mid-period hires | 666 excluded; 243 mid-period reviews kept |
| A8 | Naive `event_date` time zone | Treated as UTC | Some local time zone | Could move events near midnight to a different day (not quantified) |
| A9 | Blank `event_date` | Missing (REQ-001) | Invalid (TYP-003) | Same action (quarantine); only the reason code differs |
| A10 | Duplicate events | Keep the first exact copy; quarantine every conflicting copy | Keep the latest | 152 copies dropped; 0 conflicts |
| A11 | Observed-only domains (department, job_level, email format, event_id format, outcome_score 0–100) | Warn and keep | Quarantine | No effect today (0 violations) |
| A12 | Dates after the ingest date | Warn and keep | Quarantine | No effect today (0) |
| A13 | Completion counting | **APPROVED:** an explicit `course_completed` counts (distinct courses per employee). Inconsistent histories are reported as dataset-level informational metrics BUS-010/011, with a random-order baseline, and not as row warnings (revised 2026-10-15) | Require prior enrollment; row-level warnings | Completions unchanged (3,490 counted). 3,406 of 3,490 completions and 1,731 of 1,764 certifications lack the earlier step, equal to random order |
| A14 | Silver processing | **APPROVED: incremental.** Only new or changed Bronze files (and the files that must be re-decided with them) are reprocessed; only affected partitions are rewritten. A full rebuild happens only on `--full-refresh`, on the first run, or when the rules change. | Full rebuild every run | One new day: 0.36 s and 4 objects written, compared with 4.5 s and 55 for a full rebuild |
| A15 | Assessment history | Keep history; dedupe only same-day duplicates; Gold uses the latest per competency | Keep the latest only in Silver | No effect today (0 repeats) |
| A16 | Duplicate reviews per period | Keep the latest `submitted_date`, then the highest `review_id` | Quarantine both | No effect today (0) |
| A17 | Silver failure scope | All-or-nothing: if any entity fails, nothing is published (the pointer keeps naming the previous run) | Publish the entities that passed | No effect today (nothing fails) |
| A18 | Children of a failed parent | Skipped and reported as `SKIPPED` | Validate them anyway (every row would be quarantined as an orphan) | No effect today |
