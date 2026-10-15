# Gold layer design

Gold holds curated, analysis-ready tables, one or more per business question. They are built only from the **published Silver version**, read through `silver/_current.json`. Every Gold table has a declared grain and primary key, documented business logic, and validation checks that reconcile it with Silver before it can be published.

The business rules come from [business_definitions.md](business_definitions.md). New Gold-level proposals are marked **ASSUMPTION A19–A24** and listed at the end.

## Publishing and incremental processing

Gold follows the Silver pattern:

- **Atomic publish.** New files are written to `gold/runs/run_id=<run_id>/<table>/part-00000.parquet`. The run then publishes by replacing the single pointer object `gold/_current.json`, which lists every table's file, row count and input fingerprint, together with the Silver run it was built from. Readers must go through the pointer.
- **Incremental at table level.** Each table has an **input fingerprint**, which combines:
  - the Silver file keys of every Silver entity it reads (Silver only writes a new key when content changes)
  - the Gold logic version
  - the as-of date, for tables that use it

  If the fingerprint matches the published Gold version, the table is **reused without being recomputed or copied**. Otherwise it is rebuilt in full.
  - At this size (2,000 employees, 29,574 events) a full rebuild of an affected table takes milliseconds.
  - Partition-level updates are not worth their complexity here (see Limitations).
- **Validation before publishing.** Every rebuilt table runs its checks. If any check fails, nothing is published, a `FAILED` manifest records every check, and the job exits with code 1.
- **Retention.** Files referenced by the current or previous Gold version are kept (rollback), as in Silver.
- **As-of date (A22).** Time-relative metrics (tenure, the last 12 months of learning) use the Silver version's Bronze snapshot date, not the wall clock. The same inputs therefore always give the same Gold.

## Tables

All keys are non-null and unique. "Valid review" means a review with `is_pre_hire = false` (A7).

| # | Table | Business question | Grain / primary key | Silver inputs | Rows (sample data) |
|---|---|---|---|---|---|
| 1 | `dim_employee` | shared dimension | employee: `employee_id` | employees, teams | 2,000 |
| 2 | `performance_history` | Q1 rating history | employee × period: (`employee_id`, `review_period`) | performance_reviews | 4,000 |
| 3 | `employee_performance` | Q1 latest rating and trend | employee: `employee_id` | performance_reviews, employees | 2,000 |
| 4 | `employee_skill_gaps` | Q2 skill gaps | employee × competency: (`employee_id`, `competency_id`) | competency_assessments, competencies, employees, teams | 4,000 |
| 5 | `competency_gap_summary` | Q2 org-wide gaps / training needs | competency: `competency_id` | competency_assessments, competencies | 40 |
| 6 | `skill_distribution` | Q7 skill distribution | competency × department × proficiency level | competency_assessments, competencies, employees, teams | ≤ 2,000 |
| 7 | `employee_learning` | Q4 training completed | employee: `employee_id` | learning_activity, employees | 2,000 |
| 8 | `promotion_pipeline` | Q3 promotion readiness | employee with a talent review: `employee_id` | talent_review, performance_reviews, competency_assessments, employees, learning_activity | 2,000 |
| 9 | `attrition_risk` | Q5 attrition risk | employee: `employee_id` | talent_review, performance_reviews, learning_activity, employees | 2,000 |
| 10 | `team_performance` | Q6 performance vs team and project outcomes | team: `team_id` | teams, employees, performance_reviews, competency_assessments, projects | 100 |

### 1. `dim_employee`

- **Columns:** employee attributes plus `team_name`, `department`, `tenure_years` and `as_of`.
- **Tenure:** `tenure_years = (as_of − hire_date) / 365.25`.

### 2. `performance_history` (Q1)

- **Content:** every review, including pre-hire reviews, which are labelled `is_pre_hire` and `is_valid = false` so the history is complete. Columns: `review_id`, `rating`, period bounds, `submitted_date`, `manager_comment`.
- **For valid reviews only:**
  - `previous_valid_rating`: the employee's preceding valid review
  - `rating_change`: the difference from it

### 3. `employee_performance` (Q1, input to Q3, Q5 and Q6)

- **Why a separate table:** it holds one row for every employee, including the 270 with no valid review, who would be missing from a query on `performance_history`. It defines "current performance" once (latest valid rating, consecutive-period trend, performer flags) for `promotion_pipeline`, `attrition_risk`, `team_performance` and the notebook, so the rule is not re-implemented four times.

- **Columns:** `valid_review_count`, `pre_hire_review_count`, `latest_valid_period`, `latest_valid_rating`, `previous_valid_rating`, `rating_delta`, `rating_trend`, `is_high_performer`, `is_low_performer`.
- **Rating trend:**
  - `improving`, `stable` or `declining`, only when the latest valid review and the valid review for the **immediately preceding** half-year both exist
  - otherwise `insufficient_history`
- **Performer flags (A24):**
  - `is_high_performer`: latest valid rating ≥ 4
  - `is_low_performer`: latest valid rating ≤ 2

### 4. `employee_skill_gaps` (Q2)

- **Content:** the **latest assessment** per employee and competency (A15), with competency name and category, department, team and job level.
- **Gap columns:**
  - `gap = required_level − proficiency_level`
  - `gap_ratio = gap / required_level`
  - `has_gap = gap > 0`
  - `is_critical_gap`: missing at least half of the required level (**approved A1**)
- **Pre-hire:** `is_pre_hire` is kept and the rows are included (A5).

### 5. `competency_gap_summary` (Q2 training-needs view)

- **Per competency:** employees assessed, average proficiency and required level, employees below required, critical-gap count and rate, and a rank by critical-gap rate.
- **Purpose:** this is the "which topics do many people fall short on" view agreed during the critical-gap discussion. It complements the definition of critical; it does not change it.

### 6. `skill_distribution` (Q7)

- **Content:** the number of employees at each proficiency level, per competency and department, based on the latest assessments.
- **Share:** `share_within_group` is that count's share of the competency-department group.

### 7. `employee_learning` (Q4)

- **Scope:** all employees, with zeros where there is no activity. Quarantined and deduplicated events never reach Silver.
- **Pre-hire events:** excluded and counted in `pre_hire_events_excluded` (A4).
- **Columns:** `events_counted`, `courses_enrolled`, `courses_completed`, `certifications_earned`, `quiz_attempts`, `quiz_passes`, `avg_quiz_score`, `last_activity_date`, `events_last_12m`, `is_learning_active_12m`.
- **Counting rules:**
  - `courses_completed` is the number of distinct courses with an explicit `course_completed` event (**approved A13**).
  - `certifications_earned` is counted the same way for `certification_earned`.
- **No funnel rates (A23).** There is no quiz pass rate and no completion rate. The A13 reassessment showed that learning events are not sequenced. In the data, 203 employees have `quiz_passed` events but no `quiz_attempted` events, so a per-employee pass rate could exceed 100%. Gold reports counts and average scores only. The sequence checks remain the informational Silver metrics BUS-010/011.

### 8. `promotion_pipeline` (Q3)

- **Content:** one row per talent review.
- **Columns:** potential, readiness, HR attrition risk and `talent_review_pre_hire` (A6), plus `latest_valid_rating`, `rating_trend`, `critical_gap_count` and `courses_completed` as context.
- **Promotion-ready rule (A19):** `is_promotion_ready` = potential `High` **and** readiness `Ready Now` **and** latest valid rating ≥ 4.
  - Result: 148 employees, out of 357 who are High potential and Ready Now.
  - 45 High / Ready Now employees have no valid rating, so they cannot qualify.
  - Critical gaps are shown, not used as a filter: 61 of the 148 have at least one.

### 9. `attrition_risk` (Q5)

An explainable score built from separate signals (A20), each kept as its own column:

| Signal | Points | Employees |
|---|---|---|
| HR `attrition_risk` | High = 2, Medium = 1 | 311 High, 612 Medium |
| `is_rating_declining`: latest valid rating lower than the previous half-year's | 1 | 191 |
| `is_low_latest_rating`: latest valid rating ≤ 2 | 1 | 402 |
| `is_learning_inactive_12m`: no counted learning event in the 365 days before as-of (A21) | 1 | 56 |

`risk_points` gives `risk_band`:
- **High:** ≥ 3 points (193 employees)
- **Medium:** 2 points (271)
- **Low:** ≤ 1 point (1,536)

The signals are consistent with each other: 116 of the 191 employees with a declining rating are rated High risk by HR.

### 10. `team_performance` (Q6)

- **Per team:** headcount, employees with a valid rating, average latest valid rating, share of high performers and critical-gap count.
- **Project columns:** `project_count`, average `outcome_score`, on-time share and `has_projects`.
- **Coverage:** only 27 of 100 teams have projects. Projects link to teams, not employees.
- **Correlation:** the notebook computes the correlation between team performance and project outcome. It is weak on the sample data (Pearson 0.14, n = 27) and is reported with that caveat.

## Validation and reconciliation checks

Each rebuilt table runs these checks. Results are recorded per check in the Gold manifest, and **any failure blocks publishing**.

**Generic checks (every table):**
- the output schema equals the table contract
- the primary key is non-null and unique (this is the grain check)
- domain checks: ratings 1–5, rates in [0, 1], allowed values for trend, band and readiness

**Reconciliation with Silver**, computed independently from the Silver rows:

| Table | Check |
|---|---|
| `dim_employee` | rows = Silver employees |
| `performance_history` | rows = Silver reviews; `is_valid` count = Silver non-pre-hire reviews |
| `employee_performance` | rows = employees; Σ `valid_review_count` = valid reviews; trend counts add up to employees |
| `employee_skill_gaps` | rows = distinct (employee, competency) in Silver; critical count = critical among the latest assessments |
| `competency_gap_summary` | rows = competencies; Σ `critical_gap_count` = critical count in `employee_skill_gaps` |
| `skill_distribution` | Σ employees = rows of `employee_skill_gaps`; shares add up to 1 per group |
| `employee_learning` | rows = employees; Σ `events_counted` + Σ `pre_hire_events_excluded` = Silver learning rows; Σ `courses_completed` = distinct non-pre-hire (employee, course) completions in Silver |
| `promotion_pipeline` | rows = talent reviews; every ready row satisfies the rule |
| `attrition_risk` | rows = employees; `risk_points` = sum of the signal points; band consistent with points |
| `team_performance` | rows = teams; Σ `headcount` = employees; Σ `project_count` = projects |

## Not built (yet)

- **Course-to-competency mapping.** No source links the 42 course titles to the 40 competencies. A mapping would be invented reference data, so linking training to skill gaps is deferred until the business supplies one.
- **A PostgreSQL serving copy of Gold.** The notebook reads Gold Parquet through the pointer. A database copy for BI tools can be added later, loaded in one transaction from the published version.

## Assumptions to review

| # | Proposal | Effect on sample data |
|---|---|---|
| A19 | Promotion-ready = High potential and Ready Now and latest valid rating ≥ 4; critical gaps shown as context, not a filter | 148 ready |
| A20 | Attrition points: HR High 2 / Medium 1, +1 declining rating, +1 latest rating ≤ 2, +1 no learning in 12 months; bands High ≥ 3, Medium 2, Low ≤ 1 | 193 / 271 / 1,536 |
| A21 | Learning-activity window = 365 days before as-of | 56 inactive employees |
| A22 | As-of date = the Silver version's Bronze snapshot date | Deterministic results |
| A23 | No funnel conversion rates (quiz pass rate, completion rate) in Gold | Counts and average scores only |
| A24 | High performer = latest valid rating ≥ 4; low performer ≤ 2 | 879 high, 402 low |

Inherited and still proposed: A4 (pre-hire learning excluded), A5 (pre-hire assessments included), A6 (pre-hire talent reviews included and labelled), A7 (pre-hire reviews excluded from ratings).

## Limitations

- **Table-level reuse only.** A table that depends on learning activity is rebuilt in full whenever any learning partition changes. That is 29,574 rows, about 0.1 s here. At much larger volumes, `employee_learning` could be maintained per employee from the Silver event index.
- **Gold reads whole Silver entities into memory (pandas).** This is fine at this size. It would need a columnar engine (Spark, DuckDB) at tens of millions of rows.
