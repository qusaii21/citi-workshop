# talent-etl

Batch pipeline for ACME's performance and talent data. It ingests from two sources into a medallion data lake (Bronze → Silver → Gold) on S3:

- **PostgreSQL** (schema `hr_source`): teams, competencies, employees, projects, performance_reviews, competency_assessments, talent_review
- **S3** (`s3://$SOURCE_BUCKET/learning_activity/year=/month=/day=/*.jsonl`): learning events

> Status: source seeding, the shared foundation, **Bronze**, **Silver** and **Gold** are done and tested locally (PostgreSQL + LocalStack). The **analysis notebook** answers the seven business questions. Performance tests and cloud deployment come next. Silver uses the business rules in [docs/business_definitions.md](docs/business_definitions.md). A1 (critical gap = missing at least half of the required level) is approved; A2–A18 are proposed defaults that are not yet approved.

## Specifications

| Document | Contents |
|---|---|
| [docs/schema_contracts.md](docs/schema_contracts.md) | Source vs Silver schema per entity: types, keys, constraints, parsing rules, drift handling, quarantine format |
| [docs/data_quality.md](docs/data_quality.md) | Validation matrix (rule, severity, logic, action), how every known defect is handled, expected counts |
| [docs/source_to_target.md](docs/source_to_target.md) | Source → Bronze → Silver paths, column mappings, dedup, partitioning |
| [docs/business_definitions.md](docs/business_definitions.md) | Competency gap, valid reviews, pre-hire treatment, learning completion, exclusions, **assumptions needing approval** |
| [docs/gold_design.md](docs/gold_design.md) | Gold tables per business question: grain, keys, logic, reconciliation checks, Gold assumptions A19–A24 |

## Layout

```
talent-etl/
  job.py                 entry point (python job.py --step check|seed|bronze|silver|gold|serve|all)
  talent_etl/
    config.py            env-var configuration (local vs cloud)
    paths.py             data lake key conventions shared by all layers
    bronze.py            Bronze ingestion (PostgreSQL snapshots + JSONL mirror + manifest)
    silver.py            Silver build: validate Bronze, write typed datasets + quarantine + manifest
    silver_learning.py   incremental learning_activity processing (file ledger, event index)
    dq_report.py         data-quality tables from the published manifest (for the notebook)
    gold_tables.py       Gold table contracts, builders and reconciliation checks
    gold.py              Gold build: fingerprint-based reuse, validation, atomic publish
    serving.py           dashboard data (serving/): the notebook's figures and a paged data-lake catalog, reconciled
    explore.py           read-only access to every layer (Bronze latest ingest, published Silver and Gold)
    publish.py           shared versioned-layer helpers (pointer, manifests, started marker, cleanup)
    seed.py              load the sample data into PostgreSQL + S3 (used by the cloud job)
    contracts.py         schema contracts for all 8 entities (source + Silver schema, keys, constraints)
    dq_rules.py          data quality rule registry (id, reason code, severity, action)
    validation.py        contract validation engine: accept / warn / deduplicate / quarantine / fail
    parsing.py           strict, lossless value parsers (int, decimal, date, timestamp, ...)
    definitions.py       business definitions (competency gap, review periods, pre-hire, trends)
    logging_utils.py     JSON logs with run_id; log_step() timing/status/metrics
    retry.py             exponential backoff with jitter; transient vs permanent errors
    io_pg.py             PostgreSQL client (reconnect + retry, quoted identifiers)
    io_s3.py             S3 client (LocalStack or AWS, bytes + Parquet, retry)
  notebooks/talent_insights.ipynb  analysis notebook: the seven business questions, from published Gold
  notebooks/explore_lake.ipynb     browse every Bronze, Silver (incl. quarantine) and Gold table
  docs/                  specifications (see above)
  scripts/seed_sources.sh  loads the provided sample data into both sources
  tests/unit/            no external services needed
  tests/integration/     local PostgreSQL + LocalStack (skipped when unreachable)
```

## Run locally

Prerequisites: `./bin/start-dev.sh` is running (PostgreSQL on 5432, LocalStack on 4566).

```sh
cd data/talent-etl
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# 1. Seed both sources from sampleData/ (idempotent; safe to rerun)
./scripts/seed_sources.sh

# 2. Verify the job can reach both sources
.venv/bin/python job.py --step check

# 3. Land both sources in Bronze (default ingest date: today in UTC)
.venv/bin/python job.py --step bronze --ingest-date 2026-10-14

# 4. Validate Bronze and rebuild Silver (default: latest complete Bronze snapshot)
.venv/bin/python job.py --step silver

# 5. Build the curated Gold tables from the published Silver version
.venv/bin/python job.py --step gold          # or: --step all  (bronze -> silver -> gold -> serve)

# 5b. Publish the web dashboard's data from the published Gold version
.venv/bin/python job.py --step serve

# 6. Run the analysis notebook top to bottom (writes the outputs into the notebook)
.venv/bin/jupyter execute --inplace notebooks/talent_insights.ipynb
#    or open it in VS Code / Jupyter with the .venv kernel

# 7. Tests
.venv/bin/python -m pytest --cov --cov-report=term-missing
.venv/bin/python -m pytest -m "not integration"   # unit tests only
```

### How seeding works

`seed_sources.sh` runs the **provided** `schema.sql` and `load.sh` from the sample zip without changing them. It only wraps them as follows:

- It loads into a dedicated schema (`hr_source`, set through `PGOPTIONS=search_path`) and drops and recreates that schema each run. The provided `load.sh` fails on a second run because the tables already exist.
- It checks each table's row count against its CSV line count.
- It syncs `learning_activity/` to S3 with `--delete`, then checks the object count. The zip has no S3 loader of its own.

## Bronze layer

The Bronze layer holds the raw source data in the lake bucket (`LAKE_BUCKET`). Nothing is cleaned, cast or dropped here; that is Silver's job.

### Paths and formats

| Source | Bronze key | Format |
|---|---|---|
| PostgreSQL `hr_source.<table>` (7 tables) | `bronze/postgres/<table>/ingest_date=YYYY-MM-DD/part-00000.parquet` | Parquet, one object per table per ingest date |
| S3 `s3://$SOURCE_BUCKET/learning_activity/<rel>` | `bronze/learning_activity/<rel>`, e.g. `.../year=2024/month=02/day=28/events-000.jsonl` | Original JSONL, byte-for-byte |
| Run audit | `bronze/_manifests/ingest_date=YYYY-MM-DD/run_id=<run_id>.json` | JSON |

**PostgreSQL snapshots** keep every source column with the same name, order and values. Types are lossless: `INTEGER` becomes `int32`, `TEXT` becomes `string`, `DATE` becomes `date32`, and `NUMERIC(5,2)` becomes `decimal128(5,2)` rather than a float. Nullable integers stay integers. An empty table is written as an empty Parquet file that keeps its schema, and a WARN is logged.

**Learning activity** files are copied server-side, so the object bodies, sizes and MD5 ETags are identical to the source. The `year=/month=/day=` partitions are kept exactly. Malformed lines, and the one empty file (`year=2025/month=09/day=03`), land unchanged and are left for Silver to handle. An empty file is logged as a WARN and recorded in the manifest. It is not fatal. A file outside a day partition is also copied, logged as a WARN, and listed in the manifest.

### Metadata

Metadata is always kept separate from source fields:

- **Parquet:** five extra columns with the reserved `_` prefix. A source column that starts with `_` makes the run fail rather than be overwritten.

  | Column | Type | Value |
  |---|---|---|
  | `_run_id` | string | the run that wrote the snapshot |
  | `_ingest_date` | date | the partition date |
  | `_ingested_at` | timestamp (UTC) | one timestamp per run |
  | `_source_system` | string | `postgres` |
  | `_source_object` | string | e.g. `hr_source.employees` |

- **JSONL:** the file bodies cannot change, so lineage is stored as S3 user metadata on each copied object: `run_id`, `ingest_date`, `ingested_at` and `source_uri`.
- **Manifest:** one per run. It records the row, column and byte counts and the key for each table, plus file, byte, copied and skipped counts and the lists of empty or unpartitioned files for learning activity.

### Rerun behaviour (idempotency)

| Output | Strategy | Result of rerunning the same `--ingest-date` |
|---|---|---|
| PostgreSQL snapshot | Deterministic overwrite of the fixed key `part-00000.parquet`, then delete any other object in that partition | Still exactly one object per table and date, with the same row count. Only `_run_id` and `_ingested_at` change. |
| Learning activity | Incremental: copy only when the Bronze object is missing or its size or ETag differs from the source | Nothing is re-copied, and objects keep the lineage metadata of the run that first landed them. New or changed source files are copied on the next run. |
| Manifest | Append-only, keyed by `run_id` | One audit record per run. Manifests are records, not data. |

A new `--ingest-date` adds a new snapshot partition and keeps the earlier ones. Learning files are not partitioned by ingest date because their event-date partitions are already their identity.

### Checks and failure behaviour

- **Preflight runs before anything is written.** If any of the 7 tables is missing or the source S3 prefix is empty, the run fails with `BronzeSourceError`, which names every problem, and the job exits with code 1.
- **Every snapshot is read back.** Its row count and schema must match what was read from PostgreSQL, or the run fails with `BronzeIntegrityError`.
- **Every source file must exist in Bronze with the same size and ETag** after the copy step, or the run fails with `BronzeIntegrityError`.
- **Transient PostgreSQL and S3 errors are retried** with exponential backoff (connection drops, timeouts, throttling, 5xx). Permanent errors such as AccessDenied or a missing key fail immediately.
- **Every step is logged.** Each step logs its start, end, duration, status and row or file counts as JSON, and all lines share one `run_id`.

## Silver layer

Silver holds validated, typed and deduplicated data. Every row in it has passed its contract. The full specification is in [docs/source_to_target.md](docs/source_to_target.md).

Silver is **updated incrementally** (approved A14). A run reprocesses only new or changed Bronze files (plus the files that must be re-decided with them), rewrites only the affected month partitions, and skips PostgreSQL tables whose snapshot did not change. New files go into a new run folder `silver/runs/run_id=<run_id>/`, unchanged files are referenced rather than copied, and the version becomes visible only when the single pointer object `silver/_current.json` is replaced, which is an atomic commit. **Always read Silver through the pointer:**

```python
from talent_etl.silver import current_files, read_current
pointer = read_current(s3, bucket)                    # which run is published, rows per entity
keys = current_files(s3, bucket, "learning_activity") # the exact Parquet keys to load
```

| Output | Key (inside the published run folder `R`) | Rows (sample data) |
|---|---|---|
| Pointer: the published run, file lists, row counts | `silver/_current.json` | |
| 7 PostgreSQL entities | `silver/runs/run_id=R/<entity>/part-00000.parquet` | 100 / 40 / 2,000 / 33 / 4,000 / 4,000 / 2,000 |
| Learning activity | `.../learning_activity/event_year=YYYY/event_month=MM/part-00000.parquet` (37 partitions; each may come from a different run folder) | 29,574 |
| Incremental state (file ledger, event index) | `silver/runs/run_id=R/_state/learning_files.parquet`, `learning_index.parquet` | 783 files / 29,703 events |
| Quarantine (one per entity, always written, may be empty) | `silver/runs/run_id=R/_quarantine/<entity>/part-00000.parquet` | 142 (learning activity only) |
| Run manifest (`SUCCEEDED`, `FAILED` or `SUPERSEDED`, with per-entity counts and reason counts) | `silver/_manifests/run_id=<run_id>.json` | |

- **Input:** the `--ingest-date` Bronze snapshot, or by default the latest date that has all 7 tables, plus every learning-activity file in Bronze.
- **Columns:**
  - the contract columns, cast losslessly
  - derived columns: `is_pre_hire`, `gap`, `is_critical_gap` (missing at least half of the required level), period bounds
  - lineage columns: `_run_id`, `_bronze_ingest_date`, `_source_object`, `_source_line`, `_processed_at`
  - `_dq_warnings`: the warning reason codes that fired on the row
- **Partition columns live in the path only.** `event_year` and `event_month` are not stored inside the files, so `pd.read_parquet("<run folder>/learning_activity")` reads them back from the path.
- **Incremental runs** (measured on the sample data in LocalStack):

  | Run | Time | Objects written | Rows validated |
  |---|---|---|---|
  | First build (full) | 4.47 s | 54 | 29,868 |
  | Rerun, no new data | 0.32 s | 0 | 0 |
  | One new day (38 events) | 0.36 s | 4 (1 partition, quarantine, 2 state tables) | 38 |
  | Forced full refresh | 4.48 s | 55 | 29,906 |

  Use `python job.py --step silver --full-refresh` to rebuild everything. This happens automatically when the rules fingerprint changes. **Bump `LOGIC_VERSION` in `silver.py` when you change validation code without changing a rule or contract.**
- **Guarantee:** an incremental run publishes exactly what a full rebuild of the same Bronze would, apart from lineage columns. This is tested scenario by scenario.
- **History metrics (A13):** completions and certifications always count. How often the earlier step is missing (BUS-010 `completion_without_enrollment`, BUS-011 `certification_without_completion`) is reported per run in the manifest, next to the exact random-order baseline. It is not attached to rows. `talent_etl.dq_report` reads it, together with per-entity quality counts, for the notebook's data-quality section.
- **Crash safety:** a crash at any point before the pointer is replaced leaves published Silver exactly as it was; readers never see a half-written run. The next successful run deletes the leftover folder.
- **Retention:** every file referenced by the current or previous published version is kept (rollback = point `_current.json` back at the previous version). Other files of finished or failed runs, and run folders abandoned for more than 24 hours, are deleted after each publish. Manifests are kept.
- **Ordering guard:** a run never publishes over a run that started later (it is recorded as `SUPERSEDED`).
- **Failure:** all-or-nothing. If any entity fails a FAIL rule (missing column, zero valid rows, more than 5% quarantined), or any write or check fails, nothing is published, a `FAILED` manifest is written, and the job exits with code 1. Children of a failed parent are reported as `SKIPPED`.
- **Checks:** every object is read back (row count and schema), non-nullable columns are checked for nulls, and rows written must equal rows accepted.

## Gold layer

Gold holds 10 curated tables answering the business questions. The full specification (grain, keys, logic, checks) is in [docs/gold_design.md](docs/gold_design.md).

| Table | Question | Grain | Rows (sample data) |
|---|---|---|---|
| `dim_employee` | shared | employee | 2,000 |
| `performance_history` | Q1 rating history | employee × period | 4,000 |
| `employee_performance` | Q1 latest rating and trend | employee | 2,000 |
| `employee_skill_gaps` | Q2 skill gaps | employee × competency | 4,000 |
| `competency_gap_summary` | Q2 training needs | competency | 40 |
| `skill_distribution` | Q7 skill distribution | competency × department × level | 1,595 |
| `employee_learning` | Q4 training completed | employee | 2,000 |
| `promotion_pipeline` | Q3 promotion readiness | employee | 2,000 |
| `attrition_risk` | Q5 attrition risk | employee | 2,000 |
| `team_performance` | Q6 team and project outcomes | team | 100 |

- **Input:** the published Silver version only, read through `silver/_current.json`. The as-of date is its Bronze snapshot date, so results are reproducible.
- **Incremental:** each table has a fingerprint of the Silver files it reads, the Gold rules and (where used) the as-of date. Unchanged tables are reused without being rebuilt or copied. For example, new learning data rebuilds only `employee_learning`, `promotion_pipeline` and `attrition_risk`. `--full-refresh` rebuilds all tables, and so does a change to the Gold rules or logic version.
- **Validation:** before anything is written, every rebuilt table passes these checks (69 checks on the sample data):
  - its contract (schema and nulls)
  - its grain (a unique primary key)
  - domain and range checks
  - reconciliation with Silver (row counts and aggregates)

  Any failure means nothing is published and a `FAILED` manifest lists the checks.
- **Publishing:** the same pattern as Silver. New files go into `gold/runs/run_id=<run_id>/`, the version is published by replacing `gold/_current.json` (atomic), and files that the current or previous version reference are kept. Read with `talent_etl.gold.load_gold_table(s3, bucket, "employee_learning")`.
- **Sample-data headlines** (each one checked against an independent measurement):
  - 148 employees are promotion-ready (A19)
  - attrition bands are High 193 / Medium 271 / Low 1,536 (A20)
  - 682 critical skill gaps (approved A1)
  - 2,824 distinct course completions (approved A13)
  - rating trends: 721 improving / 692 stable / 191 declining / 396 insufficient history
  - team rating and project outcome correlate only weakly (r = 0.14 over 27 teams)

## Analysis notebook

[notebooks/talent_insights.ipynb](notebooks/talent_insights.ipynb) answers ACME's seven business questions (quoted verbatim) for HR and business readers. It opens with a traceability table that maps each question to its Gold tables, metrics and chart, and says how fully the data can answer it.

- **Data source:** only the published Gold version, through `talent_etl.gold.load_gold_table` and the same environment configuration as the pipeline. There are no file paths or credentials in the notebook.
- **Data quality and freshness first:**
  - the as-of date and the Gold and Silver versions it reads
  - Silver accept and quarantine counts per entity
  - the learning-sequence metrics with their random baseline
  - every Gold check (69 on the sample data)
- **Each question gets the same structure:**
  - the question, verbatim
  - the business objective
  - the analysis and a table
  - one chart
  - an interpretation
  - assumptions and limitations
- **Promotion readiness is labelled a proposed rule (A19)**, and attrition risk a heuristic (A20). There are no learning funnel rates, and no course-to-competency mapping.
- **Narrative check:** the last cell compares every figure quoted in the text with the data, and fails if the prose is out of date.
- **Tested:** `tests/integration/test_notebook.py` builds a lake from scratch, executes the notebook in a fresh kernel and requires zero errors.

## Web dashboard

The notebook's answers are also published as a web dashboard on the workshop website, reading live from the data lake:

```
Browser -> CloudFront (website_url)
             /                      -> frontend/ (React, deployed by bin/deploy-frontend.sh)
             /api/talent-dashboard* -> backend/talent-dashboard (Python Lambda) -> s3://<lake>/serving/
```

- **Serving step (`job.py --step serve`, part of `all`):** builds two JSON documents from the published Gold version, with the same calculations as the notebook. `summary.json` holds freshness, data quality and the seven questions' aggregates; `employees.json` holds one profile per employee (review history, skill gaps, learning, promotion and attrition signals; no emails or manager comments, as in the notebook). Fourteen reconciliation checks against Gold must pass before `serving/_current.json` is replaced; otherwise a `FAILED` manifest is written and nothing is published. Rerunning for the same Gold version is a no-op; the previous version is kept.
- **Data-lake catalog:** the serving step also writes every Bronze, Silver (including `<entity>.quarantine`) and Gold table, all rows and columns, as JSON pages of 500 rows plus `lake_index.json` (tables, row counts, column types). Silver and Gold row counts must equal their published pointers. The dashboard's "Data lake" section browses it: click a layer, then a table.
- **API (`backend/talent-dashboard`):** `GET /lake`, `GET /lake/<layer>/<table>?page=&size=25|50|100`, `GET /summary`, `GET /employees?list=all|critical_gap|promotion_ready|high_risk|top_learners&q=&department=&page=&size=`, `GET /employees/<id>`, `GET /health`. It reads only the serving pointer and its documents (bucket from `TALENT_LAKE_BUCKET`), caches them per published version, and needs only boto3. Tests: `python -m pytest backend/talent-dashboard/tests`.
- **Frontend (`frontend/`):** one page with the notebook's sections, charts and tables, a header showing the environment, Gold run id, as-of date and bucket it reads, and searchable employee lists with a profile panel.
- **Exposure:** the website is public, and this includes per-employee data (by decision; the sample data is synthetic). Do not point it at real HR data without adding authentication.
- **Deploy:** `./bin/deploy-backend.sh` (Lambda, CloudFront route, and the job that publishes `serving/`), then `./bin/deploy-frontend.sh`.

## Cloud deployment (AWS EKS)

Deployment uses the workshop's own script. Nothing is run by hand inside the cluster.

```sh
export TF_VAR_aws_eks_enabled=true
./bin/deploy-backend.sh                    # from the repository root: refreshes credentials, terraform apply
kubectl logs -f job/coding-workshop-talent-etl
./bin/cleanup-environment.sh               # tear everything down when finished (destroys all data)
```

**What the apply creates for this job** (AWS only; nothing changes under LocalStack):

| Resource | Purpose |
|---|---|
| `coding-workshop-<id>-talent-source-<id>` bucket | Learning-activity source files, plus the sample zip at `seed/…` |
| `coding-workshop-<id>-talent-lake-<id>` bucket | `bronze/`, `silver/` and `gold/` |
| Helm release `talent-etl-python-job` (Kubernetes Job `coding-workshop-talent-etl`) | Runs `job.py` on the EKS node |

**How the job runs in the pod**
- **Code:** the Helm chart ships `job.py` and `requirements.txt`, plus a base64 zip of the `talent_etl/` package. `infra/eks.tf` builds the zip at deploy time, and the pod unpacks it onto `PYTHONPATH`.
- **Configuration:** a Kubernetes Secret provides the environment:
  - `IS_LOCAL=false`
  - the Aurora `POSTGRES_*` settings
  - `AWS_REGION`, `SOURCE_BUCKET`, `LAKE_BUCKET` and `SEED_ZIP_URI`
  - `PIPELINE_STEP=all` and `SEED_IF_MISSING=true`

  The database password is passed to Helm through a values file, never on a command line.
- **What it runs:** `job.py` with no arguments, which means `--step all --seed-if-missing`.
  - On the first run it loads the sample data into Aurora (one transaction) and the source bucket, because Aurora is reachable only from inside the VPC.
  - It then runs Bronze, Silver, Gold and the dashboard's serving step.
  - Later runs skip the seeding and update Silver and Gold incrementally.
- **Image compatibility:** the image (`jupyter/all-spark-notebook`, Python 3.11) ships NumPy-1 builds of `numexpr` and `bottleneck`. `requirements.txt` pins NumPy-2-compatible versions so pandas loads cleanly.
- **Rehearsed locally:** the exact pod command was run in that image against local PostgreSQL and LocalStack. It produced 0 errors and published all 10 Gold tables.

**Infrastructure changes** (small, additive, and reversible with git):
- `infra/helm`: an optional package and an optional env Secret. Other jobs render exactly as before.
- `infra/eks.tf`: package the job and pass its environment.
- `infra/locals.tf`: `data_job_env`.
- `infra/s3.tf`: the two buckets and the sample object.
- `infra/output.tf`: the bucket names.
- `infra/rds.tf`: `ignore_changes` on the externally applied `ams:rt:ams-monitoring-policy` tag, so a deploy no longer strips it from Aurora; Aurora also joins the EKS cluster security group, so pods can reach it (participants cannot add security-group rules).
- `infra/eks.tf`: EKS Pod Identity for the job: the `eks-pod-identity-agent` add-on, a dedicated role `coding-workshop-talent-etl-<id>` with access to the two talent buckets only, and its association with the `talent-etl` service account (the nodes allow one IMDSv2 hop, so pods cannot use the node role).
- `infra/helm`: optional `serviceAccount` for the job; `config.yaml` memory units (`2G`) that KubeSpawner accepts.
- `infra/eks.tf` / `infra/locals.tf`: JupyterHub also allows the participant's directory user by name (the directory group does not exist), and is only redeployed when `config.yaml` changes; `TALENT_LAKE_BUCKET` for the dashboard Lambda.

## Configuration

Every setting comes from environment variables. When `IS_LOCAL=true` (the default), missing values fall back to the workshop's local defaults. S3 is always pointed at LocalStack with `test` credentials, so a local run can never reach real AWS, even if `~/.aws/credentials` holds real keys. When `IS_LOCAL=false`, the job refuses to start and lists every required variable that is missing.

| Variable | Local default | Cloud |
|---|---|---|
| `IS_LOCAL` | `true` | `false` |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | required / `5432` |
| `POSTGRES_NAME` / `POSTGRES_USER` / `POSTGRES_PASS` | `postgres` / `postgres` / `postgres123` | required (`sslmode=require`) |
| `POSTGRES_SOURCE_SCHEMA` | `hr_source` | `hr_source` |
| `AWS_ENDPOINT_URL_S3` (or `AWS_ENDPOINT_URL`) | `http://localhost:4566` | unused |
| `AWS_REGION` | forced to `us-east-1` | required |
| `SOURCE_BUCKET` / `SOURCE_PREFIX` | `acme-talent-source` / `learning_activity` | required / `learning_activity` |
| `LAKE_BUCKET` | `acme-talent-lake` | required |
| `RETRY_ATTEMPTS` / `RETRY_BASE_DELAY_SECONDS` / `RETRY_MAX_DELAY_SECONDS` | `4` / `1` / `30` | same |
| `LOG_LEVEL` | `INFO` | `INFO` |
| `SEED_ZIP_URI` | `sampleData/` zip in the repository | `s3://<source bucket>/seed/…` (set by Terraform) |
| `PIPELINE_STEP` / `SEED_IF_MISSING` | `check` / `false` | `all` / `true` (set by Terraform) |

Exit codes: `0` success, `1` step failed (the error is logged with a traceback), `2` invalid configuration.

The Bronze ingest date is set with `--ingest-date YYYY-MM-DD`. It defaults to today in UTC.

## Known limitations
- **Readers must use the pointer.** Silver is consistent only when read through `silver/_current.json`. Listing `silver/runs/` directly can show the previous run or an in-progress one.
- **The ordering guard is not a lock.** Two Silver runs that finish at almost the same moment can still race on the pointer; the later-started run wins only if it publishes last. Run one Silver job at a time.
- **History metrics are recomputed over the whole accepted event index whenever it changes** (about 30 ms for 29,574 events, O(N log N)). At very large volumes they could be maintained per (employee, course) pair instead.
- **Gold reuse is per table.** A table that reads learning activity is rebuilt in full when any learning partition changes. Gold also loads whole Silver entities into pandas, which is fine at this size but would need a columnar engine at tens of millions of rows.
- **Scale limits of the incremental state.** The event index is one Parquet file loaded into memory (29,703 rows today). At tens of millions of events it should be sharded, for example by month or by hash of `event_id`.
- **Bronze listing.** Every Silver run still lists all Bronze learning files to detect changes. That is cheap (783 keys), but a very large Bronze would want an inventory or event notifications.
- **PostgreSQL tables are read in full each run** to compute the snapshot hash, because Bronze holds full snapshots.
- **Revalidating a child of a changed parent rewrites the whole child table.** For example, if `employees` changes, `performance_reviews` is revalidated in full; only learning activity is narrowed to the affected employees.
- **No transactional snapshot.** The 7 PostgreSQL tables are read one after another in autocommit mode, not inside one transaction. That is fine for this static batch source. A source with live writes would need a `REPEATABLE READ` snapshot to keep the tables consistent with each other.
- **Source deletions are not propagated.** If a source JSONL file is deleted, its Bronze copy remains, because Bronze keeps raw history.
- **One run per date at a time.** Concurrent runs for the same `--ingest-date` are not coordinated. Run one at a time.
- **Same-date snapshots are replaced.** The docs ask for immutable raw directories. Here each ingest-date partition is immutable once its date has passed, but a rerun on the same date replaces that day's snapshot. That gives idempotency at the cost of keeping intra-day history.

- **Cloud packaging.** The Helm chart (`infra/helm/templates/python-job.yaml`) mounts only `job.py` and `requirements.txt` into the pod, so the `talent_etl/` package does not reach EKS yet. A bundling step is planned.
- **Data lake bucket.** No data-lake bucket exists in Terraform yet (`infra/s3.tf`). Cloud mode expects `SOURCE_BUCKET` and `LAKE_BUCKET` to be provided.
