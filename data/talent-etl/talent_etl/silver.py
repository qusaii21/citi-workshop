"""
Silver layer: validated, typed, deduplicated data built from Bronze, updated incrementally.

One run:
    1. Resolve the Bronze snapshot date: ``--ingest-date``, or the latest date for
       which all seven PostgreSQL snapshots exist.
    2. Decide between an incremental update and a full rebuild. A full rebuild happens
       only when requested (``--full-refresh``), when nothing is published yet, or when
       the validation rules changed since the published run (``rules_fingerprint``).
    3. Plan every entity without writing anything (parents first):
         - PostgreSQL tables arrive as full snapshots, so the unit of change is the table:
           an unchanged snapshot (same content hash) whose parents did not change is
           reused as-is; otherwise it is revalidated, and rewritten only if its Silver
           output actually changed.
         - learning_activity is updated file by file (see talent_etl.silver_learning).
    4. All-or-nothing: if any entity fails validation, nothing is written or published.
    5. Write the new and changed files into a NEW run folder and read each one back.
       Unchanged datasets and partitions are not copied: the pointer keeps referencing
       the files written by earlier runs.
    6. Publish atomically by replacing ``silver/_current.json`` (one PUT = the commit).
    7. Delete files that neither the current nor the previous pointer references.

Readers resolve Silver through the pointer (``read_current`` / ``current_files``) and so
always see one complete, consistent version. A crash anywhere before step 6 leaves the
published Silver exactly as it was.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa

from talent_etl.bronze import SOURCE_TABLES
from talent_etl.config import Settings
from talent_etl.contracts import CONTRACTS, QUARANTINE_SCHEMA, EntityContract
from talent_etl.definitions import CRITICAL_GAP_RATIO, MAX_QUARANTINE_RATE
from talent_etl.dq_rules import RULES
from talent_etl.io_s3 import S3Client
from talent_etl.logging_utils import log_step
from talent_etl.publish import ABANDONED_RUN_AGE, Layer, collect_garbage, mark_started
from talent_etl.paths import (
    BRONZE,
    SILVER,
    bronze_postgres_key,
    silver_current_key,
    silver_key,
    silver_manifest_key,
    silver_quarantine_key,
    silver_run_prefix,
)
from talent_etl.silver_learning import LearningIncremental, PreviousLearning, load_previous
from talent_etl.validation import (
    EntityReport,
    RawRecord,
    RecordOutcome,
    ValidationContext,
    hire_dates_from,
    validate_entity,
)

logger = logging.getLogger(__name__)

_SNAPSHOT_KEY = re.compile(r"^bronze/postgres/([a-z_]+)/ingest_date=(\d{4}-\d{2}-\d{2})/part-00000\.parquet$")
_LINEAGE = ("_run_id", "_processed_at", "_bronze_ingest_date", "_quarantined_at")

SILVER_LAYER = Layer(SILVER)

POINTER_FORMAT_VERSION = 2
# Bump when code changes how records are validated or transformed without changing a rule,
# contract or definition; a new fingerprint forces the next run to rebuild in full.
LOGIC_VERSION = 4


class SilverSourceError(RuntimeError):
    """The Bronze input needed for Silver is missing."""


class SilverValidationError(RuntimeError):
    """At least one entity failed validation; nothing was written or published."""


class SilverIntegrityError(RuntimeError):
    """A Silver dataset does not match what validation accepted; the run was not published."""


def rules_fingerprint() -> str:
    """Fingerprint of everything that decides Silver content: rules, contracts and definitions."""
    payload = json.dumps({
        "logic_version": LOGIC_VERSION,
        "rules": [asdict(rule) for rule in RULES],
        "contracts": repr(list(CONTRACTS.values())),
        "critical_gap_ratio": str(CRITICAL_GAP_RATIO),
        "max_quarantine_rate": MAX_QUARANTINE_RATE,
    }, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


@dataclass
class EntityResult:
    """Outcome for one entity in a Silver run."""

    entity: str
    status: str                                   # SUCCEEDED | FAILED | SKIPPED
    mode: str = ""                                # reused | unchanged | processed | incremental | full
    counts: dict[str, int] = field(default_factory=dict)
    reason_counts: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    dataset_issues: list[str] = field(default_factory=list)
    outputs: dict[str, int] = field(default_factory=dict)   # published data key -> rows
    quarantine_key: str | None = None
    quarantine_rows: int = 0
    written: list[str] = field(default_factory=list)        # keys written by this run
    stats: dict[str, int] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)   # dataset-level informational metrics


@dataclass
class SilverResult:
    """Outcome of one Silver run (also the manifest content)."""

    run_id: str
    status: str                       # SUCCEEDED | FAILED | SUPERSEDED
    published: bool
    full_refresh: bool
    full_refresh_reason: str | None
    bronze_ingest_date: str
    started_at: str
    lake_bucket: str
    run_prefix: str
    entities: list[EntityResult]
    manifest_key: str
    rules_fingerprint: str
    error: str | None = None


@dataclass(frozen=True)
class SilverPointer:
    """Content of ``silver/_current.json``: the one published Silver version."""

    run_id: str
    run_prefix: str
    bronze_ingest_date: str
    started_at: str
    published_at: str
    manifest_key: str
    previous_run_id: str | None
    files: dict[str, list[str]]           # entity -> data keys (may live in earlier run folders)
    quarantine: dict[str, str]            # entity -> quarantine key
    rows: dict[str, int]                  # entity -> rows
    file_rows: dict[str, int] = field(default_factory=dict)   # data key -> rows
    format_version: int = 1
    rules_fingerprint: str | None = None
    state: dict[str, Any] = field(default_factory=dict)       # incremental state per entity

    def referenced_keys(self) -> set[str]:
        """Every object a reader or the next incremental run may need."""
        keys = {k for ks in self.files.values() for k in ks} | set(self.quarantine.values())
        learning = self.state.get("learning", {})
        return keys | {learning[k] for k in ("ledger_key", "index_key") if k in learning}


def read_current(s3: S3Client, bucket: str) -> SilverPointer | None:
    """Return the published Silver pointer, or None if nothing has been published yet."""
    if not s3.object_exists(bucket, silver_current_key()):
        return None
    raw = json.loads(s3.get_bytes(bucket, silver_current_key()))
    known = {f.name for f in fields(SilverPointer)}
    return SilverPointer(**{k: v for k, v in raw.items() if k in known})


def current_files(s3: S3Client, bucket: str, entity: str) -> list[str]:
    """
    Return the Parquet keys of an entity in the published Silver version (what readers load).

    Raises:
        SilverSourceError: If no Silver version has been published.
    """
    pointer = read_current(s3, bucket)
    if pointer is None:
        raise SilverSourceError(f"no published Silver in s3://{bucket}/{silver_current_key()}; run --step silver")
    return pointer.files[entity]


def latest_complete_snapshot(s3: S3Client, bucket: str) -> date:
    """
    Return the latest ingest date for which all seven PostgreSQL snapshots exist in Bronze.

    Raises:
        SilverSourceError: If no date has a complete set of snapshots.
    """
    tables_by_date: dict[str, set[str]] = defaultdict(set)
    for obj in s3.list_objects(bucket, f"{BRONZE}/postgres/"):
        match = _SNAPSHOT_KEY.match(obj.key)
        if match:
            tables_by_date[match.group(2)].add(match.group(1))
    complete = [day for day, tables in tables_by_date.items() if set(SOURCE_TABLES) <= tables]
    if not complete:
        raise SilverSourceError(f"no complete Bronze snapshot in s3://{bucket}/{BRONZE}/postgres/; run --step bronze")
    return date.fromisoformat(max(complete))


def build_silver_rows(contract: EntityContract, outcomes: Iterable[RecordOutcome], *, run_id: str,
                      snapshot: date, processed_at: datetime) -> list[dict[str, Any]]:
    """Turn accepted outcomes into Silver rows (values, derived, lineage), in a deterministic order."""
    rows = []
    for outcome in outcomes:
        row = {name: outcome.values[name] for name in contract.column_names}
        row |= {d.name: outcome.derived.get(d.name) for d in contract.derived_columns}
        row |= {"_run_id": run_id, "_bronze_ingest_date": snapshot, "_source_object": outcome.record.source_object,
                "_processed_at": processed_at, "_dq_warnings": outcome.warnings}
        if contract.source_system == "s3_jsonl":
            row["_source_line"] = outcome.record.source_line
        rows.append(row)
    order = ("event_date", "event_id") if contract.source_system == "s3_jsonl" else contract.primary_key
    rows.sort(key=lambda r: tuple(r[name] for name in order))
    return rows


def _content_hash(rows: list[dict[str, Any]]) -> str:
    """Hash rows ignoring lineage columns, so unchanged content gives the same hash across runs."""
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps({k: v for k, v in row.items() if k not in _LINEAGE},
                                 sort_keys=True, default=str).encode())
    return digest.hexdigest()


def _table_hash(table: pa.Table) -> str:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table.combine_chunks())
    return hashlib.sha256(sink.getvalue()).hexdigest()


@dataclass
class _PgPlan:
    result: EntityResult
    report: EntityReport | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    write: bool = False
    changed: bool = False
    source_hash: str = ""
    output_hash: str = ""


class SilverBuilder:
    """Update Silver from one Bronze snapshot and publish the new version atomically."""

    def __init__(
        self,
        settings: Settings,
        s3: S3Client,
        *,
        run_id: str,
        ingest_date: date | None = None,
        full_refresh: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._s3 = s3
        self._lake = settings.s3.lake_bucket
        self._run_id = run_id
        self._requested_date = ingest_date
        self._force_full = full_refresh
        self._clock = clock
        self._started_at = clock()
        self._fingerprint = rules_fingerprint()

    def run(self) -> SilverResult:
        """
        Plan all entities, write new and changed files, then publish by replacing the pointer.

        Raises:
            SilverSourceError: Bronze input is missing.
            SilverValidationError: An entity failed or was skipped; nothing written or published.
            SilverIntegrityError: Written data does not match validated data; not published.
            Exception: Any write error; the run is recorded as FAILED and not published.
        """
        snapshot = self._resolve_snapshot()
        current = read_current(self._s3, self._lake)
        full, reason = self._full_refresh_reason(current)
        logger.info("silver mode", extra={"context": {"full_refresh": full, "reason": reason,
                                                      "previous_run_id": current.run_id if current else None}})
        base = None if full else current
        pg_plans, learning, learning_plan, results = self._plan(snapshot, base)
        if any(r.status != "SUCCEEDED" for r in results):
            failed = [f"{r.entity}: {r.status} {r.failures}" for r in results if r.status != "SUCCEEDED"]
            self._write_manifest(self._result("FAILED", snapshot, results, full, reason, error="; ".join(failed)))
            raise SilverValidationError("Silver not written; " + "; ".join(failed))

        mark_started(self._s3, self._lake, SILVER_LAYER, self._run_id, self._started_at)
        try:
            for name, plan in pg_plans.items():
                self._write_postgres(CONTRACTS[name], plan, snapshot, base)
            learning_output = learning.apply(learning_plan)
            learning_result = results[-1]
            learning_result.outputs = {k: learning_output.partition_rows[k] for k in learning_output.partition_keys.values()}
            learning_result.quarantine_key = learning_output.quarantine_key
            learning_result.quarantine_rows = learning_output.quarantine_rows
            learning_result.written = learning_output.written
            pointer = self._pointer(snapshot, current, results, pg_plans, learning_plan, learning_output)
            self._verify(pointer, results)
            latest = read_current(self._s3, self._lake)
        except Exception as exc:
            try:
                self._write_manifest(self._result("FAILED", snapshot, results, full, reason,
                                                  error=f"{type(exc).__name__}: {exc}"))
            except Exception:  # noqa: BLE001 - the original error matters more
                logger.exception("could not write FAILED manifest")
            raise

        if latest is not None and datetime.fromisoformat(latest.started_at) > self._started_at:
            result = self._result("SUPERSEDED", snapshot, results, full, reason)
            self._write_manifest(result)
            logger.warning("newer Silver run already published; this run is not published",
                           extra={"context": {"current_run_id": latest.run_id}})
            return result

        result = self._result("SUCCEEDED", snapshot, results, full, reason)
        self._write_manifest(result)
        self._s3.put_bytes(self._lake, silver_current_key(), json.dumps(asdict(pointer), indent=2).encode(),
                           content_type="application/json")
        result.published = True
        logger.info("silver published", extra={"context": {
            "run_id": self._run_id, "previous_run_id": pointer.previous_run_id, "full_refresh": full,
            "written": sum(len(r.written) for r in results)}})
        self._collect_garbage(pointer, current)
        return result

    # ---- mode and input -------------------------------------------------------------------

    def _full_refresh_reason(self, current: SilverPointer | None) -> tuple[bool, str | None]:
        if self._force_full:
            return True, "requested (--full-refresh)"
        if current is None:
            return True, "nothing published yet"
        if current.format_version < POINTER_FORMAT_VERSION or "learning" not in current.state:
            return True, "published version has no incremental state"
        if current.rules_fingerprint != self._fingerprint:
            return True, f"rules changed ({current.rules_fingerprint} -> {self._fingerprint})"
        return False, None

    def _resolve_snapshot(self) -> date:
        if self._requested_date is None:
            snapshot = latest_complete_snapshot(self._s3, self._lake)
        else:
            snapshot = self._requested_date
            missing = [t for t in SOURCE_TABLES
                       if not self._s3.object_exists(self._lake, bronze_postgres_key(t, snapshot))]
            if missing:
                raise SilverSourceError(f"Bronze snapshot {snapshot} is missing tables {missing}; run --step bronze")
        logger.info("bronze snapshot selected", extra={"context": {
            "ingest_date": snapshot.isoformat(), "requested": self._requested_date is not None}})
        return snapshot

    # ---- planning (no writes) ------------------------------------------------------------

    def _plan(self, snapshot: date, base: SilverPointer | None):
        context = ValidationContext(ingest_date=snapshot)
        pg_plans: dict[str, _PgPlan] = {}
        results: list[EntityResult] = []
        failed: set[str] = set()
        changed: set[str] = set()
        changed_employees: set[int] = set()
        learning = learning_plan = None
        for name, contract in CONTRACTS.items():
            failed_parents = sorted(fk.references for fk in contract.foreign_keys if fk.references in failed)
            if failed_parents:
                logger.error("entity skipped: parent failed", extra={"context": {
                    "entity": name, "failed_parents": failed_parents}})
                failed.add(name)
                results.append(EntityResult(name, "SKIPPED", failures=[f"parent failed: {failed_parents}"]))
                continue
            if contract.source_system == "postgres":
                plan = self._plan_postgres(contract, snapshot, context, base, changed)
                pg_plans[name] = plan
                results.append(plan.result)
                if plan.result.status != "SUCCEEDED":
                    failed.add(name)
                    continue
                if plan.changed:
                    changed.add(name)
                    if name == "employees" and base is not None:
                        changed_employees = self._employee_changes(base, context.hire_dates)
                continue
            with log_step(logger, "silver.plan.learning_activity") as metrics:
                previous = load_previous(self._s3, self._lake, base) if base else PreviousLearning()
                learning = LearningIncremental(
                    self._s3, self._lake, run_id=self._run_id, started_at=self._started_at, snapshot=snapshot,
                    context=context, previous=previous, changed_employees=changed_employees, full=base is None,
                    silver_rows=lambda c, outcomes, snap: build_silver_rows(
                        c, outcomes, run_id=self._run_id, snapshot=snap, processed_at=self._started_at),
                )
                learning_plan = learning.plan()
                metrics.update(learning_plan.stats)
            results.append(EntityResult(
                name, "FAILED" if learning_plan.failures else "SUCCEEDED",
                mode="full" if base is None else "incremental", counts=learning_plan.counts,
                reason_counts=learning_plan.reason_counts, failures=[i.detail for i in learning_plan.failures],
                dataset_issues=[i.detail for i in learning_plan.report.dataset_issues], stats=learning_plan.stats,
                metrics={"history": learning_plan.metrics},
            ))
            for issue in learning_plan.failures:
                logger.error("entity failed validation", extra={"context": {
                    "entity": name, "rule": issue.rule.rule_id, "detail": issue.detail}})
        return pg_plans, learning, learning_plan, results

    def _plan_postgres(self, contract: EntityContract, snapshot: date, context: ValidationContext,
                       base: SilverPointer | None, changed: set[str]) -> _PgPlan:
        name = contract.name
        with log_step(logger, f"silver.plan.{name}") as metrics:
            table = self._s3.read_arrow(self._lake, bronze_postgres_key(name, snapshot))
            source = table.select([c for c in table.column_names if not c.startswith("_")])
            if all(c in source.column_names for c in contract.primary_key):
                source = source.sort_by([(c, "ascending") for c in contract.primary_key])
            source_hash = _table_hash(source)
            previous = base.state["postgres"].get(name) if base else None
            parents_changed = any(fk.references in changed for fk in contract.foreign_keys)

            if previous and previous["source_hash"] == source_hash and not parents_changed:
                plan = _PgPlan(EntityResult(name, "SUCCEEDED", mode="reused", counts=previous["counts"],
                                            reason_counts=previous["reason_counts"]),
                               source_hash=source_hash, output_hash=previous["output_hash"])
                self._load_reference(contract, base, context)
            else:
                records = [RawRecord({k: v for k, v in row.items() if not k.startswith("_")}, contract.source_object)
                           for row in table.to_pylist()]
                report = validate_entity(contract, records, context, source_columns=table.column_names,
                                         source_types={f.name: f.type for f in table.schema})
                result = EntityResult(
                    name, "FAILED" if report.failed else "SUCCEEDED", counts=report.counts(),
                    reason_counts=dict(sorted(report.reason_counts().items())),
                    failures=[i.detail for i in report.failures],
                    dataset_issues=[i.detail for i in report.dataset_issues])
                for issue in report.failures:
                    logger.error("entity failed validation", extra={"context": {
                        "entity": name, "rule": issue.rule.rule_id, "detail": issue.detail}})
                plan = _PgPlan(result, report=report, source_hash=source_hash)
                if not report.failed:
                    plan.rows = build_silver_rows(contract, report.accepted, run_id=self._run_id,
                                                  snapshot=snapshot, processed_at=self._started_at)
                    plan.output_hash = _content_hash(plan.rows + report.quarantine_records())
                    context.reference_keys[name] = report.accepted_keys()
                    if name == "employees":
                        context.hire_dates = hire_dates_from(report)
                    if previous and previous["output_hash"] == plan.output_hash:
                        result.mode = "unchanged"   # revalidated, same output: keep the published files
                    else:
                        result.mode, plan.write, plan.changed = "processed", True, True
            metrics.update(mode=plan.result.mode, **plan.result.counts)
            return plan

    def _load_reference(self, contract: EntityContract, base: SilverPointer, context: ValidationContext) -> None:
        """Load an unchanged parent's accepted keys (and hire dates) from its published Silver file."""
        if not any(fk.references == contract.name for c in CONTRACTS.values() for fk in c.foreign_keys):
            return
        (pk,) = contract.primary_key
        columns = [pk, "hire_date"] if contract.name == "employees" else [pk]
        table = pa.concat_tables([self._s3.read_arrow(self._lake, k).select(columns) for k in base.files[contract.name]])
        context.reference_keys[contract.name] = set(table[pk].to_pylist())
        if contract.name == "employees":
            context.hire_dates = dict(zip(table["employee_id"].to_pylist(), table["hire_date"].to_pylist()))

    def _employee_changes(self, base: SilverPointer, new_hire_dates: dict[int, date]) -> set[int]:
        """Employees added, removed or with a changed hire date since the published version."""
        old = pa.concat_tables([self._s3.read_arrow(self._lake, k).select(["employee_id", "hire_date"])
                                for k in base.files["employees"]])
        old_hire_dates = dict(zip(old["employee_id"].to_pylist(), old["hire_date"].to_pylist()))
        return {e for e in old_hire_dates.keys() | new_hire_dates.keys()
                if old_hire_dates.get(e) != new_hire_dates.get(e)}

    # ---- writing ----------------------------------------------------------------------------

    def _write_postgres(self, contract: EntityContract, plan: _PgPlan, snapshot: date,
                        base: SilverPointer | None) -> None:
        result = plan.result
        if not plan.write:
            result.outputs = {k: base.file_rows[k] for k in base.files[contract.name]}
            result.quarantine_key = base.quarantine[contract.name]
            result.quarantine_rows = result.counts["quarantined"]
            return
        with log_step(logger, f"silver.write.{contract.name}") as metrics:
            schema = contract.silver_file_schema()
            key = silver_key(contract.name, self._run_id)
            table = pa.Table.from_pylist([{n: r[n] for n in schema.names} for r in plan.rows], schema=schema)
            self._write_checked(table, key, schema)
            result.outputs = {key: table.num_rows}
            quarantine = pa.Table.from_pylist([
                row | {"_run_id": self._run_id, "_bronze_ingest_date": snapshot, "_quarantined_at": self._started_at}
                for row in plan.report.quarantine_records()
            ], schema=QUARANTINE_SCHEMA)
            result.quarantine_key = silver_quarantine_key(contract.name, self._run_id)
            self._write_checked(quarantine, result.quarantine_key, QUARANTINE_SCHEMA)
            result.quarantine_rows = quarantine.num_rows
            result.written = [key, result.quarantine_key]
            metrics.update(rows_written=table.num_rows, quarantine_rows=quarantine.num_rows)

    def _write_checked(self, table: pa.Table, key: str, schema: pa.Schema) -> None:
        _check_not_null(table, key)
        self._s3.write_arrow(table, self._lake, key)
        written = self._s3.read_arrow(self._lake, key)
        if written.num_rows != table.num_rows or not written.schema.equals(schema):
            raise SilverIntegrityError(f"{key}: read back {written.num_rows} rows / {written.schema}")

    def _pointer(self, snapshot, current, results, pg_plans, learning_plan, learning_output) -> SilverPointer:
        by_name = {r.entity: r for r in results}
        state = {
            "postgres": {name: {"source_hash": plan.source_hash, "output_hash": plan.output_hash,
                                "counts": plan.result.counts, "reason_counts": plan.result.reason_counts}
                         for name, plan in pg_plans.items()},
            "learning": {"ledger_key": learning_output.ledger_key, "index_key": learning_output.index_key,
                         "metrics": learning_plan.metrics},
        }
        return SilverPointer(
            run_id=self._run_id, run_prefix=silver_run_prefix(self._run_id),
            bronze_ingest_date=snapshot.isoformat(), started_at=self._started_at.isoformat(),
            published_at=self._clock().isoformat(), manifest_key=silver_manifest_key(self._run_id),
            previous_run_id=current.run_id if current else None,
            files={name: sorted(r.outputs) for name, r in by_name.items()},
            quarantine={name: r.quarantine_key for name, r in by_name.items()},
            rows={name: sum(r.outputs.values()) for name, r in by_name.items()},
            file_rows={k: n for r in results for k, n in r.outputs.items()},
            format_version=POINTER_FORMAT_VERSION, rules_fingerprint=self._fingerprint, state=state,
        )

    def _verify(self, pointer: SilverPointer, results: list[EntityResult]) -> None:
        """Before publishing: every referenced object exists and rows reconcile with validation."""
        present = {obj.key for obj in self._s3.list_objects(self._lake, f"{SILVER}/runs/")}
        missing = sorted(pointer.referenced_keys() - present)
        if missing:
            raise SilverIntegrityError(f"published version would reference missing objects: {missing[:5]}")
        for result in results:
            if sum(result.outputs.values()) != result.counts["accepted"]:
                raise SilverIntegrityError(f"{result.entity}: {sum(result.outputs.values())} rows in files, "
                                           f"{result.counts['accepted']} accepted")

    # ---- manifest, publishing and cleanup -----------------------------------------------

    def _result(self, status, snapshot, results, full, reason, error=None) -> SilverResult:
        return SilverResult(
            run_id=self._run_id, status=status, published=False, full_refresh=full, full_refresh_reason=reason,
            bronze_ingest_date=snapshot.isoformat(), started_at=self._started_at.isoformat(), lake_bucket=self._lake,
            run_prefix=silver_run_prefix(self._run_id), entities=results,
            manifest_key=silver_manifest_key(self._run_id), rules_fingerprint=self._fingerprint, error=error,
        )

    def _write_manifest(self, result: SilverResult) -> None:
        self._s3.put_bytes(self._lake, result.manifest_key,
                           json.dumps(asdict(result), indent=2, default=str).encode(),
                           content_type="application/json")
        logger.info("silver manifest written", extra={"context": {"key": result.manifest_key, "status": result.status}})

    def _collect_garbage(self, pointer: SilverPointer, previous: SilverPointer | None) -> int:
        """Delete Silver files that neither the current nor the previous version references."""
        retained = pointer.referenced_keys() | (previous.referenced_keys() if previous else set())
        return collect_garbage(self._s3, self._lake, SILVER_LAYER, retained, self._clock())


def _check_not_null(table: pa.Table, label: str) -> None:
    """Raise if a non-nullable field contains nulls (Arrow does not enforce nullability on write)."""
    offenders = [f.name for f in table.schema if not f.nullable and table.column(f.name).null_count]
    if offenders:
        raise SilverIntegrityError(f"{label}: nulls in non-nullable columns {offenders}")
