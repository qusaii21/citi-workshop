"""
Incremental Silver for learning_activity (the large, growing fact).

The unit of work is a Bronze source file. Two small state tables, published with every
run, remember what earlier runs did:

    ledger  one row per Bronze file: ETag, record counts, reason counts and the
            month partitions its accepted rows landed in
    index   one row per parsed record that has an event_id: its file, status and,
            for accepted rows, the fields the history metrics need

A run then:
    1. Diffs the Bronze listing against the ledger: new/changed files (ETag) and deleted files.
    2. Expands that set to every file that must be re-decided together with it, using the
       index, until nothing changes (a fixed point):
         - files sharing an event_id with a file being reprocessed or deleted, so the
           duplicate rules (KEY-001/002) again see every copy, in source order;
         - files with events of employees whose master data changed (added, removed,
           hire date changed), so FK and pre-hire results are recomputed.
    3. Re-parses and validates only those files.
    4. Rewrites only the month partitions that gained or lost rows; all other partitions
       keep pointing at the files written by earlier runs.
    5. Recomputes the dataset-level history metrics (BUS-010/011) from the index when the
       index changed, and reuses the published ones otherwise. These metrics are never
       attached to rows, so they never cause a partition to be rewritten.

Correctness target: an incremental run publishes exactly the rows (and quarantine) that a
full rebuild of the same Bronze would, apart from lineage columns. Tests assert it.
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from talent_etl.contracts import CONTRACTS, QUARANTINE_SCHEMA
from talent_etl.dq_rules import Action
from talent_etl.io_s3 import S3Client
from talent_etl.parsing import ParseError, parse_int
from talent_etl.paths import BRONZE, bronze_learning_prefix, silver_month_key, silver_quarantine_key, silver_state_key
from talent_etl.validation import (
    EntityReport,
    HistoryEvent,
    JsonlParseResult,
    ValidationContext,
    learning_history_metrics,
    parse_jsonl,
    validate_entity,
    volume_failures,
)

logger = logging.getLogger(__name__)

ENTITY = "learning_activity"
CONTRACT = CONTRACTS[ENTITY]
ROW_ORDER = [("event_date", "ascending"), ("event_id", "ascending")]

LEDGER_SCHEMA = pa.schema([
    pa.field("source_object", pa.string(), nullable=False),
    pa.field("etag", pa.string(), nullable=False),
    pa.field("size", pa.int64(), nullable=False),
    pa.field("read", pa.int32(), nullable=False),
    pa.field("accepted", pa.int32(), nullable=False),
    pa.field("quarantined", pa.int32(), nullable=False),
    pa.field("deduplicated", pa.int32(), nullable=False),
    pa.field("partitions", pa.list_(pa.string()), nullable=False),
    pa.field("reason_counts", pa.string(), nullable=False),   # JSON {reason_code: count}
])

INDEX_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("source_object", pa.string(), nullable=False),
    pa.field("source_line", pa.int32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),            # accepted | deduplicated | quarantined
    pa.field("employee_id", pa.int32(), nullable=True),
    pa.field("course_name", pa.string(), nullable=True),
    pa.field("event_type", pa.string(), nullable=True),
    pa.field("event_date", pa.timestamp("us"), nullable=True),
    pa.field("partition", pa.string(), nullable=True),          # YYYY-MM, accepted rows only
    pa.field("engine_warnings", pa.list_(pa.string()), nullable=False),
])

_STATUS = {Action.ACCEPT: "accepted", Action.WARN: "accepted",
           Action.DEDUPLICATE: "deduplicated", Action.QUARANTINE: "quarantined"}


def partition_label(year: int, month: int) -> str:
    """Return the ledger/index label of a month partition, e.g. ``2024-02``."""
    return f"{year:04d}-{month:02d}"


@dataclass
class PreviousLearning:
    """What the published run knows about learning_activity (empty for a full rebuild)."""

    ledger: dict[str, dict[str, Any]] = field(default_factory=dict)
    index: list[dict[str, Any]] = field(default_factory=list)
    partition_keys: dict[str, str] = field(default_factory=dict)     # YYYY-MM -> Parquet key
    partition_rows: dict[str, int] = field(default_factory=dict)     # Parquet key -> rows
    quarantine_key: str | None = None
    ledger_key: str | None = None
    index_key: str | None = None
    metrics: dict[str, Any] | None = None                            # published history metrics


@dataclass
class LearningPlan:
    """Everything decided before writing; a failed plan writes nothing."""

    report: EntityReport
    counts: dict[str, int]
    reason_counts: dict[str, int]
    failures: list[Any]
    ledger: dict[str, dict[str, Any]]
    index: list[dict[str, Any]]
    new_rows: dict[str, list[dict[str, Any]]]        # partition -> new Silver rows
    removed_files: set[str]
    affected_partitions: set[str]
    metrics: dict[str, Any]                          # dataset-level history metrics (BUS-010/011)
    state_changed: bool
    stats: dict[str, int]


@dataclass
class LearningOutput:
    """The learning_activity part of the published pointer."""

    partition_keys: dict[str, str]
    partition_rows: dict[str, int]
    quarantine_key: str
    quarantine_rows: int
    ledger_key: str
    index_key: str
    written: list[str]


class LearningIncremental:
    """Plan and apply one incremental update of learning_activity."""

    def __init__(self, s3: S3Client, lake: str, *, run_id: str, started_at: datetime, snapshot: date,
                 context: ValidationContext, previous: PreviousLearning, changed_employees: set[int],
                 full: bool, silver_rows) -> None:
        self._s3 = s3
        self._lake = lake
        self._run_id = run_id
        self._started_at = started_at
        self._snapshot = snapshot
        self._context = context
        self._previous = previous
        self._changed_employees = changed_employees
        self._full = full
        self._silver_rows = silver_rows   # callable(contract, outcomes, snapshot) -> list of row dicts

    # ---- planning -----------------------------------------------------------------------

    def plan(self) -> LearningPlan:
        """Decide which files to reprocess, validate them and compute every change, without writing."""
        listing = {obj.key.removeprefix(f"{BRONZE}/"): obj
                   for obj in self._s3.list_objects(self._lake, bronze_learning_prefix())}
        if not listing:
            from talent_etl.silver import SilverSourceError  # local: avoid import cycle
            raise SilverSourceError(f"no files under s3://{self._lake}/{bronze_learning_prefix()}; run --step bronze")
        prev = self._previous
        changed = {so for so, obj in listing.items()
                   if so not in prev.ledger or prev.ledger[so]["etag"] != obj.etag}
        deleted = set(prev.ledger) - set(listing)

        files_by_id: dict[str, set[str]] = defaultdict(set)
        ids_by_file: dict[str, set[str]] = defaultdict(set)
        files_by_employee: dict[int, set[str]] = defaultdict(set)
        for row in prev.index:
            files_by_id[row["event_id"]].add(row["source_object"])
            ids_by_file[row["source_object"]].add(row["event_id"])
            if row["employee_id"] is not None:
                files_by_employee[row["employee_id"]].add(row["source_object"])

        reprocess = set(changed) | {so for e in self._changed_employees for so in files_by_employee.get(e, ())}
        reprocess -= deleted
        parsed: dict[str, JsonlParseResult] = {}
        frontier = reprocess | deleted
        while frontier:  # expand to a fixed point over shared event_ids
            ids: set[str] = set()
            for so in frontier:
                ids |= ids_by_file.get(so, set())
                if so in reprocess:
                    parsed[so] = self._parse(so)
                    ids |= {_event_id(record.values) for record in parsed[so].records} - {None}
            frontier = {so for event_id in ids for so in files_by_id.get(event_id, ())} - reprocess - deleted
            reprocess |= frontier

        order = sorted(reprocess)  # source order (file, line), exactly as a full rebuild sees it
        report = validate_entity(
            CONTRACT,
            [record for so in order for record in parsed[so].records],
            self._context,
            parse_rejects=[reject for so in order for reject in parsed[so].rejects],
            file_issues=[issue for so in order for issue in parsed[so].file_issues],
            check_volume=False,
        )
        removed_files = reprocess | deleted

        # Index and ledger after this run.
        kept_index = [row for row in prev.index if row["source_object"] not in removed_files]
        new_index = [_index_row(outcome) for outcome in report.outcomes]
        new_index = [row for row in new_index if row is not None]
        index = kept_index + new_index

        accepted = [r for r in index if r["status"] == "accepted"]
        affected = {p for so in removed_files for p in prev.ledger.get(so, {}).get("partitions", [])}
        new_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in self._silver_rows(CONTRACT, report.accepted, self._snapshot):
            new_rows[partition_label(row["event_year"], row["event_month"])].append(row)
        affected |= set(new_rows)

        # The history metrics depend only on the accepted index, which changes only when files
        # are reprocessed or deleted; otherwise the published metrics are still exact.
        recompute_metrics = bool(removed_files) or prev.metrics is None
        metrics = learning_history_metrics(
            HistoryEvent(r["event_id"], r["employee_id"], r["course_name"], r["event_type"], r["event_date"])
            for r in accepted
        ) if recompute_metrics else prev.metrics

        ledger = {so: row for so, row in prev.ledger.items() if so not in removed_files}
        outcomes_by_file: dict[str, list] = defaultdict(list)
        for outcome in report.outcomes:
            outcomes_by_file[outcome.record.source_object].append(outcome)
        rejects_by_file: dict[str, list] = defaultdict(list)
        for reject in report.parse_rejects:
            rejects_by_file[reject["source_object"]].append(reject)
        for so in order:
            ledger[so] = _ledger_row(so, listing[so], parsed[so], outcomes_by_file[so], rejects_by_file[so])

        counts, reason_counts = _entity_counts(ledger, accepted)
        failures = volume_failures(ENTITY, counts, self._context.max_quarantine_rate)
        stats = {
            "files_in_bronze": len(listing), "files_new_or_changed": len(changed), "files_deleted": len(deleted),
            "files_reprocessed": len(reprocess), "partitions_affected": len(affected),
            "rows_validated": len(report.outcomes) + len(report.parse_rejects),
            "rows_added": sum(len(rows) for rows in new_rows.values()),
            "history_metrics_recomputed": int(recompute_metrics),
        }
        logger.info("learning incremental plan", extra={"context": {"full": self._full, **stats}})
        return LearningPlan(
            report=report, counts=counts, reason_counts=reason_counts, failures=failures, ledger=ledger,
            index=index, new_rows=dict(new_rows), removed_files=removed_files, affected_partitions=affected,
            metrics=metrics, state_changed=bool(removed_files) or self._full, stats=stats,
        )

    def _parse(self, source_object: str) -> JsonlParseResult:
        return parse_jsonl(self._s3.get_bytes(self._lake, f"{BRONZE}/{source_object}"), source_object)

    # ---- applying -----------------------------------------------------------------------

    def apply(self, plan: LearningPlan) -> LearningOutput:
        """Write the affected partitions, quarantine and state into the run folder."""
        prev = self._previous
        schema = CONTRACT.silver_file_schema()
        removed = pa.array(sorted(plan.removed_files), pa.string())
        partition_keys = {p: k for p, k in prev.partition_keys.items() if p not in plan.affected_partitions}
        partition_rows = {k: prev.partition_rows[k] for k in partition_keys.values()}
        written: list[str] = []
        for label in sorted(plan.affected_partitions):
            parts = []
            if label in prev.partition_keys:
                old = self._s3.read_arrow(self._lake, prev.partition_keys[label])
                old = old.filter(pc.invert(pc.is_in(old["_source_object"], value_set=removed)))
                parts.append(old)
            new = plan.new_rows.get(label, [])
            parts.append(pa.Table.from_pylist([{n: r[n] for n in schema.names} for r in new], schema=schema))
            table = pa.concat_tables(parts).combine_chunks().sort_by(ROW_ORDER)
            if table.num_rows == 0:
                continue
            year, month = (int(x) for x in label.split("-"))
            key = silver_month_key(ENTITY, year, month, self._run_id)
            self._write_checked(table, key, schema)
            partition_keys[label], partition_rows[key] = key, table.num_rows
            written.append(key)

        if plan.removed_files or prev.quarantine_key is None:
            quarantine = self._quarantine(plan)
            quarantine_key = silver_quarantine_key(ENTITY, self._run_id)
            self._write_checked(quarantine, quarantine_key, QUARANTINE_SCHEMA)
            written.append(quarantine_key)
            quarantine_rows = quarantine.num_rows
        else:
            quarantine_key = prev.quarantine_key
            quarantine_rows = plan.counts["quarantined"]

        if plan.state_changed or prev.ledger_key is None:
            ledger_key, index_key = silver_state_key("learning_files", self._run_id), silver_state_key(
                "learning_index", self._run_id)
            self._s3.write_arrow(pa.Table.from_pylist(sorted(plan.ledger.values(), key=lambda r: r["source_object"]),
                                                      schema=LEDGER_SCHEMA), self._lake, ledger_key)
            self._s3.write_arrow(pa.Table.from_pylist(plan.index, schema=INDEX_SCHEMA), self._lake, index_key)
            written += [ledger_key, index_key]
        else:
            ledger_key, index_key = prev.ledger_key, prev.index_key
        return LearningOutput(dict(sorted(partition_keys.items())), partition_rows, quarantine_key, quarantine_rows,
                              ledger_key, index_key, written)

    def _quarantine(self, plan: LearningPlan) -> pa.Table:
        new = pa.Table.from_pylist([
            row | {"_run_id": self._run_id, "_bronze_ingest_date": self._snapshot,
                   "_quarantined_at": self._started_at}
            for row in plan.report.quarantine_records()
        ], schema=QUARANTINE_SCHEMA)
        parts = [new]
        if self._previous.quarantine_key:
            old = self._s3.read_arrow(self._lake, self._previous.quarantine_key)
            removed = pa.array(sorted(plan.removed_files), pa.string())
            parts.append(old.filter(pc.invert(pc.is_in(old["source_object"], value_set=removed))))
        return pa.concat_tables(parts).combine_chunks().sort_by(
            [("source_object", "ascending"), ("source_line", "ascending")])

    def _write_checked(self, table: pa.Table, key: str, schema: pa.Schema) -> None:
        from talent_etl.silver import SilverIntegrityError, _check_not_null  # local: avoid import cycle
        _check_not_null(table, key)
        self._s3.write_arrow(table, self._lake, key)
        written = self._s3.read_arrow(self._lake, key)
        if written.num_rows != table.num_rows or not written.schema.equals(schema):
            raise SilverIntegrityError(f"{key}: read back {written.num_rows} rows / {written.schema}")


def load_previous(s3: S3Client, lake: str, pointer: Any) -> PreviousLearning:
    """Read the learning state referenced by a published pointer."""
    state = pointer.state["learning"]
    ledger = {row["source_object"]: row for row in s3.read_arrow(lake, state["ledger_key"]).to_pylist()}
    index = s3.read_arrow(lake, state["index_key"]).to_pylist()
    keys = pointer.files[ENTITY]
    labels = {}
    for key in keys:
        year = int(key.split("event_year=")[1][:4])
        month = int(key.split("event_month=")[1][:2])
        labels[partition_label(year, month)] = key
    return PreviousLearning(
        ledger=ledger, index=index, partition_keys=labels,
        partition_rows={k: pointer.file_rows[k] for k in keys}, quarantine_key=pointer.quarantine[ENTITY],
        ledger_key=state["ledger_key"], index_key=state["index_key"], metrics=state.get("metrics"),
    )


def _event_id(values: dict[str, Any]) -> str | None:
    raw = values.get("event_id")
    return raw.strip() if isinstance(raw, str) and raw.strip() else None


def _index_row(outcome) -> dict[str, Any] | None:
    event_id = _event_id(outcome.record.values)
    if event_id is None:
        return None
    status = _STATUS[outcome.action]
    employee_id = outcome.values.get("employee_id")
    if employee_id is None:
        try:
            employee_id = parse_int(outcome.record.values.get("employee_id"))
        except ParseError:
            employee_id = None
    accepted = status == "accepted"
    return {
        "event_id": event_id, "source_object": outcome.record.source_object,
        "source_line": outcome.record.source_line, "status": status,
        "employee_id": employee_id if employee_id is not None and -2**31 <= employee_id < 2**31 else None,
        "course_name": outcome.values.get("course_name") if accepted else None,
        "event_type": outcome.values.get("event_type") if accepted else None,
        "event_date": outcome.values.get("event_date") if accepted else None,
        "partition": partition_label(outcome.derived["event_year"], outcome.derived["event_month"]) if accepted else None,
        "engine_warnings": outcome.warnings if accepted else [],
    }


def _ledger_row(source_object: str, obj, parsed: JsonlParseResult, mine: list, rejects: list) -> dict[str, Any]:
    reasons: Counter[str] = Counter(issue.reason_code for issue in parsed.file_issues)
    for outcome in mine:
        reasons.update(issue.reason_code for issue in outcome.issues)
    for reject in rejects:
        reasons.update(reject["reason_codes"])
    accepted = [o for o in mine if o.is_live]
    return {
        "source_object": source_object, "etag": obj.etag, "size": obj.size,
        "read": len(mine) + len(rejects), "accepted": len(accepted),
        "quarantined": sum(o.action is Action.QUARANTINE for o in mine) + len(rejects),
        "deduplicated": sum(o.action is Action.DEDUPLICATE for o in mine),
        "partitions": sorted({partition_label(o.derived["event_year"], o.derived["event_month"]) for o in accepted}),
        "reason_counts": json.dumps(dict(sorted(reasons.items()))),
    }


def _entity_counts(ledger: dict[str, dict], accepted_index: list[dict]) -> tuple[dict[str, int], dict[str, int]]:
    counts = {name: sum(row[name] for row in ledger.values())
              for name in ("read", "accepted", "quarantined", "deduplicated")}
    if counts["accepted"] != len(accepted_index):
        from talent_etl.silver import SilverIntegrityError
        raise SilverIntegrityError(f"ledger says {counts['accepted']} accepted, index has {len(accepted_index)}")
    counts["accepted_with_warnings"] = sum(bool(r["engine_warnings"]) for r in accepted_index)
    counts = {k: counts[k] for k in ("read", "accepted", "accepted_with_warnings", "quarantined", "deduplicated")}
    reasons: Counter[str] = Counter()
    for row in ledger.values():
        reasons.update(json.loads(row["reason_counts"]))
    return counts, dict(sorted(reasons.items()))
