"""
Gold layer: curated tables answering the business questions, built from published Silver.

One run:
    1. Read the published Silver pointer; the as-of date is its Bronze snapshot date (A22).
    2. Fingerprint every Gold table from the Silver file keys it reads (Silver writes new
       keys only when content changes), the Gold logic and rules, and the as-of date for
       time-relative tables. Tables whose fingerprint matches the published Gold version
       are reused as-is; the others are rebuilt (``--full-refresh`` rebuilds all).
    3. Build and validate every rebuilt table before writing anything: contract, grain,
       domains, and reconciliation with Silver (talent_etl.gold_tables). Any failed check
       blocks publishing and is recorded in a FAILED manifest.
    4. Write rebuilt tables into a new run folder, read each back, then publish by
       replacing ``gold/_current.json`` (atomic), and clean up unreferenced files.

Readers use ``read_current_gold`` / ``load_gold_table``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields
from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa

from talent_etl import definitions
from talent_etl.config import Settings
from talent_etl.gold_tables import TABLES, Check, GoldInputs, check_table, to_arrow
from talent_etl.io_s3 import S3Client
from talent_etl.logging_utils import log_step
from talent_etl.paths import GOLD, gold_key
from talent_etl.publish import Layer, collect_garbage, mark_started, put_json, read_json
from talent_etl.silver import SilverPointer, read_current, rules_fingerprint

logger = logging.getLogger(__name__)

GOLD_LAYER = Layer(GOLD)
# Bump when Gold code changes how a table is computed; a new fingerprint rebuilds every table.
GOLD_LOGIC_VERSION = 2   # 2: removed unused employee_performance.avg_valid_rating


class GoldSourceError(RuntimeError):
    """No published Silver version to build Gold from."""


class GoldValidationError(RuntimeError):
    """A Gold check failed; nothing was published."""


class GoldIntegrityError(RuntimeError):
    """What was written does not match what was built; nothing was published."""


@dataclass(frozen=True)
class GoldPointer:
    """Content of ``gold/_current.json``: the one published Gold version."""

    run_id: str
    run_prefix: str
    silver_run_id: str
    as_of: str
    started_at: str
    published_at: str
    manifest_key: str
    previous_run_id: str | None
    files: dict[str, str]             # table -> Parquet key (may live in an earlier run folder)
    rows: dict[str, int]
    fingerprints: dict[str, str]

    def referenced_keys(self) -> set[str]:
        """Every object a reader of this version needs."""
        return set(self.files.values())


@dataclass
class TableResult:
    """Outcome for one Gold table in a run."""

    table: str
    mode: str                         # reused | built
    rows: int
    key: str | None
    fingerprint: str
    checks: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class GoldResult:
    """Outcome of one Gold run (also the manifest content)."""

    run_id: str
    status: str                       # SUCCEEDED | FAILED | SUPERSEDED
    published: bool
    full_refresh: bool
    silver_run_id: str
    as_of: str
    started_at: str
    lake_bucket: str
    run_prefix: str
    tables: list[TableResult]
    manifest_key: str
    error: str | None = None


def read_current_gold(s3: S3Client, bucket: str) -> GoldPointer | None:
    """Return the published Gold pointer, or None if nothing has been published yet."""
    raw = read_json(s3, bucket, GOLD_LAYER.current_key())
    if raw is None:
        return None
    known = {f.name for f in fields(GoldPointer)}
    return GoldPointer(**{k: v for k, v in raw.items() if k in known})


def load_gold_table(s3: S3Client, bucket: str, name: str) -> pa.Table:
    """
    Read one table of the published Gold version.

    Raises:
        GoldSourceError: If no Gold version has been published.
    """
    pointer = read_current_gold(s3, bucket)
    if pointer is None:
        raise GoldSourceError(f"no published Gold in s3://{bucket}/{GOLD_LAYER.current_key()}; run --step gold")
    return s3.read_arrow(bucket, pointer.files[name])


def gold_rules_fingerprint() -> str:
    """Fingerprint of the Gold logic version, the Gold business constants and the Silver rules."""
    constants = {name: repr(getattr(definitions, name)) for name in sorted(dir(definitions)) if name.isupper()}
    payload = json.dumps({"gold_logic": GOLD_LOGIC_VERSION, "definitions": constants,
                          "silver_rules": rules_fingerprint()}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def table_fingerprint(name: str, silver: SilverPointer, as_of: date, rules: str) -> str:
    """Fingerprint of everything a table's content depends on."""
    table = TABLES[name]
    payload = json.dumps({
        "table": name, "rules": rules,
        "silver_files": {e: sorted(silver.files[e]) for e in sorted(table.silver_inputs)},
        "as_of": as_of.isoformat() if table.uses_as_of else None,
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


class GoldBuilder:
    """Update Gold from the published Silver version and publish it atomically."""

    def __init__(self, settings: Settings, s3: S3Client, *, run_id: str, full_refresh: bool = False,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self._s3 = s3
        self._lake = settings.s3.lake_bucket
        self._run_id = run_id
        self._full = full_refresh
        self._clock = clock
        self._started_at = clock()

    def run(self) -> GoldResult:
        """
        Build the tables whose inputs changed, validate them, and publish the new version.

        Raises:
            GoldSourceError: Silver has not been published.
            GoldValidationError: A check failed; nothing written or published.
            GoldIntegrityError: A written table does not match what was built; not published.
        """
        silver = read_current(self._s3, self._lake)
        if silver is None:
            raise GoldSourceError(f"no published Silver in s3://{self._lake}; run --step silver")
        as_of = date.fromisoformat(silver.bronze_ingest_date)
        current = read_current_gold(self._s3, self._lake)
        rules = gold_rules_fingerprint()
        inputs = GoldInputs(lambda entity: self._load_silver(silver, entity), as_of)

        results: list[TableResult] = []
        built: dict[str, pa.Table] = {}
        for name in TABLES:
            fingerprint = table_fingerprint(name, silver, as_of, rules)
            if not self._full and current and current.fingerprints.get(name) == fingerprint:
                results.append(TableResult(name, "reused", current.rows[name], current.files[name], fingerprint))
                continue
            with log_step(logger, f"gold.build.{name}") as metrics:
                df = inputs.table(name)
                arrow = to_arrow(TABLES[name], df)
                checks = check_table(TABLES[name], arrow, df, inputs)
                failed = [c for c in checks if not c.passed]
                metrics.update(rows=arrow.num_rows, checks=len(checks), failed_checks=len(failed))
                for check in failed:
                    logger.error("gold check failed", extra={"context": asdict(check)})
            built[name] = arrow
            results.append(TableResult(name, "built", arrow.num_rows, None, fingerprint,
                                       [asdict(c) for c in checks]))

        failed = [c for r in results for c in r.checks if not c["passed"]]
        if failed:
            summary = "; ".join(f"{c['table']}: {c['name']} ({c['detail']})" for c in failed)
            self._write_manifest(self._result("FAILED", silver, as_of, results, error=summary))
            raise GoldValidationError(f"Gold not published; {len(failed)} check(s) failed: {summary}")

        mark_started(self._s3, self._lake, GOLD_LAYER, self._run_id, self._started_at)
        try:
            for result in results:
                if result.mode == "built":
                    result.key = gold_key(result.table, self._run_id)
                    self._write_checked(built[result.table], result.key)
            pointer = self._pointer(silver, as_of, current, results)
            self._verify(pointer)
            latest = read_current_gold(self._s3, self._lake)
        except Exception as exc:
            try:
                self._write_manifest(self._result("FAILED", silver, as_of, results, error=f"{type(exc).__name__}: {exc}"))
            except Exception:  # noqa: BLE001 - the original error matters more
                logger.exception("could not write FAILED manifest")
            raise

        if latest is not None and datetime.fromisoformat(latest.started_at) > self._started_at:
            result = self._result("SUPERSEDED", silver, as_of, results)
            self._write_manifest(result)
            logger.warning("newer Gold run already published; this run is not published",
                           extra={"context": {"current_run_id": latest.run_id}})
            return result

        result = self._result("SUCCEEDED", silver, as_of, results)
        self._write_manifest(result)
        put_json(self._s3, self._lake, GOLD_LAYER.current_key(), asdict(pointer))
        result.published = True
        logger.info("gold published", extra={"context": {
            "run_id": self._run_id, "silver_run_id": silver.run_id,
            "built": [r.table for r in results if r.mode == "built"],
            "reused": [r.table for r in results if r.mode == "reused"]}})
        retained = pointer.referenced_keys() | (current.referenced_keys() if current else set())
        collect_garbage(self._s3, self._lake, GOLD_LAYER, retained, self._clock())
        return result

    def _load_silver(self, silver: SilverPointer, entity: str):
        tables = [self._s3.read_arrow(self._lake, key) for key in silver.files[entity]]
        return pa.concat_tables(tables).to_pandas()

    def _write_checked(self, table: pa.Table, key: str) -> None:
        self._s3.write_arrow(table, self._lake, key)
        written = self._s3.read_arrow(self._lake, key)
        if written.num_rows != table.num_rows or not written.schema.equals(table.schema):
            raise GoldIntegrityError(f"{key}: read back {written.num_rows} rows / {written.schema}")

    def _pointer(self, silver: SilverPointer, as_of: date, current: GoldPointer | None,
                 results: list[TableResult]) -> GoldPointer:
        return GoldPointer(
            run_id=self._run_id, run_prefix=GOLD_LAYER.run_prefix(self._run_id), silver_run_id=silver.run_id,
            as_of=as_of.isoformat(), started_at=self._started_at.isoformat(), published_at=self._clock().isoformat(),
            manifest_key=GOLD_LAYER.manifest_key(self._run_id), previous_run_id=current.run_id if current else None,
            files={r.table: r.key for r in results}, rows={r.table: r.rows for r in results},
            fingerprints={r.table: r.fingerprint for r in results},
        )

    def _verify(self, pointer: GoldPointer) -> None:
        present = {obj.key for obj in self._s3.list_objects(self._lake, f"{GOLD}/runs/")}
        missing = sorted(pointer.referenced_keys() - present)
        if missing:
            raise GoldIntegrityError(f"published version would reference missing objects: {missing[:5]}")

    def _result(self, status: str, silver: SilverPointer, as_of: date, results: list[TableResult],
                error: str | None = None) -> GoldResult:
        return GoldResult(
            run_id=self._run_id, status=status, published=False, full_refresh=self._full, silver_run_id=silver.run_id,
            as_of=as_of.isoformat(), started_at=self._started_at.isoformat(), lake_bucket=self._lake,
            run_prefix=GOLD_LAYER.run_prefix(self._run_id), tables=results,
            manifest_key=GOLD_LAYER.manifest_key(self._run_id), error=error,
        )

    def _write_manifest(self, result: GoldResult) -> None:
        put_json(self._s3, self._lake, result.manifest_key, asdict(result))
        logger.info("gold manifest written", extra={"context": {"key": result.manifest_key, "status": result.status}})
