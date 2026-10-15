"""
Bronze layer: land raw source data in the lake without changing it.

PostgreSQL
    Each source table is snapshotted to one Parquet object per ingest date,
    ``bronze/postgres/<table>/ingest_date=YYYY-MM-DD/part-00000.parquet``. The source
    columns keep their names, order, values and (lossless) types; five ``_``-prefixed
    metadata columns are appended. Rerunning a date overwrites that object and
    removes anything else in the partition, so each (table, date) holds exactly one
    snapshot.

S3 learning activity
    Every source object under ``learning_activity/`` is copied server-side to
    ``bronze/learning_activity/`` with the same relative key, so the year/month/day
    partitions and the bytes are identical (empty files included). Lineage goes in
    S3 user metadata because the JSONL bodies must stay untouched. An object whose
    Bronze copy already has the same size and ETag is skipped, so reruns copy only
    new or changed files and Bronze copies are never duplicated.

Every run writes a manifest, ``bronze/_manifests/ingest_date=.../run_id=....json``,
with the counts, sizes and keys it produced.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone

import pyarrow as pa

from talent_etl.config import Settings
from talent_etl.io_pg import PostgresClient
from talent_etl.io_s3 import S3Client, S3Object
from talent_etl.logging_utils import log_step
from talent_etl.paths import (
    DAY_PARTITION,
    bronze_learning_prefix,
    bronze_manifest_key,
    bronze_postgres_key,
    bronze_postgres_prefix,
)

logger = logging.getLogger(__name__)

# Source tables in the provided load order (parents before children).
SOURCE_TABLES = (
    "teams", "competencies", "employees", "projects",
    "performance_reviews", "competency_assessments", "talent_review",
)

METADATA_PREFIX = "_"
METADATA_FIELDS = (
    pa.field("_run_id", pa.string(), nullable=False),
    pa.field("_ingest_date", pa.date32(), nullable=False),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_source_object", pa.string(), nullable=False),
)


class BronzeSourceError(RuntimeError):
    """A required source (table or S3 prefix) is missing or unusable."""


class BronzeIntegrityError(RuntimeError):
    """What landed in Bronze does not match what was read from the source."""


@dataclass(frozen=True)
class TableIngestResult:
    """Outcome of snapshotting one PostgreSQL table."""

    table: str
    source_object: str
    key: str
    rows: int
    columns: int
    bytes: int
    stale_objects_removed: int


@dataclass
class LearningIngestResult:
    """Outcome of mirroring the learning_activity files."""

    source_uri: str
    target_prefix: str
    source_files: int = 0
    source_bytes: int = 0
    copied: int = 0
    skipped_unchanged: int = 0
    empty_files: list[str] = field(default_factory=list)
    unpartitioned_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BronzeResult:
    """Outcome of one Bronze run."""

    run_id: str
    ingest_date: str
    ingested_at: str
    lake_bucket: str
    tables: list[TableIngestResult]
    learning_activity: LearningIngestResult
    manifest_key: str


def add_metadata_columns(
    table: pa.Table, *, run_id: str, ingest_date: date, ingested_at: datetime,
    source_system: str, source_object: str,
) -> pa.Table:
    """
    Append the Bronze metadata columns to a source table.

    Raises:
        BronzeSourceError: If a source column already uses the reserved ``_`` prefix.
    """
    clashes = [name for name in table.column_names if name.startswith(METADATA_PREFIX)]
    if clashes:
        raise BronzeSourceError(f"{source_object}: source columns use the reserved '_' prefix: {clashes}")
    values = (run_id, ingest_date, ingested_at, source_system, source_object)
    for meta_field, value in zip(METADATA_FIELDS, values):
        table = table.append_column(meta_field, pa.array([value] * table.num_rows, type=meta_field.type))
    return table


class BronzeIngestor:
    """Ingest both sources into the Bronze layer for one ingest date."""

    def __init__(
        self,
        settings: Settings,
        pg: PostgresClient,
        s3: S3Client,
        *,
        run_id: str,
        ingest_date: date,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._settings = settings
        self._pg = pg
        self._s3 = s3
        self._run_id = run_id
        self._ingest_date = ingest_date
        self._ingested_at = clock()
        self._schema = settings.postgres.source_schema
        self._lake = settings.s3.lake_bucket
        self._source_bucket = settings.s3.source_bucket
        self._source_prefix = settings.s3.source_prefix.strip("/") + "/"

    def run(self) -> BronzeResult:
        """
        Check the sources, then ingest every table and the learning files and write a manifest.

        Raises:
            BronzeSourceError: Before anything is written, if a source is unavailable.
            BronzeIntegrityError: If a Bronze object does not match its source.
        """
        source_files = self.preflight()
        self._s3.ensure_bucket(self._lake)
        tables = [self.ingest_table(table) for table in SOURCE_TABLES]
        learning = self.ingest_learning_activity(source_files)
        result = BronzeResult(
            run_id=self._run_id,
            ingest_date=self._ingest_date.isoformat(),
            ingested_at=self._ingested_at.isoformat(),
            lake_bucket=self._lake,
            tables=tables,
            learning_activity=learning,
            manifest_key=bronze_manifest_key(self._ingest_date, self._run_id),
        )
        self._s3.put_bytes(
            self._lake, result.manifest_key,
            json.dumps(asdict(result), indent=2).encode(), content_type="application/json",
        )
        logger.info("bronze manifest written", extra={"context": {"key": result.manifest_key}})
        return result

    def preflight(self) -> list[S3Object]:
        """
        Confirm every source exists before writing anything.

        Returns:
            The source learning-activity objects (directory markers excluded).

        Raises:
            BronzeSourceError: Listing every missing table and/or an empty S3 prefix.
        """
        with log_step(logger, "bronze.preflight", schema=self._schema,
                      source_uri=f"s3://{self._source_bucket}/{self._source_prefix}") as metrics:
            problems = []
            missing = sorted(set(SOURCE_TABLES) - set(self._pg.list_tables(self._schema)))
            if missing:
                problems.append(f"missing PostgreSQL tables in schema {self._schema}: {missing}")
            files = [
                obj for obj in self._s3.list_objects(self._source_bucket, self._source_prefix)
                if not obj.key.endswith("/")
            ]
            if not files:
                problems.append(f"no objects under s3://{self._source_bucket}/{self._source_prefix}")
            metrics.update(tables_found=len(SOURCE_TABLES) - len(missing), source_files=len(files))
            if problems:
                raise BronzeSourceError("; ".join(problems) + " (run scripts/seed_sources.sh?)")
            return files

    def ingest_table(self, table: str) -> TableIngestResult:
        """Snapshot one PostgreSQL table to its Bronze partition (deterministic overwrite)."""
        source_object = f"{self._schema}.{table}"
        key = bronze_postgres_key(table, self._ingest_date)
        with log_step(logger, f"bronze.postgres.{table}", source=source_object,
                      target=f"s3://{self._lake}/{key}") as metrics:
            source = self._pg.read_table_arrow(self._schema, table)
            metrics["rows_read"] = source.num_rows
            if source.num_rows == 0:
                logger.warning("source table is empty; writing an empty snapshot",
                               extra={"context": {"table": source_object}})
            landed = add_metadata_columns(
                source, run_id=self._run_id, ingest_date=self._ingest_date,
                ingested_at=self._ingested_at, source_system="postgres", source_object=source_object,
            )
            size = self._s3.write_arrow(landed, self._lake, key)
            stale = [
                obj.key for obj in self._s3.list_objects(self._lake, bronze_postgres_prefix(table, self._ingest_date))
                if obj.key != key
            ]
            removed = self._s3.delete_keys(self._lake, stale)
            if removed:
                logger.warning("removed stale objects from partition",
                               extra={"context": {"table": source_object, "keys": stale}})
            written = self._s3.read_arrow(self._lake, key)
            if written.num_rows != source.num_rows or written.schema != landed.schema:
                raise BronzeIntegrityError(
                    f"{key}: wrote {written.num_rows} rows / schema {written.schema}, "
                    f"expected {source.num_rows} rows / schema {landed.schema}"
                )
            metrics.update(rows_written=written.num_rows, bytes=size, stale_objects_removed=removed)
            return TableIngestResult(
                table=table, source_object=source_object, key=key, rows=written.num_rows,
                columns=source.num_columns, bytes=size, stale_objects_removed=removed,
            )

    def ingest_learning_activity(self, source_files: list[S3Object]) -> LearningIngestResult:
        """Mirror the source JSONL files into Bronze byte-for-byte, skipping unchanged copies."""
        target_prefix = bronze_learning_prefix()
        result = LearningIngestResult(
            source_uri=f"s3://{self._source_bucket}/{self._source_prefix}",
            target_prefix=f"s3://{self._lake}/{target_prefix}",
        )
        with log_step(logger, "bronze.learning_activity", source=result.source_uri,
                      target=result.target_prefix) as metrics:
            existing = {obj.key: obj for obj in self._s3.list_objects(self._lake, target_prefix)}
            expected: dict[str, S3Object] = {}
            for obj in source_files:
                relative = obj.key[len(self._source_prefix):]
                target_key = target_prefix + relative
                expected[target_key] = obj
                result.source_files += 1
                result.source_bytes += obj.size
                if not DAY_PARTITION.match(relative):
                    result.unpartitioned_files.append(obj.key)
                    logger.warning("source file is outside a year=/month=/day= partition; copied as-is",
                                   extra={"context": {"key": obj.key}})
                if obj.size == 0:
                    result.empty_files.append(obj.key)
                    logger.warning("source file is empty; copied as-is", extra={"context": {"key": obj.key}})
                current = existing.get(target_key)
                if current is not None and (current.size, current.etag) == (obj.size, obj.etag):
                    result.skipped_unchanged += 1
                    continue
                self._s3.copy_object(
                    self._source_bucket, obj.key, self._lake, target_key,
                    metadata={
                        "run_id": self._run_id,
                        "ingest_date": self._ingest_date.isoformat(),
                        "ingested_at": self._ingested_at.isoformat(),
                        "source_uri": f"s3://{self._source_bucket}/{obj.key}",
                    },
                )
                result.copied += 1
            self._verify_copies(expected, target_prefix)
            metrics.update(
                files_read=result.source_files, bytes_read=result.source_bytes,
                files_copied=result.copied, files_skipped_unchanged=result.skipped_unchanged,
                empty_files=len(result.empty_files), unpartitioned_files=len(result.unpartitioned_files),
            )
        return result

    def _verify_copies(self, expected: dict[str, S3Object], target_prefix: str) -> None:
        """Check every source file exists in Bronze with the same size and ETag."""
        landed = {obj.key: obj for obj in self._s3.list_objects(self._lake, target_prefix)}
        mismatched = [
            key for key, src in expected.items()
            if key not in landed or (landed[key].size, landed[key].etag) != (src.size, src.etag)
        ]
        if mismatched:
            raise BronzeIntegrityError(
                f"{len(mismatched)} Bronze files missing or different from source, e.g. {mismatched[:5]}"
            )
