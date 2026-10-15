"""
Load the provided sample data into the two source systems (PostgreSQL and S3).

``scripts/seed_sources.sh`` does this locally with psql and the AWS CLI. This module does
the same from Python so it also works inside the cloud job, where the Aurora cluster is
only reachable from the VPC and no CLI tools are installed. The sample zip is read from
``SEED_ZIP_URI`` (``s3://bucket/key`` or a local path), defaulting locally to the copy in
``sampleData/``.

PostgreSQL: the provided ``schema.sql`` and the seven CSVs are loaded into
``POSTGRES_SOURCE_SCHEMA`` in ONE transaction (drop, recreate, COPY, verify counts), so a
failure leaves the previous source data untouched.
S3: the ``learning_activity/`` files are uploaded byte-for-byte to
``s3://SOURCE_BUCKET/learning_activity/``; keys that are not in the zip are removed (sync).
"""

from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from talent_etl.bronze import SOURCE_TABLES
from talent_etl.config import Settings
from talent_etl.io_pg import PostgresClient, is_transient_pg_error
from talent_etl.io_s3 import S3Client
from talent_etl.logging_utils import log_step
from talent_etl.retry import RetryPolicy

logger = logging.getLogger(__name__)

LOCAL_SAMPLE_ZIP = "sampleData/data-engineering-sample-performance-scenario.zip"


class SeedError(RuntimeError):
    """The sample data could not be found or loaded."""


@dataclass(frozen=True)
class SeedResult:
    """What was loaded into the sources."""

    table_rows: dict[str, int]
    learning_files: int
    learning_removed: int


def open_sample(settings: Settings, s3: S3Client) -> zipfile.ZipFile:
    """
    Open the sample zip from SEED_ZIP_URI, or locally from the repository's sampleData/.

    Raises:
        SeedError: If no sample zip can be found.
    """
    uri = settings.seed_zip_uri
    if uri and uri.startswith("s3://"):
        bucket, _, key = uri.removeprefix("s3://").partition("/")
        return zipfile.ZipFile(io.BytesIO(s3.get_bytes(bucket, key)))
    if uri:
        path = Path(uri)
    else:
        candidates = [parent / LOCAL_SAMPLE_ZIP for parent in Path(__file__).resolve().parents]
        path = next((p for p in candidates if p.exists()), candidates[0])
    if not path.exists():
        raise SeedError(f"sample zip not found at {path}; set SEED_ZIP_URI")
    return zipfile.ZipFile(path)


def _member_prefix(archive: zipfile.ZipFile) -> str:
    schema = next((n for n in archive.namelist() if n.endswith("schema.sql")), None)
    if schema is None:
        raise SeedError("sample zip has no schema.sql")
    return schema.removesuffix("schema.sql")


def sources_missing(settings: Settings, pg: PostgresClient, s3: S3Client) -> list[str]:
    """Return what is missing from the sources (empty when both are seeded)."""
    missing = [f"{settings.postgres.source_schema}.{t}" for t in sorted(
        set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)))]
    try:
        files = s3.list_objects(settings.s3.source_bucket, f"{settings.s3.source_prefix.strip('/')}/")
    except Exception:  # noqa: BLE001 - a missing bucket counts as missing data
        files = []
    if not files:
        missing.append(f"s3://{settings.s3.source_bucket}/{settings.s3.source_prefix}")
    return missing


def load_postgres(settings: Settings, archive: zipfile.ZipFile, policy: RetryPolicy) -> dict[str, int]:
    """Recreate the source schema and load the CSVs in one transaction; return rows per table."""
    prefix = _member_prefix(archive)
    schema = settings.postgres.source_schema
    ddl = archive.read(f"{prefix}schema.sql").decode()
    expected = {t: archive.read(f"{prefix}{t}.csv").count(b"\n") - 1 for t in SOURCE_TABLES}

    def load() -> dict[str, int]:
        with psycopg.connect(**settings.postgres.connect_kwargs()) as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
            for statement in (s.strip() for s in ddl.split(";")):
                body = "\n".join(line for line in statement.splitlines() if not line.strip().startswith("--"))
                if body.strip():
                    cur.execute(body)
            rows = {}
            for table in SOURCE_TABLES:  # provided load order: parents before children
                copy_sql = sql.SQL("COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true)").format(
                    sql.Identifier(schema), sql.Identifier(table))
                with cur.copy(copy_sql) as copy:
                    copy.write(archive.read(f"{prefix}{table}.csv"))
                cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
                rows[table] = cur.fetchone()[0]
            mismatched = {t: (rows[t], expected[t]) for t in SOURCE_TABLES if rows[t] != expected[t]}
            if mismatched:
                raise SeedError(f"row counts differ from the CSV files (loaded, expected): {mismatched}")
            return rows  # leaving the block commits; any error above rolls everything back

    return policy.call(load, description=f"seed {schema}", is_retryable=is_transient_pg_error)


def upload_learning(settings: Settings, archive: zipfile.ZipFile, s3: S3Client) -> tuple[int, int]:
    """Sync the zip's learning_activity/ files to the source bucket; return (uploaded, removed)."""
    prefix = _member_prefix(archive) + "learning_activity/"
    target = settings.s3.source_prefix.strip("/") + "/"
    bucket = settings.s3.source_bucket
    s3.ensure_bucket(bucket)
    wanted = {}
    for name in archive.namelist():
        if name.startswith(prefix) and not name.endswith("/"):
            wanted[target + name.removeprefix(prefix)] = archive.read(name)
    for key, body in sorted(wanted.items()):
        s3.put_bytes(bucket, key, body, content_type="application/x-ndjson")
    stale = [obj.key for obj in s3.list_objects(bucket, target) if obj.key not in wanted]
    return len(wanted), s3.delete_keys(bucket, stale)


def seed_sources(settings: Settings, s3: S3Client, policy: RetryPolicy) -> SeedResult:
    """Load the sample data into PostgreSQL and S3 (idempotent: rerunning gives the same sources)."""
    archive = open_sample(settings, s3)
    with log_step(logger, "seed.postgres", schema=settings.postgres.source_schema) as metrics:
        rows = load_postgres(settings, archive, policy)
        metrics["rows"] = rows
    with log_step(logger, "seed.s3", bucket=settings.s3.source_bucket) as metrics:
        uploaded, removed = upload_learning(settings, archive, s3)
        metrics.update(files_uploaded=uploaded, stale_removed=removed)
    return SeedResult(rows, uploaded, removed)
