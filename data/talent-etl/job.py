"""
Entry point for the ACME talent pipeline batch job.

Run locally with ``python job.py`` (``IS_LOCAL`` defaults to true); in the cloud
the Helm chart runs the same file with the injected ``POSTGRES_*`` variables.

Steps:
    check   Verify both sources are reachable and seeded (PostgreSQL schema, S3 prefix).
    bronze  Land both sources in the Bronze layer for ``--ingest-date`` (default: today, UTC).
    silver  Validate Bronze and update Silver from the ``--ingest-date`` snapshot (default:
            the latest complete Bronze snapshot): incrementally, or in full with
            ``--full-refresh`` (automatic when the rules changed).
    gold    Build the curated Gold tables from the published Silver version (tables whose
            inputs did not change are reused; ``--full-refresh`` rebuilds all).
    serve   Publish the web dashboard's data (serving/) from the published Gold version.
    all     bronze, then silver, gold and serve (``--seed-if-missing`` seeds empty sources first).
    seed    Load the provided sample data into the sources (PostgreSQL schema + S3 prefix).

Without ``--step`` the step comes from ``PIPELINE_STEP`` (default ``check``), and
``SEED_IF_MISSING=true`` turns on ``--seed-if-missing``: the cloud job is configured this
way, because the Helm chart runs ``job.py`` without arguments.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone

from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor
from talent_etl.config import ConfigError, Settings
from talent_etl.io_pg import PostgresClient
from talent_etl.io_s3 import S3Client
from talent_etl.logging_utils import configure_logging, log_step, new_run_id
from talent_etl.retry import RetryPolicy
from talent_etl.seed import seed_sources, sources_missing
from talent_etl.gold import GoldBuilder
from talent_etl.serving import publish_dashboard
from talent_etl.silver import SilverBuilder

EXPECTED_SOURCE_TABLES = tuple(sorted(SOURCE_TABLES))

logger = logging.getLogger("talent_etl.job")


def check_sources(settings: Settings, policy: RetryPolicy) -> None:
    """
    Confirm the PostgreSQL source tables and the S3 learning events exist.

    Raises:
        RuntimeError: If a source table or the S3 prefix is missing or empty.
    """
    schema = settings.postgres.source_schema
    with log_step(logger, "check.postgres", schema=schema) as metrics, \
            PostgresClient(settings.postgres, policy) as pg:
        tables = set(pg.list_tables(schema))
        missing = sorted(set(EXPECTED_SOURCE_TABLES) - tables)
        if missing:
            raise RuntimeError(f"missing source tables in schema {schema}: {missing}; run scripts/seed_sources.sh")
        metrics["row_counts"] = {table: pg.count_rows(schema, table) for table in EXPECTED_SOURCE_TABLES}

    bucket, prefix = settings.s3.source_bucket, settings.s3.source_prefix
    with log_step(logger, "check.s3", bucket=bucket, prefix=prefix) as metrics:
        objects = S3Client(settings.s3, policy).list_objects(bucket, f"{prefix}/")
        if not objects:
            raise RuntimeError(f"no objects under s3://{bucket}/{prefix}/; run scripts/seed_sources.sh")
        metrics["objects"] = len(objects)
        metrics["bytes"] = sum(obj.size for obj in objects)


def run_bronze(settings: Settings, policy: RetryPolicy, run_id: str, ingest_date: date) -> None:
    """Ingest both sources into Bronze and log a one-line summary of what landed."""
    with log_step(logger, "bronze", ingest_date=ingest_date.isoformat()) as metrics, \
            PostgresClient(settings.postgres, policy) as pg:
        result = BronzeIngestor(
            settings, pg, S3Client(settings.s3, policy), run_id=run_id, ingest_date=ingest_date
        ).run()
        learning = result.learning_activity
        metrics.update(
            table_rows={t.table: t.rows for t in result.tables},
            learning_files=learning.source_files,
            learning_copied=learning.copied,
            learning_skipped_unchanged=learning.skipped_unchanged,
            learning_empty_files=len(learning.empty_files),
            manifest=f"s3://{result.lake_bucket}/{result.manifest_key}",
        )


def run_silver(settings: Settings, policy: RetryPolicy, run_id: str, ingest_date: date | None,
               full_refresh: bool = False) -> None:
    """Update Silver from Bronze (incrementally unless a full rebuild is needed), publish it, log a summary."""
    with log_step(logger, "silver", requested_ingest_date=ingest_date and ingest_date.isoformat()) as metrics:
        result = SilverBuilder(settings, S3Client(settings.s3, policy), run_id=run_id, ingest_date=ingest_date,
                               full_refresh=full_refresh).run()
        metrics.update(
            status=result.status,
            published=result.published,
            full_refresh=result.full_refresh,
            full_refresh_reason=result.full_refresh_reason,
            modes={e.entity: e.mode for e in result.entities},
            files_written=sum(len(e.written) for e in result.entities),
            bronze_ingest_date=result.bronze_ingest_date,
            run_prefix=f"s3://{result.lake_bucket}/{result.run_prefix}",
            rows={e.entity: sum(e.outputs.values()) for e in result.entities},
            quarantined={e.entity: e.quarantine_rows for e in result.entities},
            manifest=f"s3://{result.lake_bucket}/{result.manifest_key}",
        )


def run_gold(settings: Settings, policy: RetryPolicy, run_id: str, full_refresh: bool = False) -> None:
    """Build Gold from the published Silver version, publish it, and log a summary."""
    with log_step(logger, "gold") as metrics:
        result = GoldBuilder(settings, S3Client(settings.s3, policy), run_id=run_id, full_refresh=full_refresh).run()
        metrics.update(
            status=result.status, published=result.published, silver_run_id=result.silver_run_id, as_of=result.as_of,
            modes={t.table: t.mode for t in result.tables}, rows={t.table: t.rows for t in result.tables},
            checks_run=sum(len(t.checks) for t in result.tables),
            manifest=f"s3://{result.lake_bucket}/{result.manifest_key}",
        )


def run_serve(settings: Settings, policy: RetryPolicy) -> None:
    """Publish the dashboard's data for the published Gold version, and log a summary."""
    with log_step(logger, "serve") as metrics:
        result = publish_dashboard(settings, S3Client(settings.s3, policy))
        metrics.update(status=result.status, gold_run_id=result.gold_run_id, employees=result.employees,
                       checks=result.checks, summary=f"s3://{settings.s3.lake_bucket}/{result.summary_key}")


def run_seed(settings: Settings, policy: RetryPolicy, only_if_missing: bool) -> None:
    """Load the sample data into the sources (always, or only when something is missing)."""
    s3 = S3Client(settings.s3, policy)
    with log_step(logger, "seed", only_if_missing=only_if_missing) as metrics:
        if only_if_missing:
            with PostgresClient(settings.postgres, policy) as pg:
                missing = sources_missing(settings, pg, s3)
            metrics["missing"] = missing
            if not missing:
                logger.info("sources already seeded; nothing to do")
                return
        result = seed_sources(settings, s3, policy)
        metrics.update(rows=result.table_rows, learning_files=result.learning_files)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments (defaults for --step and --seed-if-missing come from the environment)."""
    parser = argparse.ArgumentParser(description="ACME talent medallion pipeline")
    parser.add_argument("--step", choices=["check", "bronze", "silver", "gold", "serve", "all", "seed"],
                        default=os.environ.get("PIPELINE_STEP", "").strip() or "check",
                        help="pipeline step to run (default: $PIPELINE_STEP, else check)")
    parser.add_argument(
        "--seed-if-missing", action="store_true",
        default=os.environ.get("SEED_IF_MISSING", "").strip().lower() in ("1", "true", "yes"),
        help="all: load the sample data first if a source table or the S3 source prefix is missing "
             "(default: $SEED_IF_MISSING)",
    )
    parser.add_argument(
        "--ingest-date", type=date.fromisoformat, default=None,
        help="bronze: partition to write (default today, UTC; rerunning overwrites it). "
             "silver: Bronze snapshot to read (default: latest complete snapshot)",
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="silver/gold: rebuild everything instead of updating incrementally (also automatic when rules change)",
    )
    args = parser.parse_args(argv)
    steps = next(a.choices for a in parser._actions if a.dest == "step")
    if args.step not in steps:  # argparse does not validate defaults, so check the PIPELINE_STEP value here
        parser.error(f"PIPELINE_STEP must be one of {sorted(steps)}, got {args.step!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the requested step and return a process exit code."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    run_id = new_run_id()
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        configure_logging("INFO", run_id)
        logger.error("invalid configuration", extra={"context": {"error": str(exc)}})
        return 2
    configure_logging(settings.log_level, run_id)
    policy = RetryPolicy.from_config(settings.retry)
    try:
        with log_step(logger, "job", requested_step=args.step, is_local=settings.is_local):
            if args.step == "check":
                check_sources(settings, policy)
            elif args.step == "bronze":
                ingest_date = args.ingest_date or datetime.now(timezone.utc).date()
                run_bronze(settings, policy, run_id, ingest_date)
            elif args.step == "silver":
                run_silver(settings, policy, run_id, args.ingest_date, args.full_refresh)
            elif args.step == "gold":
                run_gold(settings, policy, run_id, args.full_refresh)
            elif args.step == "serve":
                run_serve(settings, policy)
            elif args.step == "seed":
                run_seed(settings, policy, only_if_missing=False)
            elif args.step == "all":
                if args.seed_if_missing:
                    run_seed(settings, policy, only_if_missing=True)
                run_bronze(settings, policy, run_id, args.ingest_date or datetime.now(timezone.utc).date())
                run_silver(settings, policy, run_id, args.ingest_date, args.full_refresh)
                run_gold(settings, policy, run_id, args.full_refresh)
                run_serve(settings, policy)
    except Exception:  # already logged with traceback by log_step
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
