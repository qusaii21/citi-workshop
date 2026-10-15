"""
Data lake path conventions, shared by every layer.

Bronze layout (all keys are relative to the lake bucket)::

    bronze/postgres/<table>/ingest_date=YYYY-MM-DD/part-00000.parquet
    bronze/learning_activity/year=YYYY/month=MM/day=DD/<original file name>
    bronze/_manifests/ingest_date=YYYY-MM-DD/run_id=<run_id>.json

Silver layout (see docs/source_to_target.md). Every run writes a new, immutable run
folder; readers find the published one through the ``_current.json`` pointer::

    silver/_current.json                                         pointer: the published run (atomic commit)
    silver/_manifests/run_id=<run_id>.json                       one per run (SUCCEEDED / FAILED / SUPERSEDED)
    silver/runs/run_id=<run_id>/_STARTED.json                    first object of a run (age for cleanup)
    silver/runs/run_id=<run_id>/<entity>/part-00000.parquet      unpartitioned entities
    silver/runs/run_id=<run_id>/learning_activity/event_year=YYYY/event_month=MM/part-00000.parquet
    silver/runs/run_id=<run_id>/_quarantine/<entity>/part-00000.parquet
    silver/runs/run_id=<run_id>/_state/<name>.parquet            incremental state (file ledger, event index)

A published run may reference files written by earlier runs (incremental reuse); the
pointer lists every key it uses, so readers never need to list folders.
"""

from __future__ import annotations

import re
from datetime import date

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
PART_FILE = "part-00000.parquet"
LEARNING_DATASET = "learning_activity"

# Hive-style day partition followed by a file name, e.g. year=2024/month=02/day=28/events-000.jsonl
DAY_PARTITION = re.compile(r"^year=\d{4}/month=(0[1-9]|1[0-2])/day=(0[1-9]|[12]\d|3[01])/[^/]+$")


def bronze_postgres_prefix(table: str, ingest_date: date) -> str:
    """Return the Bronze partition prefix for one PostgreSQL table snapshot."""
    return f"{BRONZE}/postgres/{table}/ingest_date={ingest_date.isoformat()}/"


def bronze_postgres_key(table: str, ingest_date: date) -> str:
    """Return the single Parquet object key for one PostgreSQL table snapshot."""
    return bronze_postgres_prefix(table, ingest_date) + PART_FILE


def bronze_learning_prefix() -> str:
    """Return the Bronze prefix that mirrors the source learning_activity/ tree."""
    return f"{BRONZE}/{LEARNING_DATASET}/"


def bronze_manifest_key(ingest_date: date, run_id: str) -> str:
    """Return the key of the per-run Bronze manifest (an audit record, one per run)."""
    return f"{BRONZE}/_manifests/ingest_date={ingest_date.isoformat()}/run_id={run_id}.json"


def silver_run_prefix(run_id: str) -> str:
    """Return the folder that holds every object written by one Silver run."""
    return f"{SILVER}/runs/run_id={run_id}/"


def silver_dataset_prefix(entity: str, run_id: str) -> str:
    """Return the prefix of one entity's dataset inside a Silver run folder."""
    return f"{silver_run_prefix(run_id)}{entity}/"


def silver_key(entity: str, run_id: str) -> str:
    """Return the single Parquet key of an unpartitioned Silver dataset in a run folder."""
    return silver_dataset_prefix(entity, run_id) + PART_FILE


def silver_month_key(entity: str, year: int, month: int, run_id: str) -> str:
    """Return the Parquet key of one event_year/event_month partition in a run folder."""
    return f"{silver_dataset_prefix(entity, run_id)}event_year={year:04d}/event_month={month:02d}/{PART_FILE}"


def silver_quarantine_key(entity: str, run_id: str) -> str:
    """Return the Parquet key holding every quarantined record of an entity in a run folder."""
    return f"{silver_run_prefix(run_id)}_quarantine/{entity}/{PART_FILE}"


def silver_started_key(run_id: str) -> str:
    """Return the marker written first by a run; its timestamp lets cleanup spot abandoned runs."""
    return f"{silver_run_prefix(run_id)}_STARTED.json"


def silver_current_key() -> str:
    """Return the pointer object naming the published Silver run (replaced atomically)."""
    return f"{SILVER}/_current.json"


def silver_manifest_key(run_id: str) -> str:
    """Return the key of the per-run Silver manifest."""
    return f"{SILVER}/_manifests/run_id={run_id}.json"


def silver_state_key(name: str, run_id: str) -> str:
    """Return the key of an incremental-state table (e.g. the learning file ledger) in a run folder."""
    return f"{silver_run_prefix(run_id)}_state/{name}.parquet"


def gold_key(table: str, run_id: str) -> str:
    """Return the Parquet key of one Gold table written by a run."""
    return f"{GOLD}/runs/run_id={run_id}/{table}/{PART_FILE}"
