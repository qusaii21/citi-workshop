"""
Versioned, atomically published layers (shared by Silver and Gold).

A layer such as ``silver`` or ``gold`` keeps:

    <layer>/_current.json                        pointer to the published version (replaced in one PUT)
    <layer>/_manifests/run_id=<run_id>.json      one per run (the audit trail, never deleted)
    <layer>/runs/run_id=<run_id>/_STARTED.json   first object a run writes (age for cleanup)
    <layer>/runs/run_id=<run_id>/...             files written by that run

A published version may reference files written by earlier runs, so cleanup keeps every
file that the current or the previous version references, and run folders without a
manifest until they are old enough to be considered abandoned.
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from talent_etl.io_s3 import S3Client

logger = logging.getLogger(__name__)

# A run folder with no manifest is treated as abandoned (hard crash) once it is this old.
# Younger folders without a manifest may belong to a run that is still writing, so they are kept.
ABANDONED_RUN_AGE = timedelta(hours=24)


@dataclass(frozen=True)
class Layer:
    """Key conventions of one versioned layer."""

    name: str

    def run_prefix(self, run_id: str) -> str:
        """Folder holding every object written by one run."""
        return f"{self.name}/runs/run_id={run_id}/"

    def started_key(self, run_id: str) -> str:
        """Marker written first by a run; its timestamp lets cleanup spot abandoned runs."""
        return f"{self.run_prefix(run_id)}_STARTED.json"

    def manifest_key(self, run_id: str) -> str:
        """Per-run manifest."""
        return f"{self.name}/_manifests/run_id={run_id}.json"

    def current_key(self) -> str:
        """Pointer naming the published version."""
        return f"{self.name}/_current.json"

    def run_id_of(self, key: str) -> str | None:
        """Return the run id of a key inside a run folder, or None."""
        match = re.match(rf"^{re.escape(self.name)}/runs/run_id=([^/]+)/", key)
        return match.group(1) if match else None


def put_json(s3: S3Client, bucket: str, key: str, payload: Any) -> None:
    """Write a JSON object (dates and other non-JSON types are written as strings)."""
    s3.put_bytes(bucket, key, json.dumps(payload, indent=2, default=str).encode(), content_type="application/json")


def read_json(s3: S3Client, bucket: str, key: str) -> Any | None:
    """Read a JSON object, or None if it does not exist."""
    if not s3.object_exists(bucket, key):
        return None
    return json.loads(s3.get_bytes(bucket, key))


def mark_started(s3: S3Client, bucket: str, layer: Layer, run_id: str, started_at: datetime) -> None:
    """Write the run's ``_STARTED.json`` marker (before any data file)."""
    put_json(s3, bucket, layer.started_key(run_id), {"run_id": run_id, "started_at": started_at.isoformat()})


def collect_garbage(s3: S3Client, bucket: str, layer: Layer, retained: set[str], now: datetime) -> int:
    """
    Delete a layer's objects that no retained version references.

    Kept: the pointer, every manifest, every key in ``retained``, and files of run folders
    that have no manifest yet and started less than ABANDONED_RUN_AGE ago. Deleted: other
    files of finished (manifest present) or abandoned runs, and stray objects outside
    ``runs/``. Errors are logged and never raised: the run is already published.
    """
    try:
        by_run: dict[str, list[str]] = defaultdict(list)
        doomed: list[str] = []
        for obj in s3.list_objects(bucket, f"{layer.name}/"):
            if obj.key == layer.current_key() or obj.key.startswith(f"{layer.name}/_manifests/"):
                continue
            run_id = layer.run_id_of(obj.key)
            if run_id is None:
                doomed.append(obj.key)  # outside runs/: e.g. an earlier fixed-path layout
            elif obj.key not in retained:
                by_run[run_id].append(obj.key)
        for run_id, keys in by_run.items():
            if _is_finished_or_abandoned(s3, bucket, layer, run_id, now):
                doomed += keys
        deleted = s3.delete_keys(bucket, doomed)
        logger.info(f"{layer.name} cleanup", extra={"context": {"objects_deleted": deleted,
                                                               "objects_retained": len(retained)}})
        return deleted
    except Exception:  # noqa: BLE001
        logger.warning(f"{layer.name} cleanup failed; it will be retried by the next run", exc_info=True)
        return 0


def _is_finished_or_abandoned(s3: S3Client, bucket: str, layer: Layer, run_id: str, now: datetime) -> bool:
    if s3.object_exists(bucket, layer.manifest_key(run_id)):
        return True
    marker = read_json(s3, bucket, layer.started_key(run_id))
    if marker is None:
        return True  # no marker: not written by this protocol
    return now - datetime.fromisoformat(marker["started_at"]) > ABANDONED_RUN_AGE
