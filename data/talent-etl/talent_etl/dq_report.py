"""
Data-quality reporting from the published Silver version, for the notebook's data-quality section.

Everything is read from the manifest of the run that ``silver/_current.json`` points to, so
the report always describes exactly the data readers see.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from talent_etl.io_s3 import S3Client
from talent_etl.silver import SilverSourceError, read_current


def published_manifest(s3: S3Client, bucket: str) -> dict[str, Any]:
    """
    Return the manifest of the published Silver version.

    Raises:
        SilverSourceError: If nothing has been published yet.
    """
    pointer = read_current(s3, bucket)
    if pointer is None:
        raise SilverSourceError(f"no published Silver in s3://{bucket}; run --step silver")
    return json.loads(s3.get_bytes(bucket, pointer.manifest_key))


def entity_quality_table(manifest: dict[str, Any]) -> pd.DataFrame:
    """One row per entity: read, accepted, warned, quarantined, deduplicated and quarantine rate."""
    rows = []
    for entity in manifest["entities"]:
        counts = entity["counts"]
        rows.append({
            "entity": entity["entity"], "mode": entity.get("mode", ""), **counts,
            "quarantine_rate": counts["quarantined"] / counts["read"] if counts.get("read") else 0.0,
        })
    return pd.DataFrame(rows).set_index("entity")


def history_metrics_table(manifest: dict[str, Any]) -> pd.DataFrame:
    """
    BUS-010/011 dataset-level metrics with the random-order baseline (informational, A13).

    ``vs_random`` says whether the observed rate lies inside the approximate 95% range expected
    if event types were in random order: inside means the history carries no sequence signal.
    """
    learning = next(e for e in manifest["entities"] if e["entity"] == "learning_activity")
    rows = []
    for code, metric in learning.get("metrics", {}).get("history", {}).items():
        low, high = metric["random_baseline_approx_95"] or (None, None)
        inside = metric["rate"] is not None and low <= metric["rate"] <= high
        rows.append({
            "rule_id": metric["rule_id"], "metric": code, "events": metric["events"], "flagged": metric["flagged"],
            "rate": metric["rate"], "random_baseline_rate": metric["random_baseline_rate"],
            "random_low": low, "random_high": high,
            "vs_random": "consistent with random order" if inside else "differs from random order",
        })
    return pd.DataFrame(rows).set_index("rule_id")


def gold_checks_table(s3: S3Client, bucket: str) -> pd.DataFrame:
    """
    The validation checks behind every table of the published Gold version.

    A Gold version can reuse tables built by earlier runs, so each table's checks are read
    from the manifest of the run that built it (the run folder in the table's key).
    """
    from talent_etl.gold import GOLD_LAYER, read_current_gold  # local: avoid import cycle

    pointer = read_current_gold(s3, bucket)
    if pointer is None:
        raise SilverSourceError(f"no published Gold in s3://{bucket}; run --step gold")
    manifests: dict[str, dict[str, Any]] = {}
    rows = []
    for table, key in pointer.files.items():
        run_id = GOLD_LAYER.run_id_of(key)
        if run_id not in manifests:
            manifests[run_id] = json.loads(s3.get_bytes(bucket, GOLD_LAYER.manifest_key(run_id)))
        built = next(t for t in manifests[run_id]["tables"] if t["table"] == table)
        checks = built["checks"]
        rows.append({"table": table, "rows": pointer.rows[table], "built_by_run": run_id,
                     "checks": len(checks), "passed": sum(c["passed"] for c in checks),
                     "failed": [c["name"] for c in checks if not c["passed"]]})
    return pd.DataFrame(rows).set_index("table")
