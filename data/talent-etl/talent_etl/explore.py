"""
Read-only access to every layer of the data lake, for browsing (notebooks/explore_lake.ipynb).

Each layer is read the way its readers are meant to read it:
- Bronze: the latest ingest date's PostgreSQL snapshots, and the learning-activity JSONL files.
- Silver and Gold: the published version, through ``<layer>/_current.json``.
Nothing here writes to the lake.
"""

from __future__ import annotations

import io
import json
import re

import pandas as pd

from talent_etl.gold import read_current_gold
from talent_etl.io_s3 import S3Client
from talent_etl.paths import BRONZE
from talent_etl.silver import read_current

LAYERS = ("bronze", "silver", "gold")
_INGEST_DATE = re.compile(r"/ingest_date=(\d{4}-\d{2}-\d{2})/")


def bronze_files(s3: S3Client, bucket: str) -> dict[str, list[str]]:
    """Bronze tables of the latest ingest date (one Parquet file each) and the learning JSONL files."""
    tables: dict[str, dict[str, str]] = {}
    for obj in s3.list_objects(bucket, f"{BRONZE}/postgres/"):
        match = _INGEST_DATE.search(obj.key)
        if match and obj.key.endswith(".parquet"):
            tables.setdefault(obj.key.split("/")[2], {})[match.group(1)] = obj.key
    files = {table: [by_date[max(by_date)]] for table, by_date in sorted(tables.items())}
    learning = [o.key for o in s3.list_objects(bucket, f"{BRONZE}/learning_activity/") if o.key.endswith(".jsonl")]
    if learning:
        files["learning_activity"] = learning
    return files


def table_files(s3: S3Client, bucket: str, layer: str) -> dict[str, list[str]]:
    """Every table of a layer and the files that hold it."""
    if layer == "bronze":
        return bronze_files(s3, bucket)
    if layer == "silver":
        pointer = read_current(s3, bucket)
        return {} if pointer is None else {name: list(keys) for name, keys in sorted(pointer.files.items())}
    if layer == "gold":
        pointer = read_current_gold(s3, bucket)
        return {} if pointer is None else {name: [key] for name, key in sorted(pointer.files.items())}
    raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")


def overview(s3: S3Client, bucket: str) -> pd.DataFrame:
    """One row per layer and table: how many files hold it (rows are counted when a table is loaded)."""
    rows = [{"layer": layer, "table": table, "files": len(keys), "example_file": keys[0]}
            for layer in LAYERS for table, keys in table_files(s3, bucket, layer).items()]
    return pd.DataFrame(rows, columns=["layer", "table", "files", "example_file"])


def load(s3: S3Client, bucket: str, layer: str, table: str, max_files: int | None = None) -> pd.DataFrame:
    """
    One table of a layer as pandas. ``max_files`` limits how many files are read (for large tables).

    Bronze learning activity is shown as parsed JSON with its source file and line; a line that
    is not valid JSON is kept in ``_raw`` (Bronze is a byte-for-byte copy, including bad lines).
    """
    files = table_files(s3, bucket, layer)
    if table not in files:
        raise KeyError(f"{layer} has no table {table!r}; available: {sorted(files)}")
    keys = files[table][:max_files] if max_files else files[table]
    if not keys:
        return pd.DataFrame()
    if keys[0].endswith(".jsonl"):
        return _jsonl(s3, bucket, keys)
    return pd.concat([s3.read_arrow(bucket, key).to_pandas() for key in keys], ignore_index=True)


def quarantine(s3: S3Client, bucket: str, table: str) -> pd.DataFrame:
    """The records Silver rejected for a table, with the rule and the original raw record."""
    pointer = read_current(s3, bucket)
    if pointer is None or table not in pointer.quarantine:
        return pd.DataFrame()
    return s3.read_arrow(bucket, pointer.quarantine[table]).to_pandas()


def _jsonl(s3: S3Client, bucket: str, keys: list[str]) -> pd.DataFrame:
    rows = []
    for key in keys:
        for number, line in enumerate(s3.get_bytes(bucket, key).decode("utf-8", errors="replace").splitlines(), 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                record = record if isinstance(record, dict) else {"_raw": line}
            except json.JSONDecodeError:
                record = {"_raw": line}
            rows.append({**record, "_source_file": key, "_source_line": number})
    return pd.DataFrame(rows)


def read_text(s3: S3Client, bucket: str, key: str) -> str:
    """A JSON object of the lake (pointers, manifests), pretty-printed."""
    return json.dumps(json.load(io.BytesIO(s3.get_bytes(bucket, key))), indent=2)
