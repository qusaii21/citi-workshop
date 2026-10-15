"""
Talent dashboard API: serves the ACME talent insights from the AWS data lake.

Reads only what the pipeline published: ``serving/_current.json`` names the dashboard
documents of the current Gold version (``summary.json``, ``employees.json``), which the
pipeline's ``serve`` step builds from Gold and reconciles against it. Nothing is computed
from raw data here, so the dashboard always shows exactly what the notebook shows.

Routes (under ``/api/talent-dashboard`` behind CloudFront, or at the function URL root):

    GET /summary                 freshness, data quality and the seven questions' aggregates
    GET /employees               employee list: ?list=all|performance|critical_gap|promotion_ready|high_risk|top_learners
                                 &q=<name search>&department=<name>&trend=<rating trend>&page=<n>&size=<n, max 100>
                                 (list=performance adds each employee's review history to the rows)
    GET /employees/<employee_id> one employee's full profile
    GET /lake                    the data-lake catalog: layers, their tables, row counts and columns
    GET /lake/<layer>/<table>    one table's rows, ?page=<n>&size=<25|50|100>
    GET /health                  which Gold version is being served
"""

import json
import logging
import os
import re

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ROUTE_PREFIX = "/api/talent-dashboard"
POINTER_KEY = "serving/_current.json"
MAX_PAGE_SIZE = 100
LISTS = ("all", "performance", "critical_gap", "promotion_ready", "high_risk", "top_learners")
LAKE_PAGE_SIZES = (25, 50, 100)
MAX_CACHED_CHUNKS = 8

_s3 = None
_cache = {}  # (gold_run_id, published_at) -> {"summary": ..., "employees": [...], "by_id": {...}}
_chunks = {}  # chunk key -> rows (a few recently read pages of lake tables)


class ApiError(Exception):
    """An error with an HTTP status, returned to the caller as JSON."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def handler(event=None, context=None):
    """Lambda entry point (function URL / CloudFront event, payload format 2.0)."""
    event = event or {}
    method = (event.get("requestContext", {}).get("http", {}).get("method") or "GET").upper()
    path = _route(event.get("rawPath") or "/")
    params = event.get("queryStringParameters") or {}
    try:
        if method not in ("GET", "HEAD"):
            raise ApiError(405, "only GET is supported")
        pointer = _pointer()
        if path == "/health":
            return _response(200, {"status": "ok", "gold_run_id": pointer["gold_run_id"], "as_of": pointer["as_of"],
                                   "published_at": pointer["published_at"]})
        data = _load(pointer)
        if path == "/summary":
            return _response(200, {**data["summary"], "serving": _serving_info(pointer)})
        if path == "/employees":
            return _response(200, list_employees(data["employees"], params))
        if path == "/lake":
            return _response(200, {**_lake_index(pointer), "serving": _serving_info(pointer)})
        match = re.fullmatch(r"/lake/([a-z]+)/([a-z_.]+)", path)
        if match:
            return _response(200, lake_page(pointer, match.group(1), match.group(2), params))
        match = re.fullmatch(r"/employees/(\d+)", path)
        if match:
            employee = data["by_id"].get(int(match.group(1)))
            if employee is None:
                raise ApiError(404, f"no employee {match.group(1)}")
            return _response(200, {**employee, "serving": _serving_info(pointer)})
        raise ApiError(404, f"unknown route {path}")
    except ApiError as exc:
        return _response(exc.status, {"error": str(exc)})
    except Exception:  # noqa: BLE001 - never leak internals to the browser
        logger.exception("request failed: %s %s", method, path)
        return _response(500, {"error": "internal error; see the function logs"})


def list_employees(employees, params):
    """Filter, sort and page the employee list (the notebook's per-question employee tables)."""
    name = (params.get("list") or "all").strip()
    if name not in LISTS:
        raise ApiError(400, f"list must be one of {', '.join(LISTS)}")
    page, size = _int(params, "page", 1, 1, 10_000), _int(params, "size", 25, 1, MAX_PAGE_SIZE)
    query = (params.get("q") or "").strip().lower()
    department = (params.get("department") or "").strip()
    trend = (params.get("trend") or "").strip()

    rows = [e for e in employees if _in_list(e, name)]
    if query:
        rows = [e for e in rows if query in e["name"].lower()]
    if department:
        rows = [e for e in rows if e["department"] == department]
    if trend:
        rows = [e for e in rows if e["performance"].get("rating_trend") == trend]
    rows.sort(key=_SORT[name])
    start = (page - 1) * size
    return {"list": name, "total": len(rows), "page": page, "size": size,
            "departments": sorted({e["department"] for e in employees}),
            "rows": [_row(e, with_history=name == "performance") for e in rows[start:start + size]]}


def lake_page(pointer, layer, table, params):
    """One page of a lake table from the catalog (only tables listed in the catalog index can be read)."""
    index = _lake_index(pointer)
    entry = next((t for lay in index["layers"] if lay["layer"] == layer for t in lay["tables"] if t["table"] == table), None)
    if entry is None:
        raise ApiError(404, f"no table {layer}/{table} in the data-lake catalog")
    size = _int(params, "size", 50, 1, max(LAKE_PAGE_SIZES))
    if size not in LAKE_PAGE_SIZES:
        raise ApiError(400, f"size must be one of {', '.join(map(str, LAKE_PAGE_SIZES))}")
    pages = max(1, -(-entry["rows"] // size))
    page = _int(params, "page", 1, 1, pages)
    offset, chunk_rows = (page - 1) * size, index["chunk_rows"]
    rows = []
    while len(rows) < size and offset + len(rows) < entry["rows"]:
        position = offset + len(rows)
        chunk = _lake_chunk(pointer, layer, table, position // chunk_rows)
        rows += chunk[position % chunk_rows:position % chunk_rows + size - len(rows)]
    return {"layer": layer, "table": table, "total": entry["rows"], "page": page, "pages": pages, "size": size,
            "columns": entry["columns"], "rows": rows}


def _in_list(e, name):
    if name == "critical_gap":
        return e["critical_gaps"] > 0
    if name == "promotion_ready":
        return e["promotion"].get("is_promotion_ready") is True
    if name == "high_risk":
        return e["attrition"].get("risk_band") == "High"
    return True


def _desc(value):
    """Sort key part: larger first, missing last."""
    return -value if value is not None else float("inf")


# Orders follow the notebook's tables.
_SORT = {
    "all": lambda e: (e["name"], e["employee_id"]),
    "performance": lambda e: (e["name"], e["employee_id"]),
    "critical_gap": lambda e: (-e["critical_gaps"], e["name"]),
    "promotion_ready": lambda e: (_desc(e["performance"].get("latest_valid_rating")), e["critical_gaps"], e["name"]),
    "high_risk": lambda e: (_desc(e["attrition"].get("risk_points")), e["name"]),
    "top_learners": lambda e: (_desc(e["learning"].get("courses_completed")),
                               _desc(e["learning"].get("certifications_earned")), e["name"]),
}


def _row(e, with_history=False):
    """The compact columns shown in list tables (plus the review history for the performance list)."""
    perf, learning, attrition = e["performance"], e["learning"], e["attrition"]
    history = {"review_history": e["review_history"], "previous_valid_rating": perf.get("previous_valid_rating"),
               "latest_valid_period": perf.get("latest_valid_period"),
               "valid_review_count": perf.get("valid_review_count"),
               "pre_hire_review_count": perf.get("pre_hire_review_count")} if with_history else {}
    return {**history,
        "employee_id": e["employee_id"], "name": e["name"], "department": e["department"], "team": e["team"],
        "job_level": e["job_level"], "latest_valid_rating": perf.get("latest_valid_rating"),
        "rating_trend": perf.get("rating_trend"), "critical_gaps": e["critical_gaps"],
        "critical_competencies": [g["competency_name"] for g in e["skill_gaps"] if g["is_critical_gap"]],
        "courses_completed": learning.get("courses_completed"),
        "certifications_earned": learning.get("certifications_earned"),
        "is_promotion_ready": e["promotion"].get("is_promotion_ready"),
        "risk_band": attrition.get("risk_band"), "risk_points": attrition.get("risk_points"),
        "hr_attrition_risk": attrition.get("hr_attrition_risk"),
    }


# ---------------------------------------------------------------- data access

def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")  # region and (locally) endpoint come from the environment
    return _s3


def _bucket():
    bucket = os.environ.get("TALENT_LAKE_BUCKET", "").strip()
    if not bucket:
        raise ApiError(503, "TALENT_LAKE_BUCKET is not configured")
    return bucket


def _get_json(key):
    try:
        body = _client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def _pointer():
    pointer = _get_json(POINTER_KEY)
    if pointer is None:
        raise ApiError(503, "no dashboard data published yet: run the pipeline (job.py --step all)")
    return pointer


def _load(pointer):
    """The documents of the pointer's version, read once per version and kept warm."""
    run_id = pointer["gold_run_id"]
    version = (run_id, pointer["published_at"])  # a republish of the same Gold version is a new version too
    if version not in _cache:
        summary, employees = _get_json(pointer["summary_key"]), _get_json(pointer["employees_key"])
        if summary is None or employees is None:
            raise ApiError(503, f"dashboard data for Gold version {run_id} is incomplete")
        rows = employees["employees"]
        _cache.clear()  # keep only the version being served
        _cache[version] = {"summary": summary, "employees": rows, "by_id": {e["employee_id"]: e for e in rows}}
    return _cache[version]


def _lake_index(pointer):
    if not pointer.get("lake_index_key"):
        raise ApiError(503, "the data-lake catalog is not published yet: rerun the pipeline")
    data = _load(pointer)
    if "lake_index" not in data:
        index = _get_json(pointer["lake_index_key"])
        if index is None:
            raise ApiError(503, "the data-lake catalog is missing")
        data["lake_index"] = index
    return data["lake_index"]


def _lake_chunk(pointer, layer, table, number):
    run_prefix = pointer["summary_key"].rsplit("/", 1)[0]   # serving/runs/run_id=<gold run>
    key = f"{run_prefix}/lake/{layer}/{table}/chunk-{number:05d}.json"
    if key not in _chunks:
        page = _get_json(key)
        if page is None:
            raise ApiError(503, f"data-lake page {layer}/{table}/{number} is missing")
        if len(_chunks) >= MAX_CACHED_CHUNKS:
            _chunks.pop(next(iter(_chunks)))  # drop the oldest
        _chunks[key] = page["rows"]
    return _chunks[key]


def _serving_info(pointer):
    return {"bucket": _bucket(), "gold_run_id": pointer["gold_run_id"], "published_at": pointer["published_at"]}


# ---------------------------------------------------------------- HTTP helpers

def _route(raw_path):
    path = raw_path[len(ROUTE_PREFIX):] if raw_path.startswith(ROUTE_PREFIX) else raw_path
    return "/" + path.strip("/")


def _int(params, name, default, low, high):
    raw = params.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ApiError(400, f"{name} must be an integer") from None
    if not low <= value <= high:
        raise ApiError(400, f"{name} must be between {low} and {high}")
    return value


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
        "body": json.dumps(body, separators=(",", ":")),
    }
