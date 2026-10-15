"""Tests for the talent dashboard API (run: python -m pytest backend/talent-dashboard/tests)."""

import io
import json
import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import function  # noqa: E402

POINTER = {"gold_run_id": "g1", "as_of": "2026-10-15", "published_at": "2026-10-15T07:00:00+00:00",
           "summary_key": "serving/runs/run_id=g1/summary.json", "employees_key": "serving/runs/run_id=g1/employees.json"}


def employee(eid, name, department="Sales", rating=3.0, critical=0, ready=False, band="Low", points=0, courses=0,
             trend="stable"):
    return {"employee_id": eid, "name": name, "department": department, "team": "T1", "job_level": "Mid",
            "performance": {"latest_valid_rating": rating, "rating_trend": trend, "valid_review_count": 1},
            "review_history": [{"review_period": "2025-H1", "rating": 3, "is_valid": True, "is_pre_hire": False}],
            "skill_gaps": [{"competency_name": f"Skill {i}", "is_critical_gap": True} for i in range(critical)],
            "critical_gaps": critical, "learning": {"courses_completed": courses, "certifications_earned": 0},
            "promotion": {"is_promotion_ready": ready}, "attrition": {"risk_band": band, "risk_points": points}}


EMPLOYEES = [
    employee(1, "Ada One", rating=5.0, ready=True, courses=3),
    employee(2, "Ben Two", department="IT", critical=2, band="High", points=4, trend="declining"),
    employee(3, "Cy Three", rating=None, critical=1, band="High", points=3, courses=5),
    employee(4, "Di Four", rating=4.0, ready=True, critical=1),
]


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.reads = []

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(json.dumps(self.objects[Key]).encode())}


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3({function.POINTER_KEY: POINTER, POINTER["summary_key"]: {"q1": {"rated_employees": 3}},
                   POINTER["employees_key"]: {"employees": EMPLOYEES}})
    monkeypatch.setattr(function, "_s3", fake)
    monkeypatch.setattr(function, "_cache", {})
    monkeypatch.setenv("TALENT_LAKE_BUCKET", "lake")
    return fake


def call(path, method="GET", **params):
    response = function.handler({"rawPath": path, "queryStringParameters": params or None,
                                 "requestContext": {"http": {"method": method}}})
    return response["statusCode"], json.loads(response["body"])


def test_summary_through_cloudfront_and_function_url(s3):
    status, body = call("/api/talent-dashboard/summary")
    assert status == 200 and body["q1"] == {"rated_employees": 3}
    assert body["serving"] == {"bucket": "lake", "gold_run_id": "g1", "published_at": POINTER["published_at"]}
    assert call("/summary/")[0] == 200                               # function URL root, trailing slash
    assert s3.reads.count(POINTER["summary_key"]) == 1               # documents cached per version


def test_health(s3):
    assert call("/api/talent-dashboard/health") == (200, {"status": "ok", "gold_run_id": "g1",
                                                          "as_of": "2026-10-15", "published_at": POINTER["published_at"]})


@pytest.mark.parametrize("name, expected", [
    ("all", [1, 2, 3, 4]),
    ("performance", [1, 2, 3, 4]),
    ("critical_gap", [2, 3, 4]),          # most critical gaps first, then name
    ("promotion_ready", [1, 4]),          # highest rating first
    ("high_risk", [2, 3]),                # most points first
    ("top_learners", [3, 1, 2, 4]),       # most courses first, ties by name
])
def test_lists(s3, name, expected):
    status, body = call("/api/talent-dashboard/employees", list=name)
    assert status == 200 and [r["employee_id"] for r in body["rows"]] == expected
    assert body["total"] == len(expected) and body["departments"] == ["IT", "Sales"]


def test_list_row_columns_search_filter_and_paging(s3):
    _, body = call("/api/talent-dashboard/employees", list="critical_gap", q="two")
    assert body["rows"] == [{
        "employee_id": 2, "name": "Ben Two", "department": "IT", "team": "T1", "job_level": "Mid",
        "latest_valid_rating": 3.0, "rating_trend": "declining", "critical_gaps": 2,
        "critical_competencies": ["Skill 0", "Skill 1"], "courses_completed": 0, "certifications_earned": 0,
        "is_promotion_ready": False, "risk_band": "High", "risk_points": 4, "hr_attrition_risk": None}]
    assert call("/api/talent-dashboard/employees", department="IT")[1]["total"] == 1
    _, page2 = call("/api/talent-dashboard/employees", page="2", size="3")
    assert (page2["total"], [r["employee_id"] for r in page2["rows"]]) == (4, [4])


@pytest.mark.parametrize("params, message", [
    ({"list": "secret"}, "list must be one of"), ({"size": "500"}, "size must be between"),
    ({"page": "x"}, "page must be an integer"),
])
def test_bad_parameters(s3, params, message):
    status, body = call("/api/talent-dashboard/employees", **params)
    assert status == 400 and message in body["error"]


def test_employee_detail(s3):
    status, body = call("/api/talent-dashboard/employees/3")
    assert status == 200 and body["name"] == "Cy Three" and body["serving"]["gold_run_id"] == "g1"
    assert call("/api/talent-dashboard/employees/99")[0] == 404


def test_unknown_route_and_method(s3):
    assert call("/api/talent-dashboard/nope")[0] == 404
    assert call("/api/talent-dashboard/summary", method="POST")[0] == 405


def test_not_published_yet(s3):
    del s3.objects[function.POINTER_KEY]
    status, body = call("/api/talent-dashboard/summary")
    assert status == 503 and "no dashboard data published yet" in body["error"]


def test_new_version_is_picked_up(s3):
    call("/api/talent-dashboard/summary")
    s3.objects[function.POINTER_KEY] = {**POINTER, "gold_run_id": "g2", "summary_key": "s2", "employees_key": "e2"}
    s3.objects.update({"s2": {"q1": {"rated_employees": 9}}, "e2": {"employees": EMPLOYEES[:1]}})
    assert call("/api/talent-dashboard/summary")[1]["q1"] == {"rated_employees": 9}
    assert list(function._cache) == [("g2", POINTER["published_at"])]
    s3.objects[function.POINTER_KEY] = {**s3.objects[function.POINTER_KEY], "published_at": "later"}  # republished
    s3.objects["s2"] = {"q1": {"rated_employees": 10}}
    assert call("/api/talent-dashboard/summary")[1]["q1"] == {"rated_employees": 10}


def test_missing_configuration_and_internal_errors(s3, monkeypatch):
    monkeypatch.delenv("TALENT_LAKE_BUCKET")
    assert call("/api/talent-dashboard/summary")[0] == 503
    monkeypatch.setenv("TALENT_LAKE_BUCKET", "lake")
    s3.objects[function.POINTER_KEY] = {"unexpected": True}           # malformed pointer
    status, body = call("/api/talent-dashboard/summary")
    assert status == 500 and body == {"error": "internal error; see the function logs"}


LAKE_INDEX = {"chunk_rows": 4, "layers": [
    {"layer": "bronze", "description": "raw", "tables": [
        {"table": "teams", "rows": 10, "chunks": 3, "columns": [{"name": "team_id", "type": "int64"}]}]},
    {"layer": "silver", "description": "clean", "tables": [
        {"table": "learning_activity.quarantine", "rows": 1, "chunks": 1, "columns": []}]},
]}


@pytest.fixture
def lake(s3, monkeypatch):
    run = "serving/runs/run_id=g1"
    s3.objects[function.POINTER_KEY] = {**POINTER, "lake_index_key": f"{run}/lake_index.json"}
    s3.objects[f"{run}/lake_index.json"] = LAKE_INDEX
    for chunk in range(3):
        s3.objects[f"{run}/lake/bronze/teams/chunk-{chunk:05d}.json"] = {
            "rows": [{"team_id": i} for i in range(chunk * 4, min(chunk * 4 + 4, 10))]}
    s3.objects[f"{run}/lake/silver/learning_activity.quarantine/chunk-00000.json"] = {"rows": [{"reason": "bad"}]}
    monkeypatch.setattr(function, "_chunks", {})
    monkeypatch.setattr(function, "LAKE_PAGE_SIZES", (2, 5, 25))
    return s3


def test_lake_index(lake):
    status, body = call("/api/talent-dashboard/lake")
    assert status == 200 and [layer["layer"] for layer in body["layers"]] == ["bronze", "silver"]
    assert body["serving"]["gold_run_id"] == "g1"


def test_lake_pages_span_chunks(lake):
    status, body = call("/api/talent-dashboard/lake/bronze/teams", size="5", page="1")
    assert status == 200 and [r["team_id"] for r in body["rows"]] == [0, 1, 2, 3, 4]       # chunks 0 and 1
    assert (body["total"], body["pages"], body["columns"]) == (10, 2, [{"name": "team_id", "type": "int64"}])
    assert [r["team_id"] for r in call("/api/talent-dashboard/lake/bronze/teams", size="5", page="2")[1]["rows"]] \
        == [5, 6, 7, 8, 9]                                                                    # last chunk is partial
    assert call("/api/talent-dashboard/lake/silver/learning_activity.quarantine", size="25")[1]["rows"] == [{"reason": "bad"}]


def test_lake_rejects_unknown_tables_and_bad_pages(lake):
    assert call("/api/talent-dashboard/lake/gold/teams")[0] == 404                         # not in the catalog
    assert call("/api/talent-dashboard/lake/bronze/teams", size="3")[0] == 400             # not an allowed size
    assert call("/api/talent-dashboard/lake/bronze/teams", size="5", page="3")[0] == 400   # beyond the last page


def test_lake_chunk_cache_is_bounded(lake, monkeypatch):
    monkeypatch.setattr(function, "MAX_CACHED_CHUNKS", 2)
    for page in (1, 2, 3, 4, 5):
        call("/api/talent-dashboard/lake/bronze/teams", size="2", page=str(page))
    assert len(function._chunks) == 2


def test_lake_not_published_or_missing(s3):
    assert call("/api/talent-dashboard/lake")[0] == 503                                    # pointer without a catalog
    s3.objects[function.POINTER_KEY] = {**POINTER, "lake_index_key": "missing.json"}
    assert call("/api/talent-dashboard/lake")[0] == 503


def test_performance_list_carries_review_history_and_filters_by_trend(s3):
    _, body = call("/api/talent-dashboard/employees", list="performance", trend="declining")
    assert [r["name"] for r in body["rows"]] == ["Ben Two"]
    row = body["rows"][0]
    assert row["review_history"] == [{"review_period": "2025-H1", "rating": 3, "is_valid": True, "is_pre_hire": False}]
    assert row["valid_review_count"] == 1 and "previous_valid_rating" in row
    assert "review_history" not in call("/api/talent-dashboard/employees", list="all")[1]["rows"][0]
