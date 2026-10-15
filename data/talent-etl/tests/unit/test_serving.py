"""Unit tests for talent_etl.serving (the dashboard's data), on a small hand-built Gold."""

import json
from datetime import date

import pandas as pd
import pytest

from talent_etl import serving
from talent_etl.serving import _records, build_employees, build_summary, reconcile, serving_key
from tests.unit.test_explore import lake  # noqa: F401 - fixture: a small lake on a fake S3

NAN = float("nan")


def gold_tables():
    """Three employees: 1 promotion-ready and improving, 2 declining with a critical gap, 3 hired after all reviews."""
    ids = [1, 2, 3]
    return {
        "dim_employee": pd.DataFrame({
            "employee_id": ids, "first_name": ["Ada", "Ben", "Cy"], "last_name": ["One", "Two", "Three"],
            "email": ["a@x", "b@x", "c@x"], "job_level": ["Senior", "Mid", "Junior"],
            "hire_date": [date(2020, 1, 1), date(2021, 6, 1), date(2025, 9, 1)], "team_id": [10, 10, 20],
            "team_name": ["T10", "T10", "T20"], "department": ["Sales", "Sales", "IT"],
            "tenure_years": [6.79, 5.37, 1.12], "as_of": [date(2026, 10, 14)] * 3}),
        "performance_history": pd.DataFrame({
            "employee_id": [1, 1, 2, 2, 3], "review_period": ["2024-H2", "2025-H1", "2024-H2", "2025-H1", "2025-H1"],
            "review_id": [1, 2, 3, 4, 5], "rating": [3, 4, 4, 2, 3],
            "period_start": [date(2024, 7, 1), date(2025, 1, 1), date(2024, 7, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "period_end": [date(2024, 12, 31), date(2025, 6, 30), date(2024, 12, 31), date(2025, 6, 30), date(2025, 6, 30)],
            "submitted_date": [date(2025, 1, 5)] * 5, "manager_comment": ["c"] * 5,
            "is_pre_hire": [False, False, False, False, True], "is_valid": [True, True, True, True, False],
            "previous_valid_rating": [NAN, 3.0, NAN, 4.0, NAN], "rating_change": [NAN, 1.0, NAN, -2.0, NAN]}),
        "employee_performance": pd.DataFrame({
            "employee_id": ids, "valid_review_count": [2, 2, 0], "pre_hire_review_count": [0, 0, 1],
            "latest_valid_period": ["2025-H1", "2025-H1", None], "latest_valid_rating": [4.0, 2.0, NAN],
            "previous_valid_rating": [3.0, 4.0, NAN], "rating_delta": [1.0, -2.0, NAN],
            "rating_trend": ["improving", "declining", "insufficient_history"],
            "is_high_performer": [True, False, False], "is_low_performer": [False, True, False]}),
        "employee_skill_gaps": pd.DataFrame({
            "employee_id": [1, 2], "competency_id": [100, 101], "competency_name": ["Python", "Negotiation"],
            "competency_category": ["Technical", "Domain"], "department": ["Sales", "Sales"], "team_id": [10, 10],
            "job_level": ["Senior", "Mid"], "assessment_id": [7, 8], "assessed_date": [date(2025, 3, 1)] * 2,
            "proficiency_level": [4, 1], "required_level": [3, 4], "gap": [-1, 3], "gap_ratio": [-0.33, 0.75],
            "has_gap": [False, True], "is_critical_gap": [False, True], "is_pre_hire": [False, False]}),
        "competency_gap_summary": pd.DataFrame({
            "competency_id": [100, 101], "competency_name": ["Python", "Negotiation"],
            "competency_category": ["Technical", "Domain"], "employees_assessed": [1, 1], "avg_proficiency": [4.0, 1.0],
            "avg_required": [3.0, 4.0], "employees_below_required": [0, 1], "critical_gap_count": [0, 1],
            "critical_gap_rate": [0.0, 1.0], "rank_by_critical_rate": [2, 1]}),
        "skill_distribution": pd.DataFrame({
            "competency_id": [100, 101], "competency_name": ["Python", "Negotiation"],
            "competency_category": ["Technical", "Domain"], "department": ["Sales", "Sales"],
            "proficiency_level": [4, 1], "employees": [1, 1], "share_within_group": [1.0, 1.0]}),
        "employee_learning": pd.DataFrame({
            "employee_id": ids, "events_counted": [5, 0, 1], "pre_hire_events_excluded": [0, 0, 2],
            "courses_enrolled": [2, 0, 1], "courses_completed": [5, 0, 1], "certifications_earned": [1, 0, 0],
            "quiz_attempts": [1, 0, 0], "quiz_passes": [2, 0, 0], "avg_quiz_score": [81.25, NAN, NAN],
            "last_activity_date": [date(2026, 9, 1), None, date(2026, 1, 1)], "events_last_12m": [5, 0, 1],
            "is_learning_active_12m": [True, False, True]}),
        "promotion_pipeline": pd.DataFrame({
            "employee_id": ids, "review_cycle": ["2025"] * 3, "potential_rating": ["High", "High", "High"],
            "promotion_readiness": ["Ready Now", "Ready Now", "Ready Now"], "hr_attrition_risk": ["Low", "High", "Low"],
            "talent_review_pre_hire": [False, False, True], "job_level": ["Senior", "Mid", "Junior"],
            "department": ["Sales", "Sales", "IT"], "latest_valid_rating": [4.0, 2.0, NAN],
            "rating_trend": ["improving", "declining", "insufficient_history"], "critical_gap_count": [0, 1, 0],
            "courses_completed": [5, 0, 1], "is_promotion_ready": [True, False, False]}),
        "attrition_risk": pd.DataFrame({
            "employee_id": ids, "hr_attrition_risk": ["Low", "High", "Low"], "talent_review_pre_hire": [False, False, True],
            "latest_valid_rating": [4.0, 2.0, NAN], "rating_trend": ["improving", "declining", "insufficient_history"],
            "hr_points": [0, 2, 0], "is_rating_declining": [False, True, False], "is_low_latest_rating": [False, True, False],
            "is_learning_inactive_12m": [False, True, False], "risk_points": [0, 5, 0], "risk_band": ["Low", "High", "Low"]}),
        "team_performance": pd.DataFrame({
            "team_id": [10, 20, 30, 40], "team_name": ["T10", "T20", "T30", "T40"],
            "department": ["Sales", "IT", "IT", "HR"], "headcount": [2, 1, 0, 0],
            "employees_with_valid_rating": [2, 0, 0, 0], "avg_latest_valid_rating": [3.0, NAN, 4.0, 2.0],
            "high_performer_share": [0.5, NAN, NAN, NAN], "critical_gap_count": [1, 0, 0, 0],
            "project_count": [2, 1, 1, 0], "avg_project_outcome_score": [70.0, 50.0, 80.0, NAN],
            "on_time_project_share": [1.0, 0.0, 0.5, NAN], "has_projects": [True, True, True, False]}),
    }


CONTEXT = {"freshness": {"environment": "local (LocalStack)", "gold_run_id": "g1"}, "quality": [],
           "history_metrics": [], "gold_checks": []}


def test_summary_answers_each_question():
    s = build_summary(gold_tables(), CONTEXT)
    assert s["freshness"]["gold_run_id"] == "g1" and s["schema_version"] == serving.SCHEMA_VERSION
    assert (s["q1"]["rated_employees"], s["q1"]["pre_hire_reviews"], s["q1"]["employees_with_trend"]) == (2, 1, 2)
    assert s["q1"]["rating_distribution"] == [{"rating": 2, "employees": 1, "share": 0.5},
                                              {"rating": 4, "employees": 1, "share": 0.5}]
    assert [r["employees"] for r in s["q1"]["trend_distribution"]] == [1, 0, 1, 1]  # fixed order, zeros kept
    assert (s["q2"]["critical_gaps"], s["q2"]["employees_with_critical_gap"]) == (1, 1)
    assert s["q2"]["competencies"][0]["competency_name"] == "Negotiation"           # highest rate first
    q3 = s["q3"]
    assert [x["employees"] for x in q3["stages"]] == [3, 3, 3, 1]
    assert (q3["promotion_ready"], q3["ready_now_without_valid_rating"], q3["ready_now_failing_rating"]) == (1, 1, 1)
    assert s["q4"]["totals"]["employees_inactive_12m"] == 1
    assert s["q4"]["completion_distribution"][-1] == {"courses": "4+", "employees": 1}  # 5 courses capped at 4+
    assert s["q5"]["bands"] == [{"band": "High", "employees": 1}, {"band": "Medium", "employees": 0},
                                {"band": "Low", "employees": 2}]
    assert (s["q5"]["declining"], s["q5"]["declining_hr_high"]) == (1, 1)
    q6 = s["q6"]
    assert (q6["teams_with_projects"], q6["projects"]) == (2, 4)                       # T20 has no rating, T40 no projects
    assert [x["team_name"] for x in q6["teams"]] == ["T10", "T30"]
    assert q6["pearson_r"] is None and q6["significant_5pct"] is False                # 2 teams: too few for r
    assert {d["delivery"]: d["teams"] for d in q6["delivery"]} == {"all on time": 1, "mixed": 1}
    assert s["q7"]["heatmap"] == [{"department": "Sales", "competency_category": "Domain", "avg_proficiency": 1.0,
                                   "assessments": 1},
                                  {"department": "Sales", "competency_category": "Technical", "avg_proficiency": 4.0,
                                   "assessments": 1}]
    json.dumps(s, allow_nan=False)                                                     # strict JSON: no NaN anywhere


def test_q6_without_enough_teams_reports_no_correlation():
    t = gold_tables()
    t["team_performance"] = t["team_performance"].assign(has_projects=[True, False, False, False])
    q6 = build_summary(t, CONTEXT)["q6"]
    assert (q6["pearson_r"], q6["t_statistic"], q6["significant_5pct"]) == (None, None, False)


def test_employee_profiles():
    employees = build_employees(gold_tables())
    assert [e["employee_id"] for e in employees] == [1, 2, 3]
    ada, ben, cy = employees
    assert ada["name"] == "Ada One" and ada["hire_date"] == "2020-01-01" and ada["tenure_years"] == 6.8
    assert "email" not in json.dumps(ada)                                              # not shown, as in the notebook
    assert [r["review_period"] for r in ada["review_history"]] == ["2024-H2", "2025-H1"]
    assert ada["promotion"]["is_promotion_ready"] is True and ada["learning"]["avg_quiz_score"] == 81.2
    assert (ben["critical_gaps"], ben["attrition"]["risk_band"], ben["skill_gaps"][0]["assessed_date"]) == (
        1, "High", "2025-03-01")
    assert cy["performance"]["latest_valid_rating"] is None and cy["skill_gaps"] == []
    assert cy["review_history"][0]["is_pre_hire"] is True and cy["learning"]["last_activity_date"] == "2026-01-01"
    assert employees[1]["learning"]["last_activity_date"] is None
    json.dumps(employees, allow_nan=False)


def test_reconcile_passes_and_detects_differences():
    t = gold_tables()
    summary, employees = build_summary(t, CONTEXT), build_employees(t)
    checks = reconcile(summary, employees, t)
    assert checks and all(c["passed"] for c in checks)

    summary["q3"]["promotion_ready"] = 2
    failed = [c["name"] for c in reconcile(summary, employees[:2], t) if not c["passed"]]
    assert failed == ["one profile per employee", "review histories hold every review",  # employee 3's review lost
                      "promotion-ready total in summary"]


def test_records_keep_calendar_dates_and_nulls():
    df = pd.DataFrame({"d": [date(2026, 1, 2), None], "x": [1.5, NAN], "n": pd.array([1, None], dtype="Int32")})
    assert _records(df) == [{"d": "2026-01-02", "x": 1.5, "n": 1}, {"d": None, "x": None, "n": None}]
    assert _records(pd.DataFrame({"d": [None, None]})) == [{"d": None}, {"d": None}]


def test_serving_key():
    assert serving_key("r1", "summary") == "serving/runs/run_id=r1/summary.json"


@pytest.mark.parametrize("name", ["summary", "employees"])
def test_serving_key_is_inside_the_run_folder(name):
    from talent_etl.serving import SERVING_LAYER
    assert SERVING_LAYER.run_id_of(serving_key("r9", name)) == "r9"


def test_lake_catalog_pages_every_table(lake, monkeypatch):  # noqa: F811
    monkeypatch.setattr(serving, "LAKE_CHUNK_ROWS", 2)
    index, checks = serving.publish_lake_catalog(lake, "lake", "g9")
    tables = {(layer["layer"], t["table"]): t for layer in index["layers"] for t in layer["tables"]}
    assert [layer["layer"] for layer in index["layers"]] == ["bronze", "silver", "gold"]
    assert {k: (t["rows"], t["chunks"]) for k, t in tables.items()} == {
        ("bronze", "teams"): (2, 1), ("bronze", "learning_activity"): (3, 2),
        ("silver", "learning_activity"): (2, 1), ("silver", "teams"): (2, 1),
        ("silver", "learning_activity.quarantine"): (1, 1), ("gold", "dim_employee"): (1, 1)}
    assert tables[("gold", "dim_employee")]["columns"] == [{"name": "employee_id", "type": "int64"}]
    page = json.loads(lake.get_bytes("lake", serving.lake_chunk_key("g9", "bronze", "learning_activity", 1)))
    assert page["rows"][0]["_raw"] == '["not", "an", "object"]'                  # third line, second page
    assert [c["passed"] for c in checks] == [True, True, True]                   # silver x2 + gold rows vs pointers
    assert index["chunk_rows"] == 2 and index["silver_run_id"] == "s1"


def test_lake_catalog_flags_row_count_mismatch(lake):  # noqa: F811
    lake._client.objects[("lake", "gold/d.parquet")] = lake._client.objects[("lake", "silver/t.parquet")]  # 2 rows
    _, checks = serving.publish_lake_catalog(lake, "lake", "g9")
    failed = [c for c in checks if not c["passed"]]
    assert [(c["name"], c["actual"], c["expected"]) for c in failed] == [
        ("gold.dim_employee rows match the published pointer", 2, 1)]


def test_plain_turns_arrays_into_lists():
    import numpy as np
    df = pd.DataFrame({"w": [np.array(["a", "b"]), np.array([], dtype=object)], "x": [1, 2]})
    assert _records(serving._plain(df)) == [{"w": ["a", "b"], "x": 1}, {"w": [], "x": 2}]


def test_summary_publishes_the_rules():
    r = build_summary(gold_tables(), CONTEXT)["rules"]
    assert (r["high_performer_min_rating"], r["low_performer_max_rating"], r["learning_window_days"]) == (4, 2, 365)
    assert r["attrition"] == {"hr_points": {"High": 2, "Medium": 1, "Low": 0}, "signal_points": 1,
                              "high_band_min_points": 3, "medium_band_min_points": 2}
    assert r["critical_gap_ratio"] == 0.5 and r["promotion"]["min_rating"] == 4


def test_gold_lineage_matches_what_each_builder_reads():
    from talent_etl.gold_tables import TABLES, GoldInputs
    from tests.unit.test_gold_tables import AS_OF, silver_frames

    frames = silver_frames()
    lineage = serving._lineage()
    assert set(lineage) == set(TABLES)

    class Recording(GoldInputs):
        def __init__(self):
            super().__init__(lambda entity: frames[entity].copy(), AS_OF)
            self.read_silver, self.read_gold = set(), set()

        def silver(self, entity):
            self.read_silver.add(entity)
            return super().silver(entity)

        def table(self, name):
            self.read_gold.add(name)
            return TABLES[name].build(Recording()) if name not in self._gold else super().table(name)

    for name, spec in TABLES.items():
        recording = Recording()
        spec.build(recording)                                      # only this table's own reads are recorded
        assert (sorted(recording.read_silver), sorted(recording.read_gold)) == \
            (sorted(lineage[name]["silver"]), sorted(lineage[name]["gold"])), name


def test_question_lineage_names_real_gold_columns():
    from talent_etl.gold_tables import TABLES
    assert [q["question"] for q in serving.QUESTION_LINEAGE] == [1, 2, 3, 4, 5, 6, 7]
    for q in serving.QUESTION_LINEAGE:
        for table, columns in q["tables"].items():
            assert set(columns) <= set(TABLES[table].schema.names), (q["question"], table)


def test_pipeline_description_counts_rules_and_firings():
    p = serving.pipeline({"_reason_counts": [{"malformed_json": 13}, {"malformed_json": 1, "unknown_reason": 5}]})
    assert len(p["rules"]) == 39 and p["actions"] == ["accept", "warn", "deduplicate", "quarantine", "fail"]
    assert next(r["fired"] for r in p["rules"] if r["reason_code"] == "malformed_json") == 14
    table = next(t for t in p["gold_tables"] if t["table"] == "promotion_pipeline")
    assert list(table["column_lineage"]) == [c["name"] for c in table["columns"]]      # published in table order
    assert table["primary_key"] == ["employee_id"] and "is_promotion_ready" in [c["name"] for c in table["columns"]]
    summary = build_summary(gold_tables(), {**CONTEXT, "_reason_counts": []})
    assert "_reason_counts" not in summary and summary["pipeline"]["questions"][0]["question"] == 1


def test_column_lineage_covers_every_gold_column_with_real_sources():
    from talent_etl.contracts import CONTRACTS
    from talent_etl.gold_tables import TABLES
    columns, reads = serving.column_lineage(), serving._lineage()
    silver_columns = {name: set(contract.silver_schema().names) for name, contract in CONTRACTS.items()}
    kinds = {"copied", "joined", "computed", "aggregated", "pipeline"}
    for table, spec in TABLES.items():
        assert sorted(columns[table]) == sorted(spec.schema.names), table          # every column, nothing extra
        for column, origin in columns[table].items():
            assert origin["kind"] in kinds and origin["how"], (table, column)
            assert origin["from"] or origin["kind"] == "pipeline", (table, column)
            for source in origin["from"]:
                layer, source_table, source_column = source.split(".")
                known = silver_columns if layer == "silver" else {n: set(t.schema.names) for n, t in TABLES.items()}
                assert source_column in known[source_table], (table, column, source)
                assert source_table in reads[table][layer], (table, column, source)    # only tables the builder reads


def test_every_rule_has_a_plain_language_description():
    from talent_etl.dq_rules import RULES
    plain = serving.plain_rules()
    assert set(plain) == {r.rule_id for r in RULES}                       # one per rule, nothing extra
    assert all(10 < len(text) < 200 and not text.endswith(" ") for text in plain.values())
    assert "half" in plain["BUS-009"] and "5%" in plain["VOL-002"]        # wording follows definitions.py
    assert serving.pipeline({})["rules"][0]["plain"] == plain[RULES[0].rule_id]


@pytest.mark.parametrize("name, kind, plain", [
    ("schema matches contract", "structure", "Has exactly the right columns, each with the right kind of value"),
    ("primary key ('employee_id',) not null", "structure", "Every row has its employee id"),
    ("primary key ('competency_id', 'department', 'proficiency_level') unique (grain)", "structure",
     "No duplicates: one row per skill × department × proficiency level"),
    ("rating in [1, 5]", "values", "Rating is between 1 and 5"),
    ("risk_band in ['High', 'Medium', 'Low']", "values", "Risk band is only ever one of: High, Medium, Low"),
    ("rating_trend in ['improving', 'insufficient_history']", "values", "Rating trend is only ever one of: improving, insufficient history"),
    ("team_name not null", "structure", "Team name is always filled in"),
    ("rows = Silver teams", "totals", "One row for every team"),
    ("something new", "totals", "something new"),
])
def test_explain_gold_check(name, kind, plain):
    assert serving.explain_gold_check(name) == (kind, plain)
