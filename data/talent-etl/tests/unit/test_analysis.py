"""Unit tests for talent_etl.analysis (Q6 delivery status and Q7 skills analyses)."""

import numpy as np
import pandas as pd
import pytest

from talent_etl import analysis


def test_permutation_test_matches_the_exact_answer():
    # values 1..4, groups of 2: of the 6 possible splits, 2 give |difference| >= 2, so the exact p is 1/3
    result = analysis.permutation_test([1, 2, 3, 4], [True, True, False, False], permutations=60_000)
    assert result["difference"] == -2.0 and (result["n_in"], result["n_out"]) == (2, 2)
    assert result["p_value"] == pytest.approx(1 / 3, abs=0.01)


def test_permutation_test_is_reproducible_and_handles_one_group():
    a = analysis.permutation_test(np.arange(20), np.arange(20) < 8, permutations=5_000)
    assert a == analysis.permutation_test(np.arange(20), np.arange(20) < 8, permutations=5_000)   # fixed seed
    assert a["p_value"] < 0.01                                                                    # clearly separated groups
    one_group = analysis.permutation_test([1.0, 2.0], [True, True])
    assert one_group["p_value"] is None and one_group["permutations"] == 0 and one_group["mean_out"] is None


def test_delivery_status_comparison():
    teams = pd.DataFrame({
        "team_id": [1, 2, 3, 4, 5], "has_projects": [True, True, True, True, False],
        "avg_latest_valid_rating": [4.0, 3.8, 3.0, None, 3.5], "on_time_project_share": [1.0, 1.0, 0.5, 0.0, None],
        "project_count": [1, 2, 2, 1, 0]})
    result = analysis.delivery_status_comparison(teams, permutations=2_000)
    assert (result["teams"], result["n_in"], result["n_out"]) == (3, 2, 1)     # team 4 has no rating, team 5 no projects
    assert result["mean_in"] == pytest.approx(3.9) and result["mean_out"] == pytest.approx(3.0)
    assert (result["projects_on_time"], result["projects_not_on_time"]) == (4, 2)


def skill_gaps(levels_by_department):
    rows = [{"employee_id": i, "competency_name": f"C{i % 2}", "competency_category": "Technical" if i % 2 else "Domain",
             "department": dept, "proficiency_level": level, "required_level": 3}
            for i, (dept, level) in enumerate((d, lvl) for d, levels in levels_by_department.items() for lvl in levels)]
    return pd.DataFrame(rows)


def test_competency_readiness_shares():
    gaps = skill_gaps({"A": [1, 3, 5, 4], "B": [2, 3, 3, 5]})
    table = analysis.competency_readiness(gaps).set_index("competency_name")
    levels = [f"level_{n}" for n in analysis.LEVELS]
    assert np.allclose(table[levels].sum(axis=1), 1.0)
    c0 = table.loc["C0"]                                   # levels 1, 5, 2, 3 (even rows)
    assert (c0["assessed"], c0["meets_required"], c0["expert_share"]) == (4, 0.5, 0.25)
    assert list(analysis.competency_readiness(gaps)["meets_required"]) == sorted(table["meets_required"], reverse=True)


def test_department_variation_detects_real_differences_only():
    rng = np.random.default_rng(0)
    same = skill_gaps({d: list(rng.integers(1, 6, 60)) for d in "ABCD"})
    different = skill_gaps({"A": [1] * 60, "B": [2] * 60, "C": [4] * 60, "D": [5] * 60})
    assert analysis.department_variation(same, permutations=500)["p_value"] > 0.05
    strong = analysis.department_variation(different, permutations=500)
    assert strong["p_value"] < 0.01 and (strong["departments"], strong["categories"]) == (4, 2)
