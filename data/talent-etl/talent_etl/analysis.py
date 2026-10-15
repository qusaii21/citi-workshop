"""
Statistical analyses shared by the notebook and the dashboard data (Q6 delivery status, Q7 skills).

Both read Gold tables only. Random permutations use a fixed seed, so the same data always
gives the same p-values, in the notebook and on the dashboard alike.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

SEED = 20261015
PERMUTATIONS = 100_000
LEVELS = (1, 2, 3, 4, 5)
EXPERT_MIN_LEVEL = 4


def permutation_test(values: Any, in_group: Any, permutations: int = PERMUTATIONS, seed: int = SEED) -> dict[str, Any]:
    """
    Two-sided permutation test for a difference in means between two groups.

    The group labels are shuffled ``permutations`` times; the p-value is the share of shuffles
    whose difference is at least as large (in absolute value) as the observed one, counting the
    observed arrangement itself: (hits + 1) / (permutations + 1).
    """
    x = np.asarray(values, dtype=float)
    labels = np.asarray(in_group, dtype=bool)
    k, n = int(labels.sum()), len(x)
    if k == 0 or k == n:
        return {"difference": None, "p_value": None, "permutations": 0, "n_in": k, "n_out": n - k,
                "mean_in": float(x[labels].mean()) if k else None, "mean_out": float(x[~labels].mean()) if n - k else None}
    observed = x[labels].mean() - x[~labels].mean()
    rng, total, hits, done = np.random.default_rng(seed), x.sum(), 0, 0
    while done < permutations:
        size = min(2_000, permutations - done)
        picks = np.argsort(rng.random((size, n)), axis=1)[:, :k]
        in_sum = x[picks].sum(axis=1)
        differences = in_sum / k - (total - in_sum) / (n - k)
        hits += int((np.abs(differences) >= abs(observed) - 1e-12).sum())
        done += size
    return {"difference": float(observed), "p_value": (hits + 1) / (permutations + 1), "permutations": permutations,
            "n_in": k, "n_out": n - k, "mean_in": float(x[labels].mean()), "mean_out": float(x[~labels].mean())}


def delivery_status_comparison(team_performance: pd.DataFrame, permutations: int = PERMUTATIONS) -> dict[str, Any]:
    """Q6: average team rating of teams whose projects were all on time vs teams with a project not on time."""
    with_projects = team_performance[team_performance["has_projects"]]
    teams = with_projects.dropna(subset=["avg_latest_valid_rating", "on_time_project_share"])
    all_on_time = teams["on_time_project_share"] == 1
    on_time_projects = int(round((with_projects["project_count"] * with_projects["on_time_project_share"]).sum()))
    return {**permutation_test(teams["avg_latest_valid_rating"], all_on_time, permutations),
            "teams": len(teams), "projects_on_time": on_time_projects,
            "projects_not_on_time": int(with_projects["project_count"].sum()) - on_time_projects}


def competency_readiness(skill_gaps: pd.DataFrame) -> pd.DataFrame:
    """Q7: per competency, the mix of proficiency levels, the share meeting the required level and of experts."""
    gaps = skill_gaps.assign(meets=skill_gaps["proficiency_level"] >= skill_gaps["required_level"],
                             expert=skill_gaps["proficiency_level"] >= EXPERT_MIN_LEVEL)
    keys = ["competency_name", "competency_category"]
    out = gaps.groupby(keys).agg(assessed=("employee_id", "size"), avg_proficiency=("proficiency_level", "mean"),
                                 meets_required=("meets", "mean"), expert_share=("expert", "mean"))
    mix = pd.crosstab([gaps["competency_name"], gaps["competency_category"]], gaps["proficiency_level"], normalize="index")
    mix = mix.reindex(columns=list(LEVELS), fill_value=0.0)
    mix.columns = [f"level_{level}" for level in LEVELS]
    out = out.join(mix).reset_index()
    return out.sort_values(["meets_required", "competency_name"], ascending=[False, True], ignore_index=True)


def department_variation(skill_gaps: pd.DataFrame, permutations: int = 2_000, seed: int = SEED) -> dict[str, Any]:
    """
    Q7: are department differences in proficiency bigger than chance?

    Measures the spread (standard deviation) of the department × competency-category averages,
    then shuffles the department labels across assessments to see how large that spread is when
    departments are irrelevant. p = (hits + 1) / (permutations + 1).
    """
    departments, dept_codes = np.unique(skill_gaps["department"].to_numpy(), return_inverse=True)
    categories, cat_codes = np.unique(skill_gaps["competency_category"].to_numpy(), return_inverse=True)
    levels = skill_gaps["proficiency_level"].to_numpy(dtype=float)
    cells = len(departments) * len(categories)

    def spread(codes: np.ndarray) -> float:
        cell = codes * len(categories) + cat_codes
        counts = np.bincount(cell, minlength=cells)
        means = np.bincount(cell, weights=levels, minlength=cells)[counts > 0] / counts[counts > 0]
        return float(means.std(ddof=1))

    observed = spread(dept_codes)
    rng = np.random.default_rng(seed)
    hits = sum(spread(rng.permutation(dept_codes)) >= observed - 1e-12 for _ in range(permutations))
    return {"spread": observed, "p_value": (hits + 1) / (permutations + 1), "permutations": permutations,
            "departments": len(departments), "categories": len(categories)}
