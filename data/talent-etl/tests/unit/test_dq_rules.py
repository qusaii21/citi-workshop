"""The rule registry is consistent, fully implemented and matches docs/data_quality.md."""

from pathlib import Path

import pytest

from talent_etl import dq_rules
from talent_etl.dq_rules import RULES, Action, Severity, render_matrix_markdown, rule_for

PACKAGE = Path(dq_rules.__file__).parent
DOC = PACKAGE.parent / "docs" / "data_quality.md"


def test_rule_ids_and_reason_codes_are_unique():
    assert len({r.rule_id for r in RULES}) == len(RULES)
    assert len({r.reason_code for r in RULES}) == len(RULES)


def test_severity_matches_action():
    expected = {Action.FAIL: {Severity.CRITICAL}, Action.QUARANTINE: {Severity.ERROR},
                Action.DEDUPLICATE: {Severity.WARNING}, Action.WARN: {Severity.WARNING},
                Action.ACCEPT: {Severity.INFO}}
    for rule in RULES:
        assert rule.severity in expected[rule.action], rule.rule_id


def test_every_reason_code_is_emitted_by_the_engine():
    source = (PACKAGE / "validation.py").read_text() + (PACKAGE / "parsing.py").read_text()
    missing = [r.reason_code for r in RULES if f'"{r.reason_code}"' not in source]
    assert missing == []


def test_unknown_reason_code_is_a_programming_error():
    with pytest.raises(KeyError):
        rule_for("not_a_rule")


def test_documentation_matrix_is_in_sync_with_registry():
    assert render_matrix_markdown() in DOC.read_text(), "regenerate the matrix in docs/data_quality.md"


def test_every_action_is_used():
    assert {r.action for r in RULES} == set(Action)
