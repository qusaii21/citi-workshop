"""Integration: the analysis notebook runs top to bottom, in a fresh kernel, against a freshly built lake."""

import re
from dataclasses import replace
from datetime import date
from pathlib import Path

import nbformat
import pytest
from nbclient import NotebookClient

from talent_etl.bronze import SOURCE_TABLES, BronzeIngestor
from talent_etl.gold import GoldBuilder
from talent_etl.silver import SilverBuilder

pytestmark = pytest.mark.integration

NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks" / "talent_insights.ipynb"


def test_notebook_executes_cleanly_and_its_narrative_matches_the_data(settings, pg, s3, s3_bucket, monkeypatch):
    if set(SOURCE_TABLES) - set(pg.list_tables(settings.postgres.source_schema)):
        pytest.skip("sources not seeded; run scripts/seed_sources.sh")
    lake = replace(settings, s3=replace(settings.s3, lake_bucket=s3_bucket))
    BronzeIngestor(lake, pg, s3, run_id="nb-bronze", ingest_date=date(2026, 10, 14)).run()
    SilverBuilder(lake, s3, run_id="nb-silver").run()
    GoldBuilder(lake, s3, run_id="nb-gold").run()

    monkeypatch.setenv("LAKE_BUCKET", s3_bucket)        # the kernel inherits the environment, like any reader
    notebook = nbformat.read(NOTEBOOK, as_version=4)
    NotebookClient(notebook, timeout=600, kernel_name="python3",
                   resources={"metadata": {"path": str(NOTEBOOK.parent)}}).execute()

    errors = [o for c in notebook.cells if c.cell_type == "code" for o in c.outputs if o.output_type == "error"]
    assert errors == []
    outputs = "".join(o.get("text", "") for c in notebook.cells if c.cell_type == "code" for o in c.outputs)
    assert "Lake bucket" in "".join(str(o.get("data", {})) for c in notebook.cells for o in c.get("outputs", []))
    assert re.search(r"All \d+ Gold checks passed across 10 tables\.", outputs)
    assert "figures quoted in the narrative match the published Gold data" in outputs
    questions = [c.source for c in notebook.cells if c.cell_type == "markdown" and "(verbatim):**" in c.source]
    assert len(questions) == 7
