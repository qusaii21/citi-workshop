"""Contract definitions checked against the provided schema.sql and JSONL sample (no guessing)."""

import json
import re
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

from talent_etl.contracts import CONTRACTS, QUARANTINE_SCHEMA, Kind
from talent_etl.definitions import LEARNING_EVENT_TYPES

ZIP = Path(__file__).resolve().parents[4] / "sampleData" / "data-engineering-sample-performance-scenario.zip"
DATASET = "data-engineering-performance-talent-management"
POSTGRES_ENTITIES = [name for name, c in CONTRACTS.items() if c.source_system == "postgres"]
SQL_TO_ARROW = {"INTEGER": pa.int32(), "TEXT": pa.string(), "DATE": pa.date32(), "NUMERIC(5,2)": pa.decimal128(5, 2)}


@pytest.fixture(scope="module")
def sample_zip():
    if not ZIP.exists():
        pytest.skip(f"sample data zip not found at {ZIP}")
    with zipfile.ZipFile(ZIP) as archive:
        yield archive


@pytest.fixture(scope="module")
def schema_sql(sample_zip):
    """Parse the provided schema.sql into {table: {column: definition}}."""
    sql = sample_zip.read(f"{DATASET}/schema.sql").decode()
    tables = {}
    for table, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", sql, flags=re.S):
        columns = {}
        for line in body.strip().splitlines():
            line = line.strip().rstrip(",")
            name, rest = line.split(None, 1)
            sql_type = re.match(r"(NUMERIC\(\d+,\d+\)|\w+)", rest).group(1)
            check_in = re.search(r"CHECK \(\w+ IN \((.*?)\)\)", rest)
            between = re.search(r"BETWEEN (\d+) AND (\d+)", rest)
            reference = re.search(r"REFERENCES (\w+)\((\w+)\)", rest)
            columns[name] = {
                "type": sql_type,
                "required": "NOT NULL" in rest or "PRIMARY KEY" in rest,
                "pk": "PRIMARY KEY" in rest,
                "allowed": tuple(re.findall(r"'([^']*)'", check_in.group(1))) if check_in else (),
                "between": (int(between.group(1)), int(between.group(2))) if between else None,
                "references": (reference.group(1), reference.group(2)) if reference else None,
            }
        tables[table] = columns
    return tables


def test_every_schema_sql_table_has_a_contract(schema_sql):
    assert sorted(schema_sql) == sorted(POSTGRES_ENTITIES)


@pytest.mark.parametrize("entity", POSTGRES_ENTITIES)
def test_postgres_contract_matches_schema_sql_exactly(schema_sql, entity):
    contract, source = CONTRACTS[entity], schema_sql[entity]
    assert list(contract.column_names) == list(source), "column names and order"
    for name, spec in source.items():
        column = contract.column(name)
        assert column.silver_type == SQL_TO_ARROW[spec["type"]], name
        assert column.required == spec["required"], name
        assert set(column.allowed_values) == set(spec["allowed"]), name
        if spec["between"]:
            assert (column.min_value, column.max_value) == spec["between"], name
    assert contract.primary_key == tuple(n for n, s in source.items() if s["pk"])
    assert {(fk.column, fk.references, fk.ref_column) for fk in contract.foreign_keys} == {
        (n, *s["references"]) for n, s in source.items() if s["references"]
    }


def test_learning_contract_covers_every_key_in_the_sample(sample_zip):
    contract = CONTRACTS["learning_activity"]
    known = set(contract.column_names) | {a for c in contract.columns for a in c.aliases}
    keys, event_types = set(), set()
    for name in sample_zip.namelist():
        if name.endswith(".jsonl"):
            for line in sample_zip.read(name).splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                keys |= set(record)
                event_types.add(record["event_type"])
    assert keys == known  # every observed key is a contract column or a known alias, and vice versa
    assert event_types == set(LEARNING_EVENT_TYPES)


@pytest.mark.parametrize("entity", list(CONTRACTS))
def test_silver_schema_layout(entity):
    contract = CONTRACTS[entity]
    schema = contract.silver_schema()
    names = schema.names
    source_count = len(contract.columns)
    assert names[:source_count] == list(contract.column_names)
    assert names[source_count:source_count + len(contract.derived_columns)] == [d.name for d in contract.derived_columns]
    assert all(n.startswith("_") for n in names[source_count + len(contract.derived_columns):])
    assert "_dq_warnings" in names and "_run_id" in names
    for column in contract.columns:
        assert schema.field(column.name).nullable is (not column.required)
    for name in contract.primary_key:
        assert contract.column(name).required


def test_learning_silver_types_and_partitioning():
    contract = CONTRACTS["learning_activity"]
    schema = contract.silver_schema()
    assert schema.field("employee_id").type == pa.int32()  # string in JSON, int to join PostgreSQL
    assert schema.field("event_date").type == pa.timestamp("us")
    assert schema.field("score").type == pa.float64()
    assert schema.field("_source_line").type == pa.int32()
    assert contract.partition_by == ("event_year", "event_month")
    assert schema.field("event_year").type == pa.int32()  # what hive readers infer from the path
    file_schema = contract.silver_file_schema()
    assert "event_year" not in file_schema.names and "event_month" not in file_schema.names
    assert file_schema.names == [n for n in schema.names if n not in contract.partition_by]
    assert CONTRACTS["teams"].silver_file_schema().equals(CONTRACTS["teams"].silver_schema())
    assert contract.column("text").aliases == ("notes",)


def test_contract_internal_consistency():
    for contract in CONTRACTS.values():
        names = set(contract.column_names)
        assert set(contract.primary_key) <= names
        assert set(contract.natural_key) <= names
        assert set(contract.dedup_order) <= names
        assert bool(contract.natural_key) == bool(contract.dedup_order)
        for fk in contract.foreign_keys:
            parent = CONTRACTS[fk.references]
            assert parent.primary_key == (fk.ref_column,)
            assert list(CONTRACTS).index(fk.references) < list(CONTRACTS).index(contract.name), "parents first"
        for column in contract.columns:
            if column.kind is Kind.DECIMAL:
                assert pa.types.is_decimal(column.silver_type)
            if column.pattern or column.allowed_values:
                assert column.kind is Kind.STRING


def test_quarantine_schema_preserves_identifiers_and_raw_content():
    assert QUARANTINE_SCHEMA.names == [
        "entity", "source_object", "source_line", "record_key", "raw_record", "rule_ids", "reason_codes",
        "reason_detail", "_run_id", "_bronze_ingest_date", "_quarantined_at",
    ]
    assert QUARANTINE_SCHEMA.field("raw_record").nullable is False
    assert QUARANTINE_SCHEMA.field("reason_codes").type == pa.list_(pa.string())


def test_unknown_column_lookup_raises():
    with pytest.raises(KeyError, match="no column 'nope'"):
        CONTRACTS["teams"].column("nope")
