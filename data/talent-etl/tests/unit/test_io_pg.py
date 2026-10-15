"""Unit tests for talent_etl.io_pg that do not need a database."""

from decimal import Decimal
from types import SimpleNamespace

import psycopg
import pyarrow as pa
import pytest

from talent_etl import io_pg
from talent_etl.config import Settings
from talent_etl.io_pg import PostgresClient, is_transient_pg_error
from talent_etl.retry import RetryPolicy


@pytest.mark.parametrize("exc, expected", [
    (psycopg.OperationalError("connection timeout"), True),
    (psycopg.errors.SerializationFailure(), True),
    (psycopg.errors.DeadlockDetected(), True),
    (psycopg.errors.InvalidPassword(), False),
    (psycopg.errors.UndefinedTable(), False),
    (ValueError("x"), False),
])
def test_is_transient_pg_error(exc, expected):
    assert is_transient_pg_error(exc) is expected


class FakeCursor:
    description = [type("Col", (), {"name": "n"})()]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, _query, _params):
        pass

    def fetchall(self):
        return [(7,)]


class FakeConnection:
    def __init__(self, fail):
        self.closed = False
        self.fail = fail

    def cursor(self):
        if self.fail:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return FakeCursor()

    def close(self):
        self.closed = True


def test_reconnects_after_dropped_connection(monkeypatch):
    connections = []

    def fake_connect(**kwargs):
        assert kwargs["autocommit"] is True
        conn = FakeConnection(fail=not connections)  # first connection is broken
        connections.append(conn)
        return conn

    monkeypatch.setattr(io_pg.psycopg, "connect", fake_connect)
    policy = RetryPolicy(attempts=3, base_delay=0, jitter=False, sleep=lambda _s: None)
    with PostgresClient(Settings.from_env({}).postgres, policy) as pg:
        assert pg.count_rows("hr_source", "employees") == 7
    assert len(connections) == 2
    assert connections[0].closed and connections[1].closed


def col(name, type_code, precision=None, scale=None):
    return SimpleNamespace(name=name, type_code=type_code, precision=precision, scale=scale)


@pytest.mark.parametrize("column, expected", [
    (col("i", 23), pa.int32()),
    (col("b", 20), pa.int64()),
    (col("t", 25), pa.string()),
    (col("d", 1082), pa.date32()),
    (col("ts", 1184), pa.timestamp("us", tz="UTC")),
    (col("n", 1700, 5, 2), pa.decimal128(5, 2)),
    (col("n", 1700), None),        # unconstrained NUMERIC: left to inference
    (col("j", 3802), None),        # jsonb: unmapped, inferred
])
def test_arrow_type_for(column, expected):
    assert io_pg.arrow_type_for(column) == expected


class DescribedCursor(FakeCursor):
    def __init__(self, description, rows):
        self.description = description
        self.rows = rows

    def fetchall(self):
        return self.rows


def test_read_table_arrow_is_lossless(monkeypatch):
    description = [col("id", 23), col("score", 1700, 5, 2), col("note", 25), col("extra", 3802)]
    rows = [(1, Decimal("56.87"), None, None), (None, None, "x", None)]
    conn = FakeConnection(fail=False)
    conn.cursor = lambda: DescribedCursor(description, rows)
    monkeypatch.setattr(io_pg.psycopg, "connect", lambda **_kw: conn)
    with PostgresClient(Settings.from_env({}).postgres, RetryPolicy(attempts=1)) as pg:
        table = pg.read_table_arrow("hr_source", "projects")
    assert table.schema.types == [pa.int32(), pa.decimal128(5, 2), pa.string(), pa.string()]
    assert table.column("id").to_pylist() == [1, None]  # nullable int stays int, not float
    assert table.column("score").to_pylist() == [Decimal("56.87"), None]


def test_read_table_arrow_empty_table_keeps_schema(monkeypatch):
    conn = FakeConnection(fail=False)
    conn.cursor = lambda: DescribedCursor([col("id", 23), col("d", 1082)], [])
    monkeypatch.setattr(io_pg.psycopg, "connect", lambda **_kw: conn)
    with PostgresClient(Settings.from_env({}).postgres, RetryPolicy(attempts=1)) as pg:
        table = pg.read_table_arrow("s", "t")
    assert table.num_rows == 0
    assert table.schema.names == ["id", "d"]
    assert table.schema.types == [pa.int32(), pa.date32()]
