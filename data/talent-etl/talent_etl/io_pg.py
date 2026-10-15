"""
PostgreSQL access with retries on transient connection failures.

Identifiers are always quoted through ``psycopg.sql`` so table and schema names
coming from configuration can never inject SQL.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd
import psycopg
import pyarrow as pa
from psycopg import Column, sql

from talent_etl.config import PostgresConfig
from talent_etl.retry import RetryPolicy

logger = logging.getLogger(__name__)

# PostgreSQL type OIDs (pg_type.oid) mapped to lossless Arrow types.
_ARROW_TYPES: dict[int, pa.DataType] = {
    16: pa.bool_(),                      # boolean
    20: pa.int64(),                      # bigint
    21: pa.int16(),                      # smallint
    23: pa.int32(),                      # integer
    25: pa.string(),                     # text
    700: pa.float32(),                   # real
    701: pa.float64(),                   # double precision
    1042: pa.string(),                   # char(n)
    1043: pa.string(),                   # varchar
    1082: pa.date32(),                   # date
    1114: pa.timestamp("us"),            # timestamp
    1184: pa.timestamp("us", tz="UTC"),  # timestamptz
}
_NUMERIC_OID = 1700


def arrow_type_for(column: Column) -> pa.DataType | None:
    """
    Return the Arrow type for a PostgreSQL result column, or None to let Arrow infer it.

    NUMERIC(p,s) maps to decimal128(p,s); unconstrained NUMERIC has no fixed scale,
    so it is left to inference rather than risk rounding.
    """
    if column.type_code == _NUMERIC_OID:
        if column.precision is not None and column.scale is not None:
            return pa.decimal128(column.precision, column.scale)
        return None
    return _ARROW_TYPES.get(column.type_code)


def is_transient_pg_error(exc: BaseException) -> bool:
    """Return True for errors that a reconnect and retry can fix (timeouts, dropped connections)."""
    return isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.errors.SerializationFailure,
            psycopg.errors.DeadlockDetected,
        ),
    ) and not isinstance(exc, psycopg.errors.InvalidPassword)


class PostgresClient:
    """
    Thin PostgreSQL client that reconnects and retries transient failures.

    Use as a context manager so the connection is always closed::

        with PostgresClient(settings.postgres, policy) as pg:
            df = pg.read_table("hr_source", "employees")
    """

    def __init__(self, config: PostgresConfig, retry_policy: RetryPolicy) -> None:
        self._config = config
        self._retry = retry_policy
        self._conn: psycopg.Connection | None = None

    def __enter__(self) -> PostgresClient:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection if open."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(**self._config.connect_kwargs(), autocommit=True)
        return self._conn

    def _run(
        self, description: str, query: sql.Composable, params: Sequence[Any] | None
    ) -> tuple[list[Column], list[tuple[Any, ...]]]:
        """Execute a query with retries and return (column descriptions, rows)."""

        def operation() -> tuple[list[Column], list[tuple[Any, ...]]]:
            try:
                with self._connection().cursor() as cur:
                    cur.execute(query, params)
                    if cur.description is None:
                        return [], []
                    return list(cur.description), cur.fetchall()
            except psycopg.OperationalError:
                self.close()  # force a fresh connection on the next attempt
                raise

        return self._retry.call(operation, description=description, is_retryable=is_transient_pg_error)

    def query_df(self, query: sql.Composable, params: Sequence[Any] | None = None) -> pd.DataFrame:
        """Run a SELECT and return the result as a DataFrame (empty frame keeps the columns)."""
        columns, rows = self._run("postgres query", query, params)
        return pd.DataFrame.from_records(rows, columns=[col.name for col in columns])

    def read_table(self, schema: str, table: str) -> pd.DataFrame:
        """Read a whole table, preserving PostgreSQL column order."""
        query = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        columns, rows = self._run(f"read {schema}.{table}", query, None)
        logger.debug("table read", extra={"context": {"table": f"{schema}.{table}", "rows": len(rows)}})
        return pd.DataFrame.from_records(rows, columns=[col.name for col in columns])

    def read_table_arrow(self, schema: str, table: str) -> pa.Table:
        """
        Read a whole table into Arrow with types derived from the PostgreSQL columns.

        Unlike ``read_table`` this is lossless: nullable integers stay integers,
        NUMERIC(p,s) becomes decimal128(p,s), and an empty table keeps its schema.
        """
        query = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        columns, rows = self._run(f"read {schema}.{table}", query, None)
        arrays, fields = [], []
        for index, column in enumerate(columns):
            values = [row[index] for row in rows]
            arrow_type = arrow_type_for(column)
            array = pa.array(values, type=arrow_type) if arrow_type else pa.array(values)
            if array.type == pa.null():  # unknown type and no values to infer from
                array = array.cast(pa.string())
            arrays.append(array)
            fields.append(pa.field(column.name, array.type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def count_rows(self, schema: str, table: str) -> int:
        """Return ``count(*)`` for a table."""
        query = sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        _columns, rows = self._run(f"count {schema}.{table}", query, None)
        return int(rows[0][0])

    def list_tables(self, schema: str) -> list[str]:
        """Return the base tables in a schema, sorted by name."""
        query = sql.SQL(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name"
        )
        _columns, rows = self._run(f"list tables in {schema}", query, [schema])
        return [row[0] for row in rows]

    def execute(self, query: sql.Composable, params: Sequence[Any] | None = None) -> None:
        """Execute a statement that returns no rows (DDL / DML)."""
        self._run("postgres execute", query, params)
