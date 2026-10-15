"""Fixtures for tests against the local PostgreSQL and LocalStack services."""

import uuid

import psycopg
import pytest
from botocore.exceptions import BotoCoreError, ClientError
from psycopg import sql

from talent_etl.config import Settings
from talent_etl.io_pg import PostgresClient
from talent_etl.io_s3 import S3Client
from talent_etl.retry import RetryPolicy

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def settings():
    settings = Settings.from_env()
    if not settings.is_local:
        pytest.skip("integration tests only run against local services (IS_LOCAL=true)")
    return settings


@pytest.fixture(scope="session")
def policy():
    return RetryPolicy(attempts=2, base_delay=0.1, jitter=False)


@pytest.fixture
def pg(settings, policy):
    client = PostgresClient(settings.postgres, RetryPolicy(attempts=1))
    try:
        client.count_rows("pg_catalog", "pg_class")
    except psycopg.OperationalError as exc:
        pytest.skip(f"local PostgreSQL unreachable: {exc}")
    client.close()
    with PostgresClient(settings.postgres, policy) as client:
        yield client


@pytest.fixture
def pg_schema(pg):
    """A throwaway schema, dropped after the test."""
    schema = f"it_{uuid.uuid4().hex[:8]}"
    pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    yield schema
    pg.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def s3(settings):
    client = S3Client(settings.s3, RetryPolicy(attempts=1))
    try:
        client.list_objects(settings.s3.source_bucket, "nothing/")
    except (BotoCoreError, ClientError) as exc:
        if not isinstance(exc, ClientError) or "NoSuchBucket" not in str(exc):
            pytest.skip(f"LocalStack S3 unreachable: {exc}")
    return S3Client(settings.s3, RetryPolicy(attempts=2, base_delay=0.1, jitter=False))


@pytest.fixture
def s3_bucket(s3):
    """A throwaway bucket, emptied and deleted after the test."""
    bucket = f"it-{uuid.uuid4().hex[:8]}"
    s3.ensure_bucket(bucket)
    yield bucket
    s3.delete_prefix(bucket, "")
    s3._client.delete_bucket(Bucket=bucket)
