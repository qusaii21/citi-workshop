"""Unit tests for talent_etl.io_s3 using a fake boto3 client."""

import pandas as pd
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from talent_etl.config import Settings
from talent_etl.io_s3 import S3Client, is_transient_s3_error
from talent_etl.retry import RetryPolicy
from tests.fakes import FakeS3, client_error


@pytest.fixture
def fake():
    return FakeS3()


@pytest.fixture
def s3(fake):
    policy = RetryPolicy(attempts=3, base_delay=0, jitter=False, sleep=lambda _s: None)
    return S3Client(Settings.from_env({}).s3, policy, client=fake)


@pytest.mark.parametrize("exc, expected", [
    (EndpointConnectionError(endpoint_url="http://x"), True),
    (client_error("SlowDown"), True),
    (client_error("503"), True),
    (client_error("AccessDenied"), False),
    (client_error("NoSuchKey"), False),
    (ValueError("x"), False),
])
def test_is_transient_s3_error(exc, expected):
    assert is_transient_s3_error(exc) is expected


def test_list_objects_follows_pagination(s3, fake):
    for i in range(5):
        fake.objects[("b", f"p/{i}.jsonl")] = b"x" * i
    fake.objects[("b", "other/x")] = b""
    listed = s3.list_objects("b", "p/")
    assert [o.key for o in listed] == [f"p/{i}.jsonl" for i in range(5)]
    assert [o.size for o in listed] == [0, 1, 2, 3, 4]


def test_parquet_round_trip(s3):
    df = pd.DataFrame({"employee_id": [1, 2], "rating": [4, None]})
    size = s3.write_parquet(df, "b", "silver/x.parquet")
    assert size > 0
    pd.testing.assert_frame_equal(s3.read_parquet("b", "silver/x.parquet"), df)


def test_transient_errors_are_retried(s3, fake):
    fake.failures = [client_error("SlowDown"), EndpointConnectionError(endpoint_url="http://x")]
    s3.put_bytes("b", "k", b"data")
    assert s3.get_bytes("b", "k") == b"data"


def test_access_denied_is_not_retried(s3, fake):
    fake.failures = [client_error("AccessDenied"), client_error("SlowDown")]
    with pytest.raises(ClientError, match="AccessDenied"):
        s3.put_bytes("b", "k", b"data")
    assert len(fake.failures) == 1  # the queued SlowDown was never reached: no retry happened


def test_object_exists(s3, fake):
    fake.objects[("b", "k")] = b""
    assert s3.object_exists("b", "k") is True
    assert s3.object_exists("b", "missing") is False
    fake.failures = [client_error("AccessDenied")]
    with pytest.raises(ClientError):
        s3.object_exists("b", "k")


def test_ensure_bucket_creates_only_when_missing(s3, fake):
    s3.ensure_bucket("lake")
    s3.ensure_bucket("lake")
    assert fake.created == [{"Bucket": "lake"}]  # local region us-east-1 needs no LocationConstraint


def test_ensure_bucket_sets_location_constraint_outside_us_east_1(fake):
    cloud = Settings.from_env({
        "IS_LOCAL": "false", "POSTGRES_HOST": "h", "POSTGRES_NAME": "n", "POSTGRES_USER": "u",
        "POSTGRES_PASS": "p", "AWS_REGION": "ap-south-1", "SOURCE_BUCKET": "s", "LAKE_BUCKET": "l",
    })
    S3Client(cloud.s3, RetryPolicy(attempts=1), client=fake).ensure_bucket("lake")
    assert fake.created == [{"Bucket": "lake", "CreateBucketConfiguration": {"LocationConstraint": "ap-south-1"}}]


def test_ensure_bucket_propagates_access_denied(s3, fake):
    fake.failures = [client_error("AccessDenied")]
    with pytest.raises(ClientError):
        s3.ensure_bucket("lake")
    assert fake.created == []


def test_delete_prefix(s3, fake):
    for i in range(3):
        fake.objects[("b", f"gold/t/{i}")] = b""
    fake.objects[("b", "gold/keep")] = b""
    assert s3.delete_prefix("b", "gold/t/") == 3
    assert list(fake.objects) == [("b", "gold/keep")]


def test_copy_object_keeps_bytes_and_replaces_metadata(s3, fake):
    fake.objects[("src", "a/x.jsonl")] = b'{"a": 1}\n'
    etag = s3.copy_object("src", "a/x.jsonl", "lake", "bronze/a/x.jsonl", {"run_id": "r1"})
    assert fake.objects[("lake", "bronze/a/x.jsonl")] == b'{"a": 1}\n'
    assert fake.metadata[("lake", "bronze/a/x.jsonl")] == {"run_id": "r1"}
    assert etag == s3.list_objects("lake", "bronze/")[0].etag


def test_copy_object_retries_transient_errors(s3, fake):
    fake.objects[("src", "k")] = b"x"
    fake.failures = [client_error("SlowDown")]
    s3.copy_object("src", "k", "lake", "k", {})
    assert fake.calls.count("copy_object") == 2


def test_arrow_round_trip_keeps_exact_schema(s3):
    import datetime
    import decimal

    import pyarrow as pa
    table = pa.table({
        "score": pa.array([decimal.Decimal("56.87"), None], pa.decimal128(5, 2)),
        "day": pa.array([datetime.date(2025, 1, 2), None], pa.date32()),
        "n": pa.array([1, None], pa.int32()),
    })
    assert s3.write_arrow(table, "lake", "t.parquet") > 0
    assert s3.read_arrow("lake", "t.parquet").equals(table)


def test_delete_keys_batches_and_counts(s3, fake):
    for i in range(3):
        fake.objects[("b", f"k{i}")] = b""
    assert s3.delete_keys("b", ["k0", "k2"]) == 2
    assert s3.delete_keys("b", []) == 0
    assert fake.keys("b") == ["k1"]
