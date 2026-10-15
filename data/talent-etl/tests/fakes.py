"""In-memory fakes for the boto3 S3 client and the PostgreSQL client used by unit tests."""

from __future__ import annotations

import hashlib

import pyarrow as pa
from botocore.exceptions import ClientError


def client_error(code: str) -> ClientError:
    """Build a botocore ClientError with the given error code."""
    return ClientError({"Error": {"Code": code, "Message": code}}, "Op")


class FakeS3:
    """
    Stand-in for the subset of the boto3 S3 client we use.

    ``failures`` holds errors raised by the next calls (in order), to exercise retries.
    ``page_size`` is small so listings paginate. ETags are MD5s, as on real S3.
    """

    def __init__(self, page_size: int = 2) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.buckets: set[str] = set()
        self.page_size = page_size
        self.failures: list[Exception] = []
        self.created: list[dict] = []
        self.calls: list[str] = []
        self.corrupt_copies = False

    def _maybe_fail(self, method: str) -> None:
        self.calls.append(method)
        if self.failures:
            raise self.failures.pop(0)

    def keys(self, bucket: str, prefix: str = "") -> list[str]:
        """Return the sorted keys stored in a bucket under a prefix."""
        return sorted(k for b, k in self.objects if b == bucket and k.startswith(prefix))

    def head_bucket(self, Bucket):
        self._maybe_fail("head_bucket")
        if Bucket not in self.buckets:
            raise client_error("404")

    def create_bucket(self, **kwargs):
        self._maybe_fail("create_bucket")
        self.created.append(kwargs)
        self.buckets.add(kwargs["Bucket"])

    def put_object(self, Bucket, Key, Body, ContentType):
        self._maybe_fail("put_object")
        self.objects[(Bucket, Key)] = Body
        self.metadata[(Bucket, Key)] = {}

    def copy_object(self, Bucket, Key, CopySource, Metadata, MetadataDirective):
        self._maybe_fail("copy_object")
        assert MetadataDirective == "REPLACE"
        body = self.objects[(CopySource["Bucket"], CopySource["Key"])]
        if self.corrupt_copies:
            body = body + b"#"
        self.objects[(Bucket, Key)] = body
        self.metadata[(Bucket, Key)] = dict(Metadata)
        return {"CopyObjectResult": {"ETag": f'"{hashlib.md5(body).hexdigest()}"'}}  # nosec B324

    def get_object(self, Bucket, Key):
        self._maybe_fail("get_object")
        if (Bucket, Key) not in self.objects:
            raise client_error("NoSuchKey")
        body = self.objects[(Bucket, Key)]
        return {"Body": type("Body", (), {"read": lambda self: body})()}

    def head_object(self, Bucket, Key):
        self._maybe_fail("head_object")
        if (Bucket, Key) not in self.objects:
            raise client_error("404")

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken="0"):
        self._maybe_fail("list_objects_v2")
        keys = self.keys(Bucket, Prefix)
        start = int(ContinuationToken)
        page = keys[start:start + self.page_size]
        result = {"Contents": [
            {"Key": k, "Size": len(self.objects[(Bucket, k)]),
             "ETag": f'"{hashlib.md5(self.objects[(Bucket, k)]).hexdigest()}"'}  # nosec B324
            for k in page
        ]}
        if start + self.page_size < len(keys):
            result |= {"IsTruncated": True, "NextContinuationToken": str(start + self.page_size)}
        return result

    def delete_objects(self, Bucket, Delete):
        self._maybe_fail("delete_objects")
        for item in Delete["Objects"]:
            self.objects.pop((Bucket, item["Key"]))
            self.metadata.pop((Bucket, item["Key"]), None)


class FakePostgres:
    """Stand-in for PostgresClient serving fixed Arrow tables from one schema."""

    def __init__(self, schema: str, tables: dict[str, pa.Table]) -> None:
        self.schema = schema
        self.tables = tables
        self.reads: list[str] = []

    def list_tables(self, schema: str) -> list[str]:
        return sorted(self.tables) if schema == self.schema else []

    def read_table_arrow(self, schema: str, table: str) -> pa.Table:
        self.reads.append(f"{schema}.{table}")
        return self.tables[table]
