"""
S3 access (AWS or LocalStack) with retries on transient failures.

Permanent errors such as AccessDenied or NoSuchKey are raised immediately so
IAM or path problems surface fast instead of after a full backoff cycle.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from talent_etl.config import S3Config
from talent_etl.retry import RetryPolicy

logger = logging.getLogger(__name__)

_TRANSIENT_CODES = {
    "InternalError", "RequestTimeout", "RequestTimeTooSkewed", "ServiceUnavailable",
    "SlowDown", "Throttling", "ThrottlingException", "500", "502", "503", "504",
}
_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def is_transient_s3_error(exc: BaseException) -> bool:
    """Return True for network errors, throttling and 5xx responses."""
    if isinstance(exc, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, ConnectionClosedError)):
        return True
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code", "") in _TRANSIENT_CODES
    return False


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


@dataclass(frozen=True)
class S3Object:
    """Listing entry for an S3 object."""

    key: str
    size: int
    etag: str


class S3Client:
    """S3 helper for raw bytes and Parquet frames, with retries."""

    def __init__(self, config: S3Config, retry_policy: RetryPolicy, client: Any | None = None) -> None:
        """
        Args:
            config: S3 settings; ``endpoint_url`` is set for LocalStack.
            retry_policy: Backoff policy for transient errors.
            client: Pre-built boto3 client (for tests); built from ``config`` when omitted.
        """
        self._config = config
        self._retry = retry_policy
        self._client = client or boto3.client(
            "s3",
            region_name=config.region,
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            # Our RetryPolicy owns retries; path-style addressing is needed for LocalStack.
            config=Config(
                retries={"total_max_attempts": 1},
                s3={"addressing_style": "path" if config.endpoint_url else "auto"},
            ),
        )

    def _call(self, description: str, method: str, **kwargs: Any) -> Any:
        return self._retry.call(
            lambda: getattr(self._client, method)(**kwargs),
            description=description,
            is_retryable=is_transient_s3_error,
        )

    def ensure_bucket(self, bucket: str) -> None:
        """Create the bucket if it does not exist."""
        try:
            self._call(f"head bucket {bucket}", "head_bucket", Bucket=bucket)
            return
        except ClientError as exc:
            if _error_code(exc) not in _NOT_FOUND_CODES | {"NoSuchBucket"}:
                raise
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if self._config.region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self._config.region}
        self._call(f"create bucket {bucket}", "create_bucket", **kwargs)
        logger.info("bucket created", extra={"context": {"bucket": bucket}})

    def list_objects(self, bucket: str, prefix: str) -> list[S3Object]:
        """List every object under ``prefix`` (paginated), sorted by key."""
        objects: list[S3Object] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            page = self._call(f"list s3://{bucket}/{prefix}", "list_objects_v2", **kwargs)
            objects.extend(
                S3Object(key=item["Key"], size=item["Size"], etag=item["ETag"].strip('"'))
                for item in page.get("Contents", [])
            )
            if not page.get("IsTruncated"):
                return sorted(objects, key=lambda obj: obj.key)
            token = page["NextContinuationToken"]

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return True if the object exists."""
        try:
            self._call(f"head s3://{bucket}/{key}", "head_object", Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if _error_code(exc) in _NOT_FOUND_CODES:
                return False
            raise

    def get_bytes(self, bucket: str, key: str) -> bytes:
        """Download an object's body."""
        return self._retry.call(
            lambda: self._client.get_object(Bucket=bucket, Key=key)["Body"].read(),
            description=f"get s3://{bucket}/{key}",
            is_retryable=is_transient_s3_error,
        )

    def put_bytes(self, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        """Upload bytes to an object, overwriting it if present."""
        self._call(
            f"put s3://{bucket}/{key}", "put_object",
            Bucket=bucket, Key=key, Body=data, ContentType=content_type,
        )

    def copy_object(
        self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str, metadata: dict[str, str]
    ) -> str:
        """
        Server-side copy that keeps the body byte-for-byte and replaces user metadata.

        Returns:
            The ETag of the new object (the source MD5 for non-multipart objects).
        """
        response = self._call(
            f"copy s3://{src_bucket}/{src_key} -> s3://{dst_bucket}/{dst_key}", "copy_object",
            Bucket=dst_bucket, Key=dst_key, CopySource={"Bucket": src_bucket, "Key": src_key},
            Metadata=metadata, MetadataDirective="REPLACE",
        )
        return response["CopyObjectResult"]["ETag"].strip('"')

    def delete_keys(self, bucket: str, keys: list[str]) -> int:
        """Delete the given keys in batches of 1000 and return how many were deleted."""
        for start in range(0, len(keys), 1000):
            batch = [{"Key": key} for key in keys[start:start + 1000]]
            self._call(
                f"delete {len(batch)} objects in s3://{bucket}", "delete_objects",
                Bucket=bucket, Delete={"Objects": batch, "Quiet": True},
            )
        return len(keys)

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Delete every object under ``prefix`` and return how many were deleted."""
        return self.delete_keys(bucket, [obj.key for obj in self.list_objects(bucket, prefix)])

    def write_parquet(self, df: pd.DataFrame, bucket: str, key: str) -> int:
        """Write a DataFrame as a Parquet object and return the object size in bytes."""
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        data = buffer.getvalue()
        self.put_bytes(bucket, key, data, content_type="application/vnd.apache.parquet")
        return len(data)

    def write_arrow(self, table: pa.Table, bucket: str, key: str) -> int:
        """Write an Arrow table as a Parquet object and return the object size in bytes."""
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        data = buffer.getvalue()
        self.put_bytes(bucket, key, data, content_type="application/vnd.apache.parquet")
        return len(data)

    def read_arrow(self, bucket: str, key: str) -> pa.Table:
        """Read a Parquet object into an Arrow table, keeping the stored schema exactly."""
        return pq.read_table(io.BytesIO(self.get_bytes(bucket, key)))

    def read_parquet(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a Parquet object into a DataFrame."""
        return pd.read_parquet(io.BytesIO(self.get_bytes(bucket, key)), engine="pyarrow")
