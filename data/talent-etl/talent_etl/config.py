"""
Runtime configuration loaded from environment variables.

The same code runs locally and in the cloud; ``IS_LOCAL`` selects between them.
Local mode falls back to the workshop defaults (PostgreSQL credentials from
bin/setup-environment.sh, LocalStack for S3 with test credentials, mirroring
bin/deploy-backend.sh) so that it can never reach real AWS by accident. Cloud
mode has no fallbacks: every required variable must be set.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

# Defaults installed by bin/setup-environment.sh for the local PostgreSQL server.
LOCAL_POSTGRES_NAME = "postgres"
LOCAL_POSTGRES_USER = "postgres"
LOCAL_POSTGRES_PASS = "postgres123"  # nosec B105 - public local-only workshop default
LOCAL_S3_ENDPOINT = "http://localhost:4566"
LOCAL_AWS_REGION = "us-east-1"
LOCAL_SOURCE_BUCKET = "acme-talent-source"
LOCAL_LAKE_BUCKET = "acme-talent-lake"

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_TRUE = {"true", "1", "yes", "y"}
_FALSE = {"false", "0", "no", "n"}


class ConfigError(ValueError):
    """Raised when the environment does not describe a valid configuration."""


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the PostgreSQL source / serving database."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    sslmode: str
    source_schema: str
    connect_timeout: int

    def connect_kwargs(self) -> dict[str, object]:
        """Return keyword arguments for ``psycopg.connect``."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout,
        }


@dataclass(frozen=True)
class S3Config:
    """Settings for the S3 source bucket and the data lake bucket."""

    region: str
    endpoint_url: str | None
    access_key_id: str | None = field(repr=False)
    secret_access_key: str | None = field(repr=False)
    source_bucket: str
    source_prefix: str
    lake_bucket: str


@dataclass(frozen=True)
class RetryConfig:
    """Exponential backoff settings for transient PostgreSQL / S3 failures."""

    attempts: int
    base_delay: float
    max_delay: float


@dataclass(frozen=True)
class Settings:
    """Complete pipeline configuration."""

    is_local: bool
    log_level: str
    postgres: PostgresConfig
    s3: S3Config
    retry: RetryConfig
    seed_zip_uri: str | None = None   # sample data for the seed step: s3://bucket/key or a local path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """
        Build settings from environment variables.

        Args:
            env: Variables to read; defaults to ``os.environ``.

        Raises:
            ConfigError: If a value is malformed, or a cloud-mode variable is missing.
        """
        reader = _EnvReader(os.environ if env is None else env)
        is_local = reader.boolean("IS_LOCAL", default=True)
        settings = cls(
            is_local=is_local,
            log_level=reader.choice("LOG_LEVEL", {"DEBUG", "INFO", "WARNING", "ERROR"}, "INFO"),
            postgres=_postgres_config(reader, is_local),
            s3=_s3_config(reader, is_local),
            retry=RetryConfig(
                attempts=reader.integer("RETRY_ATTEMPTS", 4, minimum=1),
                base_delay=reader.number("RETRY_BASE_DELAY_SECONDS", 1.0, minimum=0.0),
                max_delay=reader.number("RETRY_MAX_DELAY_SECONDS", 30.0, minimum=0.0),
            ),
            seed_zip_uri=reader.optional("SEED_ZIP_URI"),
        )
        reader.raise_if_missing()
        return settings


def _postgres_config(reader: _EnvReader, is_local: bool) -> PostgresConfig:
    """Build the PostgreSQL section, applying local fallbacks only when local."""
    return PostgresConfig(
        host=reader.text("POSTGRES_HOST", "localhost" if is_local else None),
        port=reader.integer("POSTGRES_PORT", 5432, minimum=1),
        dbname=reader.text("POSTGRES_NAME", LOCAL_POSTGRES_NAME if is_local else None),
        user=reader.text("POSTGRES_USER", LOCAL_POSTGRES_USER if is_local else None),
        password=reader.text("POSTGRES_PASS", LOCAL_POSTGRES_PASS if is_local else None),
        sslmode="prefer" if is_local else "require",
        source_schema=reader.identifier("POSTGRES_SOURCE_SCHEMA", "hr_source"),
        connect_timeout=reader.integer("POSTGRES_CONNECT_TIMEOUT", 10, minimum=1),
    )


def _s3_config(reader: _EnvReader, is_local: bool) -> S3Config:
    """Build the S3 section; local mode is pinned to LocalStack with test credentials."""
    if is_local:
        endpoint = reader.text(
            "AWS_ENDPOINT_URL_S3", reader.optional("AWS_ENDPOINT_URL") or LOCAL_S3_ENDPOINT
        )
        return S3Config(
            region=LOCAL_AWS_REGION,
            endpoint_url=endpoint,
            access_key_id="test",
            secret_access_key="test",  # nosec B106 - LocalStack accepts any credentials
            source_bucket=reader.text("SOURCE_BUCKET", LOCAL_SOURCE_BUCKET),
            source_prefix=reader.text("SOURCE_PREFIX", "learning_activity"),
            lake_bucket=reader.text("LAKE_BUCKET", LOCAL_LAKE_BUCKET),
        )
    return S3Config(
        region=reader.text("AWS_REGION", reader.optional("AWS_DEFAULT_REGION")),
        endpoint_url=None,
        access_key_id=None,
        secret_access_key=None,
        source_bucket=reader.text("SOURCE_BUCKET", None),
        source_prefix=reader.text("SOURCE_PREFIX", "learning_activity"),
        lake_bucket=reader.text("LAKE_BUCKET", None),
    )


class _EnvReader:
    """Typed accessors over an environment mapping that collect missing names."""

    def __init__(self, env: Mapping[str, str]) -> None:
        self._env = env
        self._missing: list[str] = []

    def _raw(self, name: str) -> str | None:
        value = self._env.get(name, "")
        value = value.strip() if isinstance(value, str) else ""
        return value or None

    def optional(self, name: str) -> str | None:
        """Return a string value, or None when unset; never recorded as missing."""
        return self._raw(name)

    def text(self, name: str, default: str | None) -> str:
        """Return a string value, recording the name as missing if there is no default."""
        value = self._raw(name)
        if value is not None:
            return value
        if default is None:
            self._missing.append(name)
            return ""
        return default

    def integer(self, name: str, default: int, minimum: int) -> int:
        """Return an integer value of at least ``minimum``."""
        raw = self._raw(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
        if value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def number(self, name: str, default: float, minimum: float) -> float:
        """Return a float value of at least ``minimum``."""
        raw = self._raw(name)
        if raw is None:
            return default
        try:
            value = float(raw)
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
        if value < minimum:
            raise ConfigError(f"{name} must be >= {minimum}, got {value}")
        return value

    def boolean(self, name: str, default: bool) -> bool:
        """Return a boolean parsed from true/false, 1/0 or yes/no."""
        raw = self._raw(name)
        if raw is None:
            return default
        if raw.lower() in _TRUE:
            return True
        if raw.lower() in _FALSE:
            return False
        raise ConfigError(f"{name} must be true or false, got {raw!r}")

    def choice(self, name: str, allowed: set[str], default: str) -> str:
        """Return an upper-cased value that must be one of ``allowed``."""
        raw = self._raw(name)
        if raw is None:
            return default
        if raw.upper() not in allowed:
            raise ConfigError(f"{name} must be one of {sorted(allowed)}, got {raw!r}")
        return raw.upper()

    def identifier(self, name: str, default: str) -> str:
        """Return a value that is safe to use as a SQL identifier."""
        value = self._raw(name) or default
        if not _IDENTIFIER.match(value):
            raise ConfigError(f"{name} must match {_IDENTIFIER.pattern}, got {value!r}")
        return value

    def raise_if_missing(self) -> None:
        """Raise one error naming every required variable that was not set."""
        if self._missing:
            raise ConfigError("missing required environment variables: " + ", ".join(self._missing))
