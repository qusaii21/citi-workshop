"""Unit tests for talent_etl.config."""

import pytest

from talent_etl.config import LOCAL_POSTGRES_PASS, ConfigError, Settings

CLOUD_ENV = {
    "IS_LOCAL": "false",
    "POSTGRES_HOST": "aurora.example",
    "POSTGRES_PORT": "5432",
    "POSTGRES_NAME": "acme",
    "POSTGRES_USER": "etl",
    "POSTGRES_PASS": "secret",
    "AWS_REGION": "ap-south-1",
    "SOURCE_BUCKET": "src-bucket",
    "LAKE_BUCKET": "lake-bucket",
}


def test_local_defaults_when_env_is_empty():
    settings = Settings.from_env({})
    assert settings.is_local is True
    assert settings.postgres.host == "localhost"
    assert settings.postgres.dbname == "postgres"
    assert settings.postgres.password == LOCAL_POSTGRES_PASS
    assert settings.postgres.sslmode == "prefer"
    assert settings.postgres.source_schema == "hr_source"
    assert settings.s3.endpoint_url == "http://localhost:4566"
    assert settings.s3.source_bucket == "acme-talent-source"
    assert settings.log_level == "INFO"


def test_workshop_empty_postgres_values_fall_back_locally():
    # docs/data-engineer.md lists POSTGRES_NAME/USER/PASS as empty when local.
    settings = Settings.from_env({"IS_LOCAL": "true", "POSTGRES_NAME": "", "POSTGRES_USER": " "})
    assert settings.postgres.dbname == "postgres"
    assert settings.postgres.user == "postgres"


def test_local_mode_is_pinned_to_localstack_even_with_real_aws_region():
    settings = Settings.from_env({"AWS_REGION": "ap-south-1"})
    assert settings.s3.region == "us-east-1"
    assert settings.s3.access_key_id == "test"


def test_local_endpoint_prefers_s3_specific_variable():
    settings = Settings.from_env({
        "AWS_ENDPOINT_URL": "http://generic:4566",
        "AWS_ENDPOINT_URL_S3": "http://s3.specific:4566",
    })
    assert settings.s3.endpoint_url == "http://s3.specific:4566"
    assert Settings.from_env({"AWS_ENDPOINT_URL": "http://generic:4566"}).s3.endpoint_url == "http://generic:4566"


def test_cloud_mode_uses_env_values_and_requires_ssl():
    settings = Settings.from_env(CLOUD_ENV)
    assert settings.is_local is False
    assert settings.postgres.sslmode == "require"
    assert settings.postgres.connect_kwargs()["host"] == "aurora.example"
    assert settings.s3.endpoint_url is None
    assert settings.s3.access_key_id is None
    assert settings.s3.region == "ap-south-1"
    assert settings.s3.lake_bucket == "lake-bucket"


def test_cloud_region_falls_back_to_aws_default_region():
    env = {k: v for k, v in CLOUD_ENV.items() if k != "AWS_REGION"} | {"AWS_DEFAULT_REGION": "eu-west-1"}
    assert Settings.from_env(env).s3.region == "eu-west-1"


def test_cloud_mode_reports_every_missing_variable_at_once():
    with pytest.raises(ConfigError) as err:
        Settings.from_env({"IS_LOCAL": "false"})
    message = str(err.value)
    for name in ("POSTGRES_HOST", "POSTGRES_NAME", "POSTGRES_USER", "POSTGRES_PASS",
                 "AWS_REGION", "SOURCE_BUCKET", "LAKE_BUCKET"):
        assert name in message


def test_password_is_not_in_repr():
    settings = Settings.from_env(CLOUD_ENV)
    assert "secret" not in repr(settings)


@pytest.mark.parametrize("raw, expected", [("TRUE", True), ("1", True), ("yes", True), ("False", False), ("0", False)])
def test_is_local_accepts_common_boolean_spellings(raw, expected):
    env = CLOUD_ENV | {"IS_LOCAL": raw}
    assert Settings.from_env(env).is_local is expected


@pytest.mark.parametrize("env, fragment", [
    ({"IS_LOCAL": "maybe"}, "IS_LOCAL"),
    ({"POSTGRES_PORT": "abc"}, "POSTGRES_PORT must be an integer"),
    ({"POSTGRES_PORT": "0"}, "POSTGRES_PORT must be >= 1"),
    ({"RETRY_BASE_DELAY_SECONDS": "x"}, "RETRY_BASE_DELAY_SECONDS must be a number"),
    ({"RETRY_MAX_DELAY_SECONDS": "-1"}, "RETRY_MAX_DELAY_SECONDS must be >= 0"),
    ({"LOG_LEVEL": "LOUD"}, "LOG_LEVEL"),
    ({"POSTGRES_SOURCE_SCHEMA": "hr; drop table x"}, "POSTGRES_SOURCE_SCHEMA"),
])
def test_malformed_values_raise_config_error(env, fragment):
    with pytest.raises(ConfigError, match=fragment):
        Settings.from_env(env)


def test_retry_and_log_level_overrides():
    settings = Settings.from_env({
        "RETRY_ATTEMPTS": "2", "RETRY_BASE_DELAY_SECONDS": "0.5",
        "RETRY_MAX_DELAY_SECONDS": "4", "LOG_LEVEL": "debug",
    })
    assert (settings.retry.attempts, settings.retry.base_delay, settings.retry.max_delay) == (2, 0.5, 4.0)
    assert settings.log_level == "DEBUG"
