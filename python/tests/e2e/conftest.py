"""Fixtures for the end-to-end suite.

These tests run the documented example patterns against real services running in
local Docker containers (Redis, PostgreSQL, localstack SQS, redpanda Kafka,
RabbitMQ). The point is a regression guard: if a future change breaks what the
examples promise, an e2e test fails.

Bring the services up, then run the suite:

    docker compose -f tests/e2e/docker-compose.yml up -d
    pytest -m e2e

Each fixture skips cleanly if its service (or client library) is not available,
so you can run a subset. e2e tests are excluded from the default `pytest` run
(see `addopts` in pyproject.toml); pass `-m e2e` to run them.
"""

from __future__ import annotations

import os
import socket

import pytest

REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL", "redis://localhost:6379")
PG_URL = os.environ.get(
    "IDEMKIT_TEST_PG_URL", "postgresql://postgres:test@localhost:55432/postgres"
)
SQS_ENDPOINT = os.environ.get("IDEMKIT_TEST_SQS_ENDPOINT", "http://localhost:4566")
KAFKA_BOOTSTRAP = os.environ.get("IDEMKIT_TEST_KAFKA_BOOTSTRAP", "localhost:9092")
RABBITMQ_URL = os.environ.get("IDEMKIT_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def _reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture
def redis_url() -> str:
    if not _reachable("localhost", 6379):
        pytest.skip("Redis not reachable on localhost:6379 (docker compose up redis)")
    return REDIS_URL


@pytest.fixture
def pg_url() -> str:
    if not _reachable("localhost", 55432):
        pytest.skip("Postgres not reachable on localhost:55432 (docker compose up postgres)")
    return PG_URL


@pytest.fixture
def sqs_client():
    boto3 = pytest.importorskip("boto3")
    if not _reachable("localhost", 4566):
        pytest.skip("localstack SQS not reachable on localhost:4566 (docker compose up localstack)")
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def kafka_bootstrap() -> str:
    pytest.importorskip("kafka")
    if not _reachable("localhost", 9092):
        pytest.skip("Kafka/redpanda not reachable on localhost:9092 (docker compose up redpanda)")
    return KAFKA_BOOTSTRAP


@pytest.fixture
def rabbitmq_url() -> str:
    pytest.importorskip("pika")
    if not _reachable("localhost", 5672):
        pytest.skip("RabbitMQ not reachable on localhost:5672 (docker compose up rabbitmq)")
    return RABBITMQ_URL
