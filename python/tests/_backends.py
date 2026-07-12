"""Shared instantiation for the optional distributed backends in tests.

Mongo and DynamoDB run only when their endpoint env vars are set (like Postgres):

    IDEMKIT_TEST_MONGO_URL=mongodb://localhost:27017
    IDEMKIT_TEST_DYNAMODB_ENDPOINT=http://localhost:8000
"""

from __future__ import annotations

import os
from typing import Any

MONGO_URL = os.environ.get("IDEMKIT_TEST_MONGO_URL")
DYNAMODB_ENDPOINT = os.environ.get("IDEMKIT_TEST_DYNAMODB_ENDPOINT")

# Params to append to a backend fixture's `params=[...]`, present only when configured.
EXTRA_BACKENDS = [p for p, url in (("mongo", MONGO_URL), ("dynamodb", DYNAMODB_ENDPOINT)) if url]

# Same, but excluding DynamoDB (its leases use the client clock, not the storage
# clock, so it is not part of the clock-skew guarantee vector).
STORAGE_CLOCK_EXTRA_BACKENDS = [p for p in EXTRA_BACKENDS if p != "dynamodb"]


def make_mongo_backend(*, database: str = "idemkit_test") -> Any:
    from idemkit.backends.mongo import MongoBackend

    return MongoBackend.from_url(MONGO_URL, database=database)


def make_dynamo_backend(*, table: str = "idemkit_test") -> Any:
    from idemkit.backends.dynamodb import DynamoBackend

    return DynamoBackend(
        table=table,
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
