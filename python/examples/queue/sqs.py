"""Consume Amazon SQS and process each message once, even across at-least-once redeliveries.

contrib.sqs presets the dedup id (the message's MessageId) and the attempt count
(SQS's ApproximateReceiveCount). run_forever polls, dispatches, and deletes on ack.
"""

from idemkit import InMemoryBackend
from idemkit.contrib.sqs import sqs_consumer


def charge_customer(body: str) -> None: ...  # your real side effect; runs once per SQS MessageId


consumer = sqs_consumer(
    backend=InMemoryBackend(),  # prod: RedisBackend/PostgresBackend, see ../shared/backends.py
    visibility_timeout_seconds=30,
)


@consumer.handle
def process(msg) -> None:
    charge_customer(msg["Body"])


# In production, run the poll loop (deletes each message on ack):
#   import boto3
#   from idemkit.contrib.sqs import run_forever
#   run_forever(consumer, sqs_client=boto3.client("sqs"), queue_url=QUEUE, visibility_timeout=30)
