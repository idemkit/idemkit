"""Amazon SQS with idempotency, wired for you.

idemkit.contrib.sqs presets the two things people get wrong with SQS:
  - the dedup id is the message's MessageId (stable across redeliveries)
  - the attempt count is SQS's own ApproximateReceiveCount (durable, not in-process)

run_forever polls the queue, dispatches each message, and deletes it on ack. This
file uses a fake SQS client so it runs with no AWS account. In real code you pass
boto3.client("sqs").

Run:  python examples/queue/sqs.py
"""

from idemkit import InMemoryBackend
from idemkit.contrib.sqs import run_forever, sqs_consumer

charged = {"n": 0}


class FakeSqs:
    """Stands in for boto3's SQS client. Delivers one message twice (an SQS
    at-least-once redelivery), then goes quiet."""

    def __init__(self):
        self._deliveries = [self._msg(1), self._msg(2)]
        self.deleted = []

    def _msg(self, receive_count):
        return {
            "MessageId": "sqs-1",  # same id on every redelivery
            "ReceiptHandle": f"rh-{receive_count}",
            "Body": "charge:42",
            "Attributes": {"ApproximateReceiveCount": str(receive_count)},
        }

    def receive_message(self, **kwargs):
        return {"Messages": [self._deliveries.pop(0)]} if self._deliveries else {}

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.deleted.append(ReceiptHandle)  # deleting == acking


# >>> idemkit plugs in here: sqs_consumer presets the SQS dedup id and receive count.
# In production the backend is Redis or Postgres.
consumer = sqs_consumer(backend=InMemoryBackend(), visibility_timeout_seconds=30)


@consumer.handle
def process(msg) -> None:
    charged["n"] += 1  # runs once per MessageId


if __name__ == "__main__":
    fake = FakeSqs()
    ticks = {"n": 0}

    def stop():
        # Stop after a few empty polls (a real worker runs forever).
        ticks["n"] += 1
        return ticks["n"] > 3

    run_forever(
        consumer,
        sqs_client=fake,       # in real code: boto3.client("sqs")
        queue_url="https://sqs.example/q",
        visibility_timeout=30,
        wait_time_seconds=0,
        stop=stop,
    )
    print("charged once:", charged["n"])  # 1
    print("messages deleted (acked):", fake.deleted)
    assert charged["n"] == 1
