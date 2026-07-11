"""Optional broker glue for idemkit's queue surface.

The core queue adapter (:class:`idemkit.IdempotentConsumer`) is broker-agnostic —
you supply small callables that read the dedup id, scope, and receive count from
whatever message object your library hands you. That is deliberately flexible but
means everyone rewrites the same wiring for the same two or three brokers.

``idemkit.contrib`` presets that wiring for the common cases so it's import-and-go:

* :mod:`idemkit.contrib.sqs` — Amazon SQS (boto3 message dicts).
* :mod:`idemkit.contrib.kafka` — Kafka (``confluent_kafka`` or ``kafka-python`` records).
* :mod:`idemkit.contrib.mcp` — enforce idempotency on MCP / LLM tool calls.

None of these import a broker SDK; they read duck-typed message objects and (for
SQS) take the client you already created. Install the broker library yourself.
"""
