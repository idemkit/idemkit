"""Optional broker glue for idemkit's queue surface.

The core queue adapter (:class:`idemkit.IdempotentConsumer`) is broker-agnostic —
you supply small callables that read the dedup id, scope, and receive count from
whatever message object your library hands you. That is deliberately flexible but
means everyone rewrites the same wiring for the same two or three brokers.

``idemkit.contrib`` presets that wiring for the common cases so it's import-and-go:

* :mod:`idemkit.contrib.sqs` — Amazon SQS (boto3 message dicts).
* :mod:`idemkit.contrib.kafka` — Kafka (``confluent_kafka`` or ``kafka-python`` records).
* :mod:`idemkit.contrib.mcp` — enforce idempotency on MCP / LLM tool calls.
* :mod:`idemkit.contrib.fastapi` — a FastAPI route class (return a dict, read
  ``request.state`` in ``scope``).
* :mod:`idemkit.contrib.drf` — a Django REST Framework view mixin (read
  ``request.user`` in ``scope``).

For Celery there is no separate module: a task body is a plain synchronous
function, so :func:`idemkit.idempotent_sync` is the tool (stack it UNDER
``@app.task``). See ``examples/method/sync_function.py``.

None of these import a broker SDK; they read duck-typed message objects and (for
SQS) take the client you already created. Install the broker library yourself.

For observability there are two ready-made event handlers, so you don't hand-roll
the metric plumbing:

* :mod:`idemkit.contrib.prometheus` — record operations / latency to Prometheus.
* :mod:`idemkit.contrib.logging` — one structured log line per operation.
"""
