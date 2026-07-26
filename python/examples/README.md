# idemkit examples

Copy-paste snippets, one problem per file. **Each file opens with the one problem it solves**, then
shows only the integration code (no demo scaffolding), uses `InMemoryBackend` so it
runs with no setup, and is verified by a test in [`../tests/examples/`](../tests/examples/),
so a broken example fails CI.

> New to the vocabulary (*lease*, *fencing token*, *scope*, *dedup id*, *visibility timeout*)? See the [Glossary](../README.md#glossary).

```bash
pip install -e ".[dev]"     # from python/
pytest tests/examples       # run every example's test
```

Find your problem in the tables below and open that file. Folders match the three
surfaces; `all_options.py` in each is the exhaustive option reference. To swap the
dev backend for real Redis/Postgres, see [`shared/backends.py`](shared/backends.py).

## `http/`: a client retried a POST

| My problem | File |
|---|---|
| A shaky client double-charges; replay the first response | `http/getting_started.py` |
| Make a WHOLE FastAPI app idempotent (scope, redact PII) | `http/fastapi_middleware.py` |
| Make a WHOLE sync Flask (or any WSGI) app idempotent | `http/flask_wsgi.py` |
| Make a WHOLE Django site idempotent (wire it in wsgi.py) | `http/django_wsgi.py` |
| Protect only SOME FastAPI routes, and return a dict | `http/fastapi_route.py` |
| Protect ONE route + catch typed exceptions | `http/route_decorator.py` |
| Dedupe inbound webhooks on the provider's event id (Stripe/GitHub) | `http/webhook.py` |
| Protect a Django REST Framework view (per-user via the DRF mixin) | `http/drf_view.py` |
| Make the replayed response differ from the original | `http/response_hook.py` |
| See every HTTP option | `http/all_options.py` |

## `queue/`: an at-least-once broker redelivered a message

| My problem | File |
|---|---|
| Run the side effect once per message, even on redelivery | `queue/getting_started.py` |
| Write a runnable poll loop for any broker (RabbitMQ/NATS/...) | `queue/generic_broker.py` |
| Consume Amazon SQS (dedup on MessageId) | `queue/sqs.py` |
| Consume Kafka (dedup on topic:partition:offset) | `queue/kafka.py` |
| Send a poison message to a DLQ after max_attempts | `queue/dead_letter.py` |
| Get the handler's return value back on a redelivery | `queue/cache_result.py` |
| Reject a reused message id that carries a different body | `queue/payload_validation.py` |
| See every queue option | `queue/all_options.py` |

## `method/`: a function (agent, job, internal call) ran twice

| My problem | File |
|---|---|
| Run a function once per set of arguments | `method/getting_started.py` |
| Run a scheduled job once per window (cron / Celery Beat overlap) | `method/cron_run_once.py` |
| Reject a reused key whose payload changed | `method/payload_validation.py` |
| Stop an LLM agent from repeating a tool call | `method/agent_loop.py` |
| Enforce idempotency on an MCP tool | `method/mcp.py` |
| Dedupe synchronous code (a Celery task, a thread, a script) | `method/sync_function.py` |
| Store a typed return value (dataclass, Pydantic) | `method/result_codecs.py` |
| Set the replay window, and test expiry without sleeps | `method/record_expiration.py` |
| Catch idemkit's typed exceptions | `method/exceptions.py` |
| Replay a deterministic failure instead of re-running it | `method/error_replay.py` |
| Skip the backend round-trip for a hot key in one process | `method/local_cache.py` |
| Reconcile a money path where your record and the provider can disagree | `method/reconciliation.py` |
| See every method option | `method/all_options.py` |

## `shared/`: any surface

| My problem | File |
|---|---|
| Point at a real store — Redis, Postgres, MongoDB, or DynamoDB (namespace, table, TLS) | `shared/backends.py` |
| Export metrics and logs (Prometheus + logging handlers) | `shared/observability.py` |
| Store dedup state in my own datastore (the 5-method Protocol) | `shared/custom_backend.py` |

Every file here is run by a test in `../tests/examples/`, so a broken example fails CI.
