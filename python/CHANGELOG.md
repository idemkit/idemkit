# Changelog

All notable changes to the idemkit Python package are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/). The project follows
[Semantic Versioning](https://semver.org/) as of 1.0: a breaking change bumps the
major version, and any such change is called out here.

## [Unreleased]

Nothing yet.

## [1.0.0] - 2026-07-27

First stable release. The public API is now covered by semantic versioning: a
breaking change bumps the major version. The public API is the names exported from
`idemkit` (its `__all__`), the `HttpConfig` / `QueueConfig` / `MethodConfig` config
objects, the backend `Protocol` (`claim` / `complete` / `release` / `renew` /
`wait_for_completion`), and the `idemkit` CLI. Internals may change without a major
bump.

### Added
- `DynamoBackend.from_url(endpoint_url)`, for parity with the `from_url`
  constructors on the other backends.

### Fixed
- The "volatile-looking field" warning no longer fires on ordinary business keys
  such as `order_id` or `customer_id`. It now matches only known per-call /
  per-request id names (`request_id`, `trace_id`, `tool_call_id`, and similar), so
  the getting-started example no longer warns about its own key.

## [0.1.0] - 2026-07-26

First public release. Pre-1.0: the API may still shift, and a minor version may
carry a breaking change until 1.0, called out here when it does.

### Added
- One correct core across three surfaces: HTTP (ASGI + WSGI middleware and a
  per-route decorator), queue consumers (`IdempotentConsumer`), and method / AI
  tool calls (`@idempotent`, `@idempotent_sync`).
- Backends: `InMemoryBackend`, `RedisBackend` (atomic Lua + pub/sub),
  `PostgresBackend` (`INSERT ... ON CONFLICT` + `LISTEN/NOTIFY`),
  `MongoBackend` (`$$NOW` server-clock leases + TTL index), and `DynamoBackend`
  (conditional writes + TTL attribute; client-clock leases). Redis and Postgres
  have the most production mileage; Mongo and DynamoDB pass the same conformance
  suite (DynamoDB except the clock-skew vector).
- Distributed-correctness core: single atomic claim, storage-clock lease,
  `claim_token` fencing, lease renewal / heartbeat, subscribe-before-read
  in-flight wait with a bounded polling fallback, corrupt-record recovery.
- `idemkit.contrib` broker presets for Amazon SQS, Kafka, RabbitMQ, and Google
  Pub/Sub, plus MCP / LLM tool enforcement (`mcp_idempotent`).
- Observability handlers: Prometheus and structured logging, plus a
  `reconciliation_handler` that surfaces the decisions where a side effect may
  have fired without a recorded result.
- Result codecs: JSON, dataclass, pydantic (v1/v2), custom `(to_dict, from_dict)`,
  and opt-in pickle (with a security warning).
- Runnable conformance suite (`idemkit conformance`) and a language-neutral
  `spec/conformance.yaml`.
- Docs: a getting-started guide per surface, configuration and operations
  references, and "The four assumptions, in code", which maps the design to the
  code that implements it.
