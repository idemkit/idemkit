# idemkit

**Idempotent execution: make any operation safe to retry, with no duplicate effects.** One correct core — a key, an atomic claim, run-once, store, replay, with a crash-safe lease and fencing — applied to the three places a retry turns into a duplicate: HTTP requests, queue messages, and method calls from keyless callers (LLM agents, jobs, internal calls).

idemkit is a language-neutral design with a public [conformance suite](./spec/conformance.yaml). It is **available today in Python** — all three surfaces, tested on real Redis and PostgreSQL. The behavioral contract is written to be portable, so other-language implementations can reuse the same conformance vectors later, but none exist yet — Python is the focus. idemkit can't promise exactly-once delivery (nothing can), but at-least-once delivery plus idempotent execution gets you to the same place.

**Using it now → [Python quickstart](./python/README.md).**

## The three problems it solves

A retry, a redelivery, or an agent re-plan can run the same side effect twice. idemkit stops that in three settings. They share one core but differ in what triggers the dedupe.

### 1. HTTP requests

A client (mobile app, browser, another service) retries a `POST`/`PATCH` after a timeout or dropped connection. Unprotected, the server charges the card or creates the order twice. idemkit keys on the client's `Idempotency-Key` header: the first request runs, and a retry carrying the same key replays the stored response instead of re-running. This is the common, well-known case — the `Idempotency-Key` header Stripe popularized.

### 2. Queue consumers

At-least-once message brokers redeliver the same message by design — that's their contract. A consumer that charges or emails on each delivery acts twice. idemkit keys on the broker's message id, so the side effect runs once per id even across redeliveries, parallel consumers, and crashes. Works with **any at-least-once broker** — SQS, Kafka, RabbitMQ, Redis Streams, Google Pub/Sub, and others.

### 3. Method calls from keyless callers

Sometimes the thing that runs twice is a plain **function call**, and the caller has no idempotency key to give you. idemkit wraps the function and keys on its **arguments**: the real call runs once, duplicates get the cached result. The keyless callers this covers:

- **LLM agents** emitting tool calls — OpenAI function calling, Anthropic tool use, the Model Context Protocol (MCP), LangChain / LlamaIndex tools. The model re-issues the same call on a retry, a re-plan, or a parallel branch, and the per-turn ids it gives change every time. *(This is the leading use case.)*
- **Background jobs, cron steps, Celery tasks** that can run again after a failure or overlap.
- **Internal calls** — another service, a worker, a CLI — with no idempotency convention.

It's the newest and most specialized surface. If the function is already fronted by an HTTP request with an `Idempotency-Key`, use the HTTP surface instead — this one is for when there's no such key.

---

If you've ever added an `Idempotency-Key` header, hand-written a "did we already process this id?" check, or deduplicated a webhook, that's the gap idemkit fills: an atomic claim under concurrency that survives crashes and never replays the wrong response.

## See it in action (Python, 30 seconds, no infrastructure)

The current implementation is Python. A flaky client retries the same charge 5 times — without idemkit the customer is billed 5 times; with it, once.

```bash
# Pre-publication: not on PyPI and the GitHub repo isn't public yet, so install
# from a local checkout. All commands below run from the repo root.
git clone https://github.com/idemkit/idemkit && cd idemkit
pip install -e "python/[asgi]" httpx
python demo/double_charge.py
```

```text
WITHOUT idemkit:
  5 identical retries  ->  5 charges  X   (customer billed 5x)

WITH idemkit:
  5 identical retries  ->  1 charge   OK  (the other 4 replayed the first result)
```

The same demo runs against a real Redis (proving it holds across separate workers, not just in one process):

```bash
IDEMKIT_DEMO_REDIS_URL=redis://localhost:6379 python demo/double_charge.py
```

Source: [`demo/double_charge.py`](./demo/double_charge.py). Install, configuration, and the per-surface guides are in the **[Python quickstart →](./python/README.md)**.

---

## Why this exists

The `Idempotency-Key` idea is old and well understood — Stripe popularized it in 2017, the IETF is drafting it. The hard part isn't the header; it's the operational layer underneath, which the draft leaves to implementers, and that's where libraries slip:

- silent duplicate execution under concurrent retries
- lost response headers on replay
- the wrong response served when the same key arrives with a different body
- permanent locks after a process crashes mid-request
- cross-tenant cache leakage, where one user gets another's stored response

A quick survey: the most-installed Python option, `asgi-idempotency-header`, hasn't shipped a feature since 2022 and its crash-recovery bug ([#16](https://github.com/snok/asgi-idempotency-header/issues/16)) is open since July 2024; the most carefully-built design, AWS Lambda Powertools, only runs on Lambda; the IETF draft expired in April 2026 without pinning the operational layer down.

idemkit's answer is the concurrency and crash handling itself — a single atomic claim (no check-then-set), a real state machine, a storage-clock lease, and a fencing token, so a stalled or crashed worker can't double-execute or overwrite a newer result. It runs wherever your code does (Redis, PostgreSQL, or in-memory), makes no false promises (5xx never cached, no "exactly-once"), and is **tested, not asserted** — a language-neutral conformance suite runs the same vectors against every backend. A per-behavior comparison is in [the spec](./spec/idemkit-unified-spec.md) §10.

---

## Current status

**Python — all three surfaces implemented** (HTTP, queue consumers, method-level calls), each with conformance vectors passing on real Redis + PostgreSQL. See [`python/README.md`](./python/README.md) for install, quickstart, and configuration. Within Python, the HTTP surface has the most production mileage; queue and method-level are newer but pass the same cross-backend correctness bar.

The spec is written to be language-neutral, so a future port in another language could reuse the same `conformance.yaml` vectors. No such port exists today, and none is scheduled — the work is to get Python adopted first.

---

## Project background

idemkit is two things at once: **a production library** that drops into a FastAPI or Starlette app, and **a reference implementation of an engineering spec** whose vectors let other implementations check compatibility, written up as candidate input to the IETF [HTTPAPI working group](https://github.com/ietf-wg-httpapi/idempotency). Working code keeps the spec honest; the spec gives the standards work something concrete to point at.

- **Use it →** [`python/README.md`](./python/README.md) · **Understand the design →** [`spec/idemkit-unified-spec.md`](./spec/idemkit-unified-spec.md)

---

## Contributing

Contributions of any size are welcome: bug reports, spec feedback, code, docs.

1. Read [`spec/idemkit-unified-spec.md`](./spec/idemkit-unified-spec.md) to understand the behavioral contract (the HTTP detail is Appendix A).
2. Browse [open issues](https://github.com/idemkit/idemkit/issues).
3. For setup and running the test suite, see [`python/README.md#contributing`](./python/README.md#contributing).
4. For anything larger, open an issue first so we can talk through the approach.

If you're an IETF participant who follows the `Idempotency-Key` draft, please open an issue. There's draft text for several of the operational behaviors the working-group draft leaves open, and feedback on it would be valuable.

---

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
