# Security

idemkit sits on a money path, so this is the honest account of what it stores, the
sharp edges, and how to report a problem.

## Reporting a vulnerability

Please report privately, not in a public issue. Use GitHub's **"Report a vulnerability"**
(Security → Advisories on the repository) so the report stays private until a fix ships.
Include a repro and the affected version. We aim to acknowledge within a few days and
to credit reporters who want it.

## What idemkit stores, and where

One record per key, in the backend you choose (Redis, Postgres, Mongo, DynamoDB, or
your own). Each record holds:

- **A hash of the key, never the raw key.** The stored `effective_key` is a
  length-prefixed SHA-256 of `(idempotency key, scope, method, path)`
  (`core/fingerprint.py`). The raw `Idempotency-Key` header is never written to the
  backend or to logs — logs carry the hash.
- **A body fingerprint** — also a SHA-256, used to reject a reused key that arrives
  with a different body. It's a one-way hash but **unsalted**, so a low-entropy body
  (say `{"amount": 500}`) can be recovered by brute force. Treat it as a tamper /
  mismatch check, not as confidentiality — encrypt at rest for the latter.
- **The response body / return value, stored verbatim** so duplicates can replay it.
  This is the one place sensitive data lands at rest: if your response contains PII
  or a PAN, that payload is written to the backend unless you redact it.

> **On a PCI / PII path, this is not optional:** set a `response_redactor` to scrub
> the stored copy **and** enable encryption at rest on the backend. idemkit stores
> response bodies verbatim by default and redacts nothing on its own.

### Handling sensitive responses

- **Redact before storing.** `response_redactor` (HTTP) strips fields from the copy
  idemkit keeps, so the first caller sees the full response but the stored/replayed
  copy is scrubbed. It is **opt-in** — nothing is redacted by default. See
  [`examples/http/fastapi_middleware.py`](examples/http/fastapi_middleware.py).
- **Encrypt at rest in the backend.** idemkit does not encrypt payloads itself; enable
  encryption at rest on Redis/Postgres/Mongo/DynamoDB, and restrict access to the
  store the same way you would any datastore holding request/response data.
- **Bound the retention.** `expires_after_seconds` sets how long a completed record
  (and its stored body) lives. Size it to your replay window, not longer.

## The pickle codec is an opt-in RCE vector

Return values are serialized with a codec. The default is **JSON**; a typed codec is
available for dataclasses/Pydantic. A `PickleResultCodec` also exists but is **opt-in
and a remote-code-execution risk**: a stored pickle that an attacker can influence
executes arbitrary code on `decode`. Constructing one emits a security warning
(`core/codecs.py`). Only use it if every writer to your backend is fully trusted;
prefer JSON or a typed codec otherwise. Cached exceptions never pickle — only the
type path and message are stored.

## Tenant isolation

A `scope` (usually a tenant/user id) is layered into the key hash, so two tenants can
send the same idempotency key without ever seeing each other's stored response. On a
multi-tenant service, set `scope`; use `scope_mode="strict"` to turn a missing tenant
id into a hard error instead of a shared namespace. See the
[Glossary](README.md#glossary) and [Configuration](docs/configuration.md).

## Supply chain

The core has **no third-party dependencies**; each backend and framework integration
is an opt-in extra, imported only when you ask for it. This keeps the default install
surface minimal. Pin idemkit and audit the extras you enable (`redis`, `postgres`,
`mongo`, `dynamodb`, `asgi`, ...) like any other dependency.

## Denial of service

- `max_request_body_bytes` / `max_body_bytes` cap what idemkit will fingerprint and
  store, so an oversized body cannot exhaust memory or the backend.
- Keys over the length limit are rejected with `400` (`urn:idemkit:missing-key`)
  rather than stored.
- On a storage outage the default `fail_closed` policy rejects with `503` rather than
  running unprotected; `fail_open` is available but trades safety for availability.

## Scope of the guarantees

idemkit gives **effectively-once**, not exactly-once, and it fences records, not side
effects already fired. The security-relevant consequence: a rejected (fenced) stale
write cannot corrupt a record, but it also cannot un-send an email or reverse a charge
a zombie worker already made. The full model and its limits are in
[CORRECTNESS.md](docs/correctness.md) and the README
[Limitations](README.md#limitations-and-when-not-to-use-it).
