# Contributing to idemkit

```bash
cd python/ && python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                     # Docker-free; Redis tests use fakeredis
```

Common tasks are wrapped in a `Makefile` (run `make` to list them):

```bash
make format      # autoformat + autofix in place  (ruff format + ruff check --fix)
make lint        # verify format + lint, fail on any violation, change nothing
make typecheck   # mypy strict
make test        # full suite on real Redis + Postgres
make check       # the whole gate: lint + typecheck + test
```

`make check` is the same gate CI runs. It fails on any lint or format deviation without touching your files (`make format` fixes them). Run it before you push.

fakeredis is not real Redis. Before a release, run against real servers:

```bash
make up          # start Redis, Postgres, and the brokers in Docker
make test
```

Tests live under `tests/`, grouped by area (`core/`, `backends/`, `http/`, `queue/`, `method/`, `contrib/`, `conformance/`, `correctness/`, `examples/`, `e2e/`). The `e2e/` tests hit dockerized brokers (SQS, Kafka, RabbitMQ) and stay out of the default run:

```bash
make up
pip install -e ".[dev,e2e]" && make test-e2e
```

House rules: concurrency changes need a race test (N concurrent, exactly one execution); spec changes update `conformance.yaml`; bug fixes include a regression test that fails before the fix.
