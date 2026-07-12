# Benchmarks

We publish no numbers on purpose. idemkit's throughput is dominated by your backend's
round-trip — network latency, contention, and consistency mode swamp the library's own
overhead — so a figure from our machine would tell you nothing useful about yours.
Measure on the store you actually deploy:

```bash
python benchmarks/bench.py                        # in-memory baseline (no I/O)
python benchmarks/bench.py --redis redis://localhost:6379
python benchmarks/bench.py --postgres postgresql://postgres:test@localhost:55432/postgres
python benchmarks/bench.py --iterations 20000 --concurrency 64
```

It reports, per backend:

- **happy path** — a unique key: `claim` + `complete` (the cost of protecting a real call).
- **replay** — a duplicate hitting an already-completed record (the cost idemkit adds to a retry).

as ops/sec and p50/p99/p99.9 latency. What to expect from the *shape* of the numbers
(not the magnitude): in-memory is sub-millisecond because there is no I/O; every real
backend is one network round-trip per operation, so its latency floor is your ping to
the store. DynamoDB uses a consistent read on the claim path, which is slower than an
eventually-consistent read but is what makes the fencing correct.

This measures speed only. Correctness is covered by the conformance suite and the
property/fault/clock-skew tests — see [CORRECTNESS.md](../docs/correctness.md).
