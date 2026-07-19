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

as ops/sec and p50/p99/p99.9 latency. Read them this way: **the in-memory row is
idemkit's own overhead** — no I/O, just the claim/complete/fingerprint work, usually
sub-millisecond. Every real backend adds one network round-trip on top, so the gap
between in-memory and your store is the store's latency, not anything idemkit spends.
(DynamoDB does a consistent read on the claim path — slower than eventually-consistent,
but that's what makes fencing correct.)

This measures speed only. Correctness is covered by the conformance suite and the
property/fault/clock-skew tests — see [CORRECTNESS.md](../docs/correctness.md).
