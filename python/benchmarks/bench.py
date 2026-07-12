"""Measure idemkit's per-operation cost on YOUR hardware and backend.

We deliberately publish no numbers: idempotency throughput is dominated by your
backend's round-trip (network, contention, consistency mode), so a number from our
laptop would mislead you. Run this against the store you actually deploy.

    python benchmarks/bench.py                        # in-memory (baseline, no I/O)
    python benchmarks/bench.py --redis redis://localhost:6379
    python benchmarks/bench.py --postgres postgresql://postgres:test@localhost:55432/postgres
    python benchmarks/bench.py --iterations 20000 --concurrency 64

It reports, per backend, the happy-path cost (a unique key: claim -> complete) and the
replay cost (a duplicate: claim hits an already-completed record), as ops/sec and
p50/p99/p99.9 latency. Correctness is covered elsewhere; this only measures speed.
"""

from __future__ import annotations

import argparse
import asyncio
import secrets
import statistics
import time


async def _make_backend(args):
    if args.redis:
        from idemkit import RedisBackend

        return RedisBackend.from_url(args.redis, namespace=f"bench-{secrets.token_hex(4)}")
    if args.postgres:
        from idemkit.backends.postgres import PostgresBackend

        return PostgresBackend.from_url(args.postgres, verify_schema=False)
    from idemkit import InMemoryBackend

    return InMemoryBackend()


def _summarize(name: str, samples: list[float]) -> None:
    samples_ms = sorted(s * 1000 for s in samples)
    n = len(samples_ms)

    def pct(p: float) -> float:
        return samples_ms[min(n - 1, int(p * n))]

    total_s = sum(samples)
    ops = n / total_s if total_s else 0.0
    print(
        f"  {name:<16} n={n:<7} {ops:>10,.0f} ops/s   "
        f"p50={pct(0.50):6.3f}ms  p99={pct(0.99):6.3f}ms  p99.9={pct(0.999):7.3f}ms  "
        f"mean={statistics.fmean(samples_ms):6.3f}ms"
    )


async def _run(args) -> None:
    backend = await _make_backend(args)
    total, conc = args.iterations, args.concurrency
    fp, fpv, lease, expiry = "fp", 1, 30.0, 300.0

    happy: list[float] = []
    replay: list[float] = []
    sem = asyncio.Semaphore(conc)

    async def happy_path(i: int) -> None:
        key = f"happy-{i}-{secrets.token_hex(6)}"
        async with sem:
            t0 = time.perf_counter()
            claim = await backend.claim(key, fp, fpv, lease)
            await backend.complete(key, claim.our_claim_token, 200, {}, b'{"ok":true}', expiry)
            happy.append(time.perf_counter() - t0)

    async def replay_path(key: str) -> None:
        async with sem:
            t0 = time.perf_counter()
            await backend.claim(key, fp, fpv, lease)  # already completed -> replay
            replay.append(time.perf_counter() - t0)

    print(f"backend: {type(backend).__name__}   iterations={total}  concurrency={conc}")
    print("warming up...")
    await asyncio.gather(*(happy_path(f"warm-{i}") for i in range(min(200, total))))
    happy.clear()

    print("measuring happy path (unique key: claim + complete)...")
    await asyncio.gather(*(happy_path(i) for i in range(total)))

    # Seed one completed key, then hammer it with duplicates.
    seed = f"replay-{secrets.token_hex(6)}"
    c = await backend.claim(seed, fp, fpv, lease)
    await backend.complete(seed, c.our_claim_token, 200, {}, b'{"ok":true}', expiry)
    print("measuring replay path (duplicate hits a completed record)...")
    await asyncio.gather(*(replay_path(seed) for _ in range(total)))

    print("\nresults (lower latency / higher ops-per-second is better):")
    _summarize("happy path", happy)
    _summarize("replay", replay)

    close = getattr(backend, "aclose", None)
    if close:
        await close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--redis", help="redis:// URL")
    p.add_argument("--postgres", help="postgresql:// URL (table must already exist)")
    p.add_argument("--iterations", type=int, default=5000)
    p.add_argument("--concurrency", type=int, default=32)
    asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    main()
