"""idemkit command-line interface.

Commands:
    idemkit init-pg <database-url>      Create idemkit_records table + indexes.
    idemkit pg-vacuum <database-url>    Delete expired records.
    idemkit conformance [--redis URL] [--postgres URL]
                                       Run the shared-core conformance vectors
                                       against InMemory (always) and any backend
                                       whose URL is given. Exits non-zero on any
                                       failure.

``init-pg`` and ``pg-vacuum`` are idempotent and safe to re-run.
"""

from __future__ import annotations

import argparse
import asyncio
import sys


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="idemkit",
        description="idemkit operational tooling (schema migration, vacuum).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init_pg = sub.add_parser(
        "init-pg",
        help="Create the idemkit_records table and indexes (idempotent).",
    )
    init_pg.add_argument("database_url", help="postgres:// URL.")

    vacuum = sub.add_parser(
        "pg-vacuum",
        help="Delete expired records from idemkit_records.",
    )
    vacuum.add_argument("database_url", help="postgres:// URL.")
    vacuum.add_argument(
        "--completed-ttl-seconds",
        type=float,
        default=86_400.0,
        help="Records COMPLETED before (now - ttl) are deleted (default: 24h).",
    )
    vacuum.add_argument(
        "--lease-grace-seconds",
        type=float,
        default=60.0,
        help="CLAIMED records with lease_until + grace < now are deleted "
        "(default: 60s).",
    )

    conformance = sub.add_parser(
        "conformance",
        help="Run the shared-core conformance vectors against backends.",
    )
    conformance.add_argument(
        "--redis", help="redis:// URL to also run the vectors against."
    )
    conformance.add_argument(
        "--postgres", help="postgres:// URL to also run the vectors against."
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _make_parser()
    args = parser.parse_args(argv)

    if args.command == "init-pg":
        from idemkit.backends.postgres import init_pg

        asyncio.run(init_pg(args.database_url))
        print("idemkit: PostgreSQL schema initialized")
        return 0

    if args.command == "pg-vacuum":
        from idemkit.backends.postgres import pg_vacuum

        n = asyncio.run(
            pg_vacuum(
                args.database_url,
                completed_ttl_seconds=args.completed_ttl_seconds,
                lease_grace_seconds=args.lease_grace_seconds,
            )
        )
        print(f"idemkit: vacuumed {n} expired record(s)")
        return 0

    if args.command == "conformance":
        return asyncio.run(_run_conformance(args.redis, args.postgres))

    parser.print_help()
    return 2


async def _run_conformance(redis_url: str | None, postgres_url: str | None) -> int:
    """Run the conformance suite against InMemory plus any configured backend."""
    from idemkit.backends.memory import InMemoryBackend
    from idemkit.conformance import BackendConformance

    all_passed = True

    async def _run(backend: object, aclose: bool) -> None:
        nonlocal all_passed
        report = await BackendConformance(backend).run()  # type: ignore[arg-type]
        print(report.report())
        if not report.passed:
            all_passed = False
        if aclose and hasattr(backend, "aclose"):
            await backend.aclose()

    await _run(InMemoryBackend(), aclose=False)

    if redis_url:
        from idemkit.backends.redis import RedisBackend

        await _run(RedisBackend.from_url(redis_url), aclose=True)

    if postgres_url:
        from idemkit.backends.postgres import PostgresBackend, init_pg

        await init_pg(postgres_url)
        await _run(PostgresBackend.from_url(postgres_url), aclose=True)

    print()
    print("idemkit: conformance PASSED" if all_passed else "idemkit: conformance FAILED")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
