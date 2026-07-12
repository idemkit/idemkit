"""Catch and branch on idemkit's typed exceptions (here: refuse a call that names no key).

All subclass IdempotencyError, so one `except IdempotencyError` catches the family:
  PayloadMismatch       same key, different validated field (see payload_validation.py)
  IdempotencyConflict   a concurrent call is still in flight past the wait timeout
  IdempotencyKeyMissing require_key is on and the call named no key
  StorageUnavailable    the backend is down and on_storage_error="fail_closed"
  ReplayedError         a cached exception whose original type couldn't be rebuilt
                        (see error_replay.py)

This example triggers and catches IdempotencyKeyMissing.
"""

from idemkit import (
    IdempotencyError,
    InMemoryBackend,
    MethodConfig,
    idempotent,
)


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(require_key=True),  # refuse to run unless the call names a key
)
async def charge(*, amount: int) -> dict:
    return {"charged": amount}


async def main() -> None:
    try:
        await charge(amount=100)  # no key strategy -> refused before the side effect
    except IdempotencyError as exc:  # catch the whole family; or IdempotencyKeyMissing
        print(f"refused: {type(exc).__name__}")
    # Name a key and it runs, and a duplicate replays.
    await charge(amount=100, idempotency_key="charge-1")
    await charge(amount=100, idempotency_key="charge-1")
