"""Store a typed return value (a dataclass or Pydantic model), not just a dict.

result_codec="dataclass" stores JSON and rebuilds the typed value on replay. The
return type is read from the annotation, so define it at module level.
"""

from dataclasses import dataclass

from idemkit import InMemoryBackend, MethodConfig, idempotent


@dataclass
class Booking:
    ref: str
    seats: int


def reserve(ref: str) -> Booking:
    return Booking(ref=ref, seats=2)


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["ref"], scope=lambda args: "tenant-1", result_codec="dataclass"
    ),
)
async def book(*, ref: str) -> Booking:
    return reserve(ref)
