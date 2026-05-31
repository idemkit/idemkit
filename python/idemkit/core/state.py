"""Core state types for idemkit, mapping directly to spec §4.1 and §4.15.

These types are the contract any backend implementation MUST honor. The
spec lives at ``spec/README.md``.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class State(str, enum.Enum):
    """Record state, spec §4.1."""

    CLAIMED = "CLAIMED"
    COMPLETED = "COMPLETED"


class Decision(str, enum.Enum):
    """Engine outcome for one request, emitted as ``idempotency.<value>``.

    Mirrors the observability event types in spec §4.15.
    """

    NEW = "new"
    REPLAYED = "replayed"
    IN_FLIGHT_WAIT = "in_flight_wait"
    CONFLICT = "conflict"
    PAYLOAD_MISMATCH = "payload_mismatch"
    LEASE_RECLAIMED = "lease_reclaimed"
    LEASE_RECLAIMED_LOSS = "lease_reclaimed_loss"
    STORAGE_ERROR = "storage_error"
    CORRUPT_RECORD = "corrupt_record"


class ClaimResultType(str, enum.Enum):
    """Outcome of one ``claim`` attempt against a backend, spec §4.1."""

    NEW_CLAIMED = "new_claimed"
    ALREADY_CLAIMED = "already_claimed"
    ALREADY_COMPLETED = "already_completed"
    LEASE_RECLAIMED = "lease_reclaimed"


@dataclass(frozen=True)
class StoredRecord:
    """A single backend record per spec §4.1.

    A two-key (claim + result) layout is forbidden; this single record is the
    only object stored per effective_key.

    ``lease_until`` is set at claim time. ``completed_at`` and the
    ``response_*`` fields are set on the conditional ``COMPLETED`` transition.
    """

    effective_key: str
    state: State
    fingerprint: str
    fingerprint_version: int
    claim_token: str
    claimed_at: float  # monotonic seconds (in-memory) or wall seconds (distributed)
    lease_until: float
    completed_at: float | None = None
    response_status: int | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    response_body: bytes = b""


@dataclass(frozen=True)
class ClaimResult:
    """What ``IdempotencyBackend.claim`` returns.

    ``our_claim_token`` is non-None only when this caller acquired the claim
    (NEW_CLAIMED or LEASE_RECLAIMED). The token MUST be passed to subsequent
    ``complete`` / ``release`` calls so the backend can verify ownership.

    ``recovered_from_corrupt`` is set when the fresh claim was issued because
    an existing stored record failed to deserialize (§4.12). The engine emits
    ``idempotency.corrupt_record`` instead of ``idempotency.new`` in that case.
    """

    result: ClaimResultType
    record: StoredRecord
    our_claim_token: str | None = None
    recovered_from_corrupt: bool = False
