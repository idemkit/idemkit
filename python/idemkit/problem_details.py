"""RFC 9457 Problem Details helpers per spec §6.3.

URI namespace ``urn:idemkit:*`` is OUR namespace. The IETF-namespace URIs
``urn:ietf:idempotency:*`` are proposed to the WG but not yet registered;
we do not use them in our own implementation.
"""

from __future__ import annotations

import json

CONTENT_TYPE = "application/problem+json"

PROBLEM_TYPES = {
    "missing-key": "urn:idemkit:missing-key",
    "payload-mismatch": "urn:idemkit:payload-mismatch",
    "in-progress": "urn:idemkit:in-progress",
    "storage-error": "urn:idemkit:storage-error",
    "identity-unavailable": "urn:idemkit:identity-unavailable",
    "expired": "urn:idemkit:expired",
}


def problem_response(
    *,
    type_key: str,
    status: int,
    title: str,
    detail: str,
) -> tuple[int, dict[str, str], bytes]:
    """Build an RFC 9457 Problem Details response.

    Returns ``(status, headers, body_bytes)``.
    """
    payload: dict[str, object] = {
        "type": PROBLEM_TYPES[type_key],
        "title": title,
        "status": status,
        "detail": detail,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "content-type": CONTENT_TYPE,
        "content-length": str(len(body)),
    }
    return status, headers, body


def missing_key() -> tuple[int, dict[str, str], bytes]:
    return problem_response(
        type_key="missing-key",
        status=400,
        title="Idempotency key required",
        detail="This endpoint requires an Idempotency-Key header.",
    )


def key_too_long(max_bytes: int = 255) -> tuple[int, dict[str, str], bytes]:
    # §6.1: oversized key → 400 with the same type URI as a missing key.
    return problem_response(
        type_key="missing-key",
        status=400,
        title="Idempotency key too long",
        detail=(
            f"The Idempotency-Key header exceeds the {max_bytes}-byte limit. "
            "Use a shorter key (a UUID is a good choice)."
        ),
    )


def payload_mismatch(stripe_compat: bool) -> tuple[int, dict[str, str], bytes]:
    return problem_response(
        type_key="payload-mismatch",
        status=409 if stripe_compat else 422,
        title="Idempotency key payload mismatch",
        detail=(
            "The idempotency key was previously used with a different request payload. "
            "To retry safely, send the same payload as before or use a new idempotency key."
        ),
    )


def in_progress(stripe_compat: bool, retry_after_seconds: int) -> tuple[int, dict[str, str], bytes]:
    status, headers, body = problem_response(
        type_key="in-progress",
        status=409 if stripe_compat else 423,
        title="Idempotency key in progress",
        detail=(
            "Another request with the same idempotency key is currently being processed. "
            "Wait and retry."
        ),
    )
    headers["retry-after"] = str(max(1, retry_after_seconds))
    return status, headers, body


def identity_unavailable() -> tuple[int, dict[str, str], bytes]:
    return problem_response(
        type_key="identity-unavailable",
        status=500,
        title="Caller identity unavailable",
        detail=(
            "The server could not determine a caller identity for this request. "
            "Idempotency was refused to preserve cross-tenant isolation. "
            "This indicates a server-side configuration issue with the "
            "scope extractor."
        ),
    )


def storage_error(retry_after_seconds: int = 5) -> tuple[int, dict[str, str], bytes]:
    status, headers, body = problem_response(
        type_key="storage-error",
        status=503,
        title="Idempotency storage unavailable",
        detail="The idempotency backend is temporarily unavailable. Retry.",
    )
    headers["retry-after"] = str(max(1, retry_after_seconds))
    return status, headers, body
