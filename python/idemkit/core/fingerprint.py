"""Length-prefixed SHA-256 hashing for fingerprint (§4.5) and effective key (§4.6).

The construction ``SHA-256(len_be32(c0) ‖ c0 ‖ len_be32(c1) ‖ c1 ‖ …)`` is
collision-resistant for arbitrary inputs, including those containing 0x00
bytes. This matters because both request bodies and idempotency keys are
user-controlled.
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import urllib.parse

FINGERPRINT_VERSION = 1


def _len_be32(b: bytes) -> bytes:
    """4-byte big-endian unsigned length prefix."""
    return struct.pack(">I", len(b))


def _length_prefixed_sha256(components: list[bytes]) -> str:
    """Lowercase hex SHA-256 of length-prefixed concatenation."""
    h = hashlib.sha256()
    for component in components:
        h.update(_len_be32(component))
        h.update(component)
    return h.hexdigest()


_PCT_RE = re.compile(rb"%([0-9a-fA-F]{2})")


def _uppercase_percent_bytes(s: bytes) -> bytes:
    """Normalize ``%xx`` escapes to uppercase hex per RFC 3986 §6.2.2.1."""
    return _PCT_RE.sub(lambda m: b"%" + m.group(1).upper(), s)


def canonicalize_path(path: str) -> str:
    """Canonicalize a URL path per spec §4.5.

    - Collapse repeated slashes.
    - Strip trailing slash except for root ``/``.
    - Normalize percent-encoding to uppercase hex.
    """
    while "//" in path:
        path = path.replace("//", "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    return _uppercase_percent_bytes(path.encode("utf-8")).decode("utf-8")


def canonicalize_query(query: str) -> str:
    """Canonicalize a query string per spec §4.5.

    Percent-decode once, sort params lexicographically by (name, value),
    re-encode with uppercase percent escapes.
    """
    if not query:
        return ""
    parsed = urllib.parse.parse_qsl(query, keep_blank_values=True)
    parsed.sort()
    encoded = urllib.parse.urlencode(parsed, doseq=False)
    return _uppercase_percent_bytes(encoded.encode("utf-8")).decode("utf-8")


def canonicalize_body(body: bytes, content_type: str | None) -> bytes:
    """Canonicalize a request body per spec §4.5.

    Returns canonicalized bytes. On parse failure, falls back to raw bytes
    (normative per spec — implementations MUST NOT raise on parse failure).
    """
    if not body or not content_type:
        return body

    ct_main = content_type.lower().split(";", 1)[0].strip()

    if ct_main == "application/json":
        try:
            parsed = json.loads(body)
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
            return body
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )

    if ct_main == "application/x-www-form-urlencoded":
        try:
            decoded = body.decode("utf-8")
        except UnicodeDecodeError:
            return body
        parsed_form = urllib.parse.parse_qsl(decoded, keep_blank_values=True)
        parsed_form.sort()
        return urllib.parse.urlencode(parsed_form).encode("utf-8")

    # Other content types fall back to the raw decoded bytes. NOTE: spec §4.5
    # describes a per-part canonicalization for multipart/form-data; v0.1 does
    # NOT implement it and treats multipart bodies as raw bytes, so a re-encoded
    # multipart body with a different boundary fingerprints differently. This is
    # a documented v0.1 limitation; full multipart canonicalization is deferred.
    return body


def compute_fingerprint(
    method: str,
    path: str,
    query: str,
    body: bytes,
    content_type: str | None,
) -> str:
    """Compute fingerprint v1 per spec §4.5."""
    return _length_prefixed_sha256(
        [
            method.upper().encode("ascii"),
            canonicalize_path(path).encode("utf-8"),
            canonicalize_query(query).encode("utf-8"),
            canonicalize_body(body, content_type),
        ]
    )


def compute_effective_key(
    idempotency_key: str,
    scope: str,
    method: str,
    path: str,
) -> str:
    """Compute effective key per spec §4.6 with length-prefixed composition."""
    return _length_prefixed_sha256(
        [
            idempotency_key.encode("utf-8"),
            scope.encode("utf-8"),
            method.upper().encode("ascii"),
            canonicalize_path(path).encode("utf-8"),
        ]
    )


def compose_key(components: list[str]) -> str:
    """Length-prefixed SHA-256 over arbitrary string scope components (spec §5.1).

    The surface-neutral effective-key builder for the queue and AI surfaces,
    where the scope is a tuple of plain strings rather than HTTP's
    (key, caller, method, path). Length-prefixing keeps it collision-resistant
    even when a component contains a 0x00 byte, same as the HTTP construction.

    Queue: ``compose_key([dedup_id, queue_or_topic, consumer_group])``.
    AI tool: ``compose_key([tool_name, version, args_hash, scope])``.
    """
    return _length_prefixed_sha256([c.encode("utf-8") for c in components])
