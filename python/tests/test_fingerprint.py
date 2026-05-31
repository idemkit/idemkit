"""Spec §4.5 fingerprint algorithm and §4.6 effective key tests."""

from __future__ import annotations

from idemkit.core.fingerprint import (
    canonicalize_body,
    canonicalize_path,
    canonicalize_query,
    compute_effective_key,
    compute_fingerprint,
)

# ---- Path canonicalization ----

def test_path_canonicalization_collapses_slashes() -> None:
    assert canonicalize_path("/a//b///c") == "/a/b/c"
    assert canonicalize_path("//") == "/"


def test_path_canonicalization_strips_trailing_slash() -> None:
    assert canonicalize_path("/users/") == "/users"
    assert canonicalize_path("/") == "/"  # root preserved


def test_path_canonicalization_uppercases_percent() -> None:
    assert canonicalize_path("/users/%2fadmin") == "/users/%2Fadmin"


# ---- Query canonicalization ----

def test_query_canonicalization_sorts_params() -> None:
    assert canonicalize_query("b=2&a=1") == canonicalize_query("a=1&b=2")


def test_query_canonicalization_sorts_multi_values() -> None:
    assert canonicalize_query("x=2&x=1") == canonicalize_query("x=1&x=2")


def test_empty_query_returns_empty_string() -> None:
    assert canonicalize_query("") == ""


# ---- Body canonicalization ----

def test_body_json_sorted_keys() -> None:
    a = canonicalize_body(b'{"b":2,"a":1}', "application/json")
    b = canonicalize_body(b'{"a":1,"b":2}', "application/json")
    assert a == b
    assert a == b'{"a":1,"b":2}'


def test_body_json_parse_failure_falls_back_to_raw() -> None:
    """Spec §4.5: malformed body MUST fall back to raw bytes, NOT raise."""
    raw = b'{"not valid json'
    assert canonicalize_body(raw, "application/json") == raw


def test_body_form_urlencoded_sorted() -> None:
    a = canonicalize_body(b"b=2&a=1", "application/x-www-form-urlencoded")
    b = canonicalize_body(b"a=1&b=2", "application/x-www-form-urlencoded")
    assert a == b


def test_body_unknown_content_type_is_raw() -> None:
    raw = b"\x00\x01\x02binary"
    assert canonicalize_body(raw, "application/octet-stream") == raw


def test_body_empty_returns_empty() -> None:
    assert canonicalize_body(b"", "application/json") == b""


def test_body_content_type_with_charset() -> None:
    """Content-Type charset suffix doesn't break the matcher."""
    canonical = canonicalize_body(b'{"a":1}', "application/json; charset=utf-8")
    assert canonical == b'{"a":1}'


# ---- Fingerprint full computation ----

def test_fingerprint_stable_across_method_case() -> None:
    fp1 = compute_fingerprint("POST", "/x", "", b"", None)
    fp2 = compute_fingerprint("post", "/x", "", b"", None)
    assert fp1 == fp2


def test_fingerprint_stable_across_param_order() -> None:
    fp1 = compute_fingerprint(
        "POST", "/x", "b=2&a=1", b'{"y":2,"x":1}', "application/json"
    )
    fp2 = compute_fingerprint(
        "post", "/x", "a=1&b=2", b'{"x":1,"y":2}', "application/json"
    )
    assert fp1 == fp2


def test_fingerprint_changes_with_body() -> None:
    fp1 = compute_fingerprint("POST", "/x", "", b'{"a":1}', "application/json")
    fp2 = compute_fingerprint("POST", "/x", "", b'{"a":2}', "application/json")
    assert fp1 != fp2


def test_fingerprint_changes_with_path() -> None:
    fp1 = compute_fingerprint("POST", "/users", "", b"", None)
    fp2 = compute_fingerprint("POST", "/users/123", "", b"", None)
    assert fp1 != fp2


def test_fingerprint_changes_with_method() -> None:
    fp1 = compute_fingerprint("POST", "/x", "", b"", None)
    fp2 = compute_fingerprint("DELETE", "/x", "", b"", None)
    assert fp1 != fp2


def test_fingerprint_known_limitation_numeric_type() -> None:
    """Documented known limitation in spec §4.5: sorted-keys is not type-canonical.

    {"a": 1} and {"a": 1.0} produce different fingerprints. This is the
    trade-off for v0.1's zero-dependency implementation; RFC 8785 is
    reserved for fingerprint_version: 2.
    """
    fp1 = compute_fingerprint("POST", "/x", "", b'{"a":1}', "application/json")
    fp2 = compute_fingerprint("POST", "/x", "", b'{"a":1.0}', "application/json")
    assert fp1 != fp2  # documents the limitation


# ---- Effective key ----

def test_length_prefix_prevents_collision_on_null_bytes() -> None:
    """A naïve 0x00 separator would let inputs collide if they contain 0x00.

    With length-prefix, ``key="ab\\x00cd"`` and ``key="ab"`` + ``identity="cd"``
    yield different hashes. Spec §4.6.
    """
    a = compute_effective_key("ab\x00cd", "tenant1", "POST", "/x")
    b = compute_effective_key("ab", "\x00cdtenant1", "POST", "/x")
    assert a != b


def test_effective_key_changes_with_caller_identity() -> None:
    a = compute_effective_key("k", "tenant-A:user-1", "POST", "/charges")
    b = compute_effective_key("k", "tenant-B:user-2", "POST", "/charges")
    assert a != b


def test_effective_key_changes_with_path() -> None:
    a = compute_effective_key("k", "tenant", "POST", "/charges")
    b = compute_effective_key("k", "tenant", "POST", "/charges/123/refund")
    assert a != b


def test_effective_key_changes_with_method() -> None:
    a = compute_effective_key("k", "tenant", "POST", "/x")
    b = compute_effective_key("k", "tenant", "DELETE", "/x")
    assert a != b


def test_effective_key_stable_across_method_case() -> None:
    a = compute_effective_key("k", "tenant", "POST", "/x")
    b = compute_effective_key("k", "tenant", "post", "/x")
    assert a == b


def test_effective_key_is_64_char_hex() -> None:
    """SHA-256 hex output is exactly 64 lowercase hex characters."""
    k = compute_effective_key("any-key", "any-tenant", "POST", "/x")
    assert len(k) == 64
    assert all(c in "0123456789abcdef" for c in k)
