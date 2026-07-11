"""Run every example as a real Python process and assert it works.

The examples double as acceptance tests for the public API: if one breaks, this
fails. Each standalone example ends in an `assert`, so a zero exit code means the
demo actually produced the right result (handler ran once, duplicate replayed).
The FastAPI examples are exercised over ASGI with a real duplicate request.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

# Standalone scripts: they run to completion and self-assert.
STANDALONE = [
    "http/double_charge.py",
    "http/flask_wsgi.py",
    "queue/quickstart.py",
    "queue/dead_letter.py",
    "queue/cache_result.py",
    "queue/sqs.py",
    "queue/kafka.py",
    "method/quickstart.py",
    "method/sync.py",
    "method/agent_loop.py",
    "method/mcp.py",
    "method/manual_clock.py",
]


@pytest.mark.parametrize("script", STANDALONE)
def test_example_runs(script: str) -> None:
    result = subprocess.run(
        [sys.executable, str(EXAMPLES / script)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, (
        f"{script} exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


def _load(rel_path: str):
    """Import an example module by file path (it has a subfolder, no package)."""
    path = EXAMPLES / rel_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_fastapi_example_dedupes_over_asgi() -> None:
    """End-to-end: a duplicate POST replays, body_fingerprint ignores the nonce,
    and the redactor scrubs the card number from the stored (replayed) copy."""
    pytest.importorskip("fastapi")
    import httpx

    mod = _load("http/fastapi_app.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "k1", "X-User-Id": "u1", "Content-Type": "application/json"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r1 = await client.post("/charge", headers=headers, content='{"amount": 50, "nonce": "a"}')
        # nonce differs, but body_fingerprint keys only on amount, so this dedupes.
        r2 = await client.post("/charge", headers=headers, content='{"amount": 50, "nonce": "b"}')

    assert r1.status_code == 200
    assert r1.json()["charged"] == 50
    assert "card_number" in r1.json()                       # first caller sees the full body
    assert r2.headers.get("idempotency-replayed") == "true"  # second was replayed
    assert r2.json()["charged"] == 50
    assert "card_number" not in r2.json()                    # redacted from the stored copy


async def test_per_route_example_dedupes_over_asgi() -> None:
    pytest.importorskip("fastapi")
    import httpx

    mod = _load("http/per_route.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "r1", "X-User-Id": "u1", "Content-Type": "application/json"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r1 = await client.post("/refund", headers=headers, content="{}")
        r2 = await client.post("/refund", headers=headers, content="{}")

    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json() == r2.json() == {"refunded": True}
