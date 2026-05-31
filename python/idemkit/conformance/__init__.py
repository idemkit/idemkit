"""idemkit conformance suite — a public, reusable correctness standard (spec §11.1).

The shared-core vectors (atomic claim, fencing, lease reclaim, in-flight wait,
TTL expiry, lease renewal) packaged so ANY library whose backend implements the
``IdempotencyBackend`` Protocol can validate itself against them, in or out of
idemkit. This is the artifact §11.1 calls for: a correctness suite the field can
reference, not a private test file.

    from idemkit.conformance import BackendConformance

    report = await BackendConformance(my_backend).run()
    assert report.passed, report.report()

The language-neutral vector descriptions (for ports to other languages) live in
``spec/conformance.yaml``; this package is the runnable Python reference.
"""

from idemkit.conformance.backend import BackendConformance
from idemkit.conformance.report import ConformanceReport, VectorResult

__all__ = [
    "BackendConformance",
    "ConformanceReport",
    "VectorResult",
]
