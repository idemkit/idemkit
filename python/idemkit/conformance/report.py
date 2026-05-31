"""Result types for the conformance runner."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class VectorResult:
    """The outcome of one conformance vector."""

    vector_id: str
    passed: bool
    description: str
    error: str | None = None

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass
class ConformanceReport:
    """The result of running a set of vectors against one backend."""

    backend_name: str
    results: list[VectorResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True only if every vector passed."""
        return all(r.passed for r in self.results)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def failures(self) -> list[VectorResult]:
        return [r for r in self.results if not r.passed]

    def summary(self) -> str:
        """One-line human summary, e.g. ``RedisBackend: 13/13 vectors passed``."""
        passed = sum(1 for r in self.results if r.passed)
        return f"{self.backend_name}: {passed}/{self.total} vectors passed"

    def report(self) -> str:
        """A multi-line report listing each vector's status."""
        lines = [self.summary()]
        for r in self.results:
            lines.append(f"  [{r.status}] {r.vector_id} — {r.description}")
            if r.error:
                lines.append(f"         {r.error}")
        return "\n".join(lines)
