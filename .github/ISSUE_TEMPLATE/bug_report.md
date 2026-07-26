---
name: Bug report
about: A correctness or behavior problem in idemkit
labels: bug
---

**Surface and backend**
Which surface (HTTP, queue, or method) and which backend (in-memory, Redis, Postgres, Mongo, DynamoDB).

**What happened**
What you saw, and what you expected instead. If a duplicate ran twice, or a result came back wrong or lost, say so directly: that is a correctness bug and gets priority.

**Minimal repro**
The smallest code that shows it. A failing test against `InMemoryBackend` is ideal.

**Versions**
idemkit, Python, and the backend server version.
