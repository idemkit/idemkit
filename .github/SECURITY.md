# Security policy

idemkit stores a hash of the idempotency key, never the raw key, and stores the
response body verbatim so duplicates can replay it. The full threat model and the
hardening steps for a money path are in [python/SECURITY.md](../python/SECURITY.md).

To report a vulnerability, follow the private reporting steps in that file rather
than opening a public issue.
