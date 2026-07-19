"""Run a scheduled job once per window, even if two schedulers fire or it reruns.

Cron, Celery Beat, and APScheduler all double-fire eventually: two beat processes,
an overlapping run, or a redeploy that replays the schedule. There's no message id
to dedup on — the natural key is the *slot*: the job name plus the window it's for
(today's date, this hour). Key on that, and the body runs once per slot; a second
firing for the same slot replays and does nothing.

Runs as-is, no infrastructure. For production swap in Redis or Postgres so two
schedulers on different hosts share the same claim; see ../shared/backends.py.

Run:  python examples/method/cron_run_once.py
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent_sync


def deliver_digest(run_date: str) -> dict:
    # Your real side effect; must happen once per run_date, not once per firing.
    return {"sent": True, "date": run_date}


backend = InMemoryBackend()


@idempotent_sync(
    backend=backend,
    # One run per (job, window): a second scheduler firing the same date is a no-op replay.
    config=MethodConfig(key_fields=["job", "run_date"]),
)
def send_daily_digest(*, job: str = "daily-digest", run_date: str) -> dict:
    return deliver_digest(run_date)


if __name__ == "__main__":
    # Two schedulers both fire for the same day -> the digest goes out once.
    send_daily_digest(run_date="2026-07-19")
    send_daily_digest(run_date="2026-07-19")  # duplicate firing, replayed
    print("digest sent once for 2026-07-19")
