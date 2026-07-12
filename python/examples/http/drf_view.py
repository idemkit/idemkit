"""You use Django REST Framework and want a view to be idempotent: a double-submitted
POST runs once, and the retry gets back the same response.

The idemkit part is tiny (the two numbered blocks below). Everything above them is
plain Django setup so this file runs on its own; in a real project it already exists.
"""

from typing import ClassVar

# --- Django setup: only here so this file runs standalone. Delete it in a real app. ---
from django.conf import settings

if not settings.configured:
    import django

    settings.configure(
        DEBUG=True,
        ALLOWED_HOSTS=["*"],
        DATABASES={},
        INSTALLED_APPS=["rest_framework"],
        REST_FRAMEWORK={"UNAUTHENTICATED_USER": None},
    )
    django.setup()
# --- end Django setup ---

from rest_framework.response import Response
from rest_framework.views import APIView

from idemkit import HttpConfig, InMemoryBackend
from idemkit.contrib.drf import idempotent_view

# 1. Build the mixin once. `backend` is where dedup state is stored; `scope` is how
#    you tell callers apart so two users can reuse the same key without colliding.
#    Here scope reads a header. In a real app it is usually `req.user.pk`: the mixin
#    runs after DRF authentication, so `req.user` is already set.
Idempotent = idempotent_view(
    backend=InMemoryBackend(),  # dev only; use Redis/Postgres in prod (see ../shared/backends.py)
    config=HttpConfig(scope=lambda req: req.headers.get("X-User", "anon")),
)


# 2. List the mixin BEFORE the DRF base class. Now a repeated POST with the same
#    Idempotency-Key runs `post` once, and the duplicate replays the first response.
class ChargeView(Idempotent, APIView):
    authentication_classes: ClassVar[list] = []  # no auth in this standalone file
    permission_classes: ClassVar[list] = []

    def post(self, request):
        return Response({"charged": True}, status=201)
