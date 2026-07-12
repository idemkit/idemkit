"""The DRF mixin dedupes a POST, replays the stored response, and scopes per user."""

from __future__ import annotations

from typing import ClassVar

import pytest

django = pytest.importorskip("django")
pytest.importorskip("rest_framework")

from django.conf import settings  # noqa: E402

if not settings.configured:
    settings.configure(
        DEBUG=True,
        ALLOWED_HOSTS=["*"],
        DATABASES={},
        INSTALLED_APPS=["rest_framework"],
        REST_FRAMEWORK={"UNAUTHENTICATED_USER": None},
    )
    django.setup()

from rest_framework.response import Response  # noqa: E402
from rest_framework.test import APIRequestFactory  # noqa: E402
from rest_framework.views import APIView  # noqa: E402

from idemkit import HttpConfig, InMemoryBackend  # noqa: E402
from idemkit.contrib.drf import idempotent_view  # noqa: E402

_factory = APIRequestFactory()


def _make_view():
    calls = {"n": 0}
    mixin = idempotent_view(
        backend=InMemoryBackend(),
        config=HttpConfig(scope=lambda req: req.headers.get("X-User", "anon")),
    )

    class ChargeView(mixin, APIView):
        authentication_classes: ClassVar[list] = []
        permission_classes: ClassVar[list] = []

        def post(self, request):
            calls["n"] += 1
            return Response({"charged": True, "n": calls["n"]}, status=201)

    return ChargeView.as_view(), calls


def _post(view, key, user="u1"):
    request = _factory.post(
        "/charge", {"a": 1}, format="json", HTTP_IDEMPOTENCY_KEY=key, HTTP_X_USER=user
    )
    return view(request)


def test_wrong_mixin_order_raises_at_class_definition() -> None:
    from idemkit import ConfigurationError

    mixin = idempotent_view(backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: "s"))
    with pytest.raises(ConfigurationError, match="AFTER its DRF base"):

        class Wrong(APIView, mixin):  # mixin AFTER the base -> hooks shadowed
            def post(self, request):
                return Response({}, status=201)


def test_drf_mixin_dedupes_and_replays() -> None:
    view, calls = _make_view()
    r1 = _post(view, "k1")
    r2 = _post(view, "k1")  # duplicate
    r1.render()
    assert r1.status_code == 201
    assert calls["n"] == 1  # the view ran once
    assert bytes(r2.content) == bytes(r1.content)  # stored response replayed
    assert r2.headers.get("idempotency-replayed") == "true"


def test_drf_mixin_scopes_by_user() -> None:
    view, calls = _make_view()
    _post(view, "same", user="u1")
    _post(view, "same", user="u2")  # different user, same key -> not deduped
    assert calls["n"] == 2


def test_drf_mixin_no_key_passes_through() -> None:
    view, calls = _make_view()
    request1 = _factory.post("/charge", {"a": 1}, format="json", HTTP_X_USER="u1")
    request2 = _factory.post("/charge", {"a": 1}, format="json", HTTP_X_USER="u1")
    view(request1)
    view(request2)
    assert calls["n"] == 2
