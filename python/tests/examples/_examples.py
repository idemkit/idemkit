"""Shared helper for the example tests: import an example module by file path.

The examples are clean, copy-paste snippets with no test scaffolding. Each test
imports the example, drives the exposed object (a decorated function, an ASGI
``app``, or a ``consumer``), spies on the example's side-effect function, and
asserts the side effect ran once (dedup) and the duplicate replayed.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

# This file is python/tests/examples/_examples.py, so the package root (which holds
# the examples/ dir) is three levels up.
EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

# Kept for tests that want a real backend URL when one is configured.
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")
PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")


def load_module(rel_path: str) -> ModuleType:
    """Import an example module by file path, fresh each call (examples are scripts).

    The module is registered in ``sys.modules`` under a unique name, so a type
    defined in it (e.g. a custom exception used with ``cache_exceptions``) resolves
    back to it exactly as it would for a normally-imported app module.
    """
    path = EXAMPLES / rel_path
    mod_name = "idemkit_example_" + rel_path.replace("/", "_").removesuffix(".py")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module
