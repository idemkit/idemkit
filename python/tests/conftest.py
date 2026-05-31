"""pytest configuration for idemkit tests.

``asyncio_mode = "auto"`` in pyproject.toml means every async ``def test_*``
is auto-marked. No explicit ``@pytest.mark.asyncio`` decoration needed.
"""
