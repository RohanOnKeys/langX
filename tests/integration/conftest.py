"""Integration tests need the built ``_langx`` extension and HPX.

Every test in this directory is skipped cleanly when the extension cannot be
imported, so the suite never fails just because HPX is absent.
"""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python" / "src"))

try:
    import langx._langx  # noqa: F401
except ImportError:
    HAVE_LANGX = False
else:
    HAVE_LANGX = True


def pytest_collection_modifyitems(config, items):
    if HAVE_LANGX:
        return
    skip = pytest.mark.skip(reason="langx._langx extension not built")
    for item in items:
        item.add_marker(skip)
