"""Import-light checks for the langx package.

These tests never import the ``_langx`` extension, so they run without HPX.
"""

import importlib.util
import pathlib

PKG = pathlib.Path(__file__).resolve().parents[2] / "python" / "src" / "langx"
MODULES = ["coordinator", "effects", "runtime", "stub_llm", "worker"]


def _load(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_package_exposes_version():
    assert _load(PKG / "__init__.py").__version__


def test_every_module_has_a_docstring():
    for name in MODULES:
        assert _load(PKG / f"{name}.py").__doc__, name
