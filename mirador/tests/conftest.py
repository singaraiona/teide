"""Shared test fixtures for Mirador."""

import sys
from pathlib import Path

import pytest

# Ensure teide Python bindings are importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "py"))

from teide import TeideLib  # noqa: E402

import mirador.app  # noqa: E402


@pytest.fixture(scope="session")
def init_teide():
    """Session-scoped fixture: create TeideLib, init sym + arena, teardown on exit."""
    import os

    lib_path = os.environ.get("TEIDE_LIB")
    lib = TeideLib(lib_path=lib_path)
    lib.sym_init()
    lib.arena_init()
    mirador.app._teide = lib
    yield lib
    lib.pool_destroy()
    lib.sym_destroy()
    lib.arena_destroy_all()
    mirador.app._teide = None
