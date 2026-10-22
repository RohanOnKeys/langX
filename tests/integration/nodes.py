"""Node functions dispatched by the integration tests."""

import os
import time


def echo(value):
    """Return the input with this process's PID."""
    return {"value": value, "pid": os.getpid()}


def sleep_ms(ms):
    """Sleep, then return how long."""
    time.sleep(ms / 1000.0)
    return ms


def fail(_):
    """Raise, to check that node errors come back as values."""
    raise RuntimeError("node failed on purpose")
