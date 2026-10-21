"""Monotonic simulator timing, independent of framework/business log sessions."""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

MARKER = "SIM_RUNTIME_TIMING="
_current: ContextVar = ContextVar("sim_timing", default=None)


class Timings:
    def __init__(self):
        self.started = time.perf_counter()
        self.phases: dict[str, float] = {}

    def snapshot(self):
        return {**{name: None for name in ("setup_s", "engine_creation_s", "execution_s", "validation_s", "shutdown_s")},
                "total_s": round(time.perf_counter() - self.started, 6),
                **{key: round(value, 6) for key, value in self.phases.items()}}


@contextmanager
def scope():
    timing = Timings()
    token = _current.set(timing)
    try:
        yield timing
    finally:
        _current.reset(token)


@contextmanager
def phase(name):
    timing = _current.get()
    started = time.perf_counter()
    try:
        yield
    finally:
        if timing is not None:
            timing.phases[name] = timing.phases.get(name, 0.0) + time.perf_counter() - started


def measured(name):
    def decorate(function):
        @wraps(function)
        def call(*args, **kwargs):
            with phase(name):
                return function(*args, **kwargs)
        return call
    return decorate


def runtime_entrypoint(function):
    @wraps(function)
    def call(*args, **kwargs):
        with scope() as timing:
            try:
                return function(*args, **kwargs)
            finally:
                payload = {name: None for name in
                           ("engine_creation_s", "execution_s", "shutdown_s")}
                payload.update(timing.snapshot(), pid=os.getpid())
                print(MARKER + json.dumps(payload, separators=(",", ":")), flush=True)
    return call


def read_runtime_timing(path, marker=MARKER):
    """Read only the top-level runner marker; nested validators remain separate."""
    for line in reversed(path.read_text(encoding="utf-8", errors="replace").splitlines()):
        if line.startswith(marker):
            try:
                value = json.loads(line[len(marker):])
            except ValueError:
                continue
            if isinstance(value, dict):
                return value
    return None
