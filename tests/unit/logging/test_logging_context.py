"""Tests for datacoolie.logging.context — ContextVar helpers."""

from __future__ import annotations

import threading

from datacoolie.logging.context import (
    clear_dataflow_id,
    get_dataflow_id,
    set_dataflow_id,
)


class TestSetGetClear:
    """Round-trip: set → get → clear."""

    def test_default_is_empty_string(self):
        assert get_dataflow_id() == ""

    def test_set_and_get(self):
        token = set_dataflow_id("df-123")
        try:
            assert get_dataflow_id() == "df-123"
        finally:
            clear_dataflow_id(token)

    def test_clear_restores_previous(self):
        outer = set_dataflow_id("df-outer")
        inner = set_dataflow_id("df-inner")
        assert get_dataflow_id() == "df-inner"
        clear_dataflow_id(inner)
        assert get_dataflow_id() == "df-outer"
        clear_dataflow_id(outer)
        assert get_dataflow_id() == ""

    def test_clear_restores_default(self):
        token = set_dataflow_id("df-456")
        clear_dataflow_id(token)
        assert get_dataflow_id() == ""


class TestThreadIsolation:
    """Each thread must see its own dataflow_id."""

    def test_threads_see_different_values(self):
        results: dict[str, str] = {}
        barrier = threading.Barrier(2)

        def worker(name: str, df_id: str) -> None:
            token = set_dataflow_id(df_id)
            try:
                barrier.wait(timeout=5)
                results[name] = get_dataflow_id()
            finally:
                clear_dataflow_id(token)

        t1 = threading.Thread(target=worker, args=("t1", "df-AAA"))
        t2 = threading.Thread(target=worker, args=("t2", "df-BBB"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert results["t1"] == "df-AAA"
        assert results["t2"] == "df-BBB"

    def test_main_thread_unaffected_by_child(self):
        token = set_dataflow_id("df-main")
        child_value: list[str] = []

        def worker() -> None:
            child_value.append(get_dataflow_id())

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5)

        assert get_dataflow_id() == "df-main"
        # Child thread inherits parent context by default in Python 3.12+
        # but even if it does, it's a copy — mutations won't affect parent.
        assert child_value[0] in ("", "df-main")
        clear_dataflow_id(token)
