"""Tests for datacoolie.logging.runtime.context — ContextVar helpers."""

from __future__ import annotations

import logging
import threading

import pytest

from datacoolie.logging.runtime.context import (
    DataflowContextFilter,
    attempt_context,
    clear_dataflow_context,
    clear_dataflow_id,
    clear_dataflow_run_id,
    dataflow_context,
    get_attempt_number,
    get_dataflow_id,
    get_dataflow_run_id,
    set_dataflow_context,
    set_dataflow_id,
    set_dataflow_run_id,
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

    def test_execution_context_restores_both_identifiers(self):
        outer = set_dataflow_context("df-outer", "run-outer")
        inner = set_dataflow_context("df-inner", "run-inner")
        assert get_dataflow_id() == "df-inner"
        assert get_dataflow_run_id() == "run-inner"
        clear_dataflow_context(inner)
        assert get_dataflow_id() == "df-outer"
        assert get_dataflow_run_id() == "run-outer"
        clear_dataflow_context(outer)
        assert get_dataflow_id() == ""
        assert get_dataflow_run_id() == ""

    def test_context_manager_restores_parent_on_error(self):
        outer = set_dataflow_context("df-parent", "run-parent")
        try:
            with dataflow_context("df-child", "run-child"):
                assert get_dataflow_id() == "df-child"
                assert get_dataflow_run_id() == "run-child"
                raise RuntimeError("expected")
        except RuntimeError:
            pass
        assert get_dataflow_id() == "df-parent"
        assert get_dataflow_run_id() == "run-parent"
        clear_dataflow_context(outer)

    def test_run_id_can_be_bound_independently(self):
        token = set_dataflow_run_id("run-only")
        try:
            assert get_dataflow_run_id() == "run-only"
            assert get_dataflow_id() == ""
        finally:
            clear_dataflow_run_id(token)

    def test_attempt_context_restores_nested_values_and_errors(self):
        assert get_attempt_number() is None

        with attempt_context(1):
            assert get_attempt_number() == 1
            with attempt_context(2):
                assert get_attempt_number() == 2
            assert get_attempt_number() == 1

            with pytest.raises(RuntimeError, match="expected"):
                with attempt_context(3):
                    assert get_attempt_number() == 3
                    raise RuntimeError("expected")
            assert get_attempt_number() == 1

        assert get_attempt_number() is None

    @pytest.mark.parametrize("number", [0, -1, True, "1"])
    def test_attempt_context_rejects_invalid_numbers(self, number):
        with pytest.raises(ValueError, match="positive integer"):
            with attempt_context(number):
                pass

    def test_context_filter_attaches_active_attempt_number(self):
        context_filter = DataflowContextFilter()
        with attempt_context(4):
            record = logging.LogRecord(
                "datacoolie.test",
                logging.INFO,
                __file__,
                1,
                "message",
                (),
                None,
            )
            assert context_filter.filter(record)
            assert record.attempt_number == 4

        record = logging.LogRecord(
            "datacoolie.test",
            logging.INFO,
            __file__,
            1,
            "message",
            (),
            None,
        )
        assert context_filter.filter(record)
        assert record.attempt_number is None


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
