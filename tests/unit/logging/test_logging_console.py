"""Dependency-free console color policy and formatter contracts."""

from __future__ import annotations

import io
import logging
import sys

import pytest

from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.presentation.console import ConsoleFormatter, should_colorize
from datacoolie.logging.configuration.constants import ConsoleColor
from datacoolie.logging.presentation.formatting import (
    ContextFormatter,
    DEFAULT_LOG_FORMAT,
    format_context_suffix,
)


def _record(level: int = logging.ERROR) -> logging.LogRecord:
    return logging.LogRecord(
        name="datacoolie.console.test",
        level=level,
        pathname=__file__,
        lineno=10,
        msg="plain message",
        args=(),
        exc_info=None,
    )


def test_console_color_config_is_normalized_and_strict() -> None:
    assert LogConfig(console_color=ConsoleColor.ALWAYS).console_color == "always"
    assert LogConfig(console_color="NEVER").console_color == "never"


def test_auto_color_is_conservative_for_non_tty_and_no_color(monkeypatch) -> None:
    stream = io.StringIO()
    assert should_colorize("auto", stream) is False
    monkeypatch.setenv("NO_COLOR", "")
    assert should_colorize("auto", stream) is False
    assert should_colorize("always", stream) is True
    assert should_colorize("never", stream) is False


def test_formatter_colors_only_a_copy_of_the_level_label() -> None:
    record = _record()
    formatter = ConsoleFormatter(
        "%(levelname)s %(message)s",
        colorize=True,
    )

    rendered = formatter.format(record)

    assert "\x1b[31mERROR\x1b[0m" in rendered
    assert "plain message" in rendered
    assert record.levelname == "ERROR"
    assert not hasattr(record, "exc_text") or record.exc_text is None


def test_formatter_keeps_persisted_style_plain_when_disabled() -> None:
    rendered = ConsoleFormatter("%(levelname)s %(message)s", colorize=False).format(
        _record(logging.WARNING)
    )
    assert rendered == "WARNING plain message"
    assert "\x1b[" not in rendered


@pytest.mark.parametrize(
    ("dataflow_id", "dataflow_run_id", "event_name", "expected"),
    [
        ("orders", "run-1", "dataflow.started", "[orders] [run-1:dataflow.started]"),
        ("orders", "run-1", None, "[orders] [run-1]"),
        ("orders", None, "dataflow.started", "[orders] [dataflow.started]"),
        ("orders", None, None, "[orders]"),
        (None, "run-1", "dataflow.started", "[run-1:dataflow.started]"),
        (None, "run-1", None, "[run-1]"),
        (None, None, "dataflow.started", "[dataflow.started]"),
        (None, None, None, ""),
        (123, {"run": "run-1"}, ["dataflow.started"], ""),
    ],
)
def test_context_suffix_uses_optional_bracket_segments(
    dataflow_id,
    dataflow_run_id,
    event_name,
    expected,
) -> None:
    assert format_context_suffix(dataflow_id, dataflow_run_id, event_name) == expected


def test_default_context_formatter_keeps_message_and_record_unchanged() -> None:
    record = _record(logging.INFO)
    record.dataflow_id = "orders"  # type: ignore[attr-defined]
    record.dataflow_run_id = "run-1"  # type: ignore[attr-defined]
    record.event_name = "dataflow.started"  # type: ignore[attr-defined]

    rendered = ContextFormatter(DEFAULT_LOG_FORMAT).format(record)

    assert "datacoolie.console.test [orders] [run-1:dataflow.started]" in rendered
    assert rendered.endswith(" - plain message")
    assert record.getMessage() == "plain message"


def test_custom_context_placeholders_are_safe_when_event_is_absent() -> None:
    rendered = ContextFormatter(
        "%(event_name)s|%(datacoolie_context)s|%(message)s"
    ).format(_record(logging.INFO))

    assert rendered == "||plain message"


def test_custom_event_placeholder_is_rendered_when_present() -> None:
    record = _record(logging.INFO)
    record.event_name = "dataflow.finished"  # type: ignore[attr-defined]

    rendered = ContextFormatter("%(event_name)s|%(message)s").format(record)

    assert rendered == "dataflow.finished|plain message"


def test_context_control_characters_are_escaped_only_for_presentation() -> None:
    record = _record(logging.INFO)
    record.dataflow_id = "orders\nnext"  # type: ignore[attr-defined]
    record.dataflow_run_id = "run\t1"  # type: ignore[attr-defined]
    record.event_name = "event\x1b[31m"  # type: ignore[attr-defined]

    rendered = ContextFormatter(DEFAULT_LOG_FORMAT).format(record)

    assert "[orders\\nnext] [run\\t1:event\\x1b[31m]" in rendered
    assert "orders\nnext" not in rendered
    assert record.dataflow_id == "orders\nnext"  # type: ignore[attr-defined]


def test_context_formatter_keeps_exception_text_on_the_copy() -> None:
    record = _record(logging.ERROR)
    record.event_name = "dataflow.failed"  # type: ignore[attr-defined]
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record.exc_info = sys.exc_info()

    rendered = ContextFormatter("%(event_name)s %(message)s").format(record)

    assert "dataflow.failed plain message" in rendered
    assert "Traceback" in rendered
    assert not hasattr(record, "exc_text") or record.exc_text is None
