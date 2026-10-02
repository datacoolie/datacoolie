"""Explicit logging startup and fail-fast configuration contracts."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys

import pytest

from datacoolie.logging.runtime.capture import CaptureHandler
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import LogLevel, PersistenceMode, StorageMode
from datacoolie.logging.runtime.manager import LogManager


@pytest.fixture(autouse=True)
def reset_manager() -> None:
    LogManager.reset()
    yield
    LogManager.reset()


def test_import_and_logger_lookup_do_not_start_logging() -> None:
    code = """
import json
import logging
import threading

root = logging.getLogger("datacoolie")
before = (len(root.handlers), root.level, root.propagate)
import datacoolie.logging as package
from datacoolie.logging.runtime.manager import LogManager, get_logger
logger = get_logger("datacoolie.import_contract")
manager = LogManager.get_instance()
after = (len(root.handlers), root.level, root.propagate)
print(json.dumps({
    "before": before,
    "after": after,
    "configured": manager._configured,
    "capture": manager.capture_handler is not None,
    "logger_level": logger.level,
    "workers": [t.name for t in threading.enumerate() if t.name.endswith("-flush")],
}))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    completed = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=os.fspath(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    observed = json.loads(completed.stdout)

    assert observed["after"] == observed["before"]
    assert observed["configured"] is False
    assert observed["capture"] is False
    assert observed["logger_level"] == logging.NOTSET
    assert observed["workers"] == []


def test_import_and_lookup_preserve_host_logging_configuration() -> None:
    code = """
import json
import logging

root = logging.getLogger("datacoolie")
handler = logging.StreamHandler()
root.addHandler(handler)
root.setLevel(logging.ERROR)
root.propagate = False
import datacoolie.logging as package
from datacoolie.logging.runtime.manager import LogManager, get_logger
get_logger("datacoolie.host_contract")
print(json.dumps({
    "same_handler": root.handlers == [handler],
    "level": root.level,
    "propagate": root.propagate,
    "configured": LogManager.get_instance()._configured,
}))
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    completed = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=os.fspath(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    observed = json.loads(completed.stdout)

    assert observed == {
        "same_handler": True,
        "level": logging.ERROR,
        "propagate": False,
        "configured": False,
    }


def test_public_facade_uses_owner_definitions_without_legacy_exports() -> None:
    import datacoolie.core as core
    import datacoolie.core.constants as core_constants
    import datacoolie.logging as package
    import datacoolie.logging.base as base
    from datacoolie.logging.configuration.config import LogConfig as OwnedLogConfig
    from datacoolie.logging.configuration.constants import LogType as OwnedLogType

    assert package.LogConfig is OwnedLogConfig
    assert package.LogType is OwnedLogType
    assert not hasattr(package, "CaptureHandler")
    assert not hasattr(package, "LogManager")
    assert not hasattr(package, "get_logger")
    assert not hasattr(package, "LogEvent")
    assert not hasattr(package, "DataflowContextFilter")
    assert not hasattr(package, "LogRecord")
    assert not hasattr(base, "CaptureHandler")
    assert not hasattr(base, "LogConfig")
    assert not hasattr(base, "LogManager")
    assert not hasattr(base, "get_logger")
    assert not hasattr(base, "PersistenceMode")
    assert not hasattr(core, "LogType")
    assert not hasattr(core_constants, "DEFAULT_PARTITION_PATTERN")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("log_level", "INF0"),
        ("file_level", "DEBG"),
        ("storage_mode", "disk"),
        ("persistence_mode", "append"),
        ("log_level", None),
        ("storage_mode", True),
    ],
)
def test_log_config_rejects_unknown_or_wrong_typed_choices(field, value) -> None:
    with pytest.raises(ValueError, match=field):
        LogConfig(**{field: value})


def test_log_config_accepts_enum_members_and_normalizes_strings() -> None:
    config = LogConfig(
        log_level=LogLevel.WARNING,
        file_level="error",
        storage_mode=StorageMode.FILE,
        persistence_mode=PersistenceMode.BATCH,
    )

    assert config.log_level == "WARNING"
    assert config.file_level == "ERROR"
    assert config.storage_mode == "file"
    assert config.persistence_mode == "batch"


def test_invalid_reconfigure_preserves_live_manager_state() -> None:
    manager = LogManager.get_instance()
    manager.configure(
        level="INFO",
        file_level="INFO",
        capture_logs=True,
        console_output=False,
    )
    logger = manager.get_logger("datacoolie.validation_contract")
    logger.info("retained")
    handler = manager.capture_handler
    root = logging.getLogger("datacoolie")
    handlers = tuple(root.handlers)
    assert handler is not None

    with pytest.raises(ValueError, match="level"):
        manager.configure(level="INF0", force=True)

    assert manager.capture_handler is handler
    assert tuple(root.handlers) == handlers
    assert manager._level == "INFO"
    assert [record.message for record in handler.get_records()] == ["retained"]


def test_invalid_capture_claim_preserves_owner_and_buffer() -> None:
    manager = LogManager.get_instance()
    owner = object()
    manager.claim_capture(owner)
    handler = manager.capture_handler
    assert handler is not None
    manager.get_logger("datacoolie.claim_contract").info("retained")

    try:
        with pytest.raises(ValueError, match="storage_mode"):
            manager.claim_capture(owner, storage_mode="disk")

        assert manager._capture_owner is owner
        assert manager.capture_handler is handler
        assert [record.message for record in handler.get_records()] == ["retained"]
    finally:
        manager.release_capture(owner)


def test_capture_handler_rejects_invalid_storage_before_mutation() -> None:
    handler = CaptureHandler(storage_mode=StorageMode.MEMORY)
    formatter = logging.Formatter("%(message)s")

    with pytest.raises(ValueError, match="storage_mode"):
        handler.reconfigure(
            level=logging.INFO,
            storage_mode="disk",
            formatter=formatter,
        )

    assert handler._storage_mode == StorageMode.MEMORY.value
    assert handler.level == logging.DEBUG
