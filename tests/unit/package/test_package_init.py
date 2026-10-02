"""Tests for datacoolie package-level factory functions in __init__.py."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_create_source_calls_registry() -> None:
    """Line 70: create_source delegates to source_registry."""
    import datacoolie
    mock_reader = MagicMock()
    with patch.object(datacoolie.source_registry, 'get', return_value=mock_reader) as mock_get:
        result = datacoolie.create_source('parquet', engine=MagicMock())
    mock_get.assert_called_once()
    assert result is mock_reader


def test_create_destination_calls_registry() -> None:
    """Line 75: create_destination delegates to destination_registry."""
    import datacoolie
    mock_writer = MagicMock()
    with patch.object(datacoolie.destination_registry, 'get', return_value=mock_writer) as mock_get:
        result = datacoolie.create_destination('parquet', engine=MagicMock())
    mock_get.assert_called_once()
    assert result is mock_writer


def test_create_transformer_calls_registry() -> None:
    """Line 80: create_transformer delegates to transformer_registry."""
    import datacoolie
    mock_transformer = MagicMock()
    with patch.object(datacoolie.transformer_registry, 'get', return_value=mock_transformer) as mock_get:
        result = datacoolie.create_transformer('schema_converter')
    mock_get.assert_called_once()
    assert result is mock_transformer
