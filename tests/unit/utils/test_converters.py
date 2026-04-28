"""Tests for datacoolie.utils.converters."""

from __future__ import annotations

import pytest

from datacoolie.utils.converters import convert_to_bool, convert_to_int, parse_json


class TestParseJson:
    def test_valid_dict(self) -> None:
        assert parse_json('{"a": 1}') == {"a": 1}

    def test_list_raises_by_default(self) -> None:
        with pytest.raises(ValueError, match="Expected JSON object"):
            parse_json("[1,2,3]")

    def test_list_returns_empty_when_not_raising(self) -> None:
        assert parse_json("[1,2,3]", raise_on_error=False) == {}

    def test_already_dict(self) -> None:
        d = {"key": "val"}
        assert parse_json(d) is d

    def test_none_returns_empty_dict(self) -> None:
        assert parse_json(None) == {}

    def test_invalid_json_raises_by_default(self) -> None:
        with pytest.raises(ValueError, match="Invalid JSON"):
            parse_json("{bad}")

    def test_invalid_json_returns_empty_when_not_raising(self) -> None:
        assert parse_json("{bad}", raise_on_error=False) == {}

    def test_non_json_string_raises_by_default(self) -> None:
        with pytest.raises(ValueError):
            parse_json("hello world")

    def test_empty_string_returns_empty_dict(self) -> None:
        assert parse_json("") == {}

    def test_whitespace_string_returns_empty(self) -> None:
        assert parse_json("   ") == {}

    def test_non_dict_json_string_raises(self) -> None:
        """JSON that parses to non-dict like 123 or 'hello'."""
        with pytest.raises(ValueError, match="Expected JSON object"):
            parse_json("123")

    def test_non_dict_json_returns_empty_when_not_raising(self) -> None:
        assert parse_json("123", raise_on_error=False) == {}

    def test_dict_non_string_non_dict_raises(self) -> None:
        """Non-string, non-dict arguments that are not None."""
        with pytest.raises(ValueError):
            parse_json(123)

    def test_dict_non_string_non_dict_returns_empty_when_not_raising(self) -> None:
        assert parse_json(123, raise_on_error=False) == {}


class TestConvertToBool:
    @pytest.mark.parametrize("val", ["true", "True", "TRUE", "yes", "1", "on", "t", "y"])
    def test_truthy_strings(self, val: str) -> None:
        assert convert_to_bool(val) is True

    @pytest.mark.parametrize("val", ["false", "False", "no", "0", "off", "f", "n", ""])
    def test_falsy_strings(self, val: str) -> None:
        assert convert_to_bool(val) is False

    def test_bool_passthrough(self) -> None:
        assert convert_to_bool(True) is True
        assert convert_to_bool(False) is False

    def test_none_returns_false(self) -> None:
        assert convert_to_bool(None) is False

    def test_unknown_value_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean value"):
            convert_to_bool("maybe")

    def test_int_truthy(self) -> None:
        assert convert_to_bool(1) is True

    def test_int_falsy(self) -> None:
        assert convert_to_bool(0) is False

    def test_invalid_int_raises(self) -> None:
        """Only 0 and 1 are valid integers."""
        with pytest.raises(ValueError, match="Invalid boolean integer"):
            convert_to_bool(2)

    def test_whitespace_string_variations(self) -> None:
        """Strings with leading/trailing whitespace."""
        assert convert_to_bool("  true  ") is True
        assert convert_to_bool("\n0\t") is False

    def test_non_string_non_numeric_raises(self) -> None:
        """Lists and dicts raise."""
        with pytest.raises(ValueError, match="Invalid boolean value"):
            convert_to_bool([True])


class TestConvertToInt:
    def test_int_passthrough(self) -> None:
        assert convert_to_int(42) == 42

    def test_string_int(self) -> None:
        assert convert_to_int("10") == 10

    def test_non_whole_float_raises(self) -> None:
        with pytest.raises(ValueError, match="non-whole float"):
            convert_to_int(3.9)

    def test_none_returns_none(self) -> None:
        assert convert_to_int(None) is None

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid integer string"):
            convert_to_int("abc")

    def test_negative(self) -> None:
        assert convert_to_int("-5") == -5

    def test_float_string(self) -> None:
        assert convert_to_int("3.9") == 3

    def test_bool_raises(self) -> None:
        """bool is a subclass of int but should raise."""
        with pytest.raises(ValueError):
            convert_to_int(True)

    def test_whole_float_converts(self) -> None:
        """Whole floats >= 2 should convert without error."""
        assert convert_to_int(2.0) == 2
        assert convert_to_int(10.0) == 10
        assert convert_to_int(-3.0) == -3

    def test_non_numeric_type_raises(self) -> None:
        """Non-string, non-numeric types raise."""
        with pytest.raises(ValueError, match="Cannot convert to integer"):
            convert_to_int([1, 2, 3])

    def test_zero_float(self) -> None:
        """0.0 float should convert to 0."""
        assert convert_to_int(0.0) == 0

    def test_one_float(self) -> None:
        """1.0 float should convert to 1."""
        assert convert_to_int(1.0) == 1

    def test_whitespace_string_returns_none(self) -> None:
        """Whitespace-only strings should return None."""
        assert convert_to_int("  ") is None
        assert convert_to_int("\t\n") is None
