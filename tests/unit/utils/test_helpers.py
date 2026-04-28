"""Tests for datacoolie.utils.helpers and datacoolie.utils.converters."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from datacoolie.utils.converters import (
    as_json,
    convert_string_to_list,
    custom_json_encoder,
    json_default,
    to_lower_case,
    to_snake_case,
)
from datacoolie.utils.helpers import (
    chunk_list,
    ensure_list,
    flatten_dict,
    generate_unique_id,
    merge_dicts,
    name_to_uuid,
    utc_now,
)


class TestNameToUuid:
    def test_deterministic(self) -> None:
        uuid1 = name_to_uuid("test_name")
        uuid2 = name_to_uuid("test_name")
        assert uuid1 == uuid2

    def test_different_names_different_uuids(self) -> None:
        uuid1 = name_to_uuid("name1")
        uuid2 = name_to_uuid("name2")
        assert uuid1 != uuid2


class TestJsonDefault:
    def test_datetime(self) -> None:
        dt = datetime(2026, 2, 12, 10, 0, 0, tzinfo=timezone.utc)
        assert json_default(dt) == "2026-02-12T10:00:00+00:00"

    def test_non_datetime_converts_to_string(self) -> None:
        assert json_default(42) == "42"
        assert json_default([1, 2, 3]) == "[1, 2, 3]"


class TestAsJson:
    def test_truthy_dict(self) -> None:
        result = as_json({"a": 1})
        assert result is not None
        import json
        assert json.loads(result) == {"a": 1}

    def test_empty_dict_returns_none(self) -> None:
        assert as_json({}) is None

    def test_none_returns_none(self) -> None:
        assert as_json(None) is None

    def test_falsy_value_returns_none(self) -> None:
        assert as_json(0) is None
        assert as_json("") is None
        assert as_json([]) is None

    def test_datetime_serialized(self) -> None:
        dt = datetime(2026, 2, 12, tzinfo=timezone.utc)
        result = as_json({"ts": dt})
        assert result is not None
        import json
        parsed = json.loads(result)
        assert "2026" in parsed["ts"]



    def test_returns_string(self) -> None:
        uid = generate_unique_id()
        assert isinstance(uid, str)
        assert len(uid) == 36  # standard UUID v4

    def test_with_prefix(self) -> None:
        uid = generate_unique_id(prefix="df")
        assert uid.startswith("df_")

    def test_unique(self) -> None:
        ids = {generate_unique_id() for _ in range(100)}
        assert len(ids) == 100


class TestUtcNow:
    def test_returns_utc_datetime(self) -> None:
        now = utc_now()
        assert isinstance(now, datetime)
        assert now.tzinfo == timezone.utc


class TestCustomJsonEncoder:
    def test_datetime(self) -> None:
        dt = datetime(2026, 2, 12, 10, 0, 0, tzinfo=timezone.utc)
        assert custom_json_encoder(dt) == "2026-02-12T10:00:00+00:00"

    def test_date(self) -> None:
        from datetime import date
        d = date(2026, 2, 12)
        assert custom_json_encoder(d) == "2026-02-12"

    def test_time(self) -> None:
        from datetime import time
        t = time(10, 0, 0)
        assert custom_json_encoder(t) == "10:00:00"

    def test_set_raises(self) -> None:
        with pytest.raises(TypeError):
            custom_json_encoder({1, 2, 3})

    def test_non_datetime_raises(self) -> None:
        with pytest.raises(TypeError):
            custom_json_encoder(42)


class TestEnsureList:
    def test_string(self) -> None:
        assert ensure_list("hello") == ["hello"]

    def test_list(self) -> None:
        assert ensure_list([1, 2]) == [1, 2]

    def test_none(self) -> None:
        assert ensure_list(None) == []

    def test_tuple_wrapped(self) -> None:
        assert ensure_list((1, 2)) == [(1, 2)]

    def test_single_int(self) -> None:
        assert ensure_list(5) == [5]

    def test_comma_separated_string(self) -> None:
        assert ensure_list("a,b,c") == ["a", "b", "c"]

    def test_comma_separated_with_whitespace(self) -> None:
        assert ensure_list("a , b , c") == ["a", "b", "c"]

    def test_whitespace_only_string(self) -> None:
        assert ensure_list("   ") == []

    def test_json_array_string(self) -> None:
        assert ensure_list('["x", "y", "z"]') == ["x", "y", "z"]

    def test_json_array_with_numbers(self) -> None:
        result = ensure_list("[1, 2, 3]")
        assert result == [1, 2, 3]

    def test_malformed_json_array_treated_as_csv(self) -> None:
        result = ensure_list("[1, 2")
        assert len(result) > 0  # Should treat as CSV fallthrough

    def test_empty_string(self) -> None:
        assert ensure_list("") == []

    def test_json_object_not_array(self) -> None:
        # JSON parsing succeeds but returns an object, falls back to CSV
        result = ensure_list('{"a": 1}')
        assert isinstance(result, list)
        # Falls through to CSV parsing since it's not a list

    def test_json_array_parse_returns_non_list(self) -> None:
        """Force test for theoretical case where json.loads returns non-list."""
        import json
        from unittest.mock import patch
        
        with patch('json.loads') as mock_loads:
            # Make json.loads return a non-list for the array string
            mock_loads.return_value = {"not": "list"}
            result = ensure_list("[1, 2, 3]")
            # Should fall back to CSV parsing
            assert isinstance(result, list)


class TestConvertStringToList:
    def test_csv(self) -> None:
        assert convert_string_to_list("a,b,c") == ["a", "b", "c"]

    def test_custom_sep(self) -> None:
        assert convert_string_to_list("a|b", separator="|") == ["a", "b"]

    def test_strips_whitespace(self) -> None:
        assert convert_string_to_list("a , b , c") == ["a", "b", "c"]

    def test_already_list(self) -> None:
        assert convert_string_to_list(["x", "y"]) == ["x", "y"]

    def test_none(self) -> None:
        assert convert_string_to_list(None) == []

    def test_empty_string(self) -> None:
        assert convert_string_to_list("") == []

    def test_non_string_non_list_returns_empty(self) -> None:
        assert convert_string_to_list(123) == []

    def test_whitespace_only_string(self) -> None:
        assert convert_string_to_list("   ") == []

    def test_custom_separator_none_value(self) -> None:
        assert convert_string_to_list(None, separator=";") == []


class TestChunkList:
    def test_even_split(self) -> None:
        chunks = chunk_list([1, 2, 3, 4], 2)
        assert chunks == [[1, 2], [3, 4]]

    def test_uneven_split(self) -> None:
        chunks = chunk_list([1, 2, 3, 4, 5], 2)
        assert chunks == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_list(self) -> None:
        chunks = chunk_list([1, 2], 10)
        assert chunks == [[1, 2]]

    def test_empty(self) -> None:
        assert chunk_list([], 5) == []

    def test_chunk_size_one(self) -> None:
        chunks = chunk_list([1, 2, 3], 1)
        assert chunks == [[1], [2], [3]]

    def test_zero_chunk_size_raises(self) -> None:
        with pytest.raises(ValueError, match="Chunk size must be positive"):
            chunk_list([1, 2], 0)

    def test_negative_chunk_size_raises(self) -> None:
        with pytest.raises(ValueError, match="Chunk size must be positive"):
            chunk_list([1, 2], -1)


class TestMergeDicts:
    def test_shallow(self) -> None:
        result = merge_dicts({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_override(self) -> None:
        result = merge_dicts({"a": 1}, {"a": 2})
        assert result == {"a": 2}

    def test_deep(self) -> None:
        base = {"nested": {"x": 1, "y": 2}}
        override = {"nested": {"y": 3, "z": 4}}
        result = merge_dicts(base, override, deep=True)
        assert result == {"nested": {"x": 1, "y": 3, "z": 4}}

    def test_empty(self) -> None:
        assert merge_dicts() == {}

    def test_single(self) -> None:
        assert merge_dicts({"a": 1}) == {"a": 1}

    def test_multiple_none_entries(self) -> None:
        result = merge_dicts(None, {"a": 1}, None, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_all_none_entries(self) -> None:
        result = merge_dicts(None, None)
        assert result == {}

    def test_shallow_override_nested(self) -> None:
        """When deep=False, nested dicts are replaced, not merged."""
        base = {"nested": {"x": 1}}
        override = {"nested": {"y": 2}}
        result = merge_dicts(base, override, deep=False)
        assert result == {"nested": {"y": 2}}

    def test_deep_with_non_dict_values(self) -> None:
        """When deep=True but value is not a dict, replace it."""
        base = {"key": {"x": 1}}
        override = {"key": "string"}
        result = merge_dicts(base, override, deep=True)
        assert result == {"key": "string"}


class TestFlattenDict:
    def test_simple(self) -> None:
        result = flatten_dict({"a": {"b": 1, "c": 2}})
        assert result == {"a.b": 1, "a.c": 2}

    def test_deeply_nested(self) -> None:
        result = flatten_dict({"a": {"b": {"c": 1}}})
        assert result == {"a.b.c": 1}

    def test_custom_sep(self) -> None:
        result = flatten_dict({"a": {"b": 1}}, sep="/")
        assert result == {"a/b": 1}

    def test_empty(self) -> None:
        assert flatten_dict({}) == {}

    def test_no_nesting(self) -> None:
        d = {"x": 1, "y": 2}
        assert flatten_dict(d) == d

    def test_mixed_nesting_and_scalar(self) -> None:
        result = flatten_dict({"a": 1, "b": {"c": 2, "d": 3}})
        assert result == {"a": 1, "b.c": 2, "b.d": 3}

    def test_with_parent_key(self) -> None:
        result = flatten_dict({"x": 1}, parent_key="prefix")
        assert result == {"prefix.x": 1}


class TestToSnakeCase:
    def test_camel_case(self) -> None:
        assert to_snake_case("MyColumn") == "my_column"

    def test_with_dashes(self) -> None:
        assert to_snake_case("order-date") == "order_date"

    def test_with_spaces(self) -> None:
        assert to_snake_case("Column Name") == "column_name"

    def test_http_status(self) -> None:
        assert to_snake_case("HTTPStatus") == "http_status"

    def test_leading_digit(self) -> None:
        assert to_snake_case("123") == "_123"

    def test_special_chars(self) -> None:
        assert to_snake_case("col@#name") == "col_name"

    def test_leading_underscore_unchanged(self) -> None:
        assert to_snake_case("__created_at") == "__created_at"

    def test_already_snake_case(self) -> None:
        assert to_snake_case("my_column") == "my_column"

    def test_multiple_consecutive_caps(self) -> None:
        assert to_snake_case("XMLParser") == "xml_parser"

    def test_single_char(self) -> None:
        assert to_snake_case("a") == "a"

    def test_single_underscore_col(self) -> None:
        assert to_snake_case("_col") == "_col"


class TestToLowerCase:
    def test_camel_case(self) -> None:
        assert to_lower_case("MyColumn") == "mycolumn"

    def test_with_dashes(self) -> None:
        assert to_lower_case("order-date") == "order_date"

    def test_with_spaces(self) -> None:
        assert to_lower_case("Column Name") == "column_name"

    def test_http_status(self) -> None:
        assert to_lower_case("HTTPStatus") == "httpstatus"

    def test_leading_digit(self) -> None:
        assert to_lower_case("123") == "_123"

    def test_special_chars(self) -> None:
        assert to_lower_case("col@#name") == "col_name"

    def test_leading_underscore_unchanged(self) -> None:
        assert to_lower_case("__created_at") == "__created_at"

    def test_already_lower(self) -> None:
        assert to_lower_case("my_column") == "my_column"

    def test_allcaps(self) -> None:
        assert to_lower_case("ALLCAPS") == "allcaps"

    def test_single_char(self) -> None:
        assert to_lower_case("a") == "a"

    def test_single_underscore_col(self) -> None:
        assert to_lower_case("_col") == "_col"

    def test_no_word_boundary_insertion(self) -> None:
        """Unlike to_snake_case, to_lower_case does NOT split camelCase."""
        assert to_lower_case("getHTTPSResponse") == "gethttpsresponse"
        assert to_lower_case("XMLParser") == "xmlparser"
