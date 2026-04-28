"""DataCoolie shared utilities."""

from datacoolie.utils.converters import (
    as_json,
    convert_string_to_list,
    convert_to_bool,
    convert_to_int,
    custom_json_encoder,
    json_default,
    parse_json,
    to_lower_case,
    to_snake_case,
)
from datacoolie.utils.helpers import (
    chunk_list,
    ensure_list,
    flatten_dict,
    generate_unique_id,
    merge_dicts,
    utc_now,
)
from datacoolie.utils.path_utils import (
    build_path,
    normalize_path,
)

__all__ = [
    "as_json",
    "chunk_list",
    "convert_string_to_list",
    "convert_to_bool",
    "convert_to_int",
    "custom_json_encoder",
    "ensure_list",
    "flatten_dict",
    "generate_unique_id",
    "json_default",
    "merge_dicts",
    "parse_json",
    "to_lower_case",
    "to_snake_case",
    "utc_now",
    "build_path",
    "normalize_path",
]
