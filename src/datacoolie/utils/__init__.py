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
from datacoolie.utils.collections import (
    chunk_list,
    ensure_list,
    flatten_dict,
    merge_dicts,
)
from datacoolie.utils.identity import generate_unique_id
from datacoolie.utils.time import utc_now
from datacoolie.utils.path_utils import (
    build_path,
    normalize_path,
    normalize_optional_base_path,
)
from datacoolie.utils.component_paths import (
    ComponentPath,
    ComponentPathError,
    normalize_component_paths,
    select_prefixed_root,
)
from datacoolie.utils.chunking import (
    generate_chunk_boundaries,
    normalize_chunk_range,
    validate_chunk_range,
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
    "generate_chunk_boundaries",
    "normalize_chunk_range",
    "validate_chunk_range",
    "generate_unique_id",
    "json_default",
    "merge_dicts",
    "parse_json",
    "to_lower_case",
    "to_snake_case",
    "utc_now",
    "build_path",
    "normalize_path",
    "normalize_optional_base_path",
    "ComponentPath",
    "ComponentPathError",
    "normalize_component_paths",
    "select_prefixed_root",
]
