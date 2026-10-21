"""Exercise authored extension snippets at their actual caller boundaries."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
import re

import pytest

from datacoolie.core.exceptions import DestinationError
from datacoolie.core.registry import PluginRegistry
from datacoolie.core.secrets.resolver import BaseSecretResolver
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.engines.base import BaseEngine
from datacoolie.metadata.base import BaseMetadataProvider
from datacoolie.platforms.base import BasePlatform
from datacoolie.sources.base import BaseSourceReader
from datacoolie.transformers.base import BaseTransformer


GUIDES = Path(__file__).resolve().parents[3] / "docs" / "extensions"


def _first_python(page: str) -> str:
    text = (GUIDES / page).read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)\n```", text, flags=re.DOTALL)[0]


@pytest.mark.integration
def test_authored_transformer_constructs_with_driver_engine_keyword() -> None:
    namespace = {}
    exec(_first_python("writing-a-transformer.md"), namespace)
    registry = PluginRegistry("docs.transformers", BaseTransformer)
    registry.register("guide_pii", namespace["PiiMaskerTransformer"])
    engine = object()
    plugin = registry.get("guide_pii", engine=engine)
    assert plugin._engine is engine


@pytest.mark.integration
@pytest.mark.parametrize(
    "page,base,class_name",
    [
        ("writing-an-engine.md", BaseEngine, "MyLibEngine"),
        ("writing-a-source.md", BaseSourceReader, "MyFormatReader"),
        ("writing-a-destination.md", BaseDestinationWriter, "MyDestinationWriter"),
        ("writing-a-platform.md", BasePlatform, "MyPlatform"),
        ("writing-a-metadata-provider.md", BaseMetadataProvider, "MyProvider"),
        ("writing-a-secret-resolver.md", BaseSecretResolver, "MyResolver"),
    ],
)
def test_authored_methods_accept_base_contract_call_shapes(page, base, class_name) -> None:
    """Bind all contract keywords without needing the illustrative mylib backend."""
    tree = ast.parse(_first_python(page))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    for method in cls.body:
        if not isinstance(method, ast.FunctionDef) or method.name == "__init__":
            continue
        real_signature = inspect.signature(getattr(base, method.name))
        # Keep only the authored signature; the backend body is intentionally partial.
        method.body = [ast.Pass()]
        method.decorator_list = []
        module = ast.Module(
            body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), method],
            type_ignores=[],
        )
        namespace = {}
        exec(compile(ast.fix_missing_locations(module), page, "exec"), namespace)
        positional = []
        keywords = {}
        for parameter in real_signature.parameters.values():
            if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
                positional.append(object())
            elif parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                keywords[parameter.name] = object()
        inspect.signature(namespace[method.name]).bind(*positional, **keywords)


@pytest.mark.integration
def test_authored_destination_rejects_window_before_backend_access() -> None:
    namespace = {}
    exec(_first_python("writing-a-destination.md"), namespace)
    writer = object.__new__(namespace["MyDestinationWriter"])
    # No engine or DataFlow: accessing either before rejecting the window fails.
    with pytest.raises(DestinationError, match="bounded replacement"):
        writer._write_internal(None, None, watermark_window=object())


@pytest.mark.integration
def test_authored_platform_constructor_initializes_inherited_secret_cache() -> None:
    namespace = {}
    exec(_first_python("writing-a-platform.md"), namespace)
    calls = []

    class FixturePlatform(namespace["MyPlatform"]):
        def _fetch_secret(self, key, source):
            calls.append((key, source))
            return "synthetic-value"

    platform = FixturePlatform(endpoint="https://storage.example")
    assert platform.get_secret("fixture", "scope-a") == "synthetic-value"
    assert platform.get_secret("fixture", "scope-a") == "synthetic-value"
    assert platform.get_secret("fixture", "scope-b") == "synthetic-value"
    assert calls == [("fixture", "scope-a"), ("fixture", "scope-b")]


@pytest.mark.integration
def test_authored_destination_forwards_merge_identity_partition_and_options():
    from unittest.mock import Mock
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source

    namespace = {}
    exec(_first_python("writing-a-destination.md"), namespace)
    writer = object.__new__(namespace["MyDestinationWriter"])
    writer._engine = Mock()
    connection = Connection(name="fixture", format="delta", configure={"base_path": "output"})
    destination = Destination(
        connection=connection, table="orders", load_type="merge_upsert",
        merge_keys=["id"], partition_columns=["region"],
        configure={"merge_options": {"source_alias": "incoming", "target_alias": "existing"}},
    )
    dataflow = DataFlow(name="guide", source=Source(connection=connection, table="input"), destination=destination)
    frame = object()
    writer._write_internal(frame, dataflow)
    writer._engine.merge_to_path.assert_called_once_with(
        frame, destination.path, merge_keys=["id", "region"], fmt="myfmt",
        partition_columns=["region"], options={"source_alias": "incoming", "target_alias": "existing"},
    )
    writer._engine.write_to_path.assert_not_called()


@pytest.mark.integration
def test_authored_rename_drop_lineage_maps_window_across_repeated_pipeline_calls():
    import polars as pl
    from datacoolie.core.exceptions import TransformError
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.engines.contracts.windows import WindowSpec
    from datacoolie.orchestration.execution.window import map_watermark_window
    from datacoolie.transformers.base import TransformerPipeline

    text = (GUIDES / "writing-a-transformer.md").read_text(encoding="utf-8")
    section = text.split("## Report renamed and removed columns", 1)[1]
    script = re.search(r"```python\n(.*?)\n```", section, re.S)[1]
    namespace = {}
    exec(script, namespace)
    engine = PolarsEngine()
    pipeline = TransformerPipeline(engine)
    pipeline.add_transformer(namespace["RenameAndDropTransformer"](engine=engine))
    for value in (2, 3):
        frame = pl.DataFrame({"UpdatedAt": [value], "raw_payload": ["fixture"], "id": [1]}).lazy()
        output = pipeline.transform(frame, None)
        assert output.collect().to_dicts() == [{"updated_at": value, "id": 1}]
        mapping = pipeline.get_column_mapping()
        assert mapping.known and mapping.mapping == {"UpdatedAt": "updated_at", "raw_payload": None, "id": "id"}
        mapped = map_watermark_window(
            WindowSpec(bounds={"UpdatedAt": (value, value + 1)}),
            column_mapping=mapping, output_columns=pipeline.get_output_columns(),
        )
        assert mapped.bounds == {"updated_at": (value, value + 1)}
        with pytest.raises(TransformError, match="removed or is unmapped"):
            map_watermark_window(
                WindowSpec(bounds={"raw_payload": (1, 2)}), column_mapping=mapping,
                output_columns=pipeline.get_output_columns(),
            )
