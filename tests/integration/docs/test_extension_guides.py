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
