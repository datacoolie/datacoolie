"""Topic navigation, copyable recipes and canonical transformer ordering."""

from pathlib import Path
import json
import re

from markdown import Markdown

from datacoolie.orchestration.execution.pipeline import DEFAULT_TRANSFORMERS


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"


def test_topic_indexes_link_to_navigable_recipe_owners_with_valid_anchors():
    from docs.scripts.gen_llms import CANONICAL_MARKDOWN, GENERATED_HTML

    config = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    assert "reference/dependencies/index.html" in GENERATED_HTML
    for prefix in ("source", "transform"):
        index = DOCS / f"guide/metadata/{prefix}-patterns.md"
        text = index.read_text(encoding="utf-8")
        targets = re.findall(r"\]\((" + prefix + r"-(?!patterns)[\w-]+\.md)#([\w-]+)\)", text)
        assert len(set(name for name, _ in targets)) == 7
        for name, anchor in targets:
            path = index.parent / name
            assert path.relative_to(DOCS).as_posix() in config
            assert path.relative_to(DOCS).as_posix() in CANONICAL_MARKDOWN
            md = Markdown(extensions=["toc", "tables", "admonition", "pymdownx.superfences", "pymdownx.details"])
            rendered = md.convert(path.read_text(encoding="utf-8"))
            assert f'id="{anchor}"' in rendered, (name, anchor)


def test_moved_recipe_json_examples_remain_parseable():
    for path in (DOCS / "guide/metadata").glob("*.md"):
        if not path.name.startswith(("source-", "transform-")) or path.name.endswith("patterns.md"):
            continue
        content = path.read_text(encoding="utf-8")
        blocks = re.findall(r"^```json\n(.*?)\n```", content, re.M | re.S)
        assert blocks, path.name
        for block in blocks:
            # Authored object-member fragments intentionally omit outer braces.
            candidate = "{" + block + "}" if block.lstrip().startswith('"') else block
            json.loads(candidate)


def test_studio_overview_routes_to_navigable_setup_tour_and_monitoring():
    from docs.scripts.gen_llms import CANONICAL_MARKDOWN

    config = (ROOT / "properdocs.yml").read_text(encoding="utf-8")
    index = DOCS / "studio/index.md"
    text = index.read_text(encoding="utf-8")
    targets = re.findall(r"\]\((setup|tour|monitoring)\.md#([\w-]+)\)", text)
    assert set(name for name, _ in targets) == {"setup", "tour", "monitoring"}
    for name, anchor in targets:
        path = index.parent / f"{name}.md"
        assert path.relative_to(DOCS).as_posix() in config
        assert path.relative_to(DOCS).as_posix() in CANONICAL_MARKDOWN
        md = Markdown(extensions=["toc", "tables", "admonition", "pymdownx.superfences", "pymdownx.details"])
        assert f'id="{anchor}"' in md.convert(path.read_text(encoding="utf-8"))


def test_canonical_order_table_matches_the_selected_default_transformers():
    from importlib import import_module
    import tomllib

    manifest = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    plugins = manifest["project"]["entry-points"]["datacoolie.transformers"]
    concept = (DOCS / "reference/concepts/transformers-and-pipeline.md").read_text(encoding="utf-8")
    for name in DEFAULT_TRANSFORMERS:
        module, class_name = plugins[name].split(":")
        cls = getattr(import_module(module), class_name)
        assert f"| **{cls(engine=object()).order}** | `{class_name}` |" in concept
    for relative in ("guide/metadata/transform-patterns.md", "extensions/writing-a-transformer.md"):
        text = (DOCS / relative).read_text(encoding="utf-8")
        assert "transformers-and-pipeline.md#the-twelve-built-ins-order-responsibility" in text
        assert "| **5** |" not in text and "| 5 |" not in text
