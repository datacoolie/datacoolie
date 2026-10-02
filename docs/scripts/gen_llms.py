"""Generate the public ``llms-full.txt`` companion from canonical docs.

The source tree keeps a small placeholder at ``docs/llms-full.txt`` so the
endpoint is discoverable in a checkout.  ProperDocs replaces that placeholder
after rendering.  Source Markdown is used for authored pages and the rendered
HTML is used for generated reference pages, which keeps API directives and
``mkdocstrings`` output in the companion without maintaining a second copy.
"""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
import posixpath
import re
import tomllib
from urllib.parse import urljoin


PUBLIC_ROOT = "https://datacoolie.github.io/datacoolie/"
DOCS_DIR = Path(__file__).resolve().parents[1]

# Keep this list intentionally bounded.  ``llms.txt`` remains the discovery
# index; this companion supplies the high-value contracts an agent commonly
# needs without copying the entire site, navigation, blog, or asset tree.
CANONICAL_MARKDOWN = (
    "index.md",
    "introduction/index.md",
    "introduction/choose-framework.md",
    "introduction/ai-skills.md",
    "guide/index.md",
    "guide/getting-started/index.md",
    "guide/getting-started/installation.md",
    "guide/getting-started/quickstart-polars.md",
    "guide/getting-started/quickstart-spark.md",
    "guide/getting-started/use-your-own-data.md",
    "guide/getting-started/multi-stage-dataflow.md",
    "guide/providers/index.md",
    "guide/providers/file.md",
    "guide/providers/database.md",
    "guide/providers/api.md",
    "guide/operations/index.md",
    "guide/operations/runtime-configuration.md",
    "guide/operations/run-stage.md",
    "guide/operations/replay-and-backfill.md",
    "guide/operations/maintenance.md",
    "guide/operations/logging.md",
    "guide/operations/troubleshooting.md",
    "guide/platforms/index.md",
    "guide/platforms/fabric.md",
    "guide/platforms/databricks.md",
    "guide/platforms/aws-glue.md",
    "guide/cli/index.md",
    "guide/cli/quickstart.md",
    "guide/cli/project.md",
    "guide/cli/commands.md",
    "guide/metadata/index.md",
    "guide/metadata/first-metadata-file.md",
    "guide/metadata/connections.md",
    "guide/metadata/dataflows.md",
    "guide/metadata/source-patterns.md",
    "guide/metadata/transform-patterns.md",
    "guide/metadata/destination-and-load-patterns.md",
    "guide/metadata/data-types.md",
    "guide/metadata/api-advanced.md",
    "guide/metadata/watermark-window-replacement.md",
    "guide/metadata/late-arriving-files.md",
    "guide/metadata/stable-keys-and-protected-output.md",
    "guide/metadata/merge-and-scd2.md",
    "guide/metadata/validation-checklist.md",
    "examples/index.md",
    "examples/runners.md",
    "examples/configuration.md",
    "examples/dataflows.md",
    "examples/operations.md",
    "extensions/index.md",
    "extensions/transformer-tutorial.md",
    "extensions/writing-a-source.md",
    "extensions/writing-a-destination.md",
    "extensions/writing-a-transformer.md",
    "extensions/writing-an-engine.md",
    "extensions/writing-a-platform.md",
    "extensions/writing-a-secret-resolver.md",
    "extensions/writing-a-metadata-provider.md",
    "project/index.md",
    "project/testing.md",
    "reference/index.md",
    "reference/concepts/architecture.md",
    "reference/concepts/metadata-model.md",
    "reference/concepts/orchestration.md",
    "reference/concepts/logging.md",
    "reference/concepts/sources-and-destinations.md",
    "reference/concepts/load-strategies.md",
    "reference/concepts/watermarks.md",
    "reference/concepts/secrets.md",
)

# Generated pages and API pages use rendered HTML so directives become content.
GENERATED_HTML = (
    "reference/metadata-schema/index.html",
    "reference/runtime-configuration/index.html",
    "reference/plugin-entry-points/index.html",
    "reference/environment-variables/index.html",
    "reference/api/core/index.html",
    "reference/api/orchestration/index.html",
    "reference/api/logging/index.html",
    "reference/api/sources/index.html",
    "reference/api/destinations/index.html",
    "reference/api/transformers/index.html",
    "reference/api/metadata/index.html",
    "reference/api/platforms/index.html",
    "reference/api/engines/index.html",
)

_LINK = re.compile(r"(?P<prefix>\]\()(?P<target>[^)]+)(?P<suffix>\))")


def _public_page_url(relative: str) -> str:
    """Return the stable trailing-slash URL for a docs-relative page."""

    path = relative.replace("\\", "/")
    if path == "index.md":
        return PUBLIC_ROOT
    if path.endswith("/index.md"):
        return urljoin(PUBLIC_ROOT, path.removesuffix("index.md"))
    return urljoin(PUBLIC_ROOT, path.removesuffix(".md") + "/")


def _package_version(docs_dir: Path) -> str:
    manifest = docs_dir.parent / "pyproject.toml"
    with manifest.open("rb") as stream:
        version = tomllib.load(stream)["project"].get("version")
    if not isinstance(version, str) or not version:
        raise ValueError(f"Missing project version in {manifest}")
    return version


def _absolute_links(markdown: str, page_source: str) -> str:
    """Make relative Markdown links usable from the standalone text file."""

    # Asset links are relative to the authored Markdown file, not the extra
    # directory introduced by the rendered page's trailing-slash URL.
    source_url = urljoin(PUBLIC_ROOT, page_source)

    def replace(match: re.Match[str]) -> str:
        target = match.group("target").strip()
        if not target or target.startswith(("#", "http://", "https://", "mailto:")):
            return match.group(0)
        suffix = ""
        if "#" in target:
            target, suffix = target.split("#", 1)
            suffix = f"#{suffix}"
        if target.endswith(".md"):
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(page_source), target)
            )
            target = _public_page_url(resolved)
        else:
            target = urljoin(source_url, target)
        return f"]({target}{suffix})"

    return _LINK.sub(replace, markdown)


def _strip_front_matter(markdown: str) -> str:
    """Remove YAML front matter that is meaningful to MkDocs, not readers."""

    if not markdown.startswith("---"):
        return markdown
    end = markdown.find("\n---", 3)
    if end < 0:
        return markdown
    return markdown[end + len("\n---") :].lstrip("\r\n")


def _clean_source(markdown: str) -> str:
    """Keep authored prose/code while dropping front matter and decorative HTML."""

    value = _strip_front_matter(markdown)
    value = re.sub(
        r"<p\s+align=\"center\">.*?</p>\s*",
        "",
        value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    value = re.sub(r"</?(?:picture|source|img)[^>]*>", "", value, flags=re.IGNORECASE)
    return value.strip()


class _ArticleText(HTMLParser):
    """Extract article text while ignoring site chrome and script/style nodes."""

    _BLOCKS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "pre", "blockquote", "tr"}
    _SKIP = {"script", "style", "nav", "header", "footer", "aside"}

    def __init__(self, *, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.parts: list[str] = []
        self._skip_depth = 0
        self._main_depth = 0
        self._main_seen = False
        self._block: str | None = None
        self._buffer: list[str] = []
        self._links: list[tuple[str, list[str]]] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        if tag in self._SKIP:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "main":
            self._main_seen = True
            self._main_depth += 1
        if tag in self._BLOCKS:
            self._block = tag
            self._buffer = []
        if tag == "a":
            href = dict(attrs).get("href", "")
            self._links.append((href, []))

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag == "a" and self._links:
            href, text = self._links.pop()
            if href and text and self._block is not None:
                target = urljoin(self.base_url, href)
                self._buffer.append(f" ({target})")
        if tag == "main" and self._main_depth:
            self._main_depth -= 1
        if tag == self._block:
            value = "".join(self._buffer).strip()
            if value:
                if tag.startswith("h") and len(tag) == 2:
                    self.parts.append(f"{'#' * int(tag[1])} {value}")
                elif tag == "pre":
                    self.parts.append(f"```\n{value}\n```")
                else:
                    self.parts.append(value)
            self._block = None
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth or (self._main_seen and not self._main_depth):
            return
        if self._block is not None:
            self._buffer.append(data)
        for _, text in self._links:
            text.append(data)

    def text(self) -> str:
        return "\n\n".join(self.parts).strip()


def _rendered_article(path: Path, *, page_url: str) -> str:
    parser = _ArticleText(base_url=page_url)
    parser.feed(path.read_text(encoding="utf-8"))
    return parser.text()


def build_llms_full(*, docs_dir: Path = DOCS_DIR, site_dir: Path | None = None) -> str:
    """Build deterministic full-text content from the selected canonical pages."""

    version = _package_version(docs_dir)
    sections: list[str] = [
        "# DataCoolie — Full Content for AI Systems",
        "",
        "> Generated from selected canonical public documentation pages. For routing, see "
        f"[llms.txt]({PUBLIC_ROOT}llms.txt).",
    ]
    for relative in CANONICAL_MARKDOWN:
        path = docs_dir / relative
        if not path.is_file():
            raise FileNotFoundError(f"Selected canonical docs page is missing: {relative}")
        page_url = _public_page_url(relative)
        content = _clean_source(path.read_text(encoding="utf-8").strip()).replace(
            "{{ datacoolie_version }}", version
        )
        content = _absolute_links(content, relative)
        sections.extend(["", f"## {page_url}", "", content])

    if site_dir is not None:
        for relative in GENERATED_HTML:
            path = site_dir / relative
            if not path.is_file():
                raise FileNotFoundError(f"Selected generated docs page is missing: {relative}")
            page_url = urljoin(PUBLIC_ROOT, relative.removesuffix("index.html"))
            content = _rendered_article(path, page_url=page_url)
            if not content:
                raise ValueError(f"Selected generated docs page has no article text: {relative}")
            sections.extend(["", f"## {page_url}", "", content])
    return "\n".join(sections).rstrip() + "\n"


def on_post_build(config) -> None:  # noqa: ANN001
    """Replace the copied placeholder after generated pages are available."""

    site_dir = Path(config["site_dir"])
    output = build_llms_full(site_dir=site_dir)
    (site_dir / "llms-full.txt").write_text(output, encoding="utf-8")
