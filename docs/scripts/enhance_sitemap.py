"""ProperDocs hook: keep sitemap metadata conservative and trustworthy.

Material for MkDocs assigns the build date to every ``lastmod`` entry. A docs
deployment therefore makes unchanged pages look newly updated. Google ignores
``priority`` and ``changefreq``, so this hook removes all three optional fields
and leaves canonical URL discovery as the sitemap's single responsibility.
"""

from __future__ import annotations

from pathlib import Path
import re

OPTIONAL_TAG_PATTERN = re.compile(
    r"\s*<(?:lastmod|changefreq|priority)>.*?</(?:lastmod|changefreq|priority)>",
    re.DOTALL,
)


def on_post_build(config) -> None:  # noqa: ANN001
    """Remove optional sitemap fields that this build cannot keep accurate."""
    site_dir = Path(config["site_dir"])
    sitemap_path = site_dir / "sitemap.xml"

    if not sitemap_path.exists():
        return

    content = sitemap_path.read_text(encoding="utf-8")
    normalized = OPTIONAL_TAG_PATTERN.sub("", content)
    sitemap_path.write_text(normalized, encoding="utf-8")
