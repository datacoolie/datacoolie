"""ProperDocs hook: enhance sitemap.xml with changefreq and priority.

Material for MkDocs generates a basic sitemap with only <loc> and <lastmod>.
This hook post-processes the generated sitemap.xml to add <changefreq>
and <priority> based on URL patterns, improving crawl efficiency signals.
"""

from __future__ import annotations

import re
from pathlib import Path


# Priority tiers based on URL path
PRIORITY_MAP = [
    (r"^/$", "1.0", "weekly"),                        # Homepage
    (r"^/getting-started/", "0.9", "monthly"),        # Getting started
    (r"^/concepts/", "0.8", "monthly"),               # Concepts
    (r"^/how-to/", "0.8", "monthly"),                 # How-to guides
    (r"^/blog/\d{4}/", "0.7", "yearly"),              # Blog posts
    (r"^/blog/$", "0.6", "weekly"),                    # Blog index
    (r"^/reference/", "0.6", "monthly"),              # Reference
    (r"^/extending/", "0.6", "monthly"),              # Extending
    (r"^/operations/", "0.5", "monthly"),             # Operations
    (r"^/adr/", "0.3", "yearly"),                     # ADRs
    (r"^/contributing/", "0.3", "yearly"),            # Contributing
]

DEFAULT_PRIORITY = "0.5"
DEFAULT_CHANGEFREQ = "monthly"


def on_post_build(config) -> None:  # noqa: ANN001
    """Post-process sitemap.xml to add changefreq and priority."""
    site_dir = Path(config["site_dir"])
    sitemap_path = site_dir / "sitemap.xml"

    if not sitemap_path.exists():
        return

    site_url = config.get("site_url", "").rstrip("/")
    content = sitemap_path.read_text(encoding="utf-8")

    def add_sitemap_metadata(match: re.Match) -> str:
        loc = match.group(1)
        lastmod_line = match.group(2) or ""

        # Extract path from full URL
        path = loc
        if site_url and loc.startswith(site_url):
            path = loc[len(site_url):]
        if not path:
            path = "/"

        priority = DEFAULT_PRIORITY
        changefreq = DEFAULT_CHANGEFREQ
        for pattern, p, cf in PRIORITY_MAP:
            if re.match(pattern, path):
                priority = p
                changefreq = cf
                break

        lines = [f"         <loc>{loc}</loc>"]
        if lastmod_line.strip():
            lines.append(f"         {lastmod_line.strip()}")
        lines.append(f"         <changefreq>{changefreq}</changefreq>")
        lines.append(f"         <priority>{priority}</priority>")

        return "    <url>\n" + "\n".join(lines) + "\n    </url>"

    # Match each <url> block
    enhanced = re.sub(
        r"<url>\s*<loc>(.*?)</loc>\s*(?:(<lastmod>.*?</lastmod>)\s*)?</url>",
        add_sitemap_metadata,
        content,
    )

    sitemap_path.write_text(enhanced, encoding="utf-8")
