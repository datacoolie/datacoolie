"""Check crawler-facing output after a ProperDocs build (standard library only)."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import gzip
from html.parser import HTMLParser
import json
from pathlib import Path
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET


class PageMetadata(HTMLParser):
    """Read metadata without running JavaScript or fetching network resources."""

    def __init__(self, html: str) -> None:
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.titles = 0
        self.meta: dict[str, str] = {}
        self.canonicals: list[str] = []
        self.schemas: list[dict] = []
        self._title = False
        self._json: list[str] | None = None
        self.feed(html)

    def handle_starttag(self, tag: str, attrs) -> None:
        values = dict(attrs)
        if tag == "title":
            self._title = True
            self.titles += 1
        if tag == "meta":
            self.meta[values.get("name") or values.get("property", "")] = values.get("content", "")
        if tag == "link" and "canonical" in values.get("rel", "").split():
            self.canonicals.append(values.get("href", ""))
        if tag == "script" and values.get("type") == "application/ld+json":
            self._json = []

    def handle_data(self, data: str) -> None:
        if self._title:
            self.title += data
        if self._json is not None:
            self._json.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._title = False
        if tag == "script" and self._json is not None:
            self.schemas.append(json.loads("".join(self._json)))
            self._json = None


def local_target(site_dir: Path, site_url: str, url: str) -> Path:
    """Map an absolute site URL to its build artifact, rejecting outside URLs."""
    base, target = urlsplit(site_url), urlsplit(url)
    if (target.scheme, target.netloc) != (base.scheme, base.netloc):
        raise ValueError(f"URL outside site origin: {url}")
    prefix = base.path.rstrip("/") + "/"
    if not target.path.startswith(prefix):
        raise ValueError(f"URL outside site path: {url}")
    relative = unquote(target.path[len(prefix):])
    result = site_dir / relative
    if not relative or relative.endswith("/"):
        result /= "index.html"
    if not result.resolve().is_relative_to(site_dir.resolve()):
        raise ValueError(f"URL escapes build directory: {url}")
    return result


def verify_site(site_dir: Path, site_url: str) -> int:
    """Fail on invalid metadata, fictitious breadcrumb pages or sitemap drift."""
    sitemap = (site_dir / "sitemap.xml").read_bytes()
    compressed = gzip.decompress((site_dir / "sitemap.xml.gz").read_bytes())
    if compressed != sitemap:
        raise ValueError("sitemap.xml.gz differs from sitemap.xml")
    urls = [element.text for element in ET.fromstring(sitemap).findall(".//{*}loc")]
    if not urls or len(urls) != len(set(urls)):
        raise ValueError("Sitemap URLs must be non-empty and unique")
    titles: dict[str, list[str]] = defaultdict(list)
    for url in urls:
        target = local_target(site_dir, site_url, url)
        page = PageMetadata(target.read_text(encoding="utf-8"))
        if page.canonicals != [url]:
            raise ValueError(f"Missing/duplicate/non-self canonical: {url}")
        if page.titles != 1 or not page.title.strip() or not page.meta.get("description"):
            raise ValueError(f"Missing title or description: {url}")
        titles[page.title].append(url)
        if "noindex" in page.meta.get("robots", "").lower():
            raise ValueError(f"Sitemap contains a noindex page: {url}")
        for key in ("og:title", "twitter:title"):
            if page.meta.get(key) != page.title:
                raise ValueError(f"{key} differs from title: {url}")
        if page.meta.get("og:url") != url or not page.schemas:
            raise ValueError(f"Missing page identity or structured data: {url}")
        image = page.meta.get("og:image", "")
        if not image.startswith("https://"):
            raise ValueError(f"Social image must be an absolute HTTPS URL: {url}")
        if image.startswith(site_url) and not local_target(site_dir, site_url, image).is_file():
            raise ValueError(f"Missing social image: {image}")
        for schema in page.schemas:
            if schema.get("@type") == "BreadcrumbList":
                items = schema["itemListElement"]
                if [item["position"] for item in items] != list(range(1, len(items) + 1)):
                    raise ValueError(f"Invalid breadcrumb positions: {url}")
                if items[-1]["item"] != url:
                    raise ValueError(f"Breadcrumb does not end at current page: {url}")
                for item in items:
                    if not item["name"] or not local_target(site_dir, site_url, item["item"]).is_file():
                        raise ValueError(f"Invalid breadcrumb target: {item}")
            if schema.get("@type") == "BlogPosting":
                date.fromisoformat(schema.get("datePublished", "")[:10])
                if page.meta.get("og:type") != "article":
                    raise ValueError(f"Blog post not identified as article: {url}")
            if schema.get("@type") == "CollectionPage" and page.meta.get("og:type") != "website":
                raise ValueError(f"Blog listing identified as article: {url}")
    duplicates = {title: paths for title, paths in titles.items() if len(paths) > 1}
    if duplicates:
        raise ValueError(f"Duplicate titles: {duplicates}")
    return len(urls)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-dir", type=Path, default=Path("site"))
    parser.add_argument("--site-url", default="https://datacoolie.github.io/datacoolie/")
    args = parser.parse_args()
    count = verify_site(args.site_dir, args.site_url)
    print(f"SEO output verified: {count} canonical pages; metadata, JSON-LD, breadcrumbs and sitemap parity OK")


if __name__ == "__main__":
    main()
