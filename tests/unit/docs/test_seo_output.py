"""Regressions for the actual SEO template and sitemap postprocessing."""

from datetime import date
import gzip
from pathlib import Path
import runpy
from types import SimpleNamespace

from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
import pytest


ROOT = Path(__file__).resolve().parents[3]
SEO = runpy.run_path(str(ROOT / "docs/scripts/seo_metadata.py"))
SITEMAP = runpy.run_path(str(ROOT / "docs/scripts/enhance_sitemap.py"))
VERIFY = runpy.run_path(str(ROOT / "scripts/verify_docs_seo.py"))
SITE_URL = "https://datacoolie.github.io/datacoolie/"


def _page(url, title, meta=None):
    return SimpleNamespace(
        url=url, title=title, meta=meta or {}, is_homepage=not url,
        canonical_url=SITE_URL + url,
    )


def _render(page):
    config = {
        "site_name": "DataCoolie", "site_url": SITE_URL,
        "site_description": "Python ETL", "repo_url": "https://github.com/datacoolie/datacoolie",
    }
    nav = SimpleNamespace(pages=[_page("", "Home"), _page("blog/", "Blog"), _page("guide/", "User guide")])
    context = SEO["on_page_context"]({"page": page, "config": config}, page, config, nav)
    env = Environment(loader=ChoiceLoader([
        FileSystemLoader(ROOT / "overrides"),
        DictLoader({"base.html": "{% block htmltitle %}{% endblock %}{% block extrahead %}{% endblock %}"}),
    ]))
    return VERIFY["PageMetadata"](env.get_template("main.html").render(**context))


@pytest.mark.parametrize("route", ["blog/", "blog/category/tutorial/", "blog/archive/2026/"])
def test_blog_listings_are_collections_with_real_breadcrumbs(route):
    result = _render(_page(route, "Tutorial"))
    assert result.meta["og:type"] == "website"
    assert any(node["@type"] == "CollectionPage" for node in result.schemas)
    assert not any(node["@type"] == "BlogPosting" for node in result.schemas)
    crumbs = next(node for node in result.schemas if node["@type"] == "BreadcrumbList")
    expected = [SITE_URL, SITE_URL + "blog/"]
    if route != "blog/":
        expected.append(SITE_URL + route)
    assert [item["item"] for item in crumbs["itemListElement"]] == expected


def test_blog_post_breadcrumb_does_not_invent_date_pages():
    route = "blog/2026/09/14/example/"
    result = _render(_page(route, "Example", {"template": "blog-post.html", "date": date(2026, 9, 14)}))
    post = next(node for node in result.schemas if node["@type"] == "BlogPosting")
    assert post["datePublished"] == "2026-09-14"
    assert result.meta["og:type"] == "article"
    crumbs = next(node for node in result.schemas if node["@type"] == "BreadcrumbList")
    assert [item["item"] for item in crumbs["itemListElement"]] == [SITE_URL, SITE_URL + "blog/", SITE_URL + route]


@pytest.mark.parametrize("title,expected", [
    ('Using "SQL" & Python', 'Using "SQL" & Python | DataCoolie'),
    ("DataCoolie user guide", "DataCoolie user guide"),
])
def test_titles_and_descriptions_survive_html_attribute_escaping(title, expected):
    description = 'Read "orders" & transform data.'
    result = _render(_page("guide/operations/run-stage/", title, {"title": title, "description": description}))
    assert result.title == result.meta["og:title"] == result.meta["twitter:title"] == expected
    assert result.meta["og:description"] == result.meta["twitter:description"] == description
    assert result.meta["robots"] == "max-image-preview:large"


def test_explicit_seo_title_and_absolute_image_are_preserved():
    result = _render(_page("guide/operations/run-stage/", "Run", {
        "seo_title": "Run a Python pipeline", "og_image": "https://example.org/image.webp",
    }))
    assert result.title == result.meta["og:title"] == "Run a Python pipeline"
    assert result.meta["og:image"] == "https://example.org/image.webp"


def test_normalized_sitemaps_have_identical_uncompressed_content(tmp_path):
    original = '<urlset><url><loc>https://example.org/</loc><lastmod>2026-09-20</lastmod><priority>1</priority><changefreq>daily</changefreq></url></urlset>'
    (tmp_path / "sitemap.xml").write_text(original, encoding="utf-8")
    (tmp_path / "sitemap.xml.gz").write_bytes(gzip.compress(original.encode()))
    SITEMAP["on_post_build"]({"site_dir": str(tmp_path)})
    normalized = (tmp_path / "sitemap.xml").read_bytes()
    assert gzip.decompress((tmp_path / "sitemap.xml.gz").read_bytes()) == normalized
    assert b"lastmod" not in normalized and b"priority" not in normalized and b"changefreq" not in normalized


def test_verifier_rejects_outside_or_traversal_urls(tmp_path):
    for url in ("https://example.org/", SITE_URL + "../../secret", SITE_URL + "%2e%2e/secret"):
        with pytest.raises(ValueError):
            VERIFY["local_target"](tmp_path, SITE_URL, url)
