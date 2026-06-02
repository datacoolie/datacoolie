"""MkDocs hook: inject FAQPage JSON-LD for pages with collapsible FAQ sections.

Scans rendered HTML for `??? question "..."` patterns (rendered as <details>
by pymdownx.details) and injects a FAQPage structured data block.
"""

from __future__ import annotations

import html
import json
import re


def on_page_content(html_content: str, page, config, files) -> str:  # noqa: ANN001
    """After Markdown→HTML, inject FAQ JSON-LD if <details> question blocks exist."""
    # Match <details> blocks created by `??? question "..."` admonitions
    pattern = re.compile(
        r'<details[^>]*class="[^"]*question[^"]*"[^>]*>\s*'
        r"<summary>(.*?)</summary>\s*"
        r"(.*?)</details>",
        re.DOTALL,
    )
    matches = pattern.findall(html_content)
    if not matches:
        return html_content

    faq_items = []
    for question_html, answer_html in matches:
        # Strip HTML tags for clean text
        question = re.sub(r"<[^>]+>", "", question_html).strip()
        answer = re.sub(r"<[^>]+>", "", answer_html).strip()
        # Collapse whitespace
        answer = re.sub(r"\s+", " ", answer)
        if question and answer:
            faq_items.append(
                {
                    "@type": "Question",
                    "name": question,
                    "acceptedAnswer": {"@type": "Answer", "text": answer},
                }
            )

    if not faq_items:
        return html_content

    schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": faq_items,
    }

    script_tag = f'\n<script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script>\n'
    return html_content + script_tag
