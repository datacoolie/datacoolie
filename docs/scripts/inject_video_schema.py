"""ProperDocs hook: inject VideoObject JSON-LD from explicit page metadata."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime
from urllib.parse import urlparse


REQUIRED_VIDEO_FIELDS = (
    "name",
    "description",
    "thumbnail_url",
    "upload_date",
    "duration",
    "embed_url",
    "content_url",
)
ISO_DURATION = re.compile(
    r"^P(?=\d|T\d)(?:\d+D)?(?:T(?=\d)(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$"
)


def _require_https_url(value: object, field: str) -> None:
    parsed = urlparse(str(value))
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"video {field} must be an HTTPS URL")


def _require_video_metadata(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("page video metadata must be a mapping")

    missing = [field for field in REQUIRED_VIDEO_FIELDS if not value.get(field)]
    if missing:
        raise ValueError(f"page video metadata is missing: {', '.join(missing)}")

    upload_date = str(value["upload_date"])
    try:
        parsed_date = datetime.fromisoformat(upload_date.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("video upload_date must be ISO 8601") from error
    if parsed_date.tzinfo is None:
        raise ValueError("video upload_date must include a timezone")

    if not ISO_DURATION.fullmatch(str(value["duration"])):
        raise ValueError("video duration must be ISO 8601")
    for field in ("thumbnail_url", "embed_url", "content_url"):
        _require_https_url(value[field], field)
    return value


def _build_clips(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("video clips must be a list")

    clips: list[dict[str, object]] = []
    previous_end = 0
    for position, clip in enumerate(value, start=1):
        if not isinstance(clip, Mapping):
            raise ValueError(f"video clip {position} must be a mapping")
        required = ("name", "start_offset", "url")
        missing = [field for field in required if clip.get(field) is None]
        if missing:
            raise ValueError(f"video clip {position} is missing: {', '.join(missing)}")

        name = clip["name"]
        start = clip["start_offset"]
        end = clip.get("end_offset")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"video clip {position} name must be non-empty")
        if not isinstance(start, int) or start < previous_end:
            raise ValueError(f"video clip {position} start_offset is invalid")
        if end is not None and (not isinstance(end, int) or end <= start):
            raise ValueError(f"video clip {position} end_offset is invalid")
        _require_https_url(clip["url"], f"clip {position} url")

        item: dict[str, object] = {
            "@type": "Clip",
            "name": name,
            "startOffset": start,
            "url": clip["url"],
        }
        if clip.get("end_offset") is not None:
            item["endOffset"] = end
            previous_end = end
        else:
            previous_end = start
        clips.append(item)
    return clips


def on_page_content(html_content: str, page, config, files) -> str:  # noqa: ANN001
    """Append one VideoObject schema block when frontmatter declares a video."""
    raw_video = page.meta.get("video")
    if raw_video is None:
        return html_content

    video = _require_video_metadata(raw_video)
    schema: dict[str, object] = {
        "@context": "https://schema.org",
        "@type": "VideoObject",
        "name": video["name"],
        "description": video["description"],
        "thumbnailUrl": [video["thumbnail_url"]],
        "uploadDate": video["upload_date"],
        "duration": video["duration"],
        "embedUrl": video["embed_url"],
        "contentUrl": video["content_url"],
    }
    clips = _build_clips(video.get("clips"))
    if clips:
        schema["hasPart"] = clips

    payload = json.dumps(schema, ensure_ascii=False).replace("</", "<\\/")
    return html_content + f'\n<script type="application/ld+json">{payload}</script>\n'
