from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx

from .config import Settings
from .utils import ensure_dir, sha256_text


@dataclass
class FetchResult:
    parcel_id: str
    url: str
    status_code: Optional[int]
    html: Optional[str]
    raw_path: Optional[Path]
    raw_sha256: Optional[str]
    raw_size: Optional[int]


def fetch_page(parcel_id: str, settings: Settings) -> FetchResult:
    url = f"{settings.base_url.rstrip('/')}/{parcel_id}"
    headers = {"User-Agent": settings.user_agent}
    last_error: Optional[Exception] = None
    with httpx.Client(timeout=settings.request_timeout, headers=headers) as client:
        for attempt in range(settings.retries + 1):
            try:
                response = client.get(url)
                html = response.text if response.text else None
                raw_path = None
                raw_sha256 = None
                raw_size = None
                if html:
                    raw_path, raw_sha256, raw_size = store_raw_html(
                        settings.raw_dir, parcel_id, html
                    )
                return FetchResult(
                    parcel_id=parcel_id,
                    url=str(response.url),
                    status_code=response.status_code,
                    html=html,
                    raw_path=raw_path,
                    raw_sha256=raw_sha256,
                    raw_size=raw_size,
                )
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt < settings.retries:
                    time.sleep(settings.backoff_seconds * (attempt + 1))
                    continue
                break
    raise RuntimeError(f"Failed to fetch {parcel_id}: {last_error}")


def store_raw_html(raw_dir: Path, parcel_id: str, html: str) -> tuple[Path, str, int]:
    ensure_dir(raw_dir)
    filename = f"{parcel_id}.html"
    path = raw_dir / filename
    path.write_text(html, encoding="utf-8")
    sha256 = sha256_text(html)
    size = len(html.encode("utf-8"))
    return path, sha256, size
