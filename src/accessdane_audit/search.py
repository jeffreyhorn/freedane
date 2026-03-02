from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from .config import Settings

PARCEL_NUMBER_RE = re.compile(r"\b\d{4}-\d{3}-\d{4}-\d\b")
PARCEL_ID_RE = re.compile(r"\b\d{10,14}\b")


@dataclass
class SearchResult:
    parcel_ids: list[str]
    truncated: bool
    url: str


def search_trs(
    settings: Settings,
    municipality_id: int,
    township: int,
    range_: int,
    section: Optional[int] = None,
    quarter: Optional[str] = None,
    quarter_quarter: Optional[str] = None,
) -> SearchResult:
    url = f"{settings.base_url.rstrip('/')}/Parcel/Search"
    params = {
        "TownShipRangeSection.SelectedMunicipality": str(municipality_id),
        "TownShipRangeSection.SelectedTownship": f"{township:02d}",
        "TownShipRangeSection.SelectedRange": f"{range_:02d}",
        "TownShipRangeSection.SelectedSection": f"{section:02d}" if section else "",
        "TownShipRangeSection.SelectedQuarter": quarter or "",
        "TownShipRangeSection.SelectedQuarterQuarter": quarter_quarter or "",
        "formName": "township_range_section",
    }
    headers = {"User-Agent": settings.user_agent}
    with httpx.Client(timeout=settings.request_timeout, headers=headers) as client:
        response = client.get(url, params=params)
        html = response.text or ""
    parcel_ids, truncated = parse_search_results(html)
    return SearchResult(
        parcel_ids=parcel_ids, truncated=truncated, url=str(response.url)
    )


def parse_search_results(html: str) -> tuple[list[str], bool]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    truncated = "Maximum result count reached" in text
    parcel_ids: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a"):
        candidate = _extract_parcel_id(
            anchor.get_text(" ", strip=True),
            _attr_text(anchor.get("href")),
        )
        if candidate and candidate not in seen:
            parcel_ids.append(candidate)
            seen.add(candidate)
    return parcel_ids, truncated


def _extract_parcel_id(text: str, href: Optional[str]) -> Optional[str]:
    match = PARCEL_NUMBER_RE.search(text)
    if match:
        return _digits_only(match.group(0))
    if href:
        path = urlparse(href).path
        match = PARCEL_ID_RE.search(path)
        if match:
            return match.group(0)
    return None


def _digits_only(value: str) -> str:
    return re.sub(r"\D", "", value)


def _attr_text(value: object) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [item for item in value if isinstance(item, str)]
        return " ".join(parts) or None
    return None
