from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def raw_html_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "raw"


@pytest.fixture(scope="session")
def load_raw_html(raw_html_dir: Path):
    def _load(parcel_id: str) -> str:
        path = raw_html_dir / f"{parcel_id}.html"
        return path.read_text(encoding="utf-8")

    return _load
