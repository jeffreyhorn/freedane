from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    base_url: str
    raw_dir: Path
    database_url: str
    user_agent: str
    request_timeout: float
    retries: int
    backoff_seconds: float


def load_settings() -> Settings:
    load_dotenv()
    base_url = os.getenv("ACCESSDANE_BASE_URL", "https://accessdane.danecounty.gov")
    raw_dir = Path(os.getenv("ACCESSDANE_RAW_DIR", "data/raw"))
    database_url = os.getenv("DATABASE_URL", "sqlite:///data/accessdane.sqlite")
    user_agent = os.getenv("ACCESSDANE_USER_AGENT", "AccessDaneAudit/0.1 (+contact)")
    request_timeout = float(os.getenv("ACCESSDANE_TIMEOUT", "30"))
    retries = int(os.getenv("ACCESSDANE_RETRIES", "3"))
    backoff_seconds = float(os.getenv("ACCESSDANE_BACKOFF", "1.5"))
    return Settings(
        base_url=base_url,
        raw_dir=raw_dir,
        database_url=database_url,
        user_agent=user_agent,
        request_timeout=request_timeout,
        retries=retries,
        backoff_seconds=backoff_seconds,
    )
