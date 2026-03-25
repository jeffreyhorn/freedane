from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional

ENVIRONMENTS: tuple[str, ...] = ("dev", "stage", "prod")
PROFILE_ENV_KEY = "ACCESSDANE_ENVIRONMENT"
LEGACY_PROFILE_ENV_KEY = "environment_name"

REQUIRED_PROFILE_KEYS: tuple[str, ...] = (
    "DATABASE_URL",
    "ACCESSDANE_BASE_URL",
    "ACCESSDANE_RAW_DIR",
    "ACCESSDANE_USER_AGENT",
    "ACCESSDANE_TIMEOUT",
    "ACCESSDANE_RETRIES",
    "ACCESSDANE_BACKOFF",
    "ACCESSDANE_REFRESH_PROFILE",
    "ACCESSDANE_FEATURE_VERSION",
    "ACCESSDANE_RULESET_VERSION",
    "ACCESSDANE_SALES_RATIO_BASE",
    "ACCESSDANE_REFRESH_TOP",
    "ACCESSDANE_ARTIFACT_BASE_DIR",
    "ACCESSDANE_REFRESH_LOG_DIR",
    "ACCESSDANE_BENCHMARK_BASE_DIR",
    "ALERT_ROUTE_GROUP",
    "PROMOTION_APPROVER_GROUP",
    "PROMOTION_FREEZE_FILE",
)


class EnvironmentProfileError(ValueError):
    pass


@dataclass(frozen=True)
class EnvironmentProfile:
    environment_name: str
    database_url: str
    base_url: str
    raw_dir: Path
    user_agent: str
    request_timeout: float
    retries: int
    backoff_seconds: float
    refresh_profile: str
    feature_version: str
    ruleset_version: str
    sales_ratio_base: str
    refresh_top: int
    artifact_base_dir: Path
    refresh_log_dir: Path
    benchmark_base_dir: Path
    alert_route_group: str
    promotion_approver_group: str
    promotion_freeze_file: Path

    @property
    def promotion_registry_root(self) -> Path:
        return self.artifact_base_dir.parent / "promotion_registry"


def load_environment_profile(
    *, environ: Optional[Mapping[str, str]] = None
) -> Optional[EnvironmentProfile]:
    values = os.environ if environ is None else environ

    canonical_env = values.get(PROFILE_ENV_KEY)
    legacy_env = values.get(LEGACY_PROFILE_ENV_KEY)
    resolved_env = canonical_env or legacy_env
    if resolved_env is None:
        return None
    if canonical_env and legacy_env and canonical_env != legacy_env:
        raise EnvironmentProfileError(
            "ACCESSDANE_ENVIRONMENT and legacy environment_name disagree."
        )
    if resolved_env not in ENVIRONMENTS:
        raise EnvironmentProfileError(
            f"{PROFILE_ENV_KEY} must be one of {ENVIRONMENTS}; got '{resolved_env}'."
        )

    missing = [
        key
        for key in REQUIRED_PROFILE_KEYS
        if values.get(key) is None or not str(values.get(key)).strip()
    ]
    if missing:
        raise EnvironmentProfileError(
            "Missing required environment profile keys: " + ", ".join(missing)
        )

    raw_dir = _resolve_path(values["ACCESSDANE_RAW_DIR"])
    artifact_base_dir = _resolve_path(values["ACCESSDANE_ARTIFACT_BASE_DIR"])
    refresh_log_dir = _resolve_path(values["ACCESSDANE_REFRESH_LOG_DIR"])
    benchmark_base_dir = _resolve_path(values["ACCESSDANE_BENCHMARK_BASE_DIR"])
    freeze_file = _resolve_path(values["PROMOTION_FREEZE_FILE"])

    for path_key, path_value in (
        ("ACCESSDANE_RAW_DIR", raw_dir),
        ("ACCESSDANE_ARTIFACT_BASE_DIR", artifact_base_dir),
        ("ACCESSDANE_REFRESH_LOG_DIR", refresh_log_dir),
        ("ACCESSDANE_BENCHMARK_BASE_DIR", benchmark_base_dir),
        ("PROMOTION_FREEZE_FILE", freeze_file),
    ):
        _validate_environment_local_path(
            key=path_key, value=path_value, environment_name=resolved_env
        )

    try:
        refresh_log_dir.relative_to(artifact_base_dir)
    except ValueError as exc:
        raise EnvironmentProfileError(
            "ACCESSDANE_REFRESH_LOG_DIR must be nested under "
            "ACCESSDANE_ARTIFACT_BASE_DIR."
        ) from exc

    request_timeout = _parse_float_key(
        values=values,
        key="ACCESSDANE_TIMEOUT",
    )
    retries = _parse_int_key(values=values, key="ACCESSDANE_RETRIES")
    backoff_seconds = _parse_float_key(
        values=values,
        key="ACCESSDANE_BACKOFF",
    )
    refresh_top = _parse_int_key(values=values, key="ACCESSDANE_REFRESH_TOP")

    return EnvironmentProfile(
        environment_name=resolved_env,
        database_url=values["DATABASE_URL"],
        base_url=values["ACCESSDANE_BASE_URL"],
        raw_dir=raw_dir,
        user_agent=values["ACCESSDANE_USER_AGENT"],
        request_timeout=request_timeout,
        retries=retries,
        backoff_seconds=backoff_seconds,
        refresh_profile=values["ACCESSDANE_REFRESH_PROFILE"],
        feature_version=values["ACCESSDANE_FEATURE_VERSION"],
        ruleset_version=values["ACCESSDANE_RULESET_VERSION"],
        sales_ratio_base=values["ACCESSDANE_SALES_RATIO_BASE"],
        refresh_top=refresh_top,
        artifact_base_dir=artifact_base_dir,
        refresh_log_dir=refresh_log_dir,
        benchmark_base_dir=benchmark_base_dir,
        alert_route_group=values["ALERT_ROUTE_GROUP"],
        promotion_approver_group=values["PROMOTION_APPROVER_GROUP"],
        promotion_freeze_file=freeze_file,
    )


def validate_artifact_path_override(
    *, profile: EnvironmentProfile, artifact_base_dir: Path
) -> None:
    _validate_environment_local_path(
        key="--artifact-base-dir",
        value=artifact_base_dir.expanduser().resolve(),
        environment_name=profile.environment_name,
    )


def _resolve_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _validate_environment_local_path(
    *, key: str, value: Path, environment_name: str
) -> None:
    parts = [part.lower() for part in value.parts]
    environments_index = _find_environments_segment(parts)
    if environments_index is not None:
        if environments_index + 1 >= len(parts):
            raise EnvironmentProfileError(
                f"{key} path must include the environment segment after 'environments'."
            )
        declared = parts[environments_index + 1]
        if declared != environment_name:
            raise EnvironmentProfileError(
                f"{key} path points to environment '{declared}', expected "
                f"'{environment_name}'."
            )
        return

    # If a path does not include '/environments/<env>/', reject obvious
    # cross-environment paths and require an explicit environment segment.
    for other_environment in ENVIRONMENTS:
        if other_environment == environment_name:
            continue
        if other_environment in parts:
            raise EnvironmentProfileError(
                f"{key} path contains foreign environment segment "
                f"'{other_environment}'."
            )
    raise EnvironmentProfileError(
        f"{key} path must include an explicit '/environments/{environment_name}/' "
        "segment."
    )


def _find_environments_segment(parts: list[str]) -> Optional[int]:
    for idx, segment in enumerate(parts):
        if segment == "environments":
            return idx
    return None


def _parse_float_key(*, values: Mapping[str, str], key: str) -> float:
    try:
        return float(values[key])
    except (TypeError, ValueError) as exc:
        raise EnvironmentProfileError(
            f"{key} must be a numeric value; got {values.get(key)!r}."
        ) from exc


def _parse_int_key(*, values: Mapping[str, str], key: str) -> int:
    try:
        return int(values[key])
    except (TypeError, ValueError) as exc:
        raise EnvironmentProfileError(
            f"{key} must be an integer value; got {values.get(key)!r}."
        ) from exc
