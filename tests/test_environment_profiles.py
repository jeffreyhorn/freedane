from __future__ import annotations

from pathlib import Path

import pytest

from accessdane_audit.environment_profiles import (
    EnvironmentProfileError,
    load_environment_profile,
)


def _profile_env(tmp_path: Path, *, environment_name: str) -> dict[str, str]:
    root = tmp_path / "data" / "environments" / environment_name
    return {
        "ACCESSDANE_ENVIRONMENT": environment_name,
        "DATABASE_URL": "sqlite:///:memory:",
        "ACCESSDANE_BASE_URL": "https://accessdane.danecounty.gov",
        "ACCESSDANE_RAW_DIR": str(root / "raw"),
        "ACCESSDANE_USER_AGENT": "AccessDaneAudit/0.1",
        "ACCESSDANE_TIMEOUT": "30",
        "ACCESSDANE_RETRIES": "3",
        "ACCESSDANE_BACKOFF": "1.5",
        "ACCESSDANE_REFRESH_PROFILE": "daily_refresh",
        "ACCESSDANE_FEATURE_VERSION": "feature_v1",
        "ACCESSDANE_RULESET_VERSION": "scoring_rules_v1",
        "ACCESSDANE_SALES_RATIO_BASE": "sales_ratio_v1",
        "ACCESSDANE_REFRESH_TOP": "100",
        "ACCESSDANE_ARTIFACT_BASE_DIR": str(root / "refresh_runs"),
        "ACCESSDANE_REFRESH_LOG_DIR": str(root / "refresh_runs" / "logs"),
        "ACCESSDANE_BENCHMARK_BASE_DIR": str(root / "benchmark_packs"),
        "ALERT_ROUTE_GROUP": "ops-alerts",
        "PROMOTION_APPROVER_GROUP": "release-approvers",
        "PROMOTION_FREEZE_FILE": str(root / "promotion_freeze.json"),
    }


def test_load_environment_profile_returns_none_when_environment_not_declared(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="dev")
    env.pop("ACCESSDANE_ENVIRONMENT")

    assert load_environment_profile(environ=env) is None


def test_load_environment_profile_parses_and_validates_required_contract(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")

    profile = load_environment_profile(environ=env)

    assert profile is not None
    assert profile.environment_name == "stage"
    assert profile.refresh_profile == "daily_refresh"
    assert profile.refresh_top == 100
    assert (
        profile.artifact_base_dir
        == (tmp_path / "data" / "environments" / "stage" / "refresh_runs").resolve()
    )
    assert (
        profile.refresh_log_dir
        == (
            tmp_path / "data" / "environments" / "stage" / "refresh_runs" / "logs"
        ).resolve()
    )


def test_load_environment_profile_rejects_cross_environment_paths(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    env["ACCESSDANE_BENCHMARK_BASE_DIR"] = str(
        tmp_path / "data" / "environments" / "prod" / "benchmark_packs"
    )

    with pytest.raises(EnvironmentProfileError, match="points to environment"):
        load_environment_profile(environ=env)


def test_load_environment_profile_rejects_cross_environment_freeze_file(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    env["PROMOTION_FREEZE_FILE"] = str(
        tmp_path / "data" / "environments" / "prod" / "promotion_freeze.json"
    )

    with pytest.raises(EnvironmentProfileError, match="points to environment"):
        load_environment_profile(environ=env)


def test_load_environment_profile_rejects_invalid_numeric_fields(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    env["ACCESSDANE_TIMEOUT"] = "not-a-number"

    with pytest.raises(EnvironmentProfileError, match="ACCESSDANE_TIMEOUT"):
        load_environment_profile(environ=env)


def test_load_environment_profile_rejects_out_of_range_refresh_top(
    tmp_path: Path,
) -> None:
    env = _profile_env(tmp_path, environment_name="stage")
    env["ACCESSDANE_REFRESH_TOP"] = "0"

    with pytest.raises(EnvironmentProfileError, match="between 1 and 1000"):
        load_environment_profile(environ=env)
