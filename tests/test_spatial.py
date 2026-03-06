from __future__ import annotations

from types import SimpleNamespace

from accessdane_audit.spatial import (
    detect_spatial_support,
    spatial_support_status_to_dict,
)


class _FakeScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


class _FakeSession:
    def __init__(self, *, dialect_name: str, responses: list[object]) -> None:
        self._bind = SimpleNamespace(dialect=SimpleNamespace(name=dialect_name))
        self._responses = list(responses)

    def get_bind(self):
        return self._bind

    def execute(self, _statement):
        if not self._responses:
            raise AssertionError("No fake response available for execute().")
        next_value = self._responses.pop(0)
        if isinstance(next_value, Exception):
            raise next_value
        return _FakeScalarResult(next_value)


def test_detect_spatial_support_uses_point_only_mode_on_sqlite() -> None:
    session = _FakeSession(dialect_name="sqlite", responses=[])

    status = detect_spatial_support(session)
    payload = spatial_support_status_to_dict(status)

    assert payload["database_dialect"] == "sqlite"
    assert payload["mode"] == "point_only"
    assert payload["postgis_available"] is False
    assert payload["geometry_columns_available"] is False
    assert "Non-PostgreSQL backend" in str(payload["note"])


def test_detect_spatial_support_uses_point_only_mode_when_postgis_missing() -> None:
    session = _FakeSession(dialect_name="postgresql", responses=[False])

    status = detect_spatial_support(session)
    payload = spatial_support_status_to_dict(status)

    assert payload["database_dialect"] == "postgresql"
    assert payload["mode"] == "point_only"
    assert payload["postgis_available"] is False
    assert payload["postgis_full_version"] is None
    assert payload["geometry_columns_available"] is False
    assert "not installed" in str(payload["note"])


def test_detect_spatial_support_uses_geometry_mode_when_postgis_available() -> None:
    session = _FakeSession(
        dialect_name="postgresql",
        responses=[
            True,
            'POSTGIS="3.5.0" [EXTENSION]',
            True,
        ],
    )

    status = detect_spatial_support(session)
    payload = spatial_support_status_to_dict(status)

    assert payload["database_dialect"] == "postgresql"
    assert payload["mode"] == "geometry_postgis"
    assert payload["postgis_available"] is True
    assert payload["geometry_columns_available"] is True
    assert payload["postgis_full_version"] == 'POSTGIS="3.5.0" [EXTENSION]'


def test_detect_spatial_support_falls_back_to_point_only_when_probe_fails() -> None:
    session = _FakeSession(
        dialect_name="postgresql",
        responses=[RuntimeError("permission denied")],
    )

    status = detect_spatial_support(session)
    payload = spatial_support_status_to_dict(status)

    assert payload["database_dialect"] == "postgresql"
    assert payload["mode"] == "point_only"
    assert payload["postgis_available"] is False
    assert payload["geometry_columns_available"] is False
    assert "probe failed" in str(payload["note"])
