from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from accessdane_audit import cli
from accessdane_audit.case_review import (
    create_case_review,
    list_case_reviews,
    update_case_review,
)
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import FraudScore, Parcel, ScoringRun


def test_case_review_create_idempotent_and_payload_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_create.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_id = _seed_single_score(session, parcel_id="P-1", year=2025)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    args = [
        "case-review",
        "create",
        "--score-id",
        str(score_id),
        "--status",
        "resolved",
        "--disposition",
        "false_positive",
        "--reviewer",
        "  Alice  ",
        "--assigned-reviewer",
        " Bob ",
        "--note",
        "  Initial review  ",
        "--evidence-link",
        "kind=reason_code,ref=ratio__low,label= Primary ",
        "--evidence-link",
        "kind=dossier,ref=CASE-1",
        "--evidence-link",
        "kind=dossier,ref=CASE-1,label=   ",
    ]

    first = runner.invoke(cli.app, args)
    assert first.exit_code == 0, first.stdout
    first_payload = json.loads(first.stdout)

    assert list(first_payload.keys()) == [
        "run",
        "request",
        "review",
        "diagnostics",
        "error",
    ]
    assert first_payload["run"] == {
        "run_id": None,
        "run_persisted": False,
        "run_type": "case_review",
        "version_tag": "case_review_v1",
        "status": "succeeded",
    }
    assert first_payload["review"]["created"] is True
    assert first_payload["review"]["status"] == "resolved"
    assert first_payload["review"]["disposition"] == "false_positive"
    assert first_payload["review"]["reviewer"] == "Alice"
    assert first_payload["review"]["assigned_reviewer"] == "Bob"
    assert first_payload["review"]["note"] == "Initial review"
    assert first_payload["review"]["evidence_links"] == [
        {"kind": "dossier", "ref": "CASE-1", "label": None},
        {"kind": "reason_code", "ref": "ratio__low", "label": "Primary"},
    ]
    assert first_payload["request"]["feature_version"] == "feature_v1"
    assert first_payload["request"]["ruleset_version"] == "scoring_rules_v1"
    assert first_payload["error"] is None

    second = runner.invoke(cli.app, args)
    assert second.exit_code == 0, second.stdout
    second_payload = json.loads(second.stdout)

    assert second_payload["review"]["id"] == first_payload["review"]["id"]
    assert second_payload["review"]["created"] is False
    assert second_payload["error"] is None


def test_case_review_create_conflict_returns_duplicate_case_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_create_conflict.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_id = _seed_single_score(session, parcel_id="P-1", year=2025)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    first = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(score_id),
            "--note",
            "first",
        ],
    )
    assert first.exit_code == 0, first.stdout

    second = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(score_id),
            "--note",
            "second",
        ],
    )
    assert second.exit_code == 1, second.stdout
    payload = json.loads(second.stdout)
    assert payload["run"]["status"] == "failed"
    assert payload["error"]["code"] == "duplicate_case_review"


def test_case_review_update_lifecycle_timestamp_rules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_update_lifecycle.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_id = _seed_single_score(session, parcel_id="P-1", year=2025)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    created = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(score_id),
        ],
    )
    assert created.exit_code == 0, created.stdout
    created_payload = json.loads(created.stdout)
    case_review_id = created_payload["review"]["id"]

    resolved = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(case_review_id),
            "--status",
            "resolved",
            "--disposition",
            "false_positive",
            "--set-evidence-link",
            "kind=dossier,ref=CASE-1",
        ],
    )
    assert resolved.exit_code == 0, resolved.stdout
    resolved_payload = json.loads(resolved.stdout)
    reviewed_at = resolved_payload["review"]["reviewed_at"]
    assert resolved_payload["review"]["updated"] is True
    assert reviewed_at is not None
    assert resolved_payload["review"]["closed_at"] is None

    reopened = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(case_review_id),
            "--status",
            "in_review",
        ],
    )
    assert reopened.exit_code == 0, reopened.stdout
    reopened_payload = json.loads(reopened.stdout)
    assert reopened_payload["review"]["status"] == "in_review"
    assert reopened_payload["review"]["disposition"] is None
    assert reopened_payload["review"]["reviewed_at"] == reviewed_at

    re_resolved = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(case_review_id),
            "--status",
            "resolved",
            "--disposition",
            "false_positive",
        ],
    )
    assert re_resolved.exit_code == 0, re_resolved.stdout
    re_resolved_payload = json.loads(re_resolved.stdout)
    assert re_resolved_payload["review"]["reviewed_at"] == reviewed_at

    closed = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(case_review_id),
            "--status",
            "closed",
        ],
    )
    assert closed.exit_code == 0, closed.stdout
    closed_payload = json.loads(closed.stdout)
    assert closed_payload["review"]["closed_at"] is not None
    assert closed_payload["review"]["reviewed_at"] == reviewed_at

    reopen_closed = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(case_review_id),
            "--status",
            "in_review",
        ],
    )
    assert reopen_closed.exit_code == 0, reopen_closed.stdout
    reopen_closed_payload = json.loads(reopen_closed.stdout)
    assert reopen_closed_payload["review"]["closed_at"] is None
    assert reopen_closed_payload["review"]["reviewed_at"] == reviewed_at


def test_case_review_update_validation_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_update_validation.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        pending_score_id = _seed_single_score(session, parcel_id="P-1", year=2025)
        resolved_score_id = _seed_single_score(session, parcel_id="P-2", year=2025)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()

    pending_create = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(pending_score_id),
        ],
    )
    assert pending_create.exit_code == 0, pending_create.stdout
    pending_id = json.loads(pending_create.stdout)["review"]["id"]

    invalid_transition = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(pending_id),
            "--status",
            "closed",
            "--disposition",
            "false_positive",
            "--set-evidence-link",
            "kind=dossier,ref=CASE-1",
        ],
    )
    assert invalid_transition.exit_code == 1, invalid_transition.stdout
    invalid_transition_payload = json.loads(invalid_transition.stdout)
    assert invalid_transition_payload["error"]["code"] == "invalid_transition"

    resolved_create = runner.invoke(
        cli.app,
        [
            "case-review",
            "create",
            "--score-id",
            str(resolved_score_id),
            "--status",
            "resolved",
            "--disposition",
            "false_positive",
            "--evidence-link",
            "kind=dossier,ref=CASE-2",
        ],
    )
    assert resolved_create.exit_code == 0, resolved_create.stdout
    resolved_id = json.loads(resolved_create.stdout)["review"]["id"]

    clear_required_links = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(resolved_id),
            "--clear-evidence-links",
        ],
    )
    assert clear_required_links.exit_code == 1, clear_required_links.stdout
    clear_required_links_payload = json.loads(clear_required_links.stdout)
    assert clear_required_links_payload["error"]["code"] == "evidence_link_required"

    mutually_exclusive = runner.invoke(
        cli.app,
        [
            "case-review",
            "update",
            "--id",
            str(resolved_id),
            "--clear-evidence-links",
            "--set-evidence-link",
            "kind=dossier,ref=CASE-2",
        ],
    )
    assert mutually_exclusive.exit_code == 2
    assert "Cannot combine" in (mutually_exclusive.stdout + mutually_exclusive.stderr)


def test_case_review_list_filters_and_ordering(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_list.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_1 = _seed_single_score(session, parcel_id="P-1", year=2025)
        score_2 = _seed_single_score(session, parcel_id="P-2", year=2025)
        score_3 = _seed_single_score(session, parcel_id="P-3", year=2024)

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: SimpleNamespace(database_url=database_url),
    )

    runner = CliRunner()
    c1 = _create_case(runner, score_1, status="pending")
    c2 = _create_case(
        runner,
        score_2,
        status="resolved",
        disposition="confirmed_issue",
        evidence=["kind=dossier,ref=C2"],
    )
    c3 = _create_case(
        runner,
        score_3,
        status="in_review",
        reviewer="Alice",
    )

    assert c1["review"]["id"] < c2["review"]["id"] < c3["review"]["id"]

    listed = runner.invoke(cli.app, ["case-review", "list"])
    assert listed.exit_code == 0, listed.stdout
    list_payload = json.loads(listed.stdout)

    assert list(list_payload.keys()) == [
        "run",
        "request",
        "summary",
        "reviews",
        "diagnostics",
        "error",
    ]
    assert list_payload["request"]["statuses"] == []
    assert list_payload["request"]["dispositions"] == []
    assert list_payload["request"]["years"] == []
    assert list_payload["summary"]["total"] == 3
    assert [row["id"] for row in list_payload["reviews"]] == [
        c3["review"]["id"],
        c2["review"]["id"],
        c1["review"]["id"],
    ]

    filtered = runner.invoke(
        cli.app,
        [
            "case-review",
            "list",
            "--status",
            "resolved",
            "--year",
            "2025",
            "--limit",
            "10",
            "--offset",
            "0",
        ],
    )
    assert filtered.exit_code == 0, filtered.stdout
    filtered_payload = json.loads(filtered.stdout)

    assert filtered_payload["request"]["statuses"] == ["resolved"]
    assert filtered_payload["request"]["years"] == [2025]
    assert filtered_payload["summary"]["total"] == 1
    assert filtered_payload["summary"]["returned"] == 1
    assert filtered_payload["reviews"][0]["id"] == c2["review"]["id"]


def test_case_review_validation_codes_for_invalid_status_and_disposition(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "case_review_validation_codes.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_id = _seed_single_score(session, parcel_id="P-1", year=2025)
        created = create_case_review(session, score_id=score_id)
        assert created["error"] is None
        case_review_id = int(created["review"]["id"])

        invalid_create_status = create_case_review(
            session,
            score_id=score_id,
            status="not_a_status",
        )
        assert invalid_create_status["run"]["status"] == "failed"
        assert invalid_create_status["error"]["code"] == "invalid_status"

        invalid_update_status = update_case_review(
            session,
            case_review_id=case_review_id,
            status="still_not_a_status",
        )
        assert invalid_update_status["run"]["status"] == "failed"
        assert invalid_update_status["error"]["code"] == "invalid_status"
        assert invalid_update_status["request"]["patch"] == {
            "status": "still_not_a_status"
        }

        mutually_exclusive_links = update_case_review(
            session,
            case_review_id=case_review_id,
            set_evidence_links=["kind=dossier,ref=CASE-1"],
            clear_evidence_links=True,
        )
        assert mutually_exclusive_links["run"]["status"] == "failed"
        assert mutually_exclusive_links["error"]["code"] == "invalid_evidence_link"
        assert mutually_exclusive_links["request"]["patch"] == {
            "evidence_links": ["kind=dossier,ref=CASE-1"],
        }

        invalid_list_status = list_case_reviews(session, statuses=["nope"])
        assert invalid_list_status["run"]["status"] == "failed"
        assert invalid_list_status["error"]["code"] == "invalid_status"

        invalid_list_disposition = list_case_reviews(
            session,
            dispositions=["bogus_disposition"],
        )
        assert invalid_list_disposition["run"]["status"] == "failed"
        assert invalid_list_disposition["error"]["code"] == "invalid_disposition"


def test_case_review_create_handles_unique_conflict_idempotently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "case_review_unique_conflict.sqlite"
    database_url = f"sqlite:///{db_path}"
    init_db(database_url)

    with session_scope(database_url) as session:
        score_id = _seed_single_score(session, parcel_id="P-9", year=2025)
        first = create_case_review(
            session,
            score_id=score_id,
            reviewer="Alice",
            note="same note",
        )
        assert first["error"] is None
        existing_id = int(first["review"]["id"])

    with session_scope(database_url) as session:
        real_execute = session.execute
        seen_case_review_lookup = False

        def execute_with_stale_lookup(statement, *args, **kwargs):
            nonlocal seen_case_review_lookup
            statement_text = str(statement).lower()
            if (
                "from case_reviews" in statement_text
                and "score_id" in statement_text
                and not seen_case_review_lookup
            ):
                seen_case_review_lookup = True
                return SimpleNamespace(scalar_one_or_none=lambda: None)
            return real_execute(statement, *args, **kwargs)

        monkeypatch.setattr(session, "execute", execute_with_stale_lookup)

        payload = create_case_review(
            session,
            score_id=score_id,
            reviewer="Alice",
            note="same note",
        )

        assert payload["error"] is None
        assert payload["review"]["created"] is False
        assert payload["review"]["id"] == existing_id


def _seed_single_score(session, *, parcel_id: str, year: int) -> int:
    run = ScoringRun(
        run_type="score_fraud",
        status="succeeded",
        version_tag="scoring_rules_v1",
        scope_json={},
        config_json={},
    )
    session.add(run)
    session.flush()

    session.add(Parcel(id=parcel_id))
    session.flush()

    score = FraudScore(
        run_id=run.id,
        feature_run_id=None,
        parcel_id=parcel_id,
        year=year,
        ruleset_version="scoring_rules_v1",
        feature_version="feature_v1",
        score_value=Decimal("80.00"),
        risk_band="high",
        requires_review=True,
        reason_code_count=1,
        score_summary_json={},
    )
    session.add(score)
    session.flush()
    return score.id


def _create_case(
    runner: CliRunner,
    score_id: int,
    *,
    status: str,
    disposition: str | None = None,
    reviewer: str | None = None,
    evidence: list[str] | None = None,
) -> dict[str, object]:
    args = [
        "case-review",
        "create",
        "--score-id",
        str(score_id),
        "--status",
        status,
    ]
    if disposition is not None:
        args.extend(["--disposition", disposition])
    if reviewer is not None:
        args.extend(["--reviewer", reviewer])
    for link in evidence or []:
        args.extend(["--evidence-link", link])

    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.stdout
    return json.loads(result.stdout)
