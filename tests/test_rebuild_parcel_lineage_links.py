from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select

from accessdane_audit import cli
from accessdane_audit.db import init_db, session_scope
from accessdane_audit.models import Fetch, Parcel, ParcelLineageLink


def _write_raw_html(tmp_path: Path, name: str, html: str) -> Path:
    path = tmp_path / f"{name}.html"
    path.write_text(html, encoding="utf-8")
    return path


def _build_lineage_html(
    *,
    parent_rows: list[tuple[str, str, str | None]] | None = None,
    child_rows: list[tuple[str, str, str | None]] | None = None,
) -> str:
    def _render_modal(
        modal_id: str,
        rows: list[tuple[str, str, str | None]],
    ) -> str:
        if not rows:
            return ""
        parts = [
            '<div class="parcelhistory">'
            "<div>"
            f'<a href="/{related_parcel_id}">{related_parcel_id}</a>'
            f'<span class="badge">{status}</span>'
            "</div>" + (f"<div>{note}</div>" if note else "") + "</div>"
            for related_parcel_id, status, note in rows
        ]
        body = "".join(parts)
        return (
            f'<div id="{modal_id}" class="modal hide fade">'
            f'<div class="modal-body">{body}</div>'
            "</div>"
        )

    return (
        "<html><body>"
        '<div id="parcel_detail_heading">Parcel Number - 154/0610-023-2040-1</div>'
        f'{_render_modal("modalParcelHistoryParents", parent_rows or [])}'
        f'{_render_modal("modalParcelHistoryChildren", child_rows or [])}'
        "</body></html>"
    )


def test_rebuild_parcel_lineage_links_full_rebuild_removes_stale_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_lineage_full.sqlite"
    database_url = f"sqlite:///{db_path}"
    active_parcel_id = "active-lineage-parcel"
    stale_parcel_id = "stale-lineage-parcel"
    raw_path = _write_raw_html(
        tmp_path,
        "active",
        _build_lineage_html(
            parent_rows=[("061002320159", "Retired", "Original parent")],
            child_rows=[("061002320501", "Current", "Current child")],
        ),
    )

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=active_parcel_id), Parcel(id=stale_parcel_id)])
        session.add(
            Fetch(
                parcel_id=active_parcel_id,
                url=f"https://example.test/{active_parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
                fetched_at=datetime.now(timezone.utc),
                parsed_at=datetime.now(timezone.utc),
            )
        )
        session.add(
            ParcelLineageLink(
                parcel_id=stale_parcel_id,
                related_parcel_id="000000000001",
                relationship_type="parent",
                built_at=datetime.now(timezone.utc),
            )
        )

    summary = cli.rebuild_parcel_lineage_links(database_url, raw_dir=tmp_path)

    with session_scope(database_url) as session:
        links = (
            session.execute(
                select(ParcelLineageLink).order_by(
                    ParcelLineageLink.parcel_id,
                    ParcelLineageLink.relationship_type,
                    ParcelLineageLink.related_parcel_id,
                )
            )
            .scalars()
            .all()
        )

    assert summary.selected_parcels == 2
    assert summary.eligible_fetches_scanned == 1
    assert summary.rows_deleted == 1
    assert summary.rows_written == 2
    assert summary.skipped_fetches == 0
    assert summary.parcel_failures == 0
    assert [(link.parcel_id, link.related_parcel_id) for link in links] == [
        (active_parcel_id, "061002320501"),
        (active_parcel_id, "061002320159"),
    ]


def test_rebuild_parcel_lineage_links_scoped_rebuild_replaces_only_selected_parcel(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_lineage_scoped.sqlite"
    database_url = f"sqlite:///{db_path}"
    selected_parcel_id = "selected-lineage-parcel"
    other_parcel_id = "other-lineage-parcel"
    raw_path = _write_raw_html(
        tmp_path,
        "selected",
        _build_lineage_html(
            parent_rows=[("061002320159", "Retired", "Replacement parent")]
        ),
    )
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=selected_parcel_id), Parcel(id=other_parcel_id)])
        session.add(
            Fetch(
                parcel_id=selected_parcel_id,
                url=f"https://example.test/{selected_parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
                fetched_at=now,
                parsed_at=now,
            )
        )
        session.add_all(
            [
                ParcelLineageLink(
                    parcel_id=selected_parcel_id,
                    related_parcel_id="000000000001",
                    relationship_type="parent",
                    built_at=now,
                ),
                ParcelLineageLink(
                    parcel_id=other_parcel_id,
                    related_parcel_id="000000000002",
                    relationship_type="child",
                    built_at=now,
                ),
            ]
        )

    summary = cli.rebuild_parcel_lineage_links(
        database_url,
        parcel_ids=[selected_parcel_id],
        raw_dir=tmp_path,
    )

    with session_scope(database_url) as session:
        links = (
            session.execute(
                select(ParcelLineageLink).order_by(
                    ParcelLineageLink.parcel_id,
                    ParcelLineageLink.relationship_type,
                    ParcelLineageLink.related_parcel_id,
                )
            )
            .scalars()
            .all()
        )

    assert summary.selected_parcels == 1
    assert summary.eligible_fetches_scanned == 1
    assert summary.rows_deleted == 1
    assert summary.rows_written == 1
    assert summary.parcel_failures == 0
    assert [
        (link.parcel_id, link.related_parcel_id, link.relationship_type)
        for link in links
    ] == [
        (other_parcel_id, "000000000002", "child"),
        (selected_parcel_id, "061002320159", "parent"),
    ]


def test_rebuild_parcel_lineage_links_uses_latest_fetch_metadata_for_duplicates(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_lineage_precedence.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "lineage-precedence-parcel"
    older_raw_path = _write_raw_html(
        tmp_path,
        "older",
        _build_lineage_html(
            parent_rows=[("061002320159", "Retired", "Older lineage note")]
        ),
    )
    newer_raw_path = _write_raw_html(
        tmp_path,
        "newer",
        _build_lineage_html(
            parent_rows=[("061002320159", "Current", "Newer lineage note")]
        ),
    )

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add(Parcel(id=parcel_id))
        older_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/older",
            status_code=200,
            raw_path=str(older_raw_path),
            fetched_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            parsed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        newer_fetch = Fetch(
            parcel_id=parcel_id,
            url="https://example.test/newer",
            status_code=200,
            raw_path=str(newer_raw_path),
            fetched_at=datetime(2025, 2, 1, tzinfo=timezone.utc),
            parsed_at=datetime(2025, 2, 1, tzinfo=timezone.utc),
        )
        session.add_all([older_fetch, newer_fetch])

    summary = cli.rebuild_parcel_lineage_links(database_url, raw_dir=tmp_path)

    with session_scope(database_url) as session:
        stored = session.get(
            ParcelLineageLink,
            (parcel_id, "061002320159", "parent"),
        )

    assert summary.selected_parcels == 1
    assert summary.eligible_fetches_scanned == 2
    assert summary.rows_written == 1
    assert summary.parcel_failures == 0
    assert stored is not None
    assert stored.related_parcel_status == "Current"
    assert stored.relationship_note == "Newer lineage note"
    assert stored.source_fetch_id is not None


def test_rebuild_parcel_lineage_links_deletes_stale_rows_when_no_current_links(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rebuild_lineage_empty.sqlite"
    database_url = f"sqlite:///{db_path}"
    parcel_id = "lineage-empty-parcel"
    other_parcel_id = "lineage-other-parcel"
    raw_path = _write_raw_html(tmp_path, "empty", _build_lineage_html())
    now = datetime.now(timezone.utc)

    init_db(database_url)

    with session_scope(database_url) as session:
        session.add_all([Parcel(id=parcel_id), Parcel(id=other_parcel_id)])
        session.add(
            Fetch(
                parcel_id=parcel_id,
                url=f"https://example.test/{parcel_id}",
                status_code=200,
                raw_path=str(raw_path),
                fetched_at=now,
                parsed_at=now,
            )
        )
        session.add_all(
            [
                ParcelLineageLink(
                    parcel_id=parcel_id,
                    related_parcel_id="000000000001",
                    relationship_type="parent",
                    built_at=now,
                ),
                ParcelLineageLink(
                    parcel_id=other_parcel_id,
                    related_parcel_id="000000000002",
                    relationship_type="child",
                    built_at=now - timedelta(days=1),
                ),
            ]
        )

    summary = cli.rebuild_parcel_lineage_links(
        database_url,
        parcel_ids=[parcel_id],
        raw_dir=tmp_path,
    )

    with session_scope(database_url) as session:
        links = (
            session.execute(
                select(ParcelLineageLink).order_by(
                    ParcelLineageLink.parcel_id,
                    ParcelLineageLink.relationship_type,
                    ParcelLineageLink.related_parcel_id,
                )
            )
            .scalars()
            .all()
        )

    assert summary.selected_parcels == 1
    assert summary.eligible_fetches_scanned == 1
    assert summary.rows_deleted == 1
    assert summary.rows_written == 0
    assert summary.skipped_fetches == 0
    assert summary.parcel_failures == 0
    assert [
        (link.parcel_id, link.related_parcel_id, link.relationship_type)
        for link in links
    ] == [(other_parcel_id, "000000000002", "child")]
