from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from html import escape
from pathlib import Path
from time import perf_counter
from typing import Mapping, Optional, Sequence

from sqlalchemy.orm import Session

from .parcel_dossier import build_parcel_dossier
from .review_queue import RISK_BAND_PRECEDENCE, build_review_queue

RUN_TYPE_INVESTIGATION_REPORT = "investigation_report"
INVESTIGATION_REPORT_VERSION_TAG = "investigation_report_v1"
DEFAULT_REPORT_HTML_PATH = Path("data/reports/investigation_report_v1.html")


@dataclass(frozen=True)
class _DossierReportRow:
    anchor_id: str
    queue_rank: int
    score_id: int
    parcel_id: str
    year: int
    score_value: str
    risk_band: str
    primary_reason_code: str
    review_status: str
    review_disposition: str
    dossier_payload: Mapping[str, object]


def build_investigation_report(
    session: Session,
    *,
    html_out: Path,
    top: int = 100,
    feature_version: str = "feature_v1",
    ruleset_version: str = "scoring_rules_v1",
    requires_review_only: bool = True,
) -> dict[str, object]:
    report_build_start = perf_counter()
    request = {
        "top": top,
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
        "requires_review_only": requires_review_only,
        "html_out": str(html_out),
    }

    queue_payload = build_review_queue(
        session,
        top=top,
        feature_version=feature_version,
        ruleset_version=ruleset_version,
        requires_review_only=requires_review_only,
    )
    queue_run = _mapping(queue_payload.get("run"))
    if queue_run.get("status") != "succeeded":
        queue_error = _mapping(queue_payload.get("error"))
        return _failure_payload(
            request=request,
            code="upstream_review_queue_failed",
            message=(
                "Failed to build report because review queue generation failed: "
                f"{_text(queue_error.get('code'))}: {_text(queue_error.get('message'))}"
            ),
        )

    queue_rows_raw = queue_payload.get("rows")
    queue_rows = (
        [row for row in queue_rows_raw if isinstance(row, Mapping)]
        if isinstance(queue_rows_raw, list)
        else []
    )

    dossier_rows: list[_DossierReportRow] = []
    dossier_failures: list[dict[str, object]] = []
    dossier_build_seconds: list[float] = []
    reason_counts: Counter[str] = Counter()
    risk_band_counts: Counter[str] = Counter()
    for row in queue_rows:
        queue_rank = _as_int(row.get("queue_rank"))
        score_id = _as_int(row.get("score_id"))
        year = _as_int(row.get("year"))
        parcel_id = _text(row.get("parcel_id")).strip()
        if queue_rank is None or score_id is None or year is None or not parcel_id:
            continue
        score_value = _text(row.get("score_value"))
        risk_band = _text(row.get("risk_band")) or "(none)"
        primary_reason_code = _text(row.get("primary_reason_code")) or "(none)"
        review_status = _text(row.get("review_status")) or "unreviewed"
        review_disposition = _text(row.get("review_disposition")) or "(none)"

        reason_counts[primary_reason_code] += 1
        risk_band_counts[risk_band] += 1

        dossier_start = perf_counter()
        dossier_payload = build_parcel_dossier(
            session,
            parcel_id=parcel_id,
            years=[year],
            feature_version=feature_version,
            ruleset_version=ruleset_version,
        )
        dossier_build_seconds.append(perf_counter() - dossier_start)
        dossier_run = _mapping(dossier_payload.get("run"))
        if dossier_run.get("status") != "succeeded":
            dossier_error = _mapping(dossier_payload.get("error"))
            dossier_failures.append(
                {
                    "score_id": score_id,
                    "parcel_id": parcel_id,
                    "year": year,
                    "error_code": _text(dossier_error.get("code")),
                    "error_message": _text(dossier_error.get("message")),
                }
            )

        dossier_rows.append(
            _DossierReportRow(
                anchor_id=f"dossier-score-{score_id}",
                queue_rank=queue_rank,
                score_id=score_id,
                parcel_id=parcel_id,
                year=year,
                score_value=score_value,
                risk_band=risk_band,
                primary_reason_code=primary_reason_code,
                review_status=review_status,
                review_disposition=review_disposition,
                dossier_payload=dossier_payload,
            )
        )

    reason_summary = sorted(
        reason_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )
    risk_summary = sorted(
        risk_band_counts.items(),
        key=lambda item: (RISK_BAND_PRECEDENCE.get(item[0], 99), item[0]),
    )

    html_text = _render_html(
        queue_summary=_mapping(queue_payload.get("summary")),
        rows=dossier_rows,
        reason_summary=reason_summary,
        risk_summary=risk_summary,
        triage_notice=(
            "Risk signals are triage guidance, not proof. "
            "Analyst review and case evidence determine findings."
        ),
    )

    html_out.parent.mkdir(parents=True, exist_ok=True)
    html_out.write_text(html_text, encoding="utf-8")
    report_build_seconds_total = perf_counter() - report_build_start
    dossier_seconds_total = sum(dossier_build_seconds)
    dossier_seconds_max = max(dossier_build_seconds) if dossier_build_seconds else 0.0
    dossier_seconds_avg = (
        dossier_seconds_total / len(dossier_build_seconds)
        if dossier_build_seconds
        else 0.0
    )

    return {
        "run": _run_payload("succeeded"),
        "request": request,
        "summary": {
            "queue_row_count": len(dossier_rows),
            "dossier_failure_count": len(dossier_failures),
            "reason_code_bucket_count": len(reason_summary),
            "risk_band_bucket_count": len(risk_summary),
            "html_size_bytes": len(html_text.encode("utf-8")),
        },
        "artifacts": {
            "html_path": str(html_out),
        },
        "diagnostics": {
            "queue_summary": _mapping(queue_payload.get("summary")),
            "dossier_failures": dossier_failures,
            "timing_seconds": {
                "report_build_total": round(report_build_seconds_total, 6),
                "dossier_build_total": round(dossier_seconds_total, 6),
                "dossier_build_avg": round(dossier_seconds_avg, 6),
                "dossier_build_max": round(dossier_seconds_max, 6),
            },
        },
        "error": None,
    }


def _run_payload(status: str) -> dict[str, object]:
    return {
        "run_id": None,
        "run_persisted": False,
        "run_type": RUN_TYPE_INVESTIGATION_REPORT,
        "version_tag": INVESTIGATION_REPORT_VERSION_TAG,
        "status": status,
    }


def _failure_payload(
    *,
    request: Mapping[str, object],
    code: str,
    message: str,
) -> dict[str, object]:
    return {
        "run": _run_payload("failed"),
        "request": request,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _render_html(
    *,
    queue_summary: Mapping[str, object],
    rows: Sequence[_DossierReportRow],
    reason_summary: Sequence[tuple[str, int]],
    risk_summary: Sequence[tuple[str, int]],
    triage_notice: str,
) -> str:
    lines: list[str] = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8">',
        '  <meta name="viewport" content="width=device-width, initial-scale=1">',
        "  <title>AccessDane Investigation Report v1</title>",
        "  <style>",
        "    :root {",
        "      --bg: #f4efe3;",
        "      --ink: #18222d;",
        "      --muted: #5a6878;",
        "      --panel: #ffffff;",
        "      --line: #d2c7b4;",
        "      --accent: #8f2f26;",
        "      --accent-soft: #fae1de;",
        "    }",
        "    body {",
        "      margin: 0;",
        (
            "      background: radial-gradient("
            "circle at 15% 0%, #efe4cf 0%, var(--bg) 40%, #efe7d7 100%"
            ");"
        ),
        "      color: var(--ink);",
        '      font-family: "Source Serif 4", Georgia, serif;',
        "      line-height: 1.45;",
        "    }",
        "    main {",
        "      max-width: 1100px;",
        "      margin: 0 auto;",
        "      padding: 2rem 1rem 4rem;",
        "    }",
        "    h1, h2, h3, h4 {",
        "      margin: 0 0 0.75rem;",
        "      line-height: 1.2;",
        "    }",
        "    h1 { font-size: 2rem; }",
        "    h2 {",
        "      font-size: 1.35rem;",
        "      margin-top: 2rem;",
        "    }",
        "    section, article {",
        "      background: var(--panel);",
        "      border: 1px solid var(--line);",
        "      border-radius: 10px;",
        "      padding: 1rem 1.1rem;",
        "      margin: 1rem 0;",
        "      box-shadow: 0 8px 20px rgba(24, 34, 45, 0.06);",
        "    }",
        "    .notice {",
        "      border-left: 5px solid var(--accent);",
        "      background: var(--accent-soft);",
        "    }",
        "    .meta { color: var(--muted); font-size: 0.95rem; }",
        "    table {",
        "      width: 100%;",
        "      border-collapse: collapse;",
        "      margin-top: 0.75rem;",
        "    }",
        "    th, td {",
        "      border-bottom: 1px solid var(--line);",
        "      text-align: left;",
        "      padding: 0.5rem;",
        "      vertical-align: top;",
        "    }",
        "    th {",
        "      font-size: 0.85rem;",
        "      letter-spacing: 0.03em;",
        "      text-transform: uppercase;",
        "      color: var(--muted);",
        "    }",
        "    a { color: #0e4f7a; }",
        "    code {",
        "      background: #f2f3f5;",
        "      border-radius: 4px;",
        "      padding: 0.08rem 0.28rem;",
        "    }",
        "    .pill {",
        "      display: inline-block;",
        "      background: #eef4ff;",
        "      border: 1px solid #c7d6f0;",
        "      border-radius: 999px;",
        "      padding: 0.08rem 0.55rem;",
        "      margin-right: 0.45rem;",
        "      font-size: 0.85rem;",
        "    }",
        "    @media (max-width: 760px) {",
        "      th, td { font-size: 0.92rem; }",
        "      h1 { font-size: 1.6rem; }",
        "    }",
        "  </style>",
        "</head>",
        '<body id="top">',
        "  <main>",
        "    <h1>AccessDane Investigation Report</h1>",
        '    <p class="meta">Version: investigation_report_v1</p>',
        f'    <section class="notice"><p>{escape(triage_notice)}</p></section>',
        '    <section id="queue-summary">',
        "      <h2>Top Review Queue</h2>",
        '      <p class="meta">',
        f"        Candidates: {escape(_text(queue_summary.get('candidate_count')))} |",
        f"        Filtered: {escape(_text(queue_summary.get('filtered_count')))} |",
        f"        Skipped: {escape(_text(queue_summary.get('skipped_count')))} |",
        f"        Returned: {escape(_text(queue_summary.get('returned_count')))}",
        "      </p>",
        "      <table>",
        "        <thead>",
        "          <tr>",
        (
            "            <th>Rank</th><th>Parcel</th><th>Year</th>"
            "<th>Score</th><th>Risk</th><th>Review status</th>"
            "<th>Disposition</th><th>Primary reason</th><th>Dossier</th>"
        ),
        "          </tr>",
        "        </thead>",
        "        <tbody>",
    ]

    for row in rows:
        lines.extend(
            [
                "          <tr>",
                f"            <td>{row.queue_rank}</td>",
                f"            <td>{escape(row.parcel_id)}</td>",
                f"            <td>{row.year}</td>",
                f"            <td>{escape(row.score_value)}</td>",
                f"            <td>{escape(row.risk_band)}</td>",
                f"            <td>{escape(row.review_status)}</td>",
                f"            <td>{escape(row.review_disposition)}</td>",
                f"            <td>{escape(row.primary_reason_code)}</td>",
                (
                    "            <td>"
                    f'<a href="#{escape(row.anchor_id)}">Open dossier</a>'
                    "</td>"
                ),
                "          </tr>",
            ]
        )

    lines.extend(
        [
            "        </tbody>",
            "      </table>",
            "    </section>",
            '    <section id="reason-summary">',
            "      <h2>Reason Code Summary</h2>",
            "      <p>",
        ]
    )
    for risk_band, count in risk_summary:
        lines.append(f'        <span class="pill">{escape(risk_band)}: {count}</span>')
    lines.extend(
        [
            "      </p>",
            "      <table>",
            "        <thead>",
            "          <tr><th>Reason code</th><th>Count</th></tr>",
            "        </thead>",
            "        <tbody>",
        ]
    )
    for reason_code, count in reason_summary:
        lines.append(
            f"          <tr><td>{escape(reason_code)}</td><td>{count}</td></tr>"
        )
    lines.extend(
        [
            "        </tbody>",
            "      </table>",
            "    </section>",
            '    <section id="dossier-drill-in">',
            "      <h2>Parcel Dossier Drill-In</h2>",
        ]
    )

    for row in rows:
        dossier_run = _mapping(row.dossier_payload.get("run"))
        dossier_error = _mapping(row.dossier_payload.get("error"))
        parcel_payload = _mapping(row.dossier_payload.get("parcel"))
        sections = _mapping(row.dossier_payload.get("sections"))
        timeline = _mapping(row.dossier_payload.get("timeline"))

        lines.extend(
            [
                f'      <article id="{escape(row.anchor_id)}">',
                (
                    f"        <h3>Rank {row.queue_rank}: "
                    f"Parcel {escape(row.parcel_id)} ({row.year})</h3>"
                ),
                "        <p>",
                (
                    '          <a href="#top">Back to queue</a> | '
                    "<code>accessdane parcel-dossier --id "
                    f"{escape(row.parcel_id)} --year {row.year}</code>"
                ),
                "        </p>",
            ]
        )

        if dossier_run.get("status") != "succeeded":
            lines.extend(
                [
                    "        <p>",
                    (
                        "          Dossier unavailable: "
                        f"{escape(_text(dossier_error.get('code')))} - "
                        f"{escape(_text(dossier_error.get('message')))}"
                    ),
                    "        </p>",
                    "      </article>",
                ]
            )
            continue

        lines.extend(
            [
                '        <p class="meta">',
                (
                    "          Municipality: "
                    f"{escape(_text(parcel_payload.get('municipality_name')))} |"
                ),
                (
                    "          Owner: "
                    f"{escape(_text(parcel_payload.get('current_owner_name')))} |"
                ),
                f"          TRS: {escape(_text(parcel_payload.get('trs_code')))}",
                "        </p>",
                "        <p>",
                (
                    "          Timeline events: "
                    f"{escape(_text(timeline.get('event_count')))}"
                ),
                "        </p>",
                "        <table>",
                "          <thead>",
                (
                    "            <tr><th>Section</th><th>Status</th><th>Rows</th>"
                    "<th>Message</th></tr>"
                ),
                "          </thead>",
                "          <tbody>",
            ]
        )

        section_order = row.dossier_payload.get("section_order")
        if isinstance(section_order, list):
            ordered_sections = [item for item in section_order if isinstance(item, str)]
        else:
            ordered_sections = sorted(sections)
        for section_name in ordered_sections:
            section_payload = _mapping(sections.get(section_name))
            section_summary = _mapping(section_payload.get("summary"))
            lines.append(
                "            <tr>"
                f"<td>{escape(section_name)}</td>"
                f"<td>{escape(_text(section_payload.get('status')))}</td>"
                f"<td>{escape(_text(section_summary.get('row_count')))}</td>"
                f"<td>{escape(_text(section_payload.get('message')))}</td>"
                "</tr>"
            )

        lines.extend(
            [
                "          </tbody>",
                "        </table>",
                "      </article>",
            ]
        )

    lines.extend(
        [
            "    </section>",
            "  </main>",
            "</body>",
            "</html>",
        ]
    )
    return "\n".join(lines) + "\n"


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _as_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None
