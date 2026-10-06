"""BCO live promotions v0.8.33.

Targeted promotion layer requested after the 2026-10-06 deep BCO review:
- promote the already-frozen stacking-brake v1 to live admission control for
  ADDITIONAL long slices only (the first trade of a fresh basket is untouched);
- harden AI Observer retry recovery for retryable HTTP/network and structured
  response/JSON failures, while retaining bounded attempts and zero execution
  authority;
- include the dedicated Entry Lab shadow dataset in both BCO ZIP exports.

This module intentionally wraps the existing v0.8.32 core rather than changing
its exit manager, harvest ladder, short research, sizing or broker safety rules.
"""
from __future__ import annotations

import csv
import io
import json
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import Response

import analysis_entrypoint as base

core = base.core

PROMOTION_VERSION = "bco_v0.8.33_stacking_brake_live_2026_10_06"
RELEASE_VERSION = "0.8.33"

# Keep existing dashboard/analysis labels truthful without altering strategy code.
core.APP_VERSION = RELEASE_VERSION
core.APP_NAME = "Project Exit Plan — BCO v0.8.33 — Live Stacking Brake + Entry Lab Shadow"
core.POLICY_VERSION = PROMOTION_VERSION
base.VISIBLE_RELEASE_VERSION = RELEASE_VERSION

# -----------------------------------------------------------------------------
# 1) LIVE PROMOTION — frozen stacking-brake v1
# -----------------------------------------------------------------------------
# The original research function remains the single source of truth for the
# frozen rule. We only add an admission gate around create_trade. create_trade
# is reached only after the existing production rules have already decided the
# signal is eligible, so this can only WITHHOLD an extra slice; it cannot create
# a new signal. Flat baskets bypass the gate completely.
_original_process_signal = core.process_signal
_original_create_trade = core.create_trade
_signal_context = threading.local()


def _promoted_process_signal(raw_signal_id: int, payload: Dict[str, Any]):
    _signal_context.raw_signal_id = int(raw_signal_id)
    _signal_context.payload = payload if isinstance(payload, dict) else {}
    try:
        return _original_process_signal(raw_signal_id, payload)
    finally:
        _signal_context.raw_signal_id = None
        _signal_context.payload = None


def _promoted_create_trade(conn, raw_signal_id: int, signal: Dict[str, Any], cycle_id: str) -> Optional[str]:
    metrics = core.basket_metrics(conn)
    open_count = int(metrics.get("open_count") or 0)

    # Explicit design decision from the review: never block the first trade of a
    # new BCO basket. The promoted brake governs stacking only.
    if open_count <= 0:
        return _original_create_trade(conn, raw_signal_id, signal, cycle_id)

    payload = getattr(_signal_context, "payload", None)
    if not isinstance(payload, dict) or not payload:
        # Fail open if the immutable point-in-time payload is unexpectedly absent.
        # Existing production rules remain authoritative rather than inventing a
        # state from incomplete information.
        return _original_create_trade(conn, raw_signal_id, signal, cycle_id)

    candidate = bool(core.bco_long_candidate(payload))
    support = core.candidate_support(conn, max_raw_signal_id=int(raw_signal_id))
    state = core.ensure_cycle(conn, core.safe_str(signal.get("timestamp_readable")), metrics)
    old_hwm = float(core.safe_float(state.get("high_water_R")) or 0.0)
    basket_r = float(metrics.get("basket_R") or 0.0)
    hwm = max(old_hwm, basket_r)
    losing_pct = float(metrics.get("losing_pct") or 0.0)
    flags = core.latest_payload_flags(payload)
    score, status, _raw_action, _reasons, giveback = core.calculate_tide_turn_status(
        candidate,
        int(support.get("candidate_true_last_3") or 0),
        hwm,
        basket_r,
        losing_pct,
        flags["close20"],
        flags["close50"],
        flags["hist_up"],
        flags["rsi_up"],
        flags["ctx_bull"],
        flags["d_bull"],
    )
    short_ctx = core._bco_short_research_context(conn, int(raw_signal_id), payload)
    decision = core._bco_stacking_brake_decision(
        tide_status=status,
        open_count=open_count,
        basket_r=basket_r,
        giveback_pct=float(giveback or 0.0),
        losing_pct=losing_pct,
        short_candidate=bool(short_ctx.get("short_candidate")),
    )

    if core.safe_str(decision.get("primary_decision")).upper() == "BLOCK":
        # The existing prospective recorder runs immediately after the attempted
        # entry and will persist primary_decision=BLOCK with entry_created=false,
        # giving us a like-for-like audit trail without creating a second schema.
        return None

    return _original_create_trade(conn, raw_signal_id, signal, cycle_id)


core.process_signal = _promoted_process_signal
core.create_trade = _promoted_create_trade
core.BCO_STACKING_BRAKE_EXECUTION_AUTHORITY = True

# Make the existing research summary/dashboard disclose that the frozen primary
# rule is now live, while retaining all historical challenger data unchanged.
_original_stacking_summary = core.bco_stacking_brake_summary
_original_stacking_html = core.build_bco_stacking_brake_html


def _promoted_stacking_summary(limit: int = 5000):
    out = _original_stacking_summary(limit)
    out["research_only"] = False
    out["execution_authority"] = True
    out["live_promotion"] = {
        "enabled": True,
        "version": PROMOTION_VERSION,
        "scope": "ADDITIONAL_BCO_LONG_STACKING_ONLY",
        "fresh_basket_first_trade_bypass": True,
        "frozen_rule_reused": core.BCO_STACKING_BRAKE_VERSION,
        "short_execution_authority": False,
    }
    return out


def _promoted_stacking_html() -> str:
    html = _original_stacking_html()
    html = html.replace(
        "<strong>RESEARCH ONLY · ZERO execution authority.</strong>",
        "<strong>LIVE PROMOTED · additional-long admission control.</strong>",
    )
    html = html.replace(
        "Production entry_allowed remains untouched. The frozen v1 challenger asks whether a fresh LONG should have been withheld once an existing basket was visibly deteriorating. No historical backfill.",
        "The frozen v1 rule now blocks additional BCO LONG slices when an existing basket is visibly deteriorating. The first trade of a fresh basket remains unchanged. Historical research remains untouched.",
    )
    return html


core.bco_stacking_brake_summary = _promoted_stacking_summary
core.build_bco_stacking_brake_html = _promoted_stacking_html

# -----------------------------------------------------------------------------
# 2) AI OBSERVER — durable bounded recovery of retryable/error holes
# -----------------------------------------------------------------------------
# AI stays research-only. These patches only make completion more reliable.
_original_ai_call = core._aiobs_openai_call
_original_retry_scan = core._aiobs_enqueue_due_retries


def _promoted_ai_call(model_input, raw_signal_id):
    out = _original_ai_call(model_input, raw_signal_id)
    if not out.get("ok"):
        err = core.safe_str(out.get("error")).lower()
        status = int(core.safe_float(out.get("http_status")) or 0)
        # A valid HTTP 200 with malformed/incomplete structured output is
        # transient from the collector's perspective. Retry it just like 429/5xx,
        # still bounded by AI_SHADOW_RETRY_MAX_ATTEMPTS.
        structured_failure = status == 200 and any(
            token in err for token in (
                "structured regime decision",
                "jsondecodeerror",
                "json decode",
                "expecting value",
                "unterminated string",
            )
        )
        if structured_failure:
            out["retryable"] = True
    return out


def _rearm_retryable_ai_errors() -> int:
    """Move recoverable terminal ERROR rows back into the durable retry queue."""
    core.ensure_ai_regime_observer_table()
    now = core.now_utc_iso()
    with core.get_conn() as conn:
        rows = conn.execute("""
            SELECT raw_signal_id
            FROM ai_regime_observer
            WHERE api_eligible=1
              AND UPPER(COALESCE(status,''))='ERROR'
              AND COALESCE(attempt_count,0) < ?
              AND COALESCE(snapshot_json,'')<>''
              AND (
                    COALESCE(retryable_error,0)=1
                 OR COALESCE(last_http_status,0) IN (429,500,502,503,504)
                 OR (COALESCE(last_http_status,0)=200 AND (
                        LOWER(COALESCE(error,'')) LIKE '%structured regime decision%'
                     OR LOWER(COALESCE(error,'')) LIKE '%json%'
                     OR LOWER(COALESCE(error,'')) LIKE '%expecting value%'
                    ))
                 OR LOWER(COALESCE(error,'')) LIKE '%urlerror%'
                 OR LOWER(COALESCE(error,'')) LIKE '%timeout%'
              )
            ORDER BY raw_signal_id ASC
            LIMIT ?
        """, (core.AI_SHADOW_RETRY_MAX_ATTEMPTS, core.AI_SHADOW_RETRY_BATCH)).fetchall()
        for row in rows:
            conn.execute("""
                UPDATE ai_regime_observer
                SET status='RETRY_WAIT',updated_at_utc=?,completed_at_utc=NULL,
                    next_retry_at_utc=?,retryable_error=1
                WHERE raw_signal_id=?
            """, (now, now, int(row["raw_signal_id"])))
        conn.commit()
    return len(rows)


def _promoted_retry_scan(force: bool = False):
    rearmed = 0
    try:
        rearmed = _rearm_retryable_ai_errors()
    except Exception:
        # Collector reliability must never impair deterministic trading.
        rearmed = 0
    out = _original_retry_scan(force=force)
    if isinstance(out, dict):
        out["rearmed_error_rows"] = rearmed
        out["execution_authority"] = False
    return out


core._aiobs_openai_call = _promoted_ai_call
core._aiobs_enqueue_due_retries = _promoted_retry_scan

# Kick a recovery scan immediately after the wrapper loads. This is read/write
# only to the AI research table; no broker path is involved.
try:
    _promoted_retry_scan(force=True)
    core._aiobs_ensure_worker()
except Exception:
    pass

# -----------------------------------------------------------------------------
# 3) ENTRY LAB — include dedicated raw dataset in both downloadable ZIPs
# -----------------------------------------------------------------------------
def _entry_lab_csv(limit: int = 100000) -> str:
    limit = max(1, min(int(limit), 100000))
    with core.get_conn() as conn:
        core.entry_lab.ensure_schema(conn)
        rows = core.fetchall_dict(conn.execute(
            "SELECT * FROM entry_lab_shadow WHERE UPPER(project)='BCO' OR UPPER(asset)='BCO' ORDER BY id ASC LIMIT ?",
            (limit,),
        ))
        conn.commit()
    out = io.StringIO()
    if not rows:
        out.write("note\nNo BCO Entry Lab rows yet\n")
        return out.getvalue()
    fields = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def _append_entry_lab_to_zip(base_response: Response, *, filename: str, limit: int) -> Response:
    body = bytes(getattr(base_response, "body", b"") or b"")
    buf = io.BytesIO(body)
    with zipfile.ZipFile(buf, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("entry-lab-shadow.csv", _entry_lab_csv(limit))
        zf.writestr("live-promotion-status.json", json.dumps({
            "release_version": RELEASE_VERSION,
            "promotion_version": PROMOTION_VERSION,
            "stacking_brake_live": True,
            "stacking_scope": "additional BCO long slices only",
            "fresh_basket_first_trade_bypass": True,
            "ai_observer_execution_authority": False,
            "entry_lab_execution_authority": False,
            "entry_lab_export_included": True,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Outermost app: override the two ZIP routes before mounting the existing wrapper.
app = FastAPI(title="Project Exit Plan — BCO v0.8.33 Promotion Wrapper")


@app.get("/export/bco-focused-research.zip")
def focused_research_zip(limit: int = 25000):
    return _append_entry_lab_to_zip(
        core.export_bco_focused_research_zip(limit),
        filename="bco-focused-research.zip",
        limit=limit,
    )


@app.get("/export/all.zip")
def all_analysis_zip():
    return _append_entry_lab_to_zip(
        core.export_all_zip(),
        filename="bco-live-analysis.zip",
        limit=100000,
    )


@app.get("/live-promotions/status")
def live_promotions_status():
    return {
        "status": "ok",
        "release_version": RELEASE_VERSION,
        "promotion_version": PROMOTION_VERSION,
        "stacking_brake": {
            "live": True,
            "frozen_rule": core.BCO_STACKING_BRAKE_VERSION,
            "scope": "additional BCO LONG slices only",
            "fresh_basket_first_trade_bypass": True,
        },
        "ai_observer": {
            "research_only": True,
            "execution_authority": False,
            "durable_retry_hardened": True,
            "max_attempts": core.AI_SHADOW_RETRY_MAX_ATTEMPTS,
        },
        "entry_lab": {
            "research_only": True,
            "execution_authority": False,
            "included_in_focused_research_zip": True,
            "included_in_full_analysis_zip": True,
        },
        "unchanged": ["ATR2 production exit manager", "MFE/classic shadows", "harvesting", "short execution", "risk sizing", "broker safety"],
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


app.mount("/", base.app)
