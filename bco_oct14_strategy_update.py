"""BCO v0.8.34 — RED/CRITICAL live stacking + Oct-14 research layers.

Approved 2026-10-14 after the chart-vs-bot audit and full-history stacking study.

LIVE CHANGE (admission only):
- additional BCO LONG slices are blocked only when the existing deterministic
  tide state is RED or CRITICAL;
- the first trade of a fresh basket still bypasses the stacking brake;
- the previous AMBER/confluence rule remains recorded as a research challenger.

RESEARCH-ONLY ADDITIONS:
- STRICT_8H_TREND + RED-only combined entry/stack challenger with fixed-horizon
  outcomes;
- basket exposure sensitivity in R per +1% BCO move, recorded on every signal;
- medium-HWM partial-banking shadow matrix (20% of profitable open R only),
  with no close/order authority.

UNCHANGED:
- 48h minimum + ATR2 production exit manager;
- MFE/classic exit shadows and 50R+ harvest ladder;
- risk sizing, hard stop, broker safety/reconciliation;
- BCO short lane and AI Observer remain research-only.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi.responses import Response

import bco_health_observability as existing
import live_promotions as promotion

core = existing.core
app = existing.app

RELEASE_VERSION = "0.8.34"
RESEARCH_VERSION = "bco_oct14_v1_2026_10_14"
POLICY_VERSION = "bco_v0.8.34_red_critical_stacking_2026_10_14"
HORIZONS = (6, 12, 24, 48, 72, 96)

# Small pre-declared research grid: extend the existing 20% harvest fraction to
# medium basket HWMs, but shadow only. Existing production harvesting starts at 50R.
PARTIAL_BANK_VARIANTS: Tuple[Tuple[str, float, float, float], ...] = (
    ("HWM5_GB40_BANK20", 5.0, 40.0, 0.20),
    ("HWM5_GB50_BANK20", 5.0, 50.0, 0.20),
    ("HWM10_GB40_BANK20", 10.0, 40.0, 0.20),
    ("HWM10_GB50_BANK20", 10.0, 50.0, 0.20),
    ("HWM20_GB40_BANK20", 20.0, 40.0, 0.20),
    ("HWM20_GB50_BANK20", 20.0, 50.0, 0.20),
)

promotion.RELEASE_VERSION = RELEASE_VERSION
promotion.PROMOTION_VERSION = POLICY_VERSION
promotion.base.VISIBLE_RELEASE_VERSION = RELEASE_VERSION
core.APP_VERSION = RELEASE_VERSION
core.POLICY_VERSION = POLICY_VERSION
core.APP_NAME = "Project Exit Plan — BCO v0.8.34 — RED-Only Stacking + Research"
core.BCO_STACKING_BRAKE_EXECUTION_AUTHORITY = True
core.BCO_STACKING_BRAKE_LIVE_RULE = "RED_CRITICAL_ONLY"
core.BCO_STRICT8H_RED_EXECUTION_AUTHORITY = False
core.BCO_MEDIUM_HWM_PARTIAL_BANK_EXECUTION_AUTHORITY = False
app.title = core.APP_NAME
app.version = RELEASE_VERSION


def _opt_bool(obj: Dict[str, Any], *keys: str) -> Optional[bool]:
    for key in keys:
        if key not in obj:
            continue
        v = obj.get(key)
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in {"1", "true", "yes", "y", "on", "bull", "bullish", "long"}:
            return True
        if s in {"0", "false", "no", "n", "off", "bear", "bearish", "short"}:
            return False
        return None
    return None


def _strict8h_state(payload: Dict[str, Any], candidate: bool) -> Dict[str, Any]:
    ctx = core.context_8h(payload)
    values = {
        "ctx_close_gt_ema20": _opt_bool(ctx, "ctx_close_gt_ema20", "close_gt_ema20"),
        "ctx_close_gt_ema50": _opt_bool(ctx, "ctx_close_gt_ema50", "close_gt_ema50"),
        "ctx_hist_up": _opt_bool(ctx, "ctx_hist_up", "hist_up"),
        "ctx_rsi_up": _opt_bool(ctx, "ctx_rsi_up", "rsi_up"),
        "ctx_bull_stack": _opt_bool(ctx, "ctx_bull_stack", "bull_stack"),
    }
    known = all(v is not None for v in values.values())
    passed = bool(candidate and known and all(bool(v) for v in values.values())) if known else None
    return {"known": known, "passed": passed, "features": values}


def _ensure_schema(conn) -> None:
    id_type = "BIGSERIAL PRIMARY KEY" if getattr(conn, "postgres", False) else "INTEGER PRIMARY KEY AUTOINCREMENT"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bco_oct14_entry_stack_research (
            id {id_type}, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            research_version TEXT NOT NULL, raw_signal_id BIGINT NOT NULL UNIQUE,
            signal_time TEXT, cycle_id TEXT, entry_price DOUBLE PRECISION, sl_pct DOUBLE PRECISION,
            open_count_before BIGINT, basket_R_before DOUBLE PRECISION, high_water_R DOUBLE PRECISION,
            giveback_pct DOUBLE PRECISION, losing_pct DOUBLE PRECISION, tide_score BIGINT, tide_status TEXT,
            red_only_block INTEGER, current_confluence_block INTEGER, current_confluence_reason TEXT,
            strict8h_known INTEGER, strict8h_pass INTEGER, strict8h_red_allow INTEGER,
            sensitivity_before_R_per_1pct DOUBLE PRECISION, sensitivity_if_added_R_per_1pct DOUBLE PRECISION,
            live_decision TEXT, live_entry_created INTEGER, live_trade_id TEXT, point_in_time_json TEXT,
            outcome_6h_r DOUBLE PRECISION, outcome_6h_mfe_r DOUBLE PRECISION, outcome_6h_mae_r DOUBLE PRECISION,
            outcome_12h_r DOUBLE PRECISION, outcome_12h_mfe_r DOUBLE PRECISION, outcome_12h_mae_r DOUBLE PRECISION,
            outcome_24h_r DOUBLE PRECISION, outcome_24h_mfe_r DOUBLE PRECISION, outcome_24h_mae_r DOUBLE PRECISION,
            outcome_48h_r DOUBLE PRECISION, outcome_48h_mfe_r DOUBLE PRECISION, outcome_48h_mae_r DOUBLE PRECISION,
            outcome_72h_r DOUBLE PRECISION, outcome_72h_mfe_r DOUBLE PRECISION, outcome_72h_mae_r DOUBLE PRECISION,
            outcome_96h_r DOUBLE PRECISION, outcome_96h_mfe_r DOUBLE PRECISION, outcome_96h_mae_r DOUBLE PRECISION
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_oct14_stack_status ON bco_oct14_entry_stack_research(tide_status,strict8h_pass)")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bco_basket_exposure_research (
            id {id_type}, created_at_utc TEXT NOT NULL, research_version TEXT NOT NULL,
            raw_signal_id BIGINT NOT NULL UNIQUE, signal_time TEXT, cycle_id TEXT,
            current_price DOUBLE PRECISION, open_count BIGINT, basket_R DOUBLE PRECISION,
            high_water_R DOUBLE PRECISION, giveback_pct DOUBLE PRECISION, profitable_open_R DOUBLE PRECISION,
            sensitivity_R_per_1pct DOUBLE PRECISION, max_trade_sensitivity_R_per_1pct DOUBLE PRECISION,
            point_in_time_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exposure_cycle ON bco_basket_exposure_research(cycle_id,raw_signal_id)")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bco_medium_hwm_partial_bank_shadow (
            id {id_type}, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            research_version TEXT NOT NULL, cycle_id TEXT NOT NULL, variant TEXT NOT NULL,
            trigger_raw_signal_id BIGINT NOT NULL, signal_time TEXT, arm_hwm_R DOUBLE PRECISION,
            trigger_giveback_pct DOUBLE PRECISION, bank_fraction DOUBLE PRECISION,
            high_water_R DOUBLE PRECISION, basket_R_at_trigger DOUBLE PRECISION,
            profitable_open_R DOUBLE PRECISION, theoretical_bank_R DOUBLE PRECISION,
            open_count BIGINT, sensitivity_R_per_1pct DOUBLE PRECISION,
            execution_authority INTEGER NOT NULL DEFAULT 0, point_in_time_json TEXT,
            observed_24h_basket_R DOUBLE PRECISION, observed_48h_basket_R DOUBLE PRECISION,
            observed_72h_basket_R DOUBLE PRECISION, observed_96h_basket_R DOUBLE PRECISION,
            cycle_ended_at_raw_signal_id BIGINT, UNIQUE(cycle_id,variant)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_partial_bank_trigger ON bco_medium_hwm_partial_bank_shadow(trigger_raw_signal_id)")


def _latest_market_price(conn) -> Optional[float]:
    row = core.fetchone_dict(conn.execute(
        "SELECT exec_close FROM raw_signals WHERE UPPER(pair)='BCO' AND exec_close IS NOT NULL ORDER BY id DESC LIMIT 1"
    )) or {}
    return core.safe_float(row.get("exec_close"))


def _open_trade_stats(conn, current_price: Optional[float]) -> Dict[str, Any]:
    rows = core.fetchall_dict(conn.execute(
        "SELECT trade_id,entry_price,current_price,sl_pct,current_R FROM trades "
        "WHERE UPPER(status)='OPEN' AND UPPER(direction)='LONG' ORDER BY id ASC"
    ))
    total_sensitivity = 0.0
    max_sensitivity = 0.0
    profitable_r = 0.0
    usable = 0
    for row in rows:
        entry = core.safe_float(row.get("entry_price"))
        sl_pct = core.safe_float(row.get("sl_pct"))
        px = current_price or core.safe_float(row.get("current_price")) or entry
        rr = float(core.safe_float(row.get("current_R")) or 0.0)
        profitable_r += max(rr, 0.0)
        if entry is None or entry <= 0 or sl_pct is None or sl_pct <= 0 or px is None or px <= 0:
            continue
        sens = (float(px) / float(entry)) / float(sl_pct)
        total_sensitivity += sens
        max_sensitivity = max(max_sensitivity, sens)
        usable += 1
    return {
        "open_count": len(rows), "usable_count": usable, "profitable_open_R": profitable_r,
        "sensitivity_R_per_1pct": total_sensitivity,
        "max_trade_sensitivity_R_per_1pct": max_sensitivity,
    }


def _candidate_point_in_time(conn, raw_signal_id: int, signal: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = core.basket_metrics(conn)
    open_count = int(metrics.get("open_count") or 0)
    candidate = bool(core.bco_long_candidate(payload))
    support = core.candidate_support(conn, max_raw_signal_id=int(raw_signal_id))
    state = core.ensure_cycle(conn, core.safe_str(signal.get("timestamp_readable")), metrics)
    old_hwm = float(core.safe_float(state.get("high_water_R")) or 0.0)
    basket_r = float(metrics.get("basket_R") or 0.0)
    hwm = max(old_hwm, basket_r)
    losing_pct = float(metrics.get("losing_pct") or 0.0)
    flags = core.latest_payload_flags(payload)
    score, status, _raw_action, reasons, giveback = core.calculate_tide_turn_status(
        candidate, int(support.get("candidate_true_last_3") or 0), hwm, basket_r, losing_pct,
        flags["close20"], flags["close50"], flags["hist_up"], flags["rsi_up"], flags["ctx_bull"], flags["d_bull"],
    )
    short_ctx = core._bco_short_research_context(conn, int(raw_signal_id), payload)
    confluence = core._bco_stacking_brake_decision(
        tide_status=status, open_count=open_count, basket_r=basket_r,
        giveback_pct=float(giveback or 0.0), losing_pct=losing_pct,
        short_candidate=bool(short_ctx.get("short_candidate")),
    )
    red_only_block = bool(open_count > 0 and core.safe_str(status).upper() in {"RED", "CRITICAL"})
    strict = _strict8h_state(payload, candidate)
    strict_red_allow = bool(strict.get("passed") and not red_only_block) if strict.get("known") else None
    entry_price = core.safe_float(signal.get("exec_close") or payload.get("exec_close") or payload.get("close") or signal.get("entry_price"))
    stats = _open_trade_stats(conn, entry_price)
    sl_pct = core.safe_float(signal.get("sl_pct")) or 3.5
    added_sens = (1.0 / float(sl_pct)) if entry_price is not None and entry_price > 0 and sl_pct and sl_pct > 0 else 0.0
    return {
        "candidate": candidate, "open_count": open_count, "basket_R": basket_r, "high_water_R": hwm,
        "giveback_pct": float(giveback or 0.0), "losing_pct": losing_pct, "tide_score": int(score or 0),
        "tide_status": core.safe_str(status).upper(), "tide_reasons": reasons, "red_only_block": red_only_block,
        "confluence": confluence, "strict8h": strict, "strict8h_red_allow": strict_red_allow,
        "entry_price": entry_price, "sl_pct": float(sl_pct or 3.5),
        "sensitivity_before_R_per_1pct": float(stats.get("sensitivity_R_per_1pct") or 0.0),
        "sensitivity_if_added_R_per_1pct": float(stats.get("sensitivity_R_per_1pct") or 0.0) + added_sens,
        "profitable_open_R": float(stats.get("profitable_open_R") or 0.0),
        "short_candidate": bool(short_ctx.get("short_candidate")),
    }


def _insert_entry_stack_research(conn, raw_signal_id: int, signal: Dict[str, Any], pit: Dict[str, Any]) -> None:
    _ensure_schema(conn)
    if conn.execute("SELECT id FROM bco_oct14_entry_stack_research WHERE raw_signal_id=? LIMIT 1", (int(raw_signal_id),)).fetchone():
        return
    strict = pit.get("strict8h") or {}
    confluence = pit.get("confluence") or {}
    live_decision = "BLOCK_RED_CRITICAL" if pit.get("red_only_block") else "ALLOW"
    conn.execute("""
        INSERT INTO bco_oct14_entry_stack_research(
            created_at_utc,updated_at_utc,research_version,raw_signal_id,signal_time,cycle_id,
            entry_price,sl_pct,open_count_before,basket_R_before,high_water_R,giveback_pct,losing_pct,
            tide_score,tide_status,red_only_block,current_confluence_block,current_confluence_reason,
            strict8h_known,strict8h_pass,strict8h_red_allow,sensitivity_before_R_per_1pct,
            sensitivity_if_added_R_per_1pct,live_decision,live_entry_created,live_trade_id,point_in_time_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        core.now_utc_iso(), core.now_utc_iso(), RESEARCH_VERSION, int(raw_signal_id),
        core.safe_str(signal.get("timestamp_readable")), core.safe_str(signal.get("cycle_id")),
        pit.get("entry_price"), pit.get("sl_pct"), int(pit.get("open_count") or 0), pit.get("basket_R"),
        pit.get("high_water_R"), pit.get("giveback_pct"), pit.get("losing_pct"), int(pit.get("tide_score") or 0),
        pit.get("tide_status"), 1 if pit.get("red_only_block") else 0,
        1 if core.safe_str(confluence.get("primary_decision")).upper() == "BLOCK" else 0,
        core.safe_str(confluence.get("primary_reason")), 1 if strict.get("known") else 0,
        (1 if strict.get("passed") else 0) if strict.get("known") else None,
        (1 if pit.get("strict8h_red_allow") else 0) if strict.get("known") else None,
        pit.get("sensitivity_before_R_per_1pct"), pit.get("sensitivity_if_added_R_per_1pct"), live_decision,
        0 if pit.get("red_only_block") else None, None,
        json.dumps({"future_data_included": False, **pit}, separators=(",", ":"), default=str),
    ))


def _mark_live_entry_result(conn, raw_signal_id: int, trade_id: Optional[str]) -> None:
    _ensure_schema(conn)
    conn.execute(
        "UPDATE bco_oct14_entry_stack_research SET updated_at_utc=?,live_entry_created=?,live_trade_id=? WHERE raw_signal_id=?",
        (core.now_utc_iso(), 1 if trade_id else 0, core.safe_str(trade_id), int(raw_signal_id)),
    )


def _red_only_create_trade(conn, raw_signal_id: int, signal: Dict[str, Any], cycle_id: str) -> Optional[str]:
    payload = getattr(promotion._signal_context, "payload", None)
    if not isinstance(payload, dict) or not payload:
        return promotion._original_create_trade(conn, raw_signal_id, signal, cycle_id)
    try:
        pit = _candidate_point_in_time(conn, int(raw_signal_id), signal, payload)
        signal_for_research = dict(signal or {})
        signal_for_research["cycle_id"] = cycle_id
        _insert_entry_stack_research(conn, int(raw_signal_id), signal_for_research, pit)
    except Exception as exc:
        try:
            core.log_event("oct14_research_capture_error", str(exc), {"raw_signal_id": int(raw_signal_id)})
        except Exception:
            pass
        pit = None
    if pit is None:
        try:
            metrics = core.basket_metrics(conn)
            open_count = int(metrics.get("open_count") or 0)
            if open_count <= 0:
                return promotion._original_create_trade(conn, raw_signal_id, signal, cycle_id)
            candidate = bool(core.bco_long_candidate(payload))
            support = core.candidate_support(conn, max_raw_signal_id=int(raw_signal_id))
            state = core.ensure_cycle(conn, core.safe_str(signal.get("timestamp_readable")), metrics)
            hwm = max(float(core.safe_float(state.get("high_water_R")) or 0.0), float(metrics.get("basket_R") or 0.0))
            flags = core.latest_payload_flags(payload)
            _score, status, _action, _reasons, _giveback = core.calculate_tide_turn_status(
                candidate, int(support.get("candidate_true_last_3") or 0), hwm,
                float(metrics.get("basket_R") or 0.0), float(metrics.get("losing_pct") or 0.0),
                flags["close20"], flags["close50"], flags["hist_up"], flags["rsi_up"], flags["ctx_bull"], flags["d_bull"],
            )
            red_block = core.safe_str(status).upper() in {"RED", "CRITICAL"}
        except Exception:
            return promotion._original_create_trade(conn, raw_signal_id, signal, cycle_id)
    else:
        red_block = bool(pit.get("red_only_block"))
    if red_block:
        try:
            _mark_live_entry_result(conn, int(raw_signal_id), None)
        except Exception:
            pass
        return None
    trade_id = promotion._original_create_trade(conn, raw_signal_id, signal, cycle_id)
    try:
        _mark_live_entry_result(conn, int(raw_signal_id), trade_id)
    except Exception:
        pass
    return trade_id


core.create_trade = _red_only_create_trade


def _update_entry_stack_outcomes(conn, max_rows: int = 1500) -> Dict[str, Any]:
    _ensure_schema(conn)
    rows = core.fetchall_dict(conn.execute("""
        SELECT * FROM bco_oct14_entry_stack_research WHERE outcome_96h_r IS NULL
        ORDER BY raw_signal_id ASC LIMIT ?
    """, (max(100, int(max_rows)),)))
    updated = 0
    for row in rows:
        rid = int(core.safe_float(row.get("raw_signal_id")) or 0)
        entry = core.safe_float(row.get("entry_price"))
        sl = core.safe_float(row.get("sl_pct"))
        if rid <= 0 or entry is None or entry <= 0 or sl is None or sl <= 0:
            continue
        future = core.fetchall_dict(conn.execute("""
            SELECT id,exec_close,exec_high,exec_low FROM raw_signals
            WHERE UPPER(pair)='BCO' AND id>? AND exec_close IS NOT NULL ORDER BY id ASC LIMIT 96
        """, (rid,)))
        if len(future) < 6:
            continue
        sets: List[str] = []
        params: List[Any] = []
        for h in HORIZONS:
            if len(future) < h or row.get(f"outcome_{h}h_r") is not None:
                continue
            seg = future[:h]
            close = core.safe_float(seg[-1].get("exec_close"))
            highs = [core.safe_float(x.get("exec_high")) for x in seg]
            lows = [core.safe_float(x.get("exec_low")) for x in seg]
            highs = [x for x in highs if x is not None]
            lows = [x for x in lows if x is not None]
            rr = ((float(close) / float(entry) - 1.0) * 100.0 / float(sl)) if close is not None else None
            mfe = ((max(highs) / float(entry) - 1.0) * 100.0 / float(sl)) if highs else None
            mae = ((min(lows) / float(entry) - 1.0) * 100.0 / float(sl)) if lows else None
            sets.extend([f"outcome_{h}h_r=?", f"outcome_{h}h_mfe_r=?", f"outcome_{h}h_mae_r=?"])
            params.extend([rr, mfe, mae])
        if sets:
            sets.append("updated_at_utc=?")
            params.extend([core.now_utc_iso(), row.get("id")])
            conn.execute("UPDATE bco_oct14_entry_stack_research SET " + ",".join(sets) + " WHERE id=?", tuple(params))
            updated += 1
    return {"updated": updated, "scanned": len(rows)}


def _capture_exposure_and_partial_bank(conn, raw_signal_id: int) -> Dict[str, Any]:
    _ensure_schema(conn)
    raw = core.fetchone_dict(conn.execute(
        "SELECT timestamp_readable,exec_close FROM raw_signals WHERE id=? LIMIT 1", (int(raw_signal_id),)
    )) or {}
    current_price = core.safe_float(raw.get("exec_close")) or _latest_market_price(conn)
    metrics = core.basket_metrics(conn)
    state = core.fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1")) or {}
    cycle_id = core.safe_str(state.get("cycle_id"))
    basket_r = float(metrics.get("basket_R") or 0.0)
    hwm = max(float(core.safe_float(state.get("high_water_R")) or 0.0), basket_r)
    giveback = ((hwm - basket_r) / hwm * 100.0) if hwm > 0 and basket_r < hwm else 0.0
    stats = _open_trade_stats(conn, current_price)
    exposure = {
        "raw_signal_id": int(raw_signal_id), "signal_time": core.safe_str(raw.get("timestamp_readable")),
        "cycle_id": cycle_id, "current_price": current_price, "open_count": int(stats.get("open_count") or 0),
        "basket_R": basket_r, "high_water_R": hwm, "giveback_pct": giveback,
        "profitable_open_R": float(stats.get("profitable_open_R") or 0.0),
        "sensitivity_R_per_1pct": float(stats.get("sensitivity_R_per_1pct") or 0.0),
        "max_trade_sensitivity_R_per_1pct": float(stats.get("max_trade_sensitivity_R_per_1pct") or 0.0),
    }
    if not conn.execute("SELECT id FROM bco_basket_exposure_research WHERE raw_signal_id=? LIMIT 1", (int(raw_signal_id),)).fetchone():
        conn.execute("""
            INSERT INTO bco_basket_exposure_research(
                created_at_utc,research_version,raw_signal_id,signal_time,cycle_id,current_price,open_count,
                basket_R,high_water_R,giveback_pct,profitable_open_R,sensitivity_R_per_1pct,
                max_trade_sensitivity_R_per_1pct,point_in_time_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            core.now_utc_iso(), RESEARCH_VERSION, int(raw_signal_id), exposure["signal_time"], cycle_id,
            current_price, exposure["open_count"], basket_r, hwm, giveback, exposure["profitable_open_R"],
            exposure["sensitivity_R_per_1pct"], exposure["max_trade_sensitivity_R_per_1pct"],
            json.dumps({"future_data_included": False, **exposure}, separators=(",", ":"), default=str),
        ))
    if cycle_id and exposure["open_count"] > 0 and 0.0 < hwm < 50.0 and basket_r > 0:
        for name, arm_hwm, trigger_gb, fraction in PARTIAL_BANK_VARIANTS:
            if hwm < arm_hwm or giveback < trigger_gb:
                continue
            if conn.execute(
                "SELECT id FROM bco_medium_hwm_partial_bank_shadow WHERE cycle_id=? AND variant=? LIMIT 1",
                (cycle_id, name),
            ).fetchone():
                continue
            theoretical_bank_r = exposure["profitable_open_R"] * fraction
            conn.execute("""
                INSERT INTO bco_medium_hwm_partial_bank_shadow(
                    created_at_utc,updated_at_utc,research_version,cycle_id,variant,trigger_raw_signal_id,
                    signal_time,arm_hwm_R,trigger_giveback_pct,bank_fraction,high_water_R,basket_R_at_trigger,
                    profitable_open_R,theoretical_bank_R,open_count,sensitivity_R_per_1pct,execution_authority,
                    point_in_time_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                core.now_utc_iso(), core.now_utc_iso(), RESEARCH_VERSION, cycle_id, name, int(raw_signal_id),
                exposure["signal_time"], arm_hwm, trigger_gb, fraction, hwm, basket_r,
                exposure["profitable_open_R"], theoretical_bank_r, exposure["open_count"],
                exposure["sensitivity_R_per_1pct"], 0,
                json.dumps({"future_data_included": False, "execution_authority": False, **exposure,
                            "variant": name, "arm_hwm_R": arm_hwm, "trigger_giveback_pct": trigger_gb,
                            "bank_fraction": fraction, "theoretical_bank_R": theoretical_bank_r},
                           separators=(",", ":"), default=str),
            ))
    pending = core.fetchall_dict(conn.execute("""
        SELECT * FROM bco_medium_hwm_partial_bank_shadow
        WHERE cycle_ended_at_raw_signal_id IS NULL ORDER BY trigger_raw_signal_id ASC LIMIT 500
    """))
    for row in pending:
        trigger_id = int(core.safe_float(row.get("trigger_raw_signal_id")) or 0)
        if trigger_id <= 0 or int(raw_signal_id) <= trigger_id:
            continue
        row_cycle = core.safe_str(row.get("cycle_id"))
        if row_cycle != cycle_id:
            conn.execute(
                "UPDATE bco_medium_hwm_partial_bank_shadow SET updated_at_utc=?,cycle_ended_at_raw_signal_id=? WHERE id=?",
                (core.now_utc_iso(), int(raw_signal_id), row.get("id")),
            )
            continue
        future_count_row = core.fetchone_dict(conn.execute(
            "SELECT COUNT(*) AS n FROM raw_signals WHERE UPPER(pair)='BCO' AND id>? AND id<=?",
            (trigger_id, int(raw_signal_id)),
        )) or {}
        elapsed = int(core.safe_float(future_count_row.get("n")) or 0)
        sets: List[str] = []
        params: List[Any] = []
        for h in (24, 48, 72, 96):
            col = f"observed_{h}h_basket_R"
            if elapsed >= h and row.get(col) is None:
                sets.append(f"{col}=?")
                params.append(basket_r)
        if sets:
            sets.append("updated_at_utc=?")
            params.extend([core.now_utc_iso(), row.get("id")])
            conn.execute("UPDATE bco_medium_hwm_partial_bank_shadow SET " + ",".join(sets) + " WHERE id=?", tuple(params))
    return exposure


_previous_process_signal = core.process_signal


def _oct14_process_signal(raw_signal_id: int, payload: Dict[str, Any]):
    out = _previous_process_signal(raw_signal_id, payload)
    try:
        with core.get_conn() as conn:
            _ensure_schema(conn)
            _capture_exposure_and_partial_bank(conn, int(raw_signal_id))
            _update_entry_stack_outcomes(conn)
    except Exception as exc:
        try:
            core.log_event("oct14_research_update_error", str(exc), {"raw_signal_id": int(raw_signal_id)})
        except Exception:
            pass
    return out


core.process_signal = _oct14_process_signal

_previous_stacking_summary = core.bco_stacking_brake_summary
_previous_stacking_html = core.build_bco_stacking_brake_html


def _oct14_stacking_summary(limit: int = 5000):
    out = _previous_stacking_summary(limit)
    out["research_only"] = False
    out["execution_authority"] = True
    out["live_promotion"] = {
        "enabled": True, "version": POLICY_VERSION, "scope": "ADDITIONAL_BCO_LONG_STACKING_ONLY",
        "execution_rule": "RED_CRITICAL_ONLY", "fresh_basket_first_trade_bypass": True,
        "source_challenger": "red_only_decision from bco_stacking_brake_v1",
        "previous_confluence_rule": "RESEARCH_ONLY", "strict8h_plus_red": "RESEARCH_ONLY",
        "short_execution_authority": False,
    }
    return out


def _oct14_stacking_html() -> str:
    html = _previous_stacking_html()
    html = html.replace(
        "The frozen v1 rule now blocks additional BCO LONG slices when an existing basket is visibly deteriorating. The first trade of a fresh basket remains unchanged. Historical research remains untouched.",
        "Live admission now blocks additional BCO LONG slices only when the deterministic tide state is RED or CRITICAL. The first trade of a fresh basket remains unchanged. The previous AMBER/confluence rule and STRICT-8H+RED remain research-only challengers.",
    )
    banner = (
        "<div class='section-note small'><strong>v0.8.34 live rule:</strong> "
        "RED/CRITICAL-only stacking brake. AMBER confluence is shadow-only. "
        "48h + ATR2 exits are unchanged.</div>"
    )
    return banner + html


core.bco_stacking_brake_summary = _oct14_stacking_summary
core.build_bco_stacking_brake_html = _oct14_stacking_html


def _table_csv(table: str, order_by: str = "id", limit: int = 100000) -> str:
    allowed = {
        "bco_oct14_entry_stack_research", "bco_basket_exposure_research",
        "bco_medium_hwm_partial_bank_shadow",
    }
    if table not in allowed:
        raise ValueError("unsupported research table")
    with core.get_conn() as conn:
        _ensure_schema(conn)
        rows = core.fetchall_dict(conn.execute(
            f"SELECT * FROM {table} ORDER BY {order_by} ASC LIMIT ?", (max(1, min(int(limit), 100000)),)
        ))
    out = io.StringIO()
    if not rows:
        out.write("note\nNo rows yet\n")
        return out.getvalue()
    fields: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def _status_payload() -> Dict[str, Any]:
    current: Dict[str, Any] = {}
    try:
        with core.get_conn() as conn:
            _ensure_schema(conn)
            px = _latest_market_price(conn)
            metrics = core.basket_metrics(conn)
            state = core.fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1")) or {}
            stats = _open_trade_stats(conn, px)
            current = {
                "cycle_id": core.safe_str(state.get("cycle_id")), "open_count": int(metrics.get("open_count") or 0),
                "basket_R": float(metrics.get("basket_R") or 0.0),
                "high_water_R": float(core.safe_float(state.get("high_water_R")) or 0.0),
                "sensitivity_R_per_1pct": float(stats.get("sensitivity_R_per_1pct") or 0.0),
            }
    except Exception as exc:
        current = {"error": f"{type(exc).__name__}: {exc}"}
    return {
        "status": "ok", "release_version": RELEASE_VERSION, "policy_version": POLICY_VERSION,
        "live": {"stacking_brake": "RED_CRITICAL_ONLY", "scope": "additional BCO LONG slices only",
                 "first_trade_bypass": True, "exit_manager": "48h minimum + ATR2 chandelier (unchanged)"},
        "research_only": {"previous_amber_confluence": True, "strict8h_plus_red": True,
                          "basket_exposure_sensitivity": True, "medium_hwm_partial_banking": True,
                          "short_lane": True, "ai_observer": True},
        "partial_bank_variants": [x[0] for x in PARTIAL_BANK_VARIANTS], "current": current,
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }


_original_zip_append = promotion._append_entry_lab_to_zip


def _oct14_zip_append(base_response: Response, *, filename: str, limit: int) -> Response:
    first = _original_zip_append(base_response, filename=filename, limit=limit)
    body = bytes(getattr(first, "body", b"") or b"")
    buf = io.BytesIO(body)
    with zipfile.ZipFile(buf, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("oct14-entry-stack-research.csv", _table_csv("bco_oct14_entry_stack_research", limit=limit))
        zf.writestr("basket-exposure-research.csv", _table_csv("bco_basket_exposure_research", limit=limit))
        zf.writestr("medium-hwm-partial-bank-shadow.csv", _table_csv("bco_medium_hwm_partial_bank_shadow", limit=limit))
        zf.writestr("oct14-strategy-status.json", json.dumps(_status_payload(), indent=2, default=str))
    return Response(content=buf.getvalue(), media_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


promotion._append_entry_lab_to_zip = _oct14_zip_append

# Add a diagnostic route, then move it before the existing catch-all root mount
# so no mounted-app startup/lifespan behaviour is changed.
_route_start = len(app.router.routes)


@app.get("/oct14-strategy/status")
def oct14_strategy_status():
    return _status_payload()


_new_routes = app.router.routes[_route_start:]
del app.router.routes[_route_start:]
app.router.routes[0:0] = _new_routes


@app.on_event("startup")
def _oct14_bootstrap_research_schema() -> None:
    try:
        with core.get_conn() as conn:
            _ensure_schema(conn)
        core.log_event(
            "oct14_strategy_update_ready",
            "BCO v0.8.34 live RED/CRITICAL stacking brake active; Strict8H+RED, exposure sensitivity and medium-HWM partial banking are research-only.",
            {"release_version": RELEASE_VERSION, "policy_version": POLICY_VERSION},
        )
    except Exception as exc:
        try:
            core.log_event("oct14_strategy_update_bootstrap_error", str(exc))
        except Exception:
            pass
