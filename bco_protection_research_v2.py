"""BCO protection research v2 — zero execution authority.

Adds only research/observability:
- HWM20 + 25%/30% giveback partial-bank shadows (20% bank fraction);
- deterministic 0/12/24/48-candle ATR2 hold replay for the active BCO cycle;
- economic campaign snapshots = realised cycle P&L + still-open P&L;
- ZIP export additions for both studies.

No production entry, stacking, exit, stop, harvest, sizing or broker-write rule changes.
"""
from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from fastapi.responses import Response

import bco_oct14_strategy_update as oct14

core = oct14.core
app = oct14.app
promotion = oct14.promotion
VERSION = "bco_protection_research_v2_2026_10_24"
HOLDS: Tuple[int, ...] = (0, 12, 24, 48)

# Extend the already-live research matrix only. Existing production 50R+ harvest
# remains untouched.
_extra = (
    ("HWM20_GB25_BANK20", 20.0, 25.0, 0.20),
    ("HWM20_GB30_BANK20", 20.0, 30.0, 0.20),
)
_existing_names = {x[0] for x in oct14.PARTIAL_BANK_VARIANTS}
oct14.PARTIAL_BANK_VARIANTS = tuple(oct14.PARTIAL_BANK_VARIANTS) + tuple(
    x for x in _extra if x[0] not in _existing_names
)


def _sf(v: Any, default: float = 0.0) -> float:
    try:
        x = core.safe_float(v)
        return float(x) if x is not None else default
    except Exception:
        try:
            return float(v)
        except Exception:
            return default


def _rows(cur) -> List[Dict[str, Any]]:
    try:
        return core.fetchall_dict(cur)
    except Exception:
        raw = cur.fetchall()
        return [dict(x) for x in raw]


def _ensure_schema(conn) -> None:
    id_type = "BIGSERIAL PRIMARY KEY" if getattr(conn, "postgres", False) else "INTEGER PRIMARY KEY AUTOINCREMENT"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bco_economic_campaign_research (
            id {id_type}, created_at_utc TEXT NOT NULL, research_version TEXT NOT NULL,
            raw_signal_id BIGINT NOT NULL UNIQUE, signal_time TEXT, cycle_id TEXT,
            open_count BIGINT, realised_cycle_R DOUBLE PRECISION, realised_cycle_gbp DOUBLE PRECISION,
            open_R DOUBLE PRECISION, open_gbp DOUBLE PRECISION,
            economic_R DOUBLE PRECISION, economic_gbp DOUBLE PRECISION,
            economic_hwm_R DOUBLE PRECISION, economic_hwm_gbp DOUBLE PRECISION,
            economic_giveback_R DOUBLE PRECISION, economic_giveback_gbp DOUBLE PRECISION,
            realised_share_of_hwm_pct DOUBLE PRECISION, point_in_time_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_econ_cycle ON bco_economic_campaign_research(cycle_id,raw_signal_id)")


def _active_cycle(conn) -> str:
    row = core.fetchone_dict(conn.execute(
        "SELECT cycle_id FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1"
    )) or {}
    return core.safe_str(row.get("cycle_id"))


def _capture_economic_campaign(conn, raw_signal_id: int) -> Optional[Dict[str, Any]]:
    _ensure_schema(conn)
    if conn.execute("SELECT id FROM bco_economic_campaign_research WHERE raw_signal_id=? LIMIT 1", (int(raw_signal_id),)).fetchone():
        return None
    raw = core.fetchone_dict(conn.execute(
        "SELECT timestamp_readable FROM raw_signals WHERE id=? LIMIT 1", (int(raw_signal_id),)
    )) or {}
    cycle_id = _active_cycle(conn)
    if not cycle_id:
        return None
    trade_rows = _rows(conn.execute(
        "SELECT status,current_R,realized_R,realized_pnl_gbp,effective_risk_gbp,requested_risk_gbp "
        "FROM trades WHERE cycle_id=? ORDER BY id ASC", (cycle_id,)
    ))
    realised_r = 0.0
    realised_gbp = 0.0
    open_r = 0.0
    open_gbp = 0.0
    open_count = 0
    for tr in trade_rows:
        status = core.safe_str(tr.get("status")).upper()
        if status in {"CLOSED", "BROKER_CLOSED"}:
            realised_r += _sf(tr.get("realized_R") if tr.get("realized_R") is not None else tr.get("realized_r"))
            realised_gbp += _sf(tr.get("realized_pnl_gbp"))
        elif status == "OPEN":
            rr = _sf(tr.get("current_R") if tr.get("current_R") is not None else tr.get("current_r"))
            risk = _sf(tr.get("effective_risk_gbp")) or _sf(tr.get("requested_risk_gbp"))
            open_r += rr
            open_gbp += rr * risk
            open_count += 1
    economic_r = realised_r + open_r
    economic_gbp = realised_gbp + open_gbp
    prior = core.fetchone_dict(conn.execute(
        "SELECT economic_hwm_R,economic_hwm_gbp FROM bco_economic_campaign_research "
        "WHERE cycle_id=? ORDER BY raw_signal_id DESC LIMIT 1", (cycle_id,)
    )) or {}
    hwm_r = max(_sf(prior.get("economic_hwm_R"), economic_r), economic_r)
    hwm_gbp = max(_sf(prior.get("economic_hwm_gbp"), economic_gbp), economic_gbp)
    giveback_r = max(0.0, hwm_r - economic_r)
    giveback_gbp = max(0.0, hwm_gbp - economic_gbp)
    realised_share = (realised_gbp / hwm_gbp * 100.0) if hwm_gbp > 0 else None
    out = {
        "raw_signal_id": int(raw_signal_id), "signal_time": core.safe_str(raw.get("timestamp_readable")),
        "cycle_id": cycle_id, "open_count": open_count,
        "realised_cycle_R": realised_r, "realised_cycle_gbp": realised_gbp,
        "open_R": open_r, "open_gbp": open_gbp,
        "economic_R": economic_r, "economic_gbp": economic_gbp,
        "economic_hwm_R": hwm_r, "economic_hwm_gbp": hwm_gbp,
        "economic_giveback_R": giveback_r, "economic_giveback_gbp": giveback_gbp,
        "realised_share_of_hwm_pct": realised_share,
    }
    conn.execute("""
        INSERT INTO bco_economic_campaign_research(
            created_at_utc,research_version,raw_signal_id,signal_time,cycle_id,open_count,
            realised_cycle_R,realised_cycle_gbp,open_R,open_gbp,economic_R,economic_gbp,
            economic_hwm_R,economic_hwm_gbp,economic_giveback_R,economic_giveback_gbp,
            realised_share_of_hwm_pct,point_in_time_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        core.now_utc_iso(), VERSION, int(raw_signal_id), out["signal_time"], cycle_id, open_count,
        realised_r, realised_gbp, open_r, open_gbp, economic_r, economic_gbp,
        hwm_r, hwm_gbp, giveback_r, giveback_gbp, realised_share,
        json.dumps({"future_data_included": False, "execution_authority": False, **out}, separators=(",", ":"), default=str),
    ))
    return out


def _market_rows(conn) -> List[Dict[str, Any]]:
    rows = _rows(conn.execute(
        "SELECT id,timestamp_readable,exec_close,exec_high,exec_low FROM raw_signals "
        "WHERE UPPER(pair)='BCO' AND exec_close IS NOT NULL ORDER BY id ASC"
    ))
    prev = None
    atr = None
    trs: List[float] = []
    for r in rows:
        close = _sf(r.get("exec_close")); high = _sf(r.get("exec_high"), close); low = _sf(r.get("exec_low"), close)
        tr = (high - low) if prev is None else max(high - low, abs(high - prev), abs(low - prev))
        trs.append(tr)
        if atr is None and len(trs) >= 14:
            atr = sum(trs[-14:]) / 14.0
        elif atr is not None:
            atr = (atr * 13.0 + tr) / 14.0
        r["_atr14"] = atr
        prev = close
    return rows


def _replay_trade(tr: Dict[str, Any], market: List[Dict[str, Any]], hold: int, last_close: float) -> Dict[str, Any]:
    entry_id = int(_sf(tr.get("entry_raw_signal_id")))
    entry = _sf(tr.get("entry_price"))
    sl_pct = _sf(tr.get("sl_pct"), 3.5) or 3.5
    hard = _sf(tr.get("hard_sl_price")) or entry * (1.0 - sl_pct / 100.0)
    high_water = entry
    active_trail: Optional[float] = None
    count = 0
    exit_row: Optional[Dict[str, Any]] = None
    exit_price: Optional[float] = None
    for row in market:
        rid = int(_sf(row.get("id")))
        if rid <= entry_id:
            continue
        count += 1
        stop = max(hard, active_trail) if active_trail is not None else hard
        if _sf(row.get("exec_low"), 1e99) <= stop:
            exit_row = row; exit_price = stop; break
        high_water = max(high_water, _sf(row.get("exec_high"), high_water))
        if count >= hold and row.get("_atr14") is not None:
            candidate = high_water - 2.0 * float(row["_atr14"])
            active_trail = candidate if active_trail is None else max(active_trail, candidate)
    if exit_price is not None:
        rr = ((exit_price / entry - 1.0) * 100.0 / sl_pct) if entry > 0 else 0.0
        return {"status": "EXIT", "exit_raw_signal_id": int(_sf(exit_row.get("id"))),
                "exit_signal_time": core.safe_str(exit_row.get("timestamp_readable")),
                "exit_price": exit_price, "realised_R": rr, "open_R": 0.0,
                "protected_floor_R": rr, "hold_candles": hold}
    cur_r = ((last_close / entry - 1.0) * 100.0 / sl_pct) if entry > 0 else 0.0
    stop = max(hard, active_trail) if active_trail is not None else hard
    stop_r = ((stop / entry - 1.0) * 100.0 / sl_pct) if entry > 0 else -1.0
    return {"status": "OPEN", "exit_raw_signal_id": None, "exit_signal_time": None,
            "exit_price": None, "realised_R": 0.0, "open_R": cur_r,
            "protected_floor_R": stop_r, "hold_candles": hold}


def early_atr2_hold_replay() -> Dict[str, Any]:
    with core.get_conn() as conn:
        cycle_id = _active_cycle(conn)
        market = _market_rows(conn)
        if not cycle_id or not market:
            return {"ok": True, "version": VERSION, "cycle_id": cycle_id, "rows": [], "summary": []}
        trades = _rows(conn.execute(
            "SELECT * FROM trades WHERE cycle_id=? AND UPPER(direction)='LONG' AND UPPER(status)<>'ENTRY_FAILED' ORDER BY id ASC",
            (cycle_id,),
        ))
    last_close = _sf(market[-1].get("exec_close"))
    rows_out: List[Dict[str, Any]] = []
    for tr in trades:
        risk = _sf(tr.get("effective_risk_gbp")) or _sf(tr.get("requested_risk_gbp"))
        for hold in HOLDS:
            r = _replay_trade(tr, market, hold, last_close)
            econ_r = r["realised_R"] if r["status"] == "EXIT" else r["open_R"]
            rows_out.append({
                "cycle_id": cycle_id, "trade_id": core.safe_str(tr.get("trade_id")),
                "entry_raw_signal_id": int(_sf(tr.get("entry_raw_signal_id"))),
                "entry_time": core.safe_str(tr.get("entry_time")), "entry_price": _sf(tr.get("entry_price")),
                "hold_candles_variant": hold, **r,
                "economic_R": econ_r, "economic_gbp_est": econ_r * risk,
                "realised_gbp_est": r["realised_R"] * risk,
                "protected_floor_gbp_est": r["protected_floor_R"] * risk,
                "execution_authority": False,
            })
    summary: List[Dict[str, Any]] = []
    for hold in HOLDS:
        xs = [x for x in rows_out if x["hold_candles_variant"] == hold]
        summary.append({
            "hold_candles": hold,
            "trade_count": len(xs), "exit_count": sum(1 for x in xs if x["status"] == "EXIT"),
            "open_count": sum(1 for x in xs if x["status"] == "OPEN"),
            "realised_R": sum(_sf(x.get("realised_R")) for x in xs),
            "realised_gbp_est": sum(_sf(x.get("realised_gbp_est")) for x in xs),
            "open_R": sum(_sf(x.get("open_R")) for x in xs),
            "economic_R": sum(_sf(x.get("economic_R")) for x in xs),
            "economic_gbp_est": sum(_sf(x.get("economic_gbp_est")) for x in xs),
            "protected_floor_R": sum(_sf(x.get("protected_floor_R")) for x in xs),
            "protected_floor_gbp_est": sum(_sf(x.get("protected_floor_gbp_est")) for x in xs),
        })
    return {"ok": True, "version": VERSION, "cycle_id": cycle_id, "execution_authority": False,
            "method": "2ATR chandelier; completed-candle trail executable next interval", "rows": rows_out, "summary": summary}


_previous_process_signal = core.process_signal

def _research_process_signal(raw_signal_id: int, payload: Dict[str, Any]):
    out = _previous_process_signal(raw_signal_id, payload)
    try:
        with core.get_conn() as conn:
            _capture_economic_campaign(conn, int(raw_signal_id))
    except Exception as exc:
        try: core.log_event("bco_protection_research_v2_error", str(exc), {"raw_signal_id": int(raw_signal_id)})
        except Exception: pass
    return out

core.process_signal = _research_process_signal


def _table_csv() -> str:
    with core.get_conn() as conn:
        _ensure_schema(conn)
        rows = _rows(conn.execute("SELECT * FROM bco_economic_campaign_research ORDER BY id ASC"))
    out = io.StringIO()
    if not rows:
        return "note\nNo rows yet\n"
    fields: List[str] = []
    for row in rows:
        for k in row:
            if k not in fields: fields.append(k)
    w = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    return out.getvalue()


def _replay_csv() -> str:
    rows = early_atr2_hold_replay().get("rows") or []
    out = io.StringIO()
    if not rows:
        return "note\nNo rows yet\n"
    fields: List[str] = []
    for row in rows:
        for k in row:
            if k not in fields: fields.append(k)
    w = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    return out.getvalue()


_previous_zip_append = promotion._append_entry_lab_to_zip

def _zip_append(base_response: Response, *, filename: str, limit: int) -> Response:
    first = _previous_zip_append(base_response, filename=filename, limit=limit)
    body = bytes(getattr(first, "body", b"") or b"")
    buf = io.BytesIO(body)
    with zipfile.ZipFile(buf, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("early-atr2-hold-replay.csv", _replay_csv())
        zf.writestr("economic-campaign-research.csv", _table_csv())
        zf.writestr("protection-research-v2.json", json.dumps({
            "version": VERSION, "execution_authority": False,
            "partial_bank_variants_added": [x[0] for x in _extra],
            "early_atr2_holds": list(HOLDS),
        }, separators=(",", ":")))
    payload = buf.getvalue()
    return Response(content=payload, media_type="application/zip", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

promotion._append_entry_lab_to_zip = _zip_append

@app.get("/analysis/protection-research-v2")
def protection_research_v2() -> Dict[str, Any]:
    return {
        "ok": True, "version": VERSION, "execution_authority": False,
        "partial_bank_variants": [x[0] for x in oct14.PARTIAL_BANK_VARIANTS],
        "early_atr2": early_atr2_hold_replay(),
    }

STATUS = {
    "installed": True, "version": VERSION, "execution_authority": False,
    "partial_bank_variants_added": [x[0] for x in _extra],
    "early_atr2_holds": list(HOLDS), "economic_campaign_hwm": True,
}
print("BCO_PROTECTION_RESEARCH_V2 " + json.dumps(STATUS, separators=(",", ":")), flush=True)
