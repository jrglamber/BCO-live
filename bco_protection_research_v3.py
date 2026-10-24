"""BCO protection research v3 — research pair-resolution repair.

Fixes the research-only BCO/BCOUSD pair mismatch that prevented fixed-horizon
outcomes from resolving. Also supplies the corrected market stream to the
0/12/24/48 ATR2 replay. Zero execution authority.
"""
from __future__ import annotations

from typing import Any, Dict, List

import bco_protection_research_v2 as v2
import bco_oct14_strategy_update as oct14

core = v2.core
app = v2.app
VERSION = "bco_protection_research_v3_2026_10_24"


def _market_rows(conn) -> List[Dict[str, Any]]:
    rows = v2._rows(conn.execute(
        "SELECT id,timestamp_readable,exec_close,exec_high,exec_low FROM raw_signals "
        "WHERE UPPER(pair) LIKE 'BCO%' AND exec_close IS NOT NULL ORDER BY id ASC"
    ))
    prev = None
    atr = None
    trs: List[float] = []
    for r in rows:
        close = v2._sf(r.get("exec_close")); high = v2._sf(r.get("exec_high"), close); low = v2._sf(r.get("exec_low"), close)
        tr = (high-low) if prev is None else max(high-low, abs(high-prev), abs(low-prev))
        trs.append(tr)
        if atr is None and len(trs) >= 14:
            atr = sum(trs[-14:]) / 14.0
        elif atr is not None:
            atr = (atr*13.0 + tr) / 14.0
        r["_atr14"] = atr
        prev = close
    return rows

# v2 replay/export resolves this global at call time.
v2._market_rows = _market_rows


def _fixed_entry_outcomes(conn, max_rows: int = 1500) -> Dict[str, Any]:
    oct14._ensure_schema(conn)
    rows = core.fetchall_dict(conn.execute("""
        SELECT * FROM bco_oct14_entry_stack_research WHERE outcome_96h_r IS NULL
        ORDER BY raw_signal_id ASC LIMIT ?
    """, (max(100, int(max_rows)),)))
    updated = 0
    for row in rows:
        rid = int(v2._sf(row.get("raw_signal_id")))
        entry = core.safe_float(row.get("entry_price")); sl = core.safe_float(row.get("sl_pct"))
        if rid <= 0 or entry is None or entry <= 0 or sl is None or sl <= 0:
            continue
        future = core.fetchall_dict(conn.execute("""
            SELECT id,exec_close,exec_high,exec_low FROM raw_signals
            WHERE UPPER(pair) LIKE 'BCO%' AND id>? AND exec_close IS NOT NULL ORDER BY id ASC LIMIT 96
        """, (rid,)))
        if len(future) < 6:
            continue
        sets: List[str] = []; params: List[Any] = []
        for h in oct14.HORIZONS:
            if len(future) < h or row.get(f"outcome_{h}h_r") is not None:
                continue
            seg = future[:h]
            close = core.safe_float(seg[-1].get("exec_close"))
            highs = [core.safe_float(x.get("exec_high")) for x in seg]; highs = [x for x in highs if x is not None]
            lows = [core.safe_float(x.get("exec_low")) for x in seg]; lows = [x for x in lows if x is not None]
            rr = ((float(close)/float(entry)-1.0)*100.0/float(sl)) if close is not None else None
            mfe = ((max(highs)/float(entry)-1.0)*100.0/float(sl)) if highs else None
            mae = ((min(lows)/float(entry)-1.0)*100.0/float(sl)) if lows else None
            sets.extend([f"outcome_{h}h_r=?",f"outcome_{h}h_mfe_r=?",f"outcome_{h}h_mae_r=?"]); params.extend([rr,mfe,mae])
        if sets:
            sets.append("updated_at_utc=?"); params.extend([core.now_utc_iso(),row.get("id")])
            conn.execute("UPDATE bco_oct14_entry_stack_research SET "+",".join(sets)+" WHERE id=?", tuple(params))
            updated += 1
    return {"updated":updated,"scanned":len(rows),"pair_filter":"BCO%"}

# The existing Oct-14 signal wrapper resolves this module global at runtime.
oct14._update_entry_stack_outcomes = _fixed_entry_outcomes


def _repair_partial_bank_outcomes(conn, raw_signal_id: int) -> None:
    active_cycle = v2._active_cycle(conn)
    metrics = core.basket_metrics(conn)
    basket_r = float(metrics.get("basket_R") or 0.0)
    pending = core.fetchall_dict(conn.execute("""
        SELECT * FROM bco_medium_hwm_partial_bank_shadow
        WHERE cycle_ended_at_raw_signal_id IS NULL ORDER BY trigger_raw_signal_id ASC LIMIT 1000
    """))
    for row in pending:
        trigger_id = int(v2._sf(row.get("trigger_raw_signal_id")))
        if trigger_id <= 0 or int(raw_signal_id) <= trigger_id:
            continue
        row_cycle = core.safe_str(row.get("cycle_id"))
        if row_cycle != active_cycle:
            conn.execute("UPDATE bco_medium_hwm_partial_bank_shadow SET updated_at_utc=?,cycle_ended_at_raw_signal_id=? WHERE id=?",
                         (core.now_utc_iso(),int(raw_signal_id),row.get("id")))
            continue
        c = core.fetchone_dict(conn.execute(
            "SELECT COUNT(*) AS n FROM raw_signals WHERE UPPER(pair) LIKE 'BCO%' AND id>? AND id<=?",
            (trigger_id,int(raw_signal_id)))) or {}
        elapsed = int(v2._sf(c.get("n")))
        sets: List[str] = []; params: List[Any] = []
        for h in (24,48,72,96):
            col=f"observed_{h}h_basket_R"
            if elapsed >= h and row.get(col) is None:
                sets.append(f"{col}=?"); params.append(basket_r)
        if sets:
            sets.append("updated_at_utc=?"); params.extend([core.now_utc_iso(),row.get("id")])
            conn.execute("UPDATE bco_medium_hwm_partial_bank_shadow SET "+",".join(sets)+" WHERE id=?",tuple(params))


_previous = core.process_signal

def _v3_process_signal(raw_signal_id: int, payload: Dict[str, Any]):
    out = _previous(raw_signal_id, payload)
    try:
        with core.get_conn() as conn:
            _repair_partial_bank_outcomes(conn, int(raw_signal_id))
    except Exception as exc:
        try: core.log_event("bco_research_pair_resolver_error",str(exc),{"raw_signal_id":int(raw_signal_id)})
        except Exception: pass
    return out

core.process_signal = _v3_process_signal

STATUS = dict(v2.STATUS)
STATUS.update({"version":VERSION,"pair_resolution_fixed":True,"fixed_pair_filter":"BCO%",
               "entry_outcome_resolver_fixed":True,"partial_bank_outcome_resolver_fixed":True,
               "execution_authority":False})
print("BCO_PROTECTION_RESEARCH_V3 "+str(STATUS),flush=True)
