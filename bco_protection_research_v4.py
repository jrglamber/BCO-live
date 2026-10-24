"""BCO protection research v4 — scope hold replay to the latest economic episode.

The legacy cycle_id can span a flat reset/reversal. This layer reconstructs the
latest contiguous LONG trade episode (last flat -> now) so 0/12/24/48 ATR2
comparisons describe the basket currently being monetised. Research only.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import bco_protection_research_v3 as v3
import bco_protection_research_v2 as v2

core = v2.core
app = v2.app
VERSION = "bco_protection_research_v4_2026_10_24"


def _dt(value: Any):
    try:
        d = core.parse_dt(value)
        if d is not None and d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        try:
            d = datetime.fromisoformat(str(value).replace("Z","+00:00"))
            return d if d.tzinfo is not None else d.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _latest_episode(conn) -> List[Dict[str, Any]]:
    trades = v2._rows(conn.execute(
        "SELECT * FROM trades WHERE UPPER(direction)='LONG' AND UPPER(status)<>'ENTRY_FAILED' "
        "AND entry_time IS NOT NULL ORDER BY entry_time ASC,id ASC"
    ))
    episodes: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    episode_end = None
    for tr in trades:
        start = _dt(tr.get("entry_time"))
        if start is None:
            continue
        end = _dt(tr.get("exit_time")) if core.safe_str(tr.get("status")).upper() in {"CLOSED","BROKER_CLOSED"} else None
        if current and episode_end is not None and start > episode_end:
            episodes.append(current); current = []; episode_end = None
        current.append(tr)
        if end is None:
            episode_end = datetime.max.replace(tzinfo=timezone.utc)
        elif episode_end is None or end > episode_end:
            episode_end = end
    if current:
        episodes.append(current)
    return episodes[-1] if episodes else []


def latest_episode_hold_replay() -> Dict[str, Any]:
    with core.get_conn() as conn:
        market = v2._market_rows(conn)
        trades = _latest_episode(conn)
    if not market or not trades:
        return {"ok":True,"version":VERSION,"episode_id":None,"rows":[],"summary":[],"execution_authority":False}
    last_close = v2._sf(market[-1].get("exec_close"))
    first = trades[0]
    episode_id = "BCO_ECON_" + core.safe_str(first.get("entry_time")).replace(":","").replace("-","")
    rows_out: List[Dict[str, Any]] = []
    for tr in trades:
        risk = v2._sf(tr.get("effective_risk_gbp")) or v2._sf(tr.get("requested_risk_gbp"))
        for hold in v2.HOLDS:
            r = v2._replay_trade(tr, market, hold, last_close)
            econ_r = r["realised_R"] if r["status"] == "EXIT" else r["open_R"]
            rows_out.append({
                "episode_id":episode_id,"trade_id":core.safe_str(tr.get("trade_id")),
                "entry_raw_signal_id":int(v2._sf(tr.get("entry_raw_signal_id"))),
                "entry_time":core.safe_str(tr.get("entry_time")),"entry_price":v2._sf(tr.get("entry_price")),
                "hold_candles_variant":hold,**r,"economic_R":econ_r,
                "economic_gbp_est":econ_r*risk,"realised_gbp_est":r["realised_R"]*risk,
                "protected_floor_gbp_est":r["protected_floor_R"]*risk,"execution_authority":False,
            })
    summary=[]
    for hold in v2.HOLDS:
        xs=[x for x in rows_out if x["hold_candles_variant"]==hold]
        summary.append({
            "hold_candles":hold,"trade_count":len(xs),
            "exit_count":sum(1 for x in xs if x["status"]=="EXIT"),
            "open_count":sum(1 for x in xs if x["status"]=="OPEN"),
            "realised_R":sum(v2._sf(x.get("realised_R")) for x in xs),
            "realised_gbp_est":sum(v2._sf(x.get("realised_gbp_est")) for x in xs),
            "open_R":sum(v2._sf(x.get("open_R")) for x in xs),
            "economic_R":sum(v2._sf(x.get("economic_R")) for x in xs),
            "economic_gbp_est":sum(v2._sf(x.get("economic_gbp_est")) for x in xs),
            "protected_floor_R":sum(v2._sf(x.get("protected_floor_R")) for x in xs),
            "protected_floor_gbp_est":sum(v2._sf(x.get("protected_floor_gbp_est")) for x in xs),
        })
    return {"ok":True,"version":VERSION,"episode_id":episode_id,"episode_start":core.safe_str(first.get("entry_time")),
            "trade_count":len(trades),"execution_authority":False,
            "method":"latest contiguous economic episode; 2ATR completed-candle trail executable next interval",
            "rows":rows_out,"summary":summary}

# v2 export and endpoint resolve this module global at call time.
v2.early_atr2_hold_replay = latest_episode_hold_replay

STATUS=dict(v3.STATUS)
STATUS.update({"version":VERSION,"latest_economic_episode_scope":True,"execution_authority":False})
print("BCO_PROTECTION_RESEARCH_V4 "+str(STATUS),flush=True)
