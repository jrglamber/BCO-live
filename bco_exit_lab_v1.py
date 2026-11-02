"""BCO Exit Lab v1 — research-only exit/protection replay.

Purpose
-------
Compare alternative BCO exit/protection policies on the same production-accepted
LONG entry set without granting any execution authority.

Research families:
- post-hold ATR chandelier grid (24/36/48/60/72h x 0.75..2.50 ATR);
- regime-aware post-hold trail (GREEN=2ATR, AMBER=1.5ATR, RED/CRITICAL=1ATR);
- economic-campaign HWM full-flatten overlays;
- approximate partial-bank overlays on the 48h+2ATR control.

Important limitations
---------------------
Earlier exits would change later stacking/admission decisions. To avoid pretending
we know those counterfactual entries, historical trade replays keep the original
accepted-entry set and cap every historical trade at its original campaign end.
Basket full-flatten overlays suppress the remainder of that original campaign.
Partial-bank overlays model fractional reduction of profitable open exposure;
OANDA discrete unit sizing means broker-realised results would differ slightly.

No broker writes, stop changes, entry decisions, sizing changes or live exit
changes are made by this module.
"""
from __future__ import annotations

import bisect
import csv
import io
import json
import math
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi.responses import Response

import bco_protection_research_v4 as protection

v2 = protection.v2
core = v2.core
app = v2.app
promotion = v2.promotion

VERSION = "bco_exit_lab_v1_2026_11_02"
EXECUTION_AUTHORITY = False
ATR_HOLDS: Tuple[int, ...] = (24, 36, 48, 60, 72)
ATR_MULTIPLIERS: Tuple[float, ...] = (0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5)
CONTROL_NAME = "H48_ATR2.00"
ADAPTIVE_VARIANTS: Tuple[Tuple[str, int], ...] = (
    ("ADAPT_H36_G2_A1.5_R1", 36),
    ("ADAPT_H48_G2_A1.5_R1", 48),
)
FULL_FLATTEN_ARMS: Tuple[float, ...] = (10.0, 15.0, 20.0, 25.0, 30.0)
FULL_FLATTEN_GIVEBACKS: Tuple[float, ...] = (10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0)
PARTIAL_BANK_VARIANTS: Tuple[Tuple[str, float, float, float], ...] = (
    ("BANK_HWM10_GB10_F33", 10.0, 10.0, 0.33),
    ("BANK_HWM10_GB10_F50", 10.0, 10.0, 0.50),
    ("BANK_HWM10_GB15_F33", 10.0, 15.0, 0.33),
    ("BANK_HWM10_GB15_F50", 10.0, 15.0, 0.50),
    ("BANK_HWM10_GB20_F50", 10.0, 20.0, 0.50),
    ("BANK_HWM15_GB15_F50", 15.0, 15.0, 0.50),
    ("BANK_HWM20_GB20_F33", 20.0, 20.0, 0.33),
    ("BANK_HWM20_GB20_F50", 20.0, 20.0, 0.50),
)

_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, Any] = {"key": None, "payload": None}


def _sf(value: Any, default: float = 0.0) -> float:
    try:
        x = core.safe_float(value)
        return float(x) if x is not None else default
    except Exception:
        try:
            return float(value)
        except Exception:
            return default


def _ss(value: Any) -> str:
    try:
        return core.safe_str(value)
    except Exception:
        return "" if value is None else str(value)


def _rows(cur) -> List[Dict[str, Any]]:
    try:
        return core.fetchall_dict(cur)
    except Exception:
        raw = cur.fetchall()
        return [dict(x) for x in raw]


def _dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        d = core.parse_dt(value)
        if d is not None and d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        try:
            d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return d if d.tzinfo is not None else d.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _cache_key(conn) -> Tuple[Any, ...]:
    raw = core.fetchone_dict(conn.execute(
        "SELECT MAX(id) AS mx,COUNT(*) AS n FROM raw_signals WHERE UPPER(pair) LIKE ?",
        ("BCO%",),
    )) or {}
    tr = core.fetchone_dict(conn.execute(
        "SELECT COUNT(*) AS n,MAX(updated_at_utc) AS mx FROM trades"
    )) or {}
    bs = core.fetchone_dict(conn.execute(
        "SELECT MAX(id) AS mx,COUNT(*) AS n FROM basket_snapshots"
    )) or {}
    return (
        int(_sf(raw.get("mx"))), int(_sf(raw.get("n"))),
        int(_sf(tr.get("n"))), _ss(tr.get("mx")),
        int(_sf(bs.get("mx"))), int(_sf(bs.get("n"))),
    )


def _load(conn) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], str]:
    market = v2._market_rows(conn)
    trades = _rows(conn.execute(
        "SELECT * FROM trades "
        "WHERE UPPER(direction)='LONG' AND UPPER(status)<>'ENTRY_FAILED' "
        "AND entry_raw_signal_id IS NOT NULL AND entry_time IS NOT NULL "
        "ORDER BY entry_raw_signal_id ASC,id ASC"
    ))
    snapshots = _rows(conn.execute(
        "SELECT raw_signal_id,cycle_id,tide_status FROM basket_snapshots "
        "WHERE raw_signal_id IS NOT NULL ORDER BY raw_signal_id ASC,id ASC"
    ))
    active = v2._active_cycle(conn)
    return market, trades, snapshots, active


def _market_index(market: Sequence[Dict[str, Any]]) -> Dict[int, int]:
    return {int(_sf(r.get("id"))): i for i, r in enumerate(market)}


def _cycle_bounds(
    trades: Sequence[Dict[str, Any]],
    market: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, datetime], Dict[str, datetime]]:
    if not market:
        return {}, {}, {}
    latest_market_dt = _dt(market[-1].get("timestamp_readable")) or datetime.now(timezone.utc)
    first: Dict[str, datetime] = {}
    last: Dict[str, datetime] = {}
    has_open: Dict[str, bool] = {}
    for tr in trades:
        cy = _ss(tr.get("cycle_id"))
        if not cy:
            continue
        ed = _dt(tr.get("entry_time"))
        xd = _dt(tr.get("exit_time"))
        if ed is not None and (cy not in first or ed < first[cy]):
            first[cy] = ed
        if xd is not None and (cy not in last or xd > last[cy]):
            last[cy] = xd
        if _ss(tr.get("status")).upper() == "OPEN":
            has_open[cy] = True
    for cy in first:
        if has_open.get(cy) or cy not in last:
            last[cy] = latest_market_dt

    end_idx: Dict[str, int] = {}
    market_times = [_dt(r.get("timestamp_readable")) or latest_market_dt for r in market]
    for cy, end_dt in last.items():
        pos = bisect.bisect_right(market_times, end_dt) - 1
        end_idx[cy] = max(0, min(pos, len(market) - 1))
    return end_idx, first, last


def _tide_series(snapshots: Sequence[Dict[str, Any]]) -> Dict[str, Tuple[List[int], List[str]]]:
    out: Dict[str, Tuple[List[int], List[str]]] = {}
    tmp: Dict[str, List[Tuple[int, str]]] = {}
    for row in snapshots:
        cy = _ss(row.get("cycle_id"))
        rid = int(_sf(row.get("raw_signal_id")))
        if not cy or rid <= 0:
            continue
        tmp.setdefault(cy, []).append((rid, _ss(row.get("tide_status")).upper()))
    for cy, pairs in tmp.items():
        pairs.sort(key=lambda x: x[0])
        out[cy] = ([x[0] for x in pairs], [x[1] for x in pairs])
    return out


def _tide_at(series: Dict[str, Tuple[List[int], List[str]]], cycle_id: str, raw_id: int) -> str:
    data = series.get(cycle_id)
    if not data:
        return ""
    ids, statuses = data
    pos = bisect.bisect_right(ids, int(raw_id)) - 1
    return statuses[pos] if pos >= 0 else ""


def _replay_trade(
    tr: Dict[str, Any],
    market: Sequence[Dict[str, Any]],
    id_to_pos: Dict[int, int],
    cycle_end_idx: Dict[str, int],
    hold: int,
    atr_mult: float,
    tide: Dict[str, Tuple[List[int], List[str]]],
    adaptive: bool = False,
) -> Optional[Dict[str, Any]]:
    cy = _ss(tr.get("cycle_id"))
    entry_id = int(_sf(tr.get("entry_raw_signal_id")))
    if entry_id not in id_to_pos or cy not in cycle_end_idx:
        return None
    start = id_to_pos[entry_id]
    end = max(start, cycle_end_idx[cy])
    entry = _sf(tr.get("entry_price"))
    sl_pct = _sf(tr.get("sl_pct"), 3.5) or 3.5
    if entry <= 0 or sl_pct <= 0:
        return None
    hard = _sf(tr.get("hard_sl_price")) or entry * (1.0 - sl_pct / 100.0)
    high_water = entry
    trail: Optional[float] = None
    count = 0

    for pos in range(start + 1, end + 1):
        row = market[pos]
        count += 1
        stop = max(hard, trail) if trail is not None else hard
        if _sf(row.get("exec_low"), 1e99) <= stop:
            rr = ((stop / entry - 1.0) * 100.0 / sl_pct)
            return {
                "trade_id": _ss(tr.get("trade_id")),
                "cycle_id": cy,
                "entry_raw_signal_id": entry_id,
                "status": "EXIT",
                "exit_raw_signal_id": int(_sf(row.get("id"))),
                "exit_signal_time": _ss(row.get("timestamp_readable")),
                "exit_price": stop,
                "economic_R": rr,
                "exit_reason": "ATR_OR_HARD_STOP",
            }
        high_water = max(high_water, _sf(row.get("exec_high"), high_water))
        atr = row.get("_atr14")
        if count >= int(hold) and atr is not None:
            mult = float(atr_mult)
            if adaptive:
                status = _tide_at(tide, cy, int(_sf(row.get("id"))))
                if status in {"RED", "CRITICAL"}:
                    mult = 1.0
                elif status == "AMBER":
                    mult = 1.5
                else:
                    mult = 2.0
            candidate = high_water - mult * float(atr)
            trail = candidate if trail is None else max(trail, candidate)

    row = market[end]
    px = _sf(row.get("exec_close"))
    rr = ((px / entry - 1.0) * 100.0 / sl_pct)
    return {
        "trade_id": _ss(tr.get("trade_id")),
        "cycle_id": cy,
        "entry_raw_signal_id": entry_id,
        "status": "CAP",
        "exit_raw_signal_id": int(_sf(row.get("id"))),
        "exit_signal_time": _ss(row.get("timestamp_readable")),
        "exit_price": px,
        "economic_R": rr,
        "exit_reason": "ORIGINAL_CAMPAIGN_BOUNDARY",
    }


def _variant_specs() -> List[Dict[str, Any]]:
    out = []
    for hold in ATR_HOLDS:
        for mult in ATR_MULTIPLIERS:
            out.append({
                "variant": f"H{hold}_ATR{mult:.2f}",
                "family": "ATR_GRID",
                "hold": int(hold),
                "atr_mult": float(mult),
                "adaptive": False,
            })
    for name, hold in ADAPTIVE_VARIANTS:
        out.append({
            "variant": name,
            "family": "ADAPTIVE_ATR",
            "hold": int(hold),
            "atr_mult": 2.0,
            "adaptive": True,
        })
    return out


def _trade_exit_matrix(
    market: Sequence[Dict[str, Any]],
    trades: Sequence[Dict[str, Any]],
    snapshots: Sequence[Dict[str, Any]],
    active_cycle: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    idx = _market_index(market)
    cycle_end_idx, _, _ = _cycle_bounds(trades, market)
    tide = _tide_series(snapshots)
    specs = _variant_specs()

    cycle_rows: List[Dict[str, Any]] = []
    summary: List[Dict[str, Any]] = []
    detail_by_variant: Dict[str, Dict[str, Any]] = {}

    for spec in specs:
        by_cycle: Dict[str, float] = {}
        trade_details: List[Dict[str, Any]] = []
        for tr in trades:
            r = _replay_trade(
                tr, market, idx, cycle_end_idx,
                spec["hold"], spec["atr_mult"], tide, spec["adaptive"],
            )
            if not r:
                continue
            cy = r["cycle_id"]
            rr = _sf(r.get("economic_R"))
            by_cycle[cy] = by_cycle.get(cy, 0.0) + rr
            trade_details.append(r)
        values = list(by_cycle.values())
        total = sum(values)
        best = max(values) if values else 0.0
        worst = min(values) if values else 0.0
        current_r = by_cycle.get(active_cycle) if active_cycle else None
        row = {
            **spec,
            "cycle_count": len(by_cycle),
            "trade_count": len(trade_details),
            "total_R": total,
            "positive_cycles": sum(1 for x in values if x > 0),
            "negative_cycles": sum(1 for x in values if x < 0),
            "best_cycle_R": best,
            "worst_cycle_R": worst,
            "loo_worst_total_R": total - best if values else 0.0,
            "current_cycle_id": active_cycle or None,
            "current_cycle_R": current_r,
            "execution_authority": False,
        }
        summary.append(row)
        for cy, rr in sorted(by_cycle.items()):
            cycle_rows.append({
                "variant": spec["variant"],
                "family": spec["family"],
                "hold": spec["hold"],
                "atr_mult": spec["atr_mult"],
                "adaptive": spec["adaptive"],
                "cycle_id": cy,
                "cycle_R": rr,
                "is_active_cycle": bool(active_cycle and cy == active_cycle),
                "execution_authority": False,
            })
        detail_by_variant[spec["variant"]] = {
            "spec": spec, "by_cycle": by_cycle, "trades": trade_details,
        }

    control = next((x for x in summary if x["variant"] == CONTROL_NAME), None)
    control_total = _sf((control or {}).get("total_R"))
    control_by = (detail_by_variant.get(CONTROL_NAME) or {}).get("by_cycle") or {}
    best_control_cycle = max(control_by, key=control_by.get) if control_by else None
    best_control_r = _sf(control_by.get(best_control_cycle)) if best_control_cycle else 0.0

    for row in summary:
        by = (detail_by_variant.get(row["variant"]) or {}).get("by_cycle") or {}
        row["delta_vs_control_R"] = _sf(row.get("total_R")) - control_total
        row["current_delta_vs_control_R"] = (
            _sf(row.get("current_cycle_R")) - _sf(control_by.get(active_cycle))
            if active_cycle and row.get("current_cycle_R") is not None
            else None
        )
        row["control_best_cycle_id"] = best_control_cycle
        candidate_best_cycle_r = _sf(by.get(best_control_cycle)) if best_control_cycle else 0.0
        row["control_best_cycle_retention_pct"] = (
            candidate_best_cycle_r / best_control_r * 100.0 if best_control_r > 0 else None
        )
        row["is_control"] = row["variant"] == CONTROL_NAME

    return summary, cycle_rows, detail_by_variant


def _actual_campaign_paths(
    market: Sequence[Dict[str, Any]],
    trades: Sequence[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    _, first, last = _cycle_bounds(trades, market)
    by_cycle: Dict[str, List[Dict[str, Any]]] = {}
    trade_groups: Dict[str, List[Dict[str, Any]]] = {}
    for tr in trades:
        cy = _ss(tr.get("cycle_id"))
        if cy:
            trade_groups.setdefault(cy, []).append(tr)

    market_with_dt = [(r, _dt(r.get("timestamp_readable"))) for r in market]
    for cy, trs in trade_groups.items():
        start = first.get(cy)
        end = last.get(cy)
        if start is None or end is None:
            continue
        hwm = -1e99
        path: List[Dict[str, Any]] = []
        for row, t in market_with_dt:
            if t is None or t < start or t > end:
                continue
            px = _sf(row.get("exec_close"))
            realised = 0.0
            open_r = 0.0
            open_count = 0
            for tr in trs:
                ent_t = _dt(tr.get("entry_time"))
                if ent_t is None or t < ent_t:
                    continue
                entry = _sf(tr.get("entry_price"))
                sl = _sf(tr.get("sl_pct"), 3.5) or 3.5
                if entry <= 0:
                    continue
                exit_t = _dt(tr.get("exit_time"))
                if exit_t is not None and t >= exit_t:
                    rr = tr.get("realized_R")
                    if rr is None:
                        rr = tr.get("realized_r")
                    if rr is None and tr.get("exit_price") is not None:
                        rr = ((_sf(tr.get("exit_price")) / entry - 1.0) * 100.0 / sl)
                    realised += _sf(rr)
                else:
                    open_r += ((px / entry - 1.0) * 100.0 / sl)
                    open_count += 1
            econ = realised + open_r
            hwm = max(hwm, econ)
            path.append({
                "raw_signal_id": int(_sf(row.get("id"))),
                "signal_time": _ss(row.get("timestamp_readable")),
                "economic_R": econ,
                "realised_R": realised,
                "open_R": open_r,
                "open_count": open_count,
                "economic_hwm_R": hwm,
            })
        by_cycle[cy] = path
    return by_cycle


def _full_flatten_matrix(paths: Dict[str, List[Dict[str, Any]]], active_cycle: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for arm in FULL_FLATTEN_ARMS:
        for gb in FULL_FLATTEN_GIVEBACKS:
            by: Dict[str, float] = {}
            triggers = 0
            for cy, path in paths.items():
                if not path:
                    continue
                final = _sf(path[-1].get("economic_R"))
                triggered = False
                for row in path:
                    hwm = _sf(row.get("economic_hwm_R"))
                    econ = _sf(row.get("economic_R"))
                    gb_pct = ((hwm - econ) / hwm * 100.0) if hwm > 0 else 0.0
                    if hwm >= arm and gb_pct >= gb:
                        final = econ
                        triggered = True
                        break
                by[cy] = final
                triggers += int(triggered)
            values = list(by.values())
            total = sum(values)
            best = max(values) if values else 0.0
            out.append({
                "variant": f"FLAT_HWM{arm:.0f}_GB{gb:.0f}",
                "family": "ECONOMIC_HWM_FULL_FLATTEN",
                "arm_hwm_R": arm,
                "giveback_pct": gb,
                "cycle_count": len(by),
                "triggered_cycles": triggers,
                "total_R": total,
                "positive_cycles": sum(1 for x in values if x > 0),
                "worst_cycle_R": min(values) if values else 0.0,
                "loo_worst_total_R": total - best if values else 0.0,
                "current_cycle_R": by.get(active_cycle) if active_cycle else None,
                "path_basis": "actual production economic campaign path",
                "execution_authority": False,
            })
    return out


def _control_trade_meta(
    market: Sequence[Dict[str, Any]],
    trades: Sequence[Dict[str, Any]],
    snapshots: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    idx = _market_index(market)
    cycle_end_idx, _, _ = _cycle_bounds(trades, market)
    tide = _tide_series(snapshots)
    by_cycle: Dict[str, List[Dict[str, Any]]] = {}
    for tr in trades:
        replay = _replay_trade(tr, market, idx, cycle_end_idx, 48, 2.0, tide, False)
        if not replay:
            continue
        cy = replay["cycle_id"]
        by_cycle.setdefault(cy, []).append({
            "trade_id": replay["trade_id"],
            "entry_pos": idx[int(_sf(tr.get("entry_raw_signal_id")))],
            "entry_price": _sf(tr.get("entry_price")),
            "sl_pct": _sf(tr.get("sl_pct"), 3.5) or 3.5,
            "exit_pos": idx.get(int(_sf(replay.get("exit_raw_signal_id"))), cycle_end_idx.get(cy, 0)),
            "exit_R": _sf(replay.get("economic_R")),
            "exit_status": replay.get("status"),
        })
    return by_cycle, cycle_end_idx


def _partial_bank_cycle(
    cycle_id: str,
    trade_meta: Dict[str, List[Dict[str, Any]]],
    cycle_end_idx: Dict[str, int],
    market: Sequence[Dict[str, Any]],
    arm_hwm_R: float,
    giveback_pct: float,
    bank_fraction: float,
) -> Tuple[float, bool, Optional[Dict[str, Any]]]:
    trs = trade_meta.get(cycle_id) or []
    if not trs or cycle_id not in cycle_end_idx:
        return 0.0, False, None
    end = cycle_end_idx[cycle_id]
    start = min(int(t["entry_pos"]) for t in trs)
    weights = [0.0] * len(trs)
    realised = 0.0
    hwm = -1e99
    triggered = False
    trigger_row: Optional[Dict[str, Any]] = None

    entries: Dict[int, List[int]] = {}
    exits: Dict[int, List[int]] = {}
    for i, tr in enumerate(trs):
        entries.setdefault(int(tr["entry_pos"]), []).append(i)
        if tr.get("exit_status") == "EXIT":
            exits.setdefault(int(tr["exit_pos"]), []).append(i)

    for pos in range(start, end + 1):
        for i in entries.get(pos, []):
            weights[i] = 1.0
        for i in exits.get(pos, []):
            if weights[i] > 0:
                realised += weights[i] * _sf(trs[i].get("exit_R"))
                weights[i] = 0.0

        px = _sf(market[pos].get("exec_close"))
        current: Dict[int, float] = {}
        open_r = 0.0
        for i, weight in enumerate(weights):
            if weight <= 0:
                continue
            tr = trs[i]
            if pos < int(tr["entry_pos"]):
                continue
            if tr.get("exit_status") == "EXIT" and pos >= int(tr["exit_pos"]):
                continue
            entry = _sf(tr.get("entry_price"))
            sl = _sf(tr.get("sl_pct"), 3.5) or 3.5
            rr = ((px / entry - 1.0) * 100.0 / sl) if entry > 0 else 0.0
            current[i] = rr
            open_r += weight * rr
        economic = realised + open_r
        hwm = max(hwm, economic)

        if (
            not triggered and hwm >= arm_hwm_R and hwm > 0
            and ((hwm - economic) / hwm * 100.0) >= giveback_pct
        ):
            banked = 0.0
            for i, rr in current.items():
                if rr <= 0 or weights[i] <= 0:
                    continue
                reduction = weights[i] * bank_fraction
                banked += reduction * rr
                weights[i] -= reduction
            realised += banked
            triggered = True
            trigger_row = {
                "raw_signal_id": int(_sf(market[pos].get("id"))),
                "signal_time": _ss(market[pos].get("timestamp_readable")),
                "economic_hwm_R": hwm,
                "economic_R_at_trigger": economic,
                "theoretical_banked_R": banked,
            }

    final_r = realised
    px = _sf(market[end].get("exec_close"))
    for i, weight in enumerate(weights):
        if weight <= 0:
            continue
        tr = trs[i]
        entry = _sf(tr.get("entry_price"))
        sl = _sf(tr.get("sl_pct"), 3.5) or 3.5
        rr = ((px / entry - 1.0) * 100.0 / sl) if entry > 0 else 0.0
        final_r += weight * rr
    return final_r, triggered, trigger_row


def _partial_bank_matrix(
    market: Sequence[Dict[str, Any]],
    trades: Sequence[Dict[str, Any]],
    snapshots: Sequence[Dict[str, Any]],
    active_cycle: str,
    control_total: float,
) -> List[Dict[str, Any]]:
    meta, end_idx = _control_trade_meta(market, trades, snapshots)
    out: List[Dict[str, Any]] = []
    for name, arm, gb, fraction in PARTIAL_BANK_VARIANTS:
        by: Dict[str, float] = {}
        triggers = 0
        trigger_details: Dict[str, Any] = {}
        for cy in meta:
            final_r, triggered, detail = _partial_bank_cycle(
                cy, meta, end_idx, market, arm, gb, fraction,
            )
            by[cy] = final_r
            triggers += int(triggered)
            if detail:
                trigger_details[cy] = detail
        values = list(by.values())
        total = sum(values)
        best = max(values) if values else 0.0
        out.append({
            "variant": name,
            "family": "CONTROL_ATR2_PLUS_PARTIAL_BANK",
            "base_exit": CONTROL_NAME,
            "arm_hwm_R": arm,
            "giveback_pct": gb,
            "bank_fraction": fraction,
            "cycle_count": len(by),
            "triggered_cycles": triggers,
            "total_R": total,
            "delta_vs_control_R": total - control_total,
            "positive_cycles": sum(1 for x in values if x > 0),
            "worst_cycle_R": min(values) if values else 0.0,
            "loo_worst_total_R": total - best if values else 0.0,
            "current_cycle_R": by.get(active_cycle) if active_cycle else None,
            "trigger_details": trigger_details,
            "approximation": "fractional reduction of profitable open exposure; broker unit discretisation ignored",
            "execution_authority": False,
        })
    return out


def _actual_trade_baseline(trades: Sequence[Dict[str, Any]], active_cycle: str) -> Dict[str, Any]:
    by: Dict[str, float] = {}
    for tr in trades:
        cy = _ss(tr.get("cycle_id"))
        if not cy:
            continue
        status = _ss(tr.get("status")).upper()
        if status in {"CLOSED", "BROKER_CLOSED"}:
            rr = tr.get("realized_R")
            if rr is None:
                rr = tr.get("realized_r")
        else:
            rr = tr.get("current_R")
            if rr is None:
                rr = tr.get("current_r")
        by[cy] = by.get(cy, 0.0) + _sf(rr)
    vals = list(by.values())
    return {
        "total_R": sum(vals),
        "positive_cycles": sum(1 for x in vals if x > 0),
        "worst_cycle_R": min(vals) if vals else 0.0,
        "current_cycle_R": by.get(active_cycle) if active_cycle else None,
        "by_cycle": by,
    }


def exit_lab_results(force: bool = False) -> Dict[str, Any]:
    with core.get_conn() as conn:
        key = _cache_key(conn)
        if not force:
            with _CACHE_LOCK:
                if _CACHE.get("key") == key and _CACHE.get("payload") is not None:
                    return _CACHE["payload"]
        market, trades, snapshots, active_cycle = _load(conn)

    if not market or not trades:
        payload = {
            "ok": True, "version": VERSION, "execution_authority": False,
            "active_cycle_id": active_cycle or None,
            "trade_exit_summary": [], "trade_exit_cycles": [],
            "full_flatten_summary": [], "partial_bank_summary": [],
            "note": "No market/trade history available.",
        }
        return payload

    trade_summary, cycle_rows, detail = _trade_exit_matrix(
        market, trades, snapshots, active_cycle,
    )
    control = next((x for x in trade_summary if x["variant"] == CONTROL_NAME), {}) or {}
    control_total = _sf(control.get("total_R"))

    paths = _actual_campaign_paths(market, trades)
    flatten = _full_flatten_matrix(paths, active_cycle)
    banks = _partial_bank_matrix(
        market, trades, snapshots, active_cycle, control_total,
    )
    actual = _actual_trade_baseline(trades, active_cycle)

    top_total = sorted(
        trade_summary,
        key=lambda x: (_sf(x.get("total_R")), _sf(x.get("loo_worst_total_R"))),
        reverse=True,
    )[:10]
    top_robust = sorted(
        trade_summary,
        key=lambda x: (_sf(x.get("loo_worst_total_R")), _sf(x.get("total_R"))),
        reverse=True,
    )[:10]
    top_current = sorted(
        [x for x in trade_summary if x.get("current_cycle_R") is not None],
        key=lambda x: _sf(x.get("current_cycle_R")),
        reverse=True,
    )[:10]

    payload = {
        "ok": True,
        "version": VERSION,
        "execution_authority": False,
        "generated_at_utc": core.now_utc_iso(),
        "active_cycle_id": active_cycle or None,
        "methodology": {
            "entry_set": "same production-accepted LONG entries",
            "historical_cycle_cap": True,
            "path_dependence_handling": (
                "Earlier exits would alter future stacking. Historical trade replays therefore "
                "retain the original accepted-entry set and cap at the original campaign boundary."
            ),
            "atr_execution": "completed-candle trail becomes executable on the next interval",
            "partial_bank_approximation": (
                "fractional reduction of profitable open exposure; discrete broker units ignored"
            ),
            "full_flatten_basis": "actual production economic campaign path",
            "future_data_in_signal_decisions": False,
        },
        "actual_production_baseline": actual,
        "control": control,
        "trade_exit_summary": trade_summary,
        "trade_exit_cycles": cycle_rows,
        "rankings": {
            "top_total_R": top_total,
            "top_leave_one_best_cycle_out": top_robust,
            "top_active_cycle_R": top_current,
        },
        "full_flatten_summary": flatten,
        "partial_bank_summary": banks,
        "promotion_state": {
            "automatic_promotion_allowed": False,
            "live_change_made": False,
            "status": "RESEARCH_ONLY",
        },
    }

    with _CACHE_LOCK:
        _CACHE["key"] = key
        _CACHE["payload"] = payload
    return payload


def _csv(rows: Sequence[Dict[str, Any]], exclude: Sequence[str] = ()) -> str:
    clean: List[Dict[str, Any]] = []
    for row in rows:
        clean.append({k: v for k, v in row.items() if k not in set(exclude)})
    out = io.StringIO()
    if not clean:
        return "note\nNo rows yet\n"
    fields: List[str] = []
    for row in clean:
        for key in row:
            if key not in fields:
                fields.append(key)
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(clean)
    return out.getvalue()


@app.get("/analysis/exit-lab-v1")
def exit_lab_endpoint(force: bool = False) -> Dict[str, Any]:
    return exit_lab_results(force=bool(force))


@app.get("/export/bco-exit-lab-v1-summary.csv")
def exit_lab_summary_csv() -> Response:
    rows = exit_lab_results().get("trade_exit_summary") or []
    return Response(
        content=_csv(rows),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="bco-exit-lab-v1-summary.csv"'},
    )


_previous_zip_append = promotion._append_entry_lab_to_zip


def _zip_append(base_response: Response, *, filename: str, limit: int) -> Response:
    first = _previous_zip_append(base_response, filename=filename, limit=limit)
    body = bytes(getattr(first, "body", b"") or b"")
    buf = io.BytesIO(body)
    result = exit_lab_results()
    with zipfile.ZipFile(buf, mode="a", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "bco-exit-lab-v1-summary.csv",
            _csv(result.get("trade_exit_summary") or []),
        )
        zf.writestr(
            "bco-exit-lab-v1-cycles.csv",
            _csv(result.get("trade_exit_cycles") or []),
        )
        zf.writestr(
            "bco-exit-lab-v1-full-flatten.csv",
            _csv(result.get("full_flatten_summary") or []),
        )
        zf.writestr(
            "bco-exit-lab-v1-partial-bank.csv",
            _csv(result.get("partial_bank_summary") or [], exclude=("trigger_details",)),
        )
        zf.writestr(
            "bco-exit-lab-v1.json",
            json.dumps({
                "version": VERSION,
                "execution_authority": False,
                "methodology": result.get("methodology"),
                "active_cycle_id": result.get("active_cycle_id"),
                "control": result.get("control"),
                "rankings": result.get("rankings"),
                "promotion_state": result.get("promotion_state"),
            }, separators=(",", ":"), default=str),
        )
    payload = buf.getvalue()
    return Response(
        content=payload,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


promotion._append_entry_lab_to_zip = _zip_append

STATUS = {
    "installed": True,
    "version": VERSION,
    "execution_authority": False,
    "live_change_made": False,
    "control": CONTROL_NAME,
    "atr_holds": list(ATR_HOLDS),
    "atr_multipliers": list(ATR_MULTIPLIERS),
    "adaptive_variants": [x[0] for x in ADAPTIVE_VARIANTS],
    "full_flatten_matrix": True,
    "partial_bank_matrix": [x[0] for x in PARTIAL_BANK_VARIANTS],
    "historical_cycle_cap": True,
}
print("BCO_EXIT_LAB_V1 " + json.dumps(STATUS, separators=(",", ":")), flush=True)
