"""BCO 30R pre-harvest promotion — approved 2026-10-30.

LIVE CHANGE (basket monetisation only):
- add a 30R checkpoint that banks 20% of the remaining profitable BCO basket;
- preserve the existing 50R -> 20%, 100R -> 20%, 150R+ -> 25% every 50R ladder;
- 30R may execute only while CURRENT basket R is >= 30R. A historical HWM above
  30R cannot trigger a delayed/retroactive close after the basket has fallen back.

UNCHANGED:
- 48h minimum hold and ATR2 trade manager;
- hard stops, sizing, entry/stacking rules, broker safety and reconciliation;
- 50R+ harvest percentages and spacing;
- research lanes.
"""
from __future__ import annotations

import re
import threading
from typing import Any, Dict, List, Tuple

import bco_oct14_strategy_update as base

core = base.core
app = base.app

VERSION = "bco_harvest_30r_v1_2026_10_30"
PRE_HARVEST_R = 30.0
PRE_HARVEST_FRACTION = 0.20
LEGACY_STEP_R = 50.0
LEGACY_50_FRACTION = float(getattr(core, "BCO_BANK_50_FRACTION", 0.20))
LEGACY_100_FRACTION = float(getattr(core, "BCO_BANK_100_FRACTION", 0.20))
LEGACY_150_PLUS_FRACTION = float(getattr(core, "BCO_BANK_150_PLUS_FRACTION", 0.25))

_old_execute_protection = core.execute_protection
_old_harvest_html = core._bco_standard_profit_harvesting_html
_ctx = threading.local()


def _fraction_for_level(level_r: float) -> float:
    level = float(level_r)
    if abs(level - PRE_HARVEST_R) < 1e-9:
        return PRE_HARVEST_FRACTION
    if level <= 50.0 + 1e-9:
        return LEGACY_50_FRACTION
    if level <= 100.0 + 1e-9:
        return LEGACY_100_FRACTION
    return LEGACY_150_PLUS_FRACTION


def _levels_up_to(value_r: float) -> List[Tuple[float, float]]:
    top = min(max(0.0, float(core.safe_float(value_r) or 0.0)), float(core.BCO_BANK_MAX_LEVEL_R))
    out: List[Tuple[float, float]] = []

    # The hourly/fallback path historically evaluates HWM. Only expose the new
    # 30R checkpoint there while the CURRENT basket itself is still >=30R.
    if top >= PRE_HARVEST_R and not bool(getattr(_ctx, "suppress_pre_harvest", False)):
        out.append((PRE_HARVEST_R, PRE_HARVEST_FRACTION))

    # Preserve the established production ladder exactly.
    if top >= 50.0:
        out.append((50.0, LEGACY_50_FRACTION))
    if top >= 100.0:
        out.append((100.0, LEGACY_100_FRACTION))
    level = 150.0
    while level <= top + 1e-9:
        out.append((level, LEGACY_150_PLUS_FRACTION))
        level += LEGACY_STEP_R
    return out


def _execute_protection_with_30r_guard(conn, signal_time: str) -> Dict[str, Any]:
    metrics = core.basket_metrics(conn)
    current_r = float(core.safe_float(metrics.get("basket_R")) or 0.0)
    previous = bool(getattr(_ctx, "suppress_pre_harvest", False))
    _ctx.suppress_pre_harvest = current_r < PRE_HARVEST_R
    try:
        return _old_execute_protection(conn, signal_time)
    finally:
        _ctx.suppress_pre_harvest = previous


def _resolved_harvest_levels() -> set[int]:
    try:
        with core.get_conn() as conn:
            state = core.fetchone_dict(conn.execute(
                "SELECT cycle_id FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1"
            )) or {}
            cycle_id = core.safe_str(state.get("cycle_id"))
            if not cycle_id:
                return set()
            rows = core.fetchall_dict(conn.execute(
                "SELECT threshold_R,status FROM protection_stages WHERE cycle_id=? AND stage_type='BANK'",
                (cycle_id,),
            ))
        terminal = {"EXECUTED", "NO_ELIGIBLE", "EXPIRED_FLAT"}
        return {
            int(round(float(core.safe_float(r.get("threshold_R")) or 0.0)))
            for r in rows
            if core.safe_str(r.get("status")).upper() in terminal
        }
    except Exception:
        return set()


def _next_harvest() -> Tuple[float, float]:
    resolved = _resolved_harvest_levels()
    candidates = [PRE_HARVEST_R, 50.0, 100.0]
    level = 150.0
    max_level = float(core.BCO_BANK_MAX_LEVEL_R)
    while level <= max_level + 1e-9:
        candidates.append(level)
        level += LEGACY_STEP_R
    for level in candidates:
        if int(round(level)) not in resolved:
            return level, _fraction_for_level(level)
    return max_level + LEGACY_STEP_R, LEGACY_150_PLUS_FRACTION


def _harvest_html_with_30r_policy() -> str:
    html = _old_harvest_html()
    next_level, next_fraction = _next_harvest()
    pattern = (
        r'<div class="mini-card"><div class="k">Next Harvest</div>'
        r'<div class="v pos">[^<]*R</div>'
        r'<div class="small">Bank [^<]*% of remaining profitable basket</div></div>'
    )
    replacement = (
        '<div class="mini-card"><div class="k">Next Harvest</div>'
        f'<div class="v pos">{next_level:.0f}R</div>'
        f'<div class="small">Bank {next_fraction*100:.0f}% of remaining profitable basket</div></div>'
    )
    html = re.sub(pattern, replacement, html, count=1)
    banner = (
        "<div class='section-note small'><strong>Live harvest ladder:</strong> "
        "30R → bank 20% · 50R → 20% · 100R → 20% · "
        "150R+ → 25% every additional 50R. "
        "<strong>30R is current-R gated:</strong> an old HWM cannot trigger it retroactively."
        "</div>"
    )
    return banner + html


def harvest_policy_status() -> Dict[str, Any]:
    next_level, next_fraction = _next_harvest()
    try:
        with core.get_conn() as conn:
            metrics = core.basket_metrics(conn)
    except Exception:
        metrics = {}
    current_r = float(core.safe_float((metrics or {}).get("basket_R")) or 0.0)
    return {
        "ok": True,
        "version": VERSION,
        "execution_authority": True,
        "change_scope": "basket_monetisation_only",
        "current_basket_R": current_r,
        "pre_harvest": {
            "threshold_R": PRE_HARVEST_R,
            "bank_fraction": PRE_HARVEST_FRACTION,
            "requires_current_R_at_or_above_threshold": True,
            "retroactive_hwm_trigger_blocked": True,
        },
        "legacy_ladder_preserved": {
            "50R_fraction": LEGACY_50_FRACTION,
            "100R_fraction": LEGACY_100_FRACTION,
            "150R_plus_fraction": LEGACY_150_PLUS_FRACTION,
            "continues_every_50R": True,
        },
        "next_harvest_R": next_level,
        "next_harvest_fraction": next_fraction,
        "unchanged": [
            "48h minimum hold", "ATR2 trade manager", "hard stop", "risk sizing",
            "entry and stacking rules", "broker safety and reconciliation",
        ],
    }


# Install the policy. FIRST_LEVEL=30 lets the intrahour live broker harvester
# evaluate at 30R; the custom generator preserves 50/100/150+ exactly.
core.BCO_BANK_FIRST_LEVEL_R = PRE_HARVEST_R
core.bco_bank_fraction_for_level = _fraction_for_level
core.bco_bank_levels_up_to = _levels_up_to
core.execute_protection = _execute_protection_with_30r_guard
core._bco_standard_profit_harvesting_html = _harvest_html_with_30r_policy

app.add_api_route("/bco-harvest-policy", harvest_policy_status, methods=["GET"], include_in_schema=False)

STATUS = {
    "installed": True,
    "version": VERSION,
    "execution_authority": True,
    "pre_harvest_R": PRE_HARVEST_R,
    "pre_harvest_fraction": PRE_HARVEST_FRACTION,
    "retroactive_hwm_trigger_blocked": True,
    "legacy_50R_plus_ladder_preserved": True,
}
print("BCO_HARVEST_30R", STATUS, flush=True)
