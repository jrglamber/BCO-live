"""BCO live-entry pause gate — approved 2026-11-02.

Pauses NEW live BCO trades while exit research continues. Candidate generation and
all research/shadow capture remain active. Existing position management, stops,
harvesting, reconciliation and accounting are unchanged.

Reversible with BCO_LIVE_ENTRIES_PAUSED=false + redeploy.
"""
from __future__ import annotations
import os
from typing import Any, Dict, Optional
import bco_oct14_strategy_update as base

core = base.core
app = base.app
VERSION = "bco_live_entry_pause_v2_2026_11_02"
PAUSED = str(os.getenv("BCO_LIVE_ENTRIES_PAUSED", "true")).strip().lower() not in {"0", "false", "no", "off"}

_original_create_trade = core.create_trade


def _pause_aware_create_trade(conn, raw_signal_id: int, signal: Dict[str, Any], cycle_id: str) -> Optional[str]:
    # Gate only production trade creation. Do NOT alter bco_long_candidate: all
    # candidate, Entry Lab, AI Observer and other shadow research still sees the
    # original signal and can record what would have happened.
    if PAUSED and str(getattr(core, "OANDA_ENV", "")).strip().lower() == "live":
        try:
            core.log_event(
                "BCO_LIVE_ENTRY_PAUSED",
                "New BCO live entry suppressed while Exit Lab research is active",
                {
                    "raw_signal_id": int(raw_signal_id),
                    "cycle_id": core.safe_str(cycle_id),
                    "signal_id": core.safe_str((signal or {}).get("signal_id")),
                    "pause_version": VERSION,
                },
            )
        except Exception:
            pass
        return None
    return _original_create_trade(conn, raw_signal_id, signal, cycle_id)


core.create_trade = _pause_aware_create_trade
core.BCO_LIVE_ENTRIES_PAUSED = bool(PAUSED)
core.BCO_LIVE_ENTRY_PAUSE_VERSION = VERSION


@app.get("/bco-live-entry-pause-v2", include_in_schema=False)
def bco_live_entry_pause_status_v2():
    return {
        "ok": True,
        "version": VERSION,
        "paused": bool(PAUSED),
        "scope": "production_trade_creation_only",
        "candidate_signal_preserved_for_research": True,
        "management_preserved": True,
        "re_enable": "set BCO_LIVE_ENTRIES_PAUSED=false and redeploy",
    }


STATUS = {
    "installed": True,
    "version": VERSION,
    "paused": bool(PAUSED),
    "scope": "production_trade_creation_only",
    "candidate_signal_preserved_for_research": True,
    "management_preserved": True,
}
print("BCO_LIVE_ENTRY_PAUSE_V2", STATUS, flush=True)
