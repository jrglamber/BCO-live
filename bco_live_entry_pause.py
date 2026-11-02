"""BCO live-entry pause gate — approved 2026-11-02.

Purpose:
- pause NEW BCO live entries while exit strategy research continues;
- preserve open-position management, stops, harvesting, reconciliation, accounting,
  broker monitoring and all research/observer layers;
- reversible by setting BCO_LIVE_ENTRIES_PAUSED=false and redeploying.

This module changes admission only. It does not close trades, move stops, alter
risk sizing, or modify any exit/harvest rule.
"""
from __future__ import annotations

import os
from typing import Any, Dict

import bco_oct14_strategy_update as base

core = base.core
app = base.app
VERSION = "bco_live_entry_pause_v1_2026_11_02"

PAUSED = str(os.getenv("BCO_LIVE_ENTRIES_PAUSED", "true")).strip().lower() not in {"0", "false", "no", "off"}

_original_candidate = core.bco_long_candidate


def _paused_candidate(payload: Dict[str, Any]) -> bool:
    # Preserve the original signal logic for research elsewhere, but prevent the
    # production admission path from creating new LONG trades while paused.
    if PAUSED and str(getattr(core, "OANDA_ENV", "")).strip().lower() == "live":
        return False
    return bool(_original_candidate(payload))


core.bco_long_candidate = _paused_candidate
core.BCO_LIVE_ENTRIES_PAUSED = bool(PAUSED)
core.BCO_LIVE_ENTRY_PAUSE_VERSION = VERSION


@app.get("/bco-live-entry-pause", include_in_schema=False)
def bco_live_entry_pause_status():
    return {
        "ok": True,
        "version": VERSION,
        "paused": bool(PAUSED),
        "scope": "new_live_entries_only",
        "execution_effect": "blocks_new_BCO_LONG_admission_when_live",
        "unchanged": [
            "open-position management",
            "hard stops",
            "ATR exit management",
            "profit harvesting",
            "broker reconciliation",
            "accounting",
            "research and observer capture",
        ],
    }


STATUS = {
    "installed": True,
    "version": VERSION,
    "paused": bool(PAUSED),
    "scope": "new_live_entries_only",
    "management_preserved": True,
}
print("BCO_LIVE_ENTRY_PAUSE", STATUS, flush=True)
