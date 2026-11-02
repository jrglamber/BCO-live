"""BCO live-entry pause state — approved 2026-11-02.

Purpose:
- pause NEW BCO live entries while exit strategy research continues;
- preserve open-position management, stops, harvesting, reconciliation, accounting,
  broker monitoring and all research/observer layers.

This module does not replace bco_long_candidate. Research/shadow layers continue
seeing the original candidate signal. The production admission path must check
BCO_LIVE_ENTRIES_PAUSED before placing a new trade.
"""
from __future__ import annotations
import os
import bco_oct14_strategy_update as base

core = base.core
app = base.app
VERSION = "bco_live_entry_pause_v2_2026_11_02"
PAUSED = str(os.getenv("BCO_LIVE_ENTRIES_PAUSED", "true")).strip().lower() not in {"0", "false", "no", "off"}

core.BCO_LIVE_ENTRIES_PAUSED = bool(PAUSED)
core.BCO_LIVE_ENTRY_PAUSE_VERSION = VERSION

@app.get("/bco-live-entry-pause-v2", include_in_schema=False)
def bco_live_entry_pause_status_v2():
    return {
        "ok": True,
        "version": VERSION,
        "paused": bool(PAUSED),
        "scope": "new_live_entries_only",
        "candidate_signal_preserved_for_research": True,
        "management_preserved": True,
    }

STATUS = {
    "installed": True,
    "version": VERSION,
    "paused": bool(PAUSED),
    "scope": "new_live_entries_only",
    "candidate_signal_preserved_for_research": True,
    "management_preserved": True,
}
print("BCO_LIVE_ENTRY_PAUSE_V2", STATUS, flush=True)
