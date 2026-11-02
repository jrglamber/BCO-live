"""BCO production entrypoint + read-only broker-backed risk telemetry."""
from __future__ import annotations
import bco_oct14_strategy_update as base

# Production Postgres compatibility hotfix (2026-11-02).
# app.py stores SQL in SQLite-style qmark form and converts '?' -> '%s' for
# psycopg. Literal percent signs must also be escaped as '%%' or psycopg treats
# them as malformed placeholders (for example: LIKE 'ARMED%'). Keep the fix at
# the production entrypoint so strategy logic remains unchanged.
core = base.core


def _postgres_safe_qmark_to_pg(sql: str) -> str:
    return str(sql).replace("%", "%%").replace("?", "%s")


core._qmark_to_pg = _postgres_safe_qmark_to_pg
core.BCO_POSTGRES_PERCENT_COMPAT_VERSION = "bco_pg_percent_compat_2026_11_02"

import portfolio_live_risk as live_risk

app = base.app


def _portfolio_risk_endpoint():
    return live_risk._read()


def _install_route():
    before = list(getattr(getattr(app, "router", None), "routes", []) or [])
    app.add_api_route("/api/portfolio-risk", _portfolio_risk_endpoint, methods=["GET"], include_in_schema=False)
    routes = getattr(getattr(app, "router", None), "routes", None)
    if routes is not None:
        matches = [r for r in routes if str(getattr(r, "path", "")) == "/api/portfolio-risk"]
        if matches:
            newest = matches[-1]
            try:
                routes.remove(newest)
                routes.insert(0, newest)
            except Exception:
                pass
    registered = any(str(getattr(r, "path", "")) == "/api/portfolio-risk" for r in (routes or []))
    if not registered:
        raise RuntimeError("BCO /api/portfolio-risk route failed to register")
    return {
        "installed": True,
        "version": live_risk.VERSION,
        "mode": "bco",
        "execution_authority": False,
        "route_registered": registered,
        "routes_before": len(before),
        "routes_after": len(routes or []),
    }

LIVE_RISK_INSTALL = _install_route()
print("BCO_LIVE_RISK_TELEMETRY", LIVE_RISK_INSTALL, flush=True)
print("BCO_POSTGRES_PERCENT_COMPAT", core.BCO_POSTGRES_PERCENT_COMPAT_VERSION, flush=True)

import bco_protection_research_v4 as protection_research
PROTECTION_RESEARCH_INSTALL = protection_research.STATUS
print("BCO_PROTECTION_RESEARCH_IMPORT", PROTECTION_RESEARCH_INSTALL, flush=True)

import bco_harvest_30r as harvest_30r
HARVEST_30R_INSTALL = harvest_30r.STATUS
print("BCO_HARVEST_30R_IMPORT", HARVEST_30R_INSTALL, flush=True)

import bco_exit_lab_v1 as exit_lab
EXIT_LAB_INSTALL = exit_lab.STATUS
print("BCO_EXIT_LAB_V1_IMPORT", EXIT_LAB_INSTALL, flush=True)

import bco_live_entry_pause_v2 as live_entry_pause
LIVE_ENTRY_PAUSE_INSTALL = live_entry_pause.STATUS
print("BCO_LIVE_ENTRY_PAUSE_V2_IMPORT", LIVE_ENTRY_PAUSE_INSTALL, flush=True)

# Procfile-synchronised fresh-build marker 2026-11-02.
