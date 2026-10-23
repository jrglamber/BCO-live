"""BCO production entrypoint with UK-day realised P&L telemetry.

Read-only accounting adapter layered on the existing live-risk entrypoint.
No trading, sizing, stacking, exit, stop, harvest or broker-write logic changes.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo

import bco_live_risk_entrypoint as base

app = base.app
core = base.base.core
VERSION = "bco_daily_realised_v1_2026_10_23"
_original_summary = core.aggregate_portfolio_summary


def _parse_utc(value: Any):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _today_realised(payload: Dict[str, Any]) -> float:
    now_local = datetime.now(ZoneInfo("Europe/London"))
    start = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    end = now_local.astimezone(timezone.utc)
    epoch = _parse_utc(((payload.get("accounting") or {}).get("live_accounting_epoch_utc")))
    if epoch is not None and epoch > start:
        start = epoch
    with core.get_conn() as conn:
        row = core._bco_realised_period(conn, start.isoformat(), end.isoformat()) or {}
    return float(core.safe_float(row.get("realized_pnl_gbp")) or 0.0)


def daily_portfolio_summary() -> Dict[str, Any]:
    payload = copy.deepcopy(
        _original_summary(x_aggregate_secret=(getattr(core, "AGGREGATE_SOURCE_SECRET", "") or None))
    )
    accounting = payload.setdefault("accounting", {})
    try:
        accounting["realised_today_gbp"] = round(_today_realised(payload), 2)
        accounting["realised_today_basis"] = "BCO_LIVE_TRADES_EUROPE_LONDON_DAY"
        accounting.pop("realised_today_error", None)
    except Exception as exc:
        accounting["realised_today_gbp"] = None
        accounting["realised_today_error"] = f"{type(exc).__name__}: {exc}"
    payload["daily_accounting_adapter"] = VERSION
    return payload


def _install() -> None:
    app.add_api_route(
        "/api/portfolio-summary",
        daily_portfolio_summary,
        methods=["GET"],
        name="bco_daily_portfolio_summary",
        include_in_schema=False,
    )
    route = next(
        (r for r in reversed(list(getattr(app, "routes", [])))
         if str(getattr(r, "path", "")) == "/api/portfolio-summary"
         and getattr(r, "endpoint", None) is daily_portfolio_summary),
        None,
    )
    if route is None:
        raise RuntimeError("BCO daily portfolio-summary route failed to register")
    app.routes.remove(route)
    app.routes.insert(0, route)


_install()
print(f"BCO_DAILY_REALISED installed=True version={VERSION} execution_authority=False", flush=True)
