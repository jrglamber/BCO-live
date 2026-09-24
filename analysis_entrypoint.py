"""Project Exit Plan — BCO read-only analysis interface v1.

Observability only. This module imports the production BCO application and attaches
read-only endpoints. It contains no broker-write, strategy, sizing, exit, stop,
harvest, or research-decision authority.
"""
from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict

import app as core
from fastapi import FastAPI, Request
from fastapi.responses import Response

# Stable outer app: explicit wrapper routes take precedence over the unchanged core app.
app = FastAPI(title="Project Exit Plan — Wrapper")
ANALYSIS_INTERFACE_VERSION = "1.1.0"
VISIBLE_RELEASE_VERSION = "0.8.20"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scalar(conn, sql: str, params=()):
    try:
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        try:
            return row[0]
        except Exception:
            vals = list(row)
            return vals[0] if vals else None
    except Exception:
        return None


def _row(conn, sql: str, params=()) -> Dict[str, Any]:
    try:
        return core.fetchone_dict(conn.execute(sql, params)) or {}
    except Exception:
        return {}


def _analysis_db_snapshot() -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False}
    try:
        with core.get_conn() as conn:
            out["ok"] = True
            out["raw_signal_count"] = _scalar(conn, "SELECT COUNT(*) FROM raw_signals")
            out["latest_signal"] = _row(
                conn,
                "SELECT id,timestamp_readable,exec_close FROM raw_signals ORDER BY id DESC LIMIT 1",
            )
            out["execution_audit_count"] = _scalar(conn, "SELECT COUNT(*) FROM execution_audit")
            out["latest_execution_audit"] = _row(
                conn,
                "SELECT id,created_at_utc,action,success,message FROM execution_audit ORDER BY id DESC LIMIT 1",
            )
            out["harvest_outcome_count"] = _scalar(conn, "SELECT COUNT(*) FROM harvest_execution_outcomes")
            out["latest_harvest"] = _row(
                conn,
                "SELECT id,threshold_R,broker_realized_pl_gbp,financing_gbp,net_realized_gbp,sync_status FROM harvest_execution_outcomes ORDER BY id DESC LIMIT 1",
            )
            out["manager_review_count"] = _scalar(conn, "SELECT COUNT(*) FROM trade_manager_reviews")
            out["directional_research_count"] = _scalar(conn, "SELECT COUNT(*) FROM bco_directional_intelligence_research")
            out["stacking_brake_count"] = _scalar(conn, "SELECT COUNT(*) FROM bco_stacking_brake_research")
            out["broker_queue_pending"] = _scalar(
                conn,
                "SELECT COUNT(*) FROM broker_action_queue WHERE UPPER(COALESCE(status,'')) IN ('PENDING','RETRY')",
            )
            out["broker_queue_failed_final"] = _scalar(
                conn,
                "SELECT COUNT(*) FROM broker_action_queue WHERE UPPER(COALESCE(status,''))='FAILED_FINAL'",
            )
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


@app.get("/analysis/status")
def analysis_status():
    """Compact public-safe observability endpoint. No secrets/account IDs."""
    bootstrap = dict(getattr(core, "_bootstrap_state", {}) or {})
    health: Dict[str, Any]
    try:
        health = core.operational_health() if str(bootstrap.get("status", "")).upper() == "READY" else {
            "status": "initializing",
            "bootstrap": bootstrap,
        }
    except Exception as exc:
        health = {"status": "degraded", "error": f"{type(exc).__name__}: {exc}"}

    return {
        "status": "ok",
        "project": "BCO-live",
        "analysis_interface_version": ANALYSIS_INTERFACE_VERSION,
        "app_name": getattr(core, "APP_NAME", "BCO"),
        "app_version": VISIBLE_RELEASE_VERSION,
        "policy_version": getattr(core, "POLICY_VERSION", None),
        "environment": getattr(core, "OANDA_ENV", None),
        "read_only_interface": True,
        "execution_authority": False,
        "time_utc": _utc_now(),
        "operational_health": health,
        "data": _analysis_db_snapshot(),
    }


@app.get("/analysis/quality")
def analysis_quality():
    """Small automatic integrity certificate built only from read operations."""
    data = _analysis_db_snapshot()
    checks = {
        "database_read": bool(data.get("ok")),
        "has_signals": bool((data.get("raw_signal_count") or 0) > 0),
        "broker_queue_clear": (data.get("broker_queue_pending") in (0, None)),
        "no_failed_final_broker_actions": (data.get("broker_queue_failed_final") in (0, None)),
    }
    return {
        "status": "ok" if all(checks.values()) else "check",
        "project": "BCO-live",
        "analysis_interface_version": ANALYSIS_INTERFACE_VERSION,
        "checks": checks,
        "data": data,
        "read_only_interface": True,
        "execution_authority": False,
        "time_utc": _utc_now(),
    }


def _rewrite_dashboard_version(body: bytes, content_type: str) -> bytes:
    if "text/html" not in (content_type or "").lower():
        return body
    try:
        text = body.decode("utf-8")
        current = getattr(core, "APP_VERSION", None)
        if current and str(current) != VISIBLE_RELEASE_VERSION:
            text = text.replace(str(current), VISIBLE_RELEASE_VERSION)
        return text.encode("utf-8")
    except Exception:
        return body


async def _dashboard_passthrough(request: Request, path: str) -> Response:
    scope = dict(request.scope)
    scope["path"] = path
    scope["raw_path"] = path.encode("utf-8")
    messages = []
    async def receive():
        return await request.receive()
    async def send(message):
        messages.append(message)
    await core.app(scope, receive, send)
    start = next((m for m in messages if m["type"] == "http.response.start"), None)
    chunks = [m.get("body", b"") for m in messages if m["type"] == "http.response.body"]
    if not start:
        return Response(status_code=500)
    headers = dict(start.get("headers", []))
    body = _rewrite_dashboard_version(b"".join(chunks), headers.get(b"content-type", b"").decode("latin-1"))
    out_headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in start.get("headers", []) if k.lower() not in (b"content-length", b"content-encoding")}
    return Response(content=body, status_code=start["status"], headers=out_headers, media_type=None)


@app.get("/")
async def visible_root(request: Request):
    return await _dashboard_passthrough(request, "/")


@app.get("/dashboard")
async def visible_dashboard(request: Request):
    return await _dashboard_passthrough(request, "/dashboard")


# Catch-all mount stays last so wrapper routes above win; all other routes remain core-owned.
app.mount("/", core.app)
