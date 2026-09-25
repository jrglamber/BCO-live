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
ANALYSIS_INTERFACE_VERSION = "2.3.0"
VISIBLE_RELEASE_VERSION = "0.8.27"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scalar(conn, sql: str, params=()):
    try:
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        # psycopg dict-like rows iterate keys, so list(row)[0] returns
        # the literal column name ("count"). Prefer mapping values first.
        if hasattr(row, "values"):
            vals = list(row.values())
            return vals[0] if vals else None
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



def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _table_inventory():
    with core.get_conn() as conn:
        rows = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
            ORDER BY table_name
        """).fetchall()
    return [r.get("table_name") if isinstance(r, dict) else r[0] for r in rows]


def _table_columns(table: str):
    with core.get_conn() as conn:
        rows = conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
        """, (table,)).fetchall()
    return [r.get("column_name") if isinstance(r, dict) else r[0] for r in rows]


BCO_ANALYSIS_SLICE_TABLES = {
    "signals": ("raw_signals",),
    "execution": ("execution_audit", "broker_action_queue"),
    "harvest": ("harvest_execution_outcomes",),
    "exits": ("trade_manager_reviews",),
    "research": ("bco_directional_intelligence_research", "bco_stacking_brake_research"),
}


def _recent_rows(table: str, limit: int):
    if table not in set(_table_inventory()):
        return []
    cols = _table_columns(table)
    order_col = next((x for x in ("created_at_utc", "updated_at_utc", "signal_time", "timestamp_readable", "id") if x in cols), None)
    sql = f'SELECT * FROM "{table}"'
    if order_col:
        sql += f' ORDER BY "{order_col}" DESC'
    sql += ' LIMIT %s'
    with core.get_conn() as conn:
        rows = conn.execute(sql, (max(1, min(int(limit), 250)),)).fetchall()
    return [{k: _jsonable(v) for k, v in (row.items() if isinstance(row, dict) else zip(cols, row))} for row in rows]


@app.get("/analysis/schema")
def analysis_schema():
    try:
        tables = _table_inventory()
        return {"status":"ok","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,"read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"data":{"ok":True,"tables":tables}}
    except Exception as exc:
        return {"status":"degraded","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,"read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"data":{"ok":False,"tables":[],"error":f"{type(exc).__name__}: {exc}"}}


@app.get("/analysis/catalog")
def analysis_catalog():
    catalog = {}
    try:
        for table in _table_inventory():
            low = table.lower()
            if any(h in low for h in ("signal","trade","basket","harvest","manager","exit","research","execution","hwm","highwater")):
                catalog[table] = _table_columns(table)
        status, error = "ok", None
    except Exception as exc:
        status, error = "degraded", f"{type(exc).__name__}: {exc}"
    return {"status":status,"project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,"read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"data":{"catalog":catalog,"error":error}}


@app.get("/analysis/slice/{slice_name}")
def analysis_slice(slice_name: str, limit: int = 100):
    allowed = BCO_ANALYSIS_SLICE_TABLES.get(slice_name)
    if not allowed:
        return {"status":"error","error":"unknown analysis slice","allowed":sorted(BCO_ANALYSIS_SLICE_TABLES)}
    data = {}
    for table in allowed:
        try:
            rows = _recent_rows(table, limit)
            if rows:
                data[table] = rows
        except Exception as exc:
            data[table] = {"error":f"{type(exc).__name__}: {exc}"}
    return {"status":"ok","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,"read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"slice":slice_name,"limit_per_table":max(1,min(int(limit),250)),"data":data}




@app.get("/analysis/episode-index")
def analysis_episode_index(limit: int = 100):
    """Cycle-aware BCO history derived from persisted production audit tables."""
    n = max(1, min(int(limit), 250))
    sql = """
    WITH cycles AS (
      SELECT cycle_id, MIN(created_at_utc) first_seen_at, MAX(created_at_utc) last_seen_at,
             COUNT(*) manager_review_count, COUNT(DISTINCT trade_id) reviewed_trade_count,
             MAX(current_r) max_trade_r, MIN(current_r) min_trade_r
      FROM trade_manager_reviews
      WHERE cycle_id IS NOT NULL AND cycle_id <> ''
      GROUP BY cycle_id
    ), harvest AS (
      SELECT cycle_id, COUNT(*) harvest_count, MAX(threshold_r) max_harvest_threshold_r,
             SUM(COALESCE(model_realized_r,0)) harvested_model_r,
             SUM(COALESCE(net_realized_gbp,0)) harvested_net_gbp
      FROM harvest_execution_outcomes
      WHERE cycle_id IS NOT NULL AND cycle_id <> ''
      GROUP BY cycle_id
    )
    SELECT c.*, COALESCE(h.harvest_count,0) harvest_count,
           h.max_harvest_threshold_r, COALESCE(h.harvested_model_r,0) harvested_model_r,
           COALESCE(h.harvested_net_gbp,0) harvested_net_gbp
    FROM cycles c LEFT JOIN harvest h USING (cycle_id)
    ORDER BY c.first_seen_at DESC LIMIT %s
    """
    try:
        with core.get_conn() as conn:
            rows = conn.execute(sql, (n,)).fetchall()
        episodes = [{k:_jsonable(v) for k,v in row.items()} for row in rows]
        error = None
    except Exception as exc:
        episodes, error = [], f"{type(exc).__name__}: {exc}"
    return {"status":"ok" if error is None else "degraded","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,"read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"limit":n,"episodes":episodes,"error":error}



@app.get("/analysis/adaptive-protection-context")
def bco_adaptive_protection_context(limit: int = 200):
    """Causal, research-only decision ledger derived from persisted manager reviews."""
    n=max(20,min(int(limit),250))
    try:
        rows=_recent_rows("trade_manager_reviews",n); rows=list(reversed(rows)); out=[]; by_cycle={}
        for x in rows:
            cid=x.get("cycle_id")
            if not cid: continue
            r=x.get("current_r")
            try: r=float(r)
            except Exception: r=None
            tid=x.get("trade_id")
            key=(cid,tid)
            st=by_cycle.setdefault(key,{"peak_trade_r":None,"prev_r":None,"prev_at":None})
            if r is not None: st["peak_trade_r"]=r if st["peak_trade_r"] is None else max(st["peak_trade_r"],r)
            delta=(r-st["prev_r"]) if r is not None and st["prev_r"] is not None else None
            repair=bool(delta is not None and delta>0 and st["peak_trade_r"] is not None and r<st["peak_trade_r"])
            out.append({"event_at":x.get("created_at_utc") or x.get("updated_at_utc"),"cycle_id":cid,"trade_id":x.get("trade_id"),
              "current_r":r,"peak_trade_r_to_date":st["peak_trade_r"],"delta_r":delta,"repair_attempt":repair,
              "decision":x.get("decision") or x.get("action") or x.get("manager_action"),
              "age_hours":x.get("age_hours") or x.get("trade_age_hours")})
            if r is not None: st["prev_r"]=r
            st["prev_at"]=out[-1]["event_at"]
        return {"status":"ok","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,
          "read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"study_version":"bco_adaptive_context_v1",
          "news_layer_included":False,"principles":{"point_in_time_only":True,"future_fields_are_labels_only":True,
          "observers":["volatility/regime","cross-market confirmation","change/acceleration","failed-repair quality","decision ledger/ablation"],
          "note":"Single-market BCO uses manager-review path as the initial causal ledger. External context must be recorded prospectively, not backfilled from hindsight."},
          "observations":out}
    except Exception as exc:
        return {"status":"error","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,
          "read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"study_version":"bco_adaptive_context_v1","observations":[],"error":type(exc).__name__+": "+str(exc)}


@app.get("/analysis/cycle-economic-context")
def bco_cycle_economic_context(limit: int = 250):
    """Cycle-level research view derived from contemporaneous per-trade manager reviews."""
    try:
        rows=list(reversed(_recent_rows("trade_manager_reviews",limit)))
        grouped={}
        for x in rows:
            cid=x.get("cycle_id"); at=x.get("created_at_utc") or x.get("updated_at_utc")
            if not cid or not at: continue
            # Reviews are emitted as a batch; second-level timestamps belong to one cycle observation.
            bucket=str(at)[:16]
            g=grouped.setdefault((cid,bucket),{"event_at":at,"cycle_id":cid,"trades":{}})
            tid=x.get("trade_id"); r=x.get("current_r")
            try: r=float(r)
            except Exception: continue
            if tid: g["trades"][tid]=r
        out=[]; state={}
        for _,g in sorted(grouped.items(),key=lambda kv:str(kv[1]["event_at"])):
            vals=list(g["trades"].values())
            if not vals: continue
            cid=g["cycle_id"]; basket=sum(vals); st=state.setdefault(cid,{"hwm":basket,"prev":None})
            st["hwm"]=max(st["hwm"],basket); hwm=st["hwm"]; delta=None if st["prev"] is None else basket-st["prev"]
            gb=((hwm-basket)/hwm*100.0) if hwm>0 else None
            out.append({"event_at":g["event_at"],"cycle_id":cid,"reviewed_trade_count":len(vals),"reviewed_trade_r_sum":basket,
              "reviewed_trade_hwm_r":hwm,"giveback_pct":gb,"delta_r":delta,"repair_attempt":bool(delta is not None and delta>0 and basket<hwm),
              "scope_note":"sum of trades present in contemporaneous manager-review batch; research proxy, not broker/account P&L"})
            st["prev"]=basket
        return {"status":"ok","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,
          "read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"study_version":"bco_cycle_economic_context_v1",
          "limitations":["manager-review batches can contain only the reviewed/eligible subset","do not equate reviewed_trade_r_sum with full broker basket economics"],"observations":out}
    except Exception as exc:
        return {"status":"error","project":"BCO-live","analysis_interface_version":ANALYSIS_INTERFACE_VERSION,"app_version":VISIBLE_RELEASE_VERSION,
          "read_only_interface":True,"execution_authority":False,"time_utc":_utc_now(),"study_version":"bco_cycle_economic_context_v1","observations":[],"error":type(exc).__name__+": "+str(exc)}


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


@app.on_event("startup")
async def start_core_app() -> None:
    await core.app.router.startup()


@app.on_event("shutdown")
async def stop_core_app() -> None:
    await core.app.router.shutdown()


# Catch-all mount stays last so wrapper routes above win; all other routes remain core-owned.
app.mount("/", core.app)
