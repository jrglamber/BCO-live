# BCO Live v0.1.1 — instrument price precision fix
# Project Exit Plan
#
# Design sources:
# - BCO research/live-sim v10.1.08 for BCO candidate, 3.5% SL, 48h+ management,
#   100/200/300R banking and runner protection behaviour.
# - Main live v10.1.26 for staged basket phases/defence, instrument ownership,
#   OANDA safety gates, reconciliation and audit philosophy.
#
# SAFETY: broker writes are blocked by default. To write, ALL safety gates must
# be deliberately opened. The app may only write the exact configured BCO instrument.

from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

APP_NAME = "BCO Live v0.1.1 — Locked Production Bootstrap + Price Precision Fix"
APP_VERSION = "0.1.1"
POLICY_VERSION = "bco_live_v0.1.1_index_v10.1.26_behaviour_price_precision"


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def safe_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace(",", "").replace("£", "").replace("$", "").replace("%", "").strip())
    except Exception:
        return None


def parse_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    s = safe_str(v).lower()
    if s in {"1", "true", "yes", "y", "on", "long", "buy", "take"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def esc(v: Any) -> str:
    s = safe_str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# -----------------------------------------------------------------------------
# Runtime configuration
# -----------------------------------------------------------------------------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "change-me-too")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_CONNECT_TIMEOUT_SECONDS = max(2, min(int(float(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "10"))), 60))

# Strategy — intentionally fixed/simple for first promotion.
BCO_ASSET = "BCOUSD"
BCO_DIRECTION = os.getenv("BCO_DIRECTION", "long").strip().lower()
BCO_RISK_PER_TRADE_GBP = max(0.01, float(os.getenv("BCO_RISK_PER_TRADE_GBP", "5")))
BCO_SL_PCT = max(0.1, float(os.getenv("BCO_SL_PCT", "3.5")))
BCO_MIN_HOLD_HOURS = max(1, int(float(os.getenv("BCO_MIN_HOLD_HOURS", "48"))))
BCO_MAX_OPEN_TRADES = max(1, int(float(os.getenv("BCO_MAX_OPEN_TRADES", "250"))))
BCO_FRESH_SIGNAL_MAX_AGE_SECONDS = max(60, int(float(os.getenv("BCO_FRESH_SIGNAL_MAX_AGE_SECONDS", "7200"))))
BCO_EXECUTION_MULTIPLIER = 1.00  # hard lock; research cannot alter live sizing.

# Managed runner protection, mirroring the current live philosophy.
BCO_PROTECT_48 = float(os.getenv("BCO_PROTECT_48", "0.25"))
BCO_PROTECT_72 = float(os.getenv("BCO_PROTECT_72", "0.50"))
BCO_PROTECT_96 = float(os.getenv("BCO_PROTECT_96", "0.65"))
BCO_PROTECT_120 = float(os.getenv("BCO_PROTECT_120", "0.75"))
BCO_MIN_STOP_STEP_PCT = max(0.0, float(os.getenv("BCO_MIN_STOP_STEP_PCT", "0.02")))

# Immediate basket banking / cohort ratchet — v10.1.26 behaviour.
BCO_BANK_LEVELS = [(100.0, 0.20), (200.0, 0.25), (300.0, 0.50)]
BCO_COHORT_LEVELS = [(150.0, 0.15), (200.0, 0.30), (300.0, 0.50)]

# Young/developing/mature/heavy staging copied from the current live system.
MIN_OPEN_FOR_LIGHT_TRIM = max(1, int(float(os.getenv("MIN_OPEN_FOR_LIGHT_TRIM", "10"))))
MIN_OPEN_FOR_NORMAL_TRIM = max(MIN_OPEN_FOR_LIGHT_TRIM, int(float(os.getenv("MIN_OPEN_FOR_NORMAL_TRIM", "25"))))
MIN_OPEN_FOR_FULL_CLOSE = max(MIN_OPEN_FOR_NORMAL_TRIM, int(float(os.getenv("MIN_OPEN_FOR_FULL_CLOSE", "50"))))
HEAVY_BASKET_OPEN_TRADES = max(MIN_OPEN_FOR_FULL_CLOSE, int(float(os.getenv("HEAVY_BASKET_OPEN_TRADES", "100"))))
MIN_OPEN_FOR_ENTRY_BLOCK = max(1, int(float(os.getenv("MIN_OPEN_FOR_ENTRY_BLOCK", "10"))))
MIN_OPEN_FOR_STRICT_ENTRY_BLOCK = max(MIN_OPEN_FOR_ENTRY_BLOCK, int(float(os.getenv("MIN_OPEN_FOR_STRICT_ENTRY_BLOCK", "25"))))
ENTRY_BLOCK_TINY_SEVERE_R = float(os.getenv("ENTRY_BLOCK_TINY_SEVERE_R", "-3.0"))
ENTRY_BLOCK_EARLY_SEVERE_R = float(os.getenv("ENTRY_BLOCK_EARLY_SEVERE_R", "-2.0"))

# OANDA. Exact BCO symbol MUST be discovered/configured before writes are possible.
OANDA_ENABLED = env_bool("OANDA_ENABLED", True)
OANDA_ENV = os.getenv("OANDA_ENV", "practice").strip().lower()
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_API_TOKEN = os.getenv("OANDA_API_TOKEN", "").strip()
OANDA_API_BASE = os.getenv(
    "OANDA_API_BASE",
    "https://api-fxpractice.oanda.com" if OANDA_ENV != "live" else "https://api-fxtrade.oanda.com",
).rstrip("/")
OANDA_TIMEOUT_SECONDS = max(2.0, min(float(os.getenv("OANDA_TIMEOUT_SECONDS", "8")), 30.0))
BCO_OANDA_INSTRUMENT = os.getenv("BCO_OANDA_INSTRUMENT", "").strip().upper()

# Multiple independent locks. Initial Railway deployment should leave all writes blocked.
BROKER_READ_ONLY = env_bool("BROKER_READ_ONLY", True)
BROKER_EXECUTION_ENABLED = env_bool("BROKER_EXECUTION_ENABLED", False)
BROKER_KILL_SWITCH = env_bool("BROKER_KILL_SWITCH", True)
BCO_LIVE_EXECUTION_ARMED = env_bool("BCO_LIVE_EXECUTION_ARMED", False)
BCO_AUTO_ENTRY_ENABLED = env_bool("BCO_AUTO_ENTRY_ENABLED", False)
BCO_AUTO_MANAGEMENT_ENABLED = env_bool("BCO_AUTO_MANAGEMENT_ENABLED", False)
BCO_PRACTICE_SMOKE_TEST_ENABLED = env_bool("BCO_PRACTICE_SMOKE_TEST_ENABLED", False)

BROKER_MAX_SPREAD_PCT = max(0.0, float(os.getenv("BROKER_MAX_SPREAD_PCT", "0.20")))
BROKER_MAX_RISK_OVERAGE_PCT = max(0.0, float(os.getenv("BROKER_MAX_RISK_OVERAGE_PCT", "25")))
BROKER_RECONCILE_INTERVAL_SECONDS = max(15, min(int(float(os.getenv("BROKER_RECONCILE_INTERVAL_SECONDS", "60"))), 300))

# Shared-account guard. This service may read account-wide NAV/margin, but all
# writes and strategy P&L are restricted to BCO-owned trades only.
FORBIDDEN_FOREIGN_INSTRUMENT_TOKENS = {
    "NAS100", "SPX500", "US500", "XAU", "XAG", "JP225", "NIKKEI"
}

app = FastAPI(title=APP_NAME, version=APP_VERSION)
_db_lock = threading.RLock()
_worker_stop = threading.Event()
_worker_started = False


# -----------------------------------------------------------------------------
# Database compatibility — Postgres production, SQLite only for local dev/tests.
# -----------------------------------------------------------------------------
USE_POSTGRES = bool(DATABASE_URL and not DATABASE_URL.startswith("sqlite:"))
SQLITE_DEV_PATH = os.getenv("SQLITE_DEV_PATH", "/tmp/bco_live_dev.sqlite")


class DBConn:
    def __init__(self, raw: Any, postgres: bool):
        self.raw = raw
        self.postgres = postgres

    def execute(self, sql: str, params: Sequence[Any] = ()):
        if self.postgres:
            from psycopg.rows import dict_row  # type: ignore
            cur = self.raw.cursor(row_factory=dict_row)
            cur.execute(_qmark_to_pg(sql), tuple(params))
            return cur
        cur = self.raw.execute(sql, tuple(params))
        return cur

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        self.raw.close()


def _qmark_to_pg(sql: str) -> str:
    # This app controls its SQL and does not use literal '?' inside quoted values.
    return sql.replace("?", "%s")


@contextmanager
def get_conn():
    if USE_POSTGRES:
        import psycopg  # type: ignore
        raw = psycopg.connect(DATABASE_URL, connect_timeout=DB_CONNECT_TIMEOUT_SECONDS)
        conn = DBConn(raw, True)
    else:
        import sqlite3
        raw = sqlite3.connect(SQLITE_DEV_PATH, timeout=30, check_same_thread=False)
        raw.row_factory = sqlite3.Row
        conn = DBConn(raw, False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetchone_dict(cur: Any) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    return dict(row) if row is not None else None


def fetchall_dict(cur: Any) -> List[Dict[str, Any]]:
    return [dict(r) for r in cur.fetchall()]


def db_insert_id(conn: DBConn, sql: str, params: Sequence[Any]) -> int:
    if conn.postgres:
        row = fetchone_dict(conn.execute(sql.rstrip().rstrip(";") + " RETURNING id", params))
        return int((row or {}).get("id") or 0)
    cur = conn.execute(sql, params)
    return int(cur.lastrowid)


def init_db() -> None:
    with _db_lock, get_conn() as conn:
        id_type = "BIGSERIAL PRIMARY KEY" if conn.postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
        bool_type = "BOOLEAN" if conn.postgres else "INTEGER"
        # Keep timestamps as ISO TEXT to match the existing Project Exit Plan data model.
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS raw_signals (
                id {id_type},
                received_at_utc TEXT NOT NULL,
                pair TEXT NOT NULL,
                signal_id TEXT NOT NULL UNIQUE,
                timestamp_readable TEXT,
                exec_close DOUBLE PRECISION,
                exec_high DOUBLE PRECISION,
                exec_low DOUBLE PRECISION,
                forward_test_candidate {bool_type},
                candidate_8h {bool_type},
                signal_side TEXT,
                model_name TEXT,
                raw_json TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_raw_signal_time ON raw_signals(timestamp_readable)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS trades (
                id {id_type},
                trade_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'OPEN',
                direction TEXT NOT NULL,
                cycle_id TEXT,
                entry_raw_signal_id BIGINT,
                entry_signal_id TEXT,
                entry_time TEXT,
                entry_price DOUBLE PRECISION,
                broker_trade_id TEXT,
                broker_instrument TEXT,
                broker_units DOUBLE PRECISION,
                requested_risk_gbp DOUBLE PRECISION,
                effective_risk_gbp DOUBLE PRECISION,
                sl_pct DOUBLE PRECISION,
                hard_sl_price DOUBLE PRECISION,
                managed_stop_price DOUBLE PRECISION,
                managed_stop_stage TEXT,
                current_price DOUBLE PRECISION,
                highest_high DOUBLE PRECISION,
                lowest_low DOUBLE PRECISION,
                hold_candles BIGINT DEFAULT 0,
                return_pct DOUBLE PRECISION DEFAULT 0,
                mfe_pct DOUBLE PRECISION DEFAULT 0,
                mae_pct DOUBLE PRECISION DEFAULT 0,
                current_R DOUBLE PRECISION DEFAULT 0,
                decision_48 TEXT DEFAULT '',
                decision_72 TEXT DEFAULT '',
                exit_time TEXT,
                exit_price DOUBLE PRECISION,
                exit_reason TEXT,
                realized_R DOUBLE PRECISION,
                realized_pnl_gbp DOUBLE PRECISION,
                broker_realized_pl_home DOUBLE PRECISION,
                financing_home DOUBLE PRECISION,
                created_at_utc TEXT,
                updated_at_utc TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_trades_status ON trades(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_trades_broker_id ON trades(broker_trade_id)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS basket_state (
                singleton_key TEXT PRIMARY KEY,
                cycle_id TEXT,
                cycle_started_at TEXT,
                status TEXT DEFAULT 'FLAT',
                last_signal_time TEXT,
                open_count BIGINT DEFAULT 0,
                basket_R DOUBLE PRECISION DEFAULT 0,
                basket_pnl_gbp DOUBLE PRECISION DEFAULT 0,
                high_water_R DOUBLE PRECISION DEFAULT 0,
                high_water_seen_at TEXT,
                giveback_pct DOUBLE PRECISION DEFAULT 0,
                losing_pct DOUBLE PRECISION DEFAULT 0,
                basket_phase TEXT DEFAULT 'FLAT',
                tide_score BIGINT DEFAULT 0,
                tide_status TEXT DEFAULT 'GREEN',
                manager_action TEXT DEFAULT 'NO_OPEN_BASKET',
                manager_detail TEXT DEFAULT '',
                realized_R_cycle DOUBLE PRECISION DEFAULT 0,
                banked_R_cycle DOUBLE PRECISION DEFAULT 0,
                updated_at_utc TEXT
            )
        """)
        # Postgres and SQLite-compatible upsert.
        if conn.postgres:
            conn.execute("""
                INSERT INTO basket_state(singleton_key,status,updated_at_utc)
                VALUES('BCO_LONG','FLAT',?) ON CONFLICT(singleton_key) DO NOTHING
            """, (now_utc_iso(),))
        else:
            conn.execute("INSERT OR IGNORE INTO basket_state(singleton_key,status,updated_at_utc) VALUES('BCO_LONG','FLAT',?)", (now_utc_iso(),))

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS basket_decisions (
                id {id_type},
                created_at_utc TEXT,
                raw_signal_id BIGINT,
                signal_time TEXT,
                cycle_id TEXT,
                candidate {bool_type},
                entry_allowed {bool_type},
                entry_created {bool_type},
                open_before BIGINT,
                open_after BIGINT,
                basket_R_before DOUBLE PRECISION,
                basket_R_after DOUBLE PRECISION,
                high_water_R DOUBLE PRECISION,
                giveback_pct DOUBLE PRECISION,
                losing_pct DOUBLE PRECISION,
                basket_phase TEXT,
                tide_score BIGINT,
                tide_status TEXT,
                manager_action TEXT,
                manager_detail TEXT,
                defence_closed_count BIGINT DEFAULT 0,
                defence_trade_ids TEXT,
                banked_R_this_hour DOUBLE PRECISION DEFAULT 0,
                banked_trade_ids TEXT,
                note TEXT
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bco_decision_signal ON basket_decisions(raw_signal_id)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS protection_stages (
                id {id_type},
                created_at_utc TEXT,
                updated_at_utc TEXT,
                cycle_id TEXT,
                stage_type TEXT,
                threshold_R DOUBLE PRECISION,
                fraction DOUBLE PRECISION,
                status TEXT,
                target_bank_R DOUBLE PRECISION,
                executed_R DOUBLE PRECISION DEFAULT 0,
                armed_at_signal_time TEXT,
                executed_at_signal_time TEXT,
                selected_trade_ids TEXT,
                cohort_trade_ids TEXT,
                reason TEXT
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bco_protection_stage ON protection_stages(cycle_id,stage_type,threshold_R)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS managed_stop_events (
                id {id_type},
                created_at_utc TEXT,
                signal_time TEXT,
                cycle_id TEXT,
                trade_id TEXT,
                broker_trade_id TEXT,
                hold_candles BIGINT,
                event_type TEXT,
                rule_stage TEXT,
                old_stop_price DOUBLE PRECISION,
                new_stop_price DOUBLE PRECISION,
                protect_fraction DOUBLE PRECISION,
                protected_R DOUBLE PRECISION,
                broker_write_success {bool_type},
                note TEXT
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS execution_audit (
                id {id_type},
                created_at_utc TEXT,
                action TEXT,
                success {bool_type},
                trade_id TEXT,
                broker_trade_id TEXT,
                instrument TEXT,
                requested_units DOUBLE PRECISION,
                filled_units DOUBLE PRECISION,
                intended_price DOUBLE PRECISION,
                actual_price DOUBLE PRECISION,
                spread_pct DOUBLE PRECISION,
                requested_risk_gbp DOUBLE PRECISION,
                effective_risk_gbp DOUBLE PRECISION,
                message TEXT,
                raw_json TEXT
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS system_events (
                id {id_type},
                created_at_utc TEXT,
                event_type TEXT,
                message TEXT,
                raw_json TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at_utc TEXT
            )
        """)


def log_event(event_type: str, message: str, raw: Optional[Dict[str, Any]] = None) -> None:
    try:
        with get_conn() as conn:
            conn.execute("INSERT INTO system_events(created_at_utc,event_type,message,raw_json) VALUES(?,?,?,?)",
                         (now_utc_iso(), event_type, safe_str(message)[:3000], json.dumps(raw or {})[:12000]))
    except Exception:
        pass


def audit(action: str, success: bool, **kwargs: Any) -> None:
    try:
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO execution_audit(
                    created_at_utc,action,success,trade_id,broker_trade_id,instrument,
                    requested_units,filled_units,intended_price,actual_price,spread_pct,
                    requested_risk_gbp,effective_risk_gbp,message,raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                now_utc_iso(), action, bool(success), kwargs.get("trade_id"), kwargs.get("broker_trade_id"),
                kwargs.get("instrument"), kwargs.get("requested_units"), kwargs.get("filled_units"),
                kwargs.get("intended_price"), kwargs.get("actual_price"), kwargs.get("spread_pct"),
                kwargs.get("requested_risk_gbp"), kwargs.get("effective_risk_gbp"), safe_str(kwargs.get("message"))[:3000],
                json.dumps(kwargs.get("raw") or {})[:12000],
            ))
    except Exception:
        pass


# -----------------------------------------------------------------------------
# BCO signal parsing
# -----------------------------------------------------------------------------
def extract_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    return body.get("payload") if isinstance(body.get("payload"), dict) else body


def normalise_pair(v: Any) -> str:
    raw = safe_str(v).upper().replace("_", "").replace("-", "").replace(":", "")
    if raw in {"BCO", "BCOUSD", "BRENT", "UKOIL", "UKOILUSD"} or "BCO" in raw or "BRENT" in raw:
        return BCO_ASSET
    return raw


def directional_side(v: Any) -> str:
    s = safe_str(v).lower()
    if s in {"buy", "long", "bull", "bullish"}:
        return "long"
    if s in {"sell", "short", "bear", "bearish"}:
        return "short"
    return ""


def context_8h(payload: Dict[str, Any]) -> Dict[str, Any]:
    contexts = payload.get("contexts")
    if isinstance(contexts, list):
        for c in contexts:
            if isinstance(c, dict) and safe_str(c.get("context_tf") or c.get("tf")).upper() == "8H":
                return c
    return payload if safe_str(payload.get("context_tf")).upper() in {"8H", ""} else {}


def bco_long_candidate(payload: Dict[str, Any]) -> bool:
    ctx = context_8h(payload)
    candidate = parse_bool(ctx.get("forward_test_candidate"), parse_bool(payload.get("forward_test_candidate"), False))
    if "rule_trend_long_v1" in ctx:
        candidate = candidate and parse_bool(ctx.get("rule_trend_long_v1"), False)
    side = directional_side(payload.get("signal_side") or payload.get("side") or payload.get("direction"))
    return bool(candidate and side in {"", "long"})


def signal_received_dt(payload: Dict[str, Any]) -> Optional[datetime]:
    # TradingView payload timestamps are not guaranteed timezone-aware. Freshness is
    # therefore based on receipt time by default; source timestamp is stored for audit.
    return datetime.now(timezone.utc)


def store_signal(body: Dict[str, Any]) -> Tuple[int, Dict[str, Any], bool]:
    payload = extract_payload(body)
    pair = normalise_pair(payload.get("pair") or payload.get("ticker") or payload.get("symbol"))
    if pair != BCO_ASSET:
        raise HTTPException(status_code=400, detail=f"BCO Live accepts BCO only; received {pair or 'unknown'}")
    signal_id = safe_str(payload.get("signal_id"))
    timestamp = safe_str(payload.get("timestamp") or payload.get("timestamp_readable") or payload.get("rule_entry_timestamp"))
    if not signal_id:
        signal_id = f"BCO_{timestamp}_{safe_str(payload.get('signal_side'))}_{safe_str(payload.get('exec_close'))}"
    close = safe_float(payload.get("exec_close") or payload.get("close") or payload.get("rule_entry_price"))
    high = safe_float(payload.get("exec_high") or payload.get("high"))
    low = safe_float(payload.get("exec_low") or payload.get("low"))
    candidate = bco_long_candidate(payload)
    side = directional_side(payload.get("signal_side"))
    model = safe_str(payload.get("model_name") or payload.get("model_version"))
    now = now_utc_iso()
    with _db_lock, get_conn() as conn:
        existing = fetchone_dict(conn.execute("SELECT id FROM raw_signals WHERE signal_id=?", (signal_id,)))
        if existing:
            return int(existing["id"]), payload, True
        raw_id = db_insert_id(conn, """
            INSERT INTO raw_signals(
                received_at_utc,pair,signal_id,timestamp_readable,exec_close,exec_high,exec_low,
                forward_test_candidate,candidate_8h,signal_side,model_name,raw_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (now, pair, signal_id, timestamp, close, high, low, bool(candidate), bool(candidate), side, model, json.dumps(body)))
    return raw_id, payload, False


# -----------------------------------------------------------------------------
# Current live basket-manager behaviour port
# -----------------------------------------------------------------------------
def basket_phase_from_count(open_count: Any) -> str:
    c = int(safe_float(open_count) or 0)
    if c <= 0: return "FLAT"
    if c <= 9: return "TINY"
    if c <= 24: return "EARLY"
    if c <= 49: return "DEVELOPING"
    if c < HEAVY_BASKET_OPEN_TRADES: return "MATURE"
    return "HEAVY"


def basket_execution_stage(open_count: Any) -> Dict[str, Any]:
    c = int(safe_float(open_count) or 0)
    if c <= 0:
        return {"stage":"FLAT","max_close_fraction":0.0,"full_close_eligible":False}
    if c < MIN_OPEN_FOR_LIGHT_TRIM:
        return {"stage":"TINY_OBSERVE_ONLY","max_close_fraction":0.0,"full_close_eligible":False}
    if c < MIN_OPEN_FOR_NORMAL_TRIM:
        return {"stage":"EARLY_LIGHT_DEFENCE","max_close_fraction":0.10,"full_close_eligible":False}
    if c < MIN_OPEN_FOR_FULL_CLOSE:
        return {"stage":"DEVELOPING_NORMAL_TRIM","max_close_fraction":0.25,"full_close_eligible":False}
    if c < HEAVY_BASKET_OPEN_TRADES:
        return {"stage":"MATURE_STRONG_DEFENCE","max_close_fraction":0.50,"full_close_eligible":True}
    return {"stage":"HEAVY_FULL_DEFENCE","max_close_fraction":1.0,"full_close_eligible":True}


def calculate_tide_turn_status(
    latest_candidate_bool: bool,
    candidate_true_last_3: int,
    basket_hwm: float,
    basket_R: float,
    losing_pct: float,
    close_gt_ema20: Optional[bool],
    close_gt_ema50: Optional[bool],
    hist_up: Optional[bool],
    rsi_up: Optional[bool],
    ctx_bull_stack: Optional[bool],
    d_bull: Optional[bool],
) -> Tuple[int, str, str, List[str], float]:
    score = 0
    reasons: List[str] = []
    if not latest_candidate_bool: score += 1; reasons.append("latest_candidate_false")
    if candidate_true_last_3 <= 1: score += 1; reasons.append("candidate_weak_last_3")
    giveback = ((basket_hwm - basket_R) / basket_hwm * 100.0) if basket_hwm > 0 and basket_R < basket_hwm else 0.0
    if basket_hwm > 0 and giveback >= 40: score += 1; reasons.append("basket_giveback_40pct_plus")
    if basket_hwm > 0 and giveback >= 60: score += 1; reasons.append("basket_giveback_60pct_plus")
    if losing_pct >= 50: score += 1; reasons.append("majority_trades_losing")
    if basket_R <= 0: score += 1; reasons.append("basket_not_positive")
    if basket_R <= -2: score += 1; reasons.append("basket_minus_2R_or_worse")
    if close_gt_ema20 is False: score += 1; reasons.append("price_below_ema20")
    if close_gt_ema50 is False: score += 1; reasons.append("price_below_ema50")
    if hist_up is False: score += 1; reasons.append("macd_hist_not_up")
    if rsi_up is False: score += 1; reasons.append("rsi_not_up")
    if ctx_bull_stack is False: score += 1; reasons.append("context_not_bull_stack")
    if d_bull is False: score += 1; reasons.append("daily_not_bull")
    if score <= 2: return score, "GREEN", "HOLD", reasons, giveback
    if score <= 4: return score, "AMBER", "PAUSE_NEW_ENTRIES", reasons, giveback
    if score <= 6: return score, "RED", "WOULD_REDUCE_WEAKEST_30_PERCENT", reasons, giveback
    return score, "CRITICAL", "WOULD_CLOSE_FULL_BASKET", reasons, giveback


def calculate_tiered_basket_defence(
    tide_status: str, basket_giveback_pct: float, losing_pct: float, open_basket_R: float,
    open_count: int, latest_candidate_bool: bool = False, candidate_true_last_3: int = 0,
) -> Tuple[str, str]:
    status = safe_str(tide_status).upper()
    phase = basket_phase_from_count(open_count)
    support = bool(latest_candidate_bool or candidate_true_last_3 >= 2)
    if open_count <= 0: return "NO_OPEN_BASKET", "No open trades."
    stage = basket_execution_stage(open_count)
    maxf = float(stage.get("max_close_fraction") or 0.0)
    if status in {"AMBER","RED","CRITICAL"} and maxf <= 0:
        if support and open_basket_R > -1.0:
            return "CONTINUE_STACKING_CAUTION", f"{phase}/{stage['stage']} {status}: young basket; continue cautiously."
        return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", f"{phase}/{stage['stage']} {status}: pause/observe; no mechanical close."
    if status == "GREEN": return "HOLD", f"{phase} basket healthy."
    if status == "AMBER":
        if phase in {"TINY","EARLY","DEVELOPING"}:
            if support and open_basket_R > -1.0 and basket_giveback_pct < 60:
                return "CONTINUE_STACKING_CAUTION", f"{phase} AMBER but candidate support remains."
            return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", f"{phase} AMBER. Pause and observe."
        if basket_giveback_pct >= 70 and losing_pct >= 60 and open_basket_R <= 0:
            return "WOULD_REDUCE_WEAKEST_25_PERCENT", f"{phase} AMBER with severe giveback/loss pressure."
        return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", f"{phase} AMBER. Pause stacking first."
    if status == "RED":
        if phase in {"TINY","EARLY"}:
            if open_basket_R <= -1.5 or losing_pct >= 70:
                return "WOULD_REDUCE_WEAKEST_25_PERCENT", "EARLY RED with severe damage."
            return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", "EARLY RED. Pause/observe."
        if phase == "DEVELOPING":
            if open_basket_R > 0 and basket_giveback_pct < 65:
                return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", "DEVELOPING RED but basket remains positive."
            return "WOULD_REDUCE_WEAKEST_25_PERCENT", "DEVELOPING RED with material damage."
        if phase == "MATURE":
            if open_basket_R > 0 and basket_giveback_pct < 50 and support:
                return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", "MATURE RED but positive/supported."
            if basket_giveback_pct >= 70 or open_basket_R <= -1.0 or losing_pct >= 60:
                return "WOULD_REDUCE_WEAKEST_30_PERCENT", "MATURE RED with severe pressure."
            return "WOULD_REDUCE_WEAKEST_25_PERCENT", "MATURE RED."
        if open_basket_R > 0 and basket_giveback_pct < 40 and support:
            return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", "HEAVY RED but positive/supported."
        if basket_giveback_pct >= 70 or open_basket_R <= -1.0 or losing_pct >= 60:
            return "WOULD_REDUCE_WEAKEST_50_PERCENT", "HEAVY RED with severe pressure."
        return "WOULD_REDUCE_WEAKEST_30_PERCENT", "HEAVY RED."
    if status == "CRITICAL":
        maxf = float(stage.get("max_close_fraction") or 0.0)
        full = bool(stage.get("full_close_eligible"))
        if maxf <= 0:
            return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", f"{phase} CRITICAL but tiny observe-only stage."
        if maxf <= 0.10:
            if open_basket_R <= -2 or losing_pct >= 80:
                return "WOULD_REDUCE_WEAKEST_10_PERCENT", "CRITICAL early-light stage; severe pressure."
            return "PAUSE_NEW_ENTRIES_HOLD_EXISTING", "CRITICAL early basket but not deeply damaged."
        if maxf <= 0.25:
            if open_basket_R <= -2 or losing_pct >= 80:
                return "WOULD_REDUCE_WEAKEST_25_PERCENT", "CRITICAL developing basket; full close blocked."
            return "WOULD_REDUCE_WEAKEST_10_PERCENT", "CRITICAL developing basket; light trim."
        if phase == "MATURE":
            if full and (open_basket_R <= -2 or losing_pct >= 75):
                return "WOULD_CLOSE_FULL_BASKET", "MATURE CRITICAL with deep loss pressure."
            return "WOULD_REDUCE_WEAKEST_50_PERCENT", "MATURE CRITICAL; retain core exposure."
        if full and (open_basket_R <= -2 or losing_pct >= 70):
            return "WOULD_CLOSE_FULL_BASKET", "HEAVY CRITICAL with deep loss pressure."
        return "WOULD_REDUCE_WEAKEST_50_PERCENT", "HEAVY CRITICAL but not full-close severe."
    return "HOLD", "Unknown tide state; default hold."


def action_close_fraction(action: str) -> float:
    a = safe_str(action).upper()
    if "CLOSE_FULL" in a: return 1.0
    m = re.search(r"(10|20|25|30|50)_PERCENT", a)
    return float(m.group(1)) / 100.0 if m else 0.0


def action_blocks_entries(action: str) -> bool:
    a = safe_str(action).upper()
    if "CONTINUE_STACKING_CAUTION" in a: return False
    return any(x in a for x in ["PAUSE","NO_NEW","STOP_STACK","REDUCE","TRIM","CLOSE","DEFENCE","DEFENSE","HARVEST"])


def staged_entry_block(action: str, open_count: int, basket_r: float, tide_status: str) -> Tuple[bool, str]:
    if not action_blocks_entries(action): return False, "action_does_not_block_entries"
    if open_count <= 0: return False, "flat_basket_bypass"
    if open_count < MIN_OPEN_FOR_ENTRY_BLOCK:
        return (basket_r <= ENTRY_BLOCK_TINY_SEVERE_R,
                "tiny_severe_R_block" if basket_r <= ENTRY_BLOCK_TINY_SEVERE_R else "tiny_allow_development")
    if open_count < MIN_OPEN_FOR_STRICT_ENTRY_BLOCK:
        severe = safe_str(tide_status).upper() in {"RED","CRITICAL"} and basket_r <= ENTRY_BLOCK_EARLY_SEVERE_R
        return severe, "early_severe_R_block" if severe else "early_allow_development"
    return True, "strict_developed_basket_block"


# -----------------------------------------------------------------------------
# OANDA read/write layer with hard BCO ownership
# -----------------------------------------------------------------------------
def oanda_orders_allowed() -> Tuple[bool, str]:
    if not OANDA_ENABLED: return False, "OANDA_ENABLED=false"
    if OANDA_ENV not in {"practice", "live"}: return False, f"unsupported OANDA_ENV={OANDA_ENV}"
    if BROKER_READ_ONLY: return False, "BROKER_READ_ONLY=true"
    if not BROKER_EXECUTION_ENABLED: return False, "BROKER_EXECUTION_ENABLED=false"
    if BROKER_KILL_SWITCH: return False, "BROKER_KILL_SWITCH=true"
    if not BCO_LIVE_EXECUTION_ARMED: return False, "BCO_LIVE_EXECUTION_ARMED=false"
    if not OANDA_ACCOUNT_ID or not OANDA_API_TOKEN: return False, "missing OANDA credentials"
    if not BCO_OANDA_INSTRUMENT: return False, "BCO_OANDA_INSTRUMENT not configured"
    if any(tok in BCO_OANDA_INSTRUMENT for tok in FORBIDDEN_FOREIGN_INSTRUMENT_TOKENS):
        return False, "configured BCO instrument resembles a foreign strategy instrument"
    return True, "broker writes unlocked for exact BCO instrument only"


def assert_owned_instrument(instrument: str) -> None:
    inst = safe_str(instrument).upper()
    if not BCO_OANDA_INSTRUMENT:
        raise RuntimeError("BCO_OANDA_INSTRUMENT is blank; broker write prohibited")
    if inst != BCO_OANDA_INSTRUMENT:
        raise RuntimeError(f"foreign instrument write blocked: {inst} != {BCO_OANDA_INSTRUMENT}")


def oanda_request(path: str, method: str = "GET", body: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not OANDA_ENABLED:
        return {"ok": False, "error": "OANDA_ENABLED=false"}
    if not OANDA_ACCOUNT_ID or not OANDA_API_TOKEN:
        return {"ok": False, "error": "missing OANDA account/token"}
    clean = "/" + path.lstrip("/")
    url = OANDA_API_BASE + clean
    if params:
        url += "?" + urllib.parse.urlencode({k:v for k,v in params.items() if v is not None})
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method.upper(), headers={
        "Authorization": f"Bearer {OANDA_API_TOKEN}", "Accept":"application/json", "Content-Type":"application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=OANDA_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
            return {"ok": True, "status_code": resp.status, "data": json.loads(raw) if raw else {}}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try: parsed = json.loads(raw) if raw else {}
        except Exception: parsed = {"raw": raw}
        return {"ok": False, "status_code": e.code, "error": str(e), "data": parsed}
    except Exception as e:
        return {"ok": False, "status_code": None, "error": str(e)}


def oanda_write(path: str, method: str, body: Dict[str, Any], instrument: str, action: str, trade_id: str = "", broker_trade_id: str = "") -> Dict[str, Any]:
    try:
        assert_owned_instrument(instrument)
    except Exception as e:
        audit(action, False, trade_id=trade_id, broker_trade_id=broker_trade_id, instrument=instrument, message=str(e), raw=body)
        return {"ok": False, "blocked": True, "error": str(e)}
    allowed, reason = oanda_orders_allowed()
    if not allowed:
        audit(action, False, trade_id=trade_id, broker_trade_id=broker_trade_id, instrument=instrument, message=reason, raw=body)
        return {"ok": False, "blocked": True, "error": reason}
    resp = oanda_request(path, method=method, body=body)
    audit(action, bool(resp.get("ok")), trade_id=trade_id, broker_trade_id=broker_trade_id, instrument=instrument,
          message="ok" if resp.get("ok") else safe_str(resp.get("error")), raw=resp.get("data") or resp)
    return resp


def account_summary() -> Dict[str, Any]:
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok": False, "error": "OANDA not configured"}
    r = oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/summary")
    acct = (r.get("data") or {}).get("account", {}) if r.get("ok") else {}
    return {
        "ok": bool(r.get("ok")), "NAV": safe_float(acct.get("NAV")), "balance": safe_float(acct.get("balance")),
        "marginUsed": safe_float(acct.get("marginUsed")), "marginAvailable": safe_float(acct.get("marginAvailable")),
        "currency": safe_str(acct.get("currency")), "error": r.get("error")
    }


def discover_bco_instruments() -> Dict[str, Any]:
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok": False, "error": "OANDA not configured", "matches": []}
    r = oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/instruments")
    items = (r.get("data") or {}).get("instruments", []) if r.get("ok") else []
    matches = []
    for i in items:
        name = safe_str(i.get("name")).upper(); display = safe_str(i.get("displayName")).upper()
        if any(k in name or k in display for k in ["BCO", "BRENT", "UK OIL", "UKOIL"]):
            matches.append(i)
    return {"ok": bool(r.get("ok")), "configured": BCO_OANDA_INSTRUMENT, "matches": matches, "error": r.get("error")}


def instrument_details(instrument: str) -> Optional[Dict[str, Any]]:
    r = oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/instruments", params={"instruments": instrument})
    items = (r.get("data") or {}).get("instruments", []) if r.get("ok") else []
    return items[0] if items else None


def current_price(instrument: str) -> Dict[str, Any]:
    r = oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing", params={"instruments": instrument})
    prices = (r.get("data") or {}).get("prices", []) if r.get("ok") else []
    if not prices:
        return {"ok": False, "error": r.get("error") or "no price"}
    p = prices[0]
    bid = safe_float((p.get("bids") or [{}])[0].get("price")); ask = safe_float((p.get("asks") or [{}])[0].get("price"))
    if bid is None or ask is None:
        return {"ok": False, "error": "bid/ask missing", "raw": p}
    mid = (bid + ask) / 2.0
    spread_pct = (ask - bid) / mid * 100.0 if mid else None
    factors = p.get("quoteHomeConversionFactors") if isinstance(p.get("quoteHomeConversionFactors"), dict) else {}
    return {"ok": True, "bid": bid, "ask": ask, "mid": mid, "spread_pct": spread_pct,
            "negative_home_factor": safe_float(factors.get("negativeUnits")), "positive_home_factor": safe_float(factors.get("positiveUnits")), "raw": p}


def floor_to_precision(value: float, precision: int) -> float:
    factor = 10 ** max(0, precision)
    return math.floor(value * factor + 1e-12) / factor


def format_oanda_price(value: float, precision: int) -> str:
    """Format an order/stop price to the instrument's broker precision.

    OANDA rejects stopLossOnFill / trade stop updates when the submitted price
    contains more decimals than the instrument's displayPrecision. BCO_USD
    currently reports displayPrecision=3, but we derive it from instrument
    metadata rather than hard-coding it into order construction.
    """
    p = max(0, int(precision or 0))
    return f"{float(value):.{p}f}"


def risk_preview(target_risk_gbp: float = BCO_RISK_PER_TRADE_GBP) -> Dict[str, Any]:
    inst = BCO_OANDA_INSTRUMENT
    if not inst:
        return {"ok": False, "blocked": True, "error": "Set BCO_OANDA_INSTRUMENT after discovery."}
    details = instrument_details(inst)
    price = current_price(inst)
    if not details or not price.get("ok"):
        return {"ok": False, "error": "instrument/pricing unavailable", "details": details, "price": price}
    entry = float(price["ask"])  # long entry
    sl_price = entry * (1.0 - BCO_SL_PCT / 100.0)
    delta_quote = entry - sl_price
    home_factor = safe_float(price.get("negative_home_factor"))
    if home_factor is None or home_factor <= 0:
        # Diagnostic fallback only; do not auto execute if conversion factor is absent.
        return {"ok": False, "blocked": True, "error": "OANDA quote->home loss conversion factor unavailable", "price": price, "details": details}
    risk_per_unit_home = delta_quote * home_factor
    raw_units = float(target_risk_gbp) / risk_per_unit_home if risk_per_unit_home > 0 else 0.0
    precision = int(safe_float(details.get("tradeUnitsPrecision")) or 0)
    display_precision = int(safe_float(details.get("displayPrecision")) or 0)
    minimum = float(safe_float(details.get("minimumTradeSize")) or 1.0)
    units = floor_to_precision(raw_units, precision)
    used_min = False
    if units < minimum:
        units = minimum
        used_min = True
    effective_risk = units * risk_per_unit_home
    overage_pct = ((effective_risk / target_risk_gbp) - 1.0) * 100.0 if target_risk_gbp > 0 else 0.0
    return {
        "ok": True, "instrument": inst, "target_risk_gbp": target_risk_gbp, "entry_price": entry,
        "stop_price": sl_price, "stop_price_formatted": format_oanda_price(sl_price, display_precision),
        "display_precision": display_precision, "sl_pct": BCO_SL_PCT, "units": units, "raw_units": raw_units,
        "units_precision": precision, "minimum_trade_size": minimum, "used_minimum_trade_size": used_min,
        "effective_risk_gbp": effective_risk, "risk_overage_pct": overage_pct,
        "spread_pct": price.get("spread_pct"), "home_loss_conversion_factor": home_factor,
        "acceptable_risk_overage": overage_pct <= BROKER_MAX_RISK_OVERAGE_PCT,
        "acceptable_spread": (safe_float(price.get("spread_pct")) or 0.0) <= BROKER_MAX_SPREAD_PCT,
    }


def extract_fill(resp: Dict[str, Any]) -> Dict[str, Any]:
    data = resp.get("data") if isinstance(resp, dict) else {}
    fill = data.get("orderFillTransaction") if isinstance(data, dict) and isinstance(data.get("orderFillTransaction"), dict) else {}
    trade_opened = fill.get("tradeOpened") if isinstance(fill.get("tradeOpened"), dict) else {}
    return {
        "broker_trade_id": safe_str(trade_opened.get("tradeID")),
        "price": safe_float(fill.get("price")),
        "units": abs(safe_float(fill.get("units")) or safe_float(trade_opened.get("units")) or 0.0),
        "transaction_id": safe_str(fill.get("id")),
    }


def open_bco_broker_trade(local_trade_id: str) -> Dict[str, Any]:
    preview = risk_preview(BCO_RISK_PER_TRADE_GBP)
    if not preview.get("ok"):
        return {"ok": False, "error": preview.get("error"), "preview": preview}
    if not preview.get("acceptable_risk_overage"):
        return {"ok": False, "blocked": True, "error": "risk overage exceeds guardrail", "preview": preview}
    if not preview.get("acceptable_spread"):
        return {"ok": False, "blocked": True, "error": "spread exceeds guardrail", "preview": preview}
    units = preview["units"]
    stop = preview["stop_price"]
    stop_text = safe_str(preview.get("stop_price_formatted"))
    if not stop_text:
        stop_text = format_oanda_price(stop, int(preview.get("display_precision") or 0))
    # Whole-unit BCO instruments should be sent as whole-unit strings where possible.
    units_precision = int(preview.get("units_precision") or 0)
    units_text = f"{float(units):.{units_precision}f}"
    body = {"order": {
        "type": "MARKET", "instrument": BCO_OANDA_INSTRUMENT, "units": units_text,
        "timeInForce": "FOK", "positionFill": "DEFAULT",
        "stopLossOnFill": {"price": stop_text}
    }}
    resp = oanda_write(f"/v3/accounts/{OANDA_ACCOUNT_ID}/orders", "POST", body, BCO_OANDA_INSTRUMENT, "OPEN_BCO", trade_id=local_trade_id)
    fill = extract_fill(resp)
    audit("OPEN_BCO_DETAILS", bool(resp.get("ok")), trade_id=local_trade_id, broker_trade_id=fill.get("broker_trade_id"),
          instrument=BCO_OANDA_INSTRUMENT, requested_units=units, filled_units=fill.get("units"), intended_price=preview.get("entry_price"),
          actual_price=fill.get("price"), spread_pct=preview.get("spread_pct"), requested_risk_gbp=BCO_RISK_PER_TRADE_GBP,
          effective_risk_gbp=preview.get("effective_risk_gbp"), message="broker open result", raw=resp.get("data") or resp)
    return {"ok": bool(resp.get("ok")), "response": resp, "fill": fill, "preview": preview}


def update_broker_stop(broker_trade_id: str, stop_price: float, local_trade_id: str) -> Dict[str, Any]:
    # Use the broker-reported display precision for every stop amendment too.
    # This prevents the same precision rejection that can affect stopLossOnFill.
    details = instrument_details(BCO_OANDA_INSTRUMENT) or {}
    display_precision = int(safe_float(details.get("displayPrecision")) or 0)
    if display_precision <= 0:
        return {"ok": False, "blocked": True, "error": "instrument displayPrecision unavailable for stop update"}
    stop_text = format_oanda_price(stop_price, display_precision)
    body = {"stopLoss": {"price": stop_text, "timeInForce": "GTC"}}
    return oanda_write(f"/v3/accounts/{OANDA_ACCOUNT_ID}/trades/{broker_trade_id}/orders", "PUT", body,
                       BCO_OANDA_INSTRUMENT, "UPDATE_STOP", trade_id=local_trade_id, broker_trade_id=broker_trade_id)


def close_broker_trade(broker_trade_id: str, local_trade_id: str, reason: str) -> Dict[str, Any]:
    return oanda_write(f"/v3/accounts/{OANDA_ACCOUNT_ID}/trades/{broker_trade_id}/close", "PUT", {"units":"ALL"},
                       BCO_OANDA_INSTRUMENT, f"CLOSE_BCO:{reason}", trade_id=local_trade_id, broker_trade_id=broker_trade_id)


# -----------------------------------------------------------------------------
# Trade/basket engine
# -----------------------------------------------------------------------------
def candidate_support(conn: DBConn, limit: int = 3) -> Dict[str, Any]:
    rows = fetchall_dict(conn.execute("SELECT candidate_8h FROM raw_signals ORDER BY id DESC LIMIT ?", (limit,)))
    vals = [parse_bool(r.get("candidate_8h"), False) for r in rows]
    n = sum(1 for v in vals if v)
    return {"latest_candidate": bool(vals[0]) if vals else False, "candidate_true_last_3": n, "supported": bool((vals and vals[0]) or n >= 2)}


def basket_metrics(conn: DBConn) -> Dict[str, Any]:
    rows = fetchall_dict(conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time ASC,id ASC"))
    count = len(rows)
    br = sum(float(safe_float(r.get("current_R")) or 0.0) for r in rows)
    losing = sum(1 for r in rows if float(safe_float(r.get("current_R")) or 0.0) < 0)
    return {"rows":rows,"open_count":count,"basket_R":br,"basket_pnl_gbp":br*BCO_RISK_PER_TRADE_GBP,
            "losing_pct":losing/count*100.0 if count else 0.0,"phase":basket_phase_from_count(count)}


def ensure_cycle(conn: DBConn, signal_time: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    state = fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'")) or {}
    if metrics.get("open_count", 0) <= 0:
        return state
    if not safe_str(state.get("cycle_id")) or safe_str(state.get("status")).upper() == "FLAT":
        clean = re.sub(r"[^0-9A-Za-z]", "", signal_time or now_utc_iso())[-20:]
        cycle = f"BCO_LONG_{clean}"
        conn.execute("""
            UPDATE basket_state SET cycle_id=?,cycle_started_at=?,status='ACTIVE',high_water_R=0,high_water_seen_at=?,
                realized_R_cycle=0,banked_R_cycle=0,updated_at_utc=? WHERE singleton_key='BCO_LONG'
        """, (cycle, signal_time, signal_time, now_utc_iso()))
        conn.execute("UPDATE trades SET cycle_id=? WHERE status='OPEN' AND (cycle_id IS NULL OR cycle_id='')", (cycle,))
        state = fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'")) or {}
    return state


def regime(conn: DBConn) -> str:
    rows = fetchall_dict(conn.execute("SELECT exec_close,exec_high,exec_low FROM raw_signals WHERE exec_close IS NOT NULL ORDER BY id DESC LIMIT 121"))
    if len(rows) < 121: return "unknown"
    latest = safe_float(rows[0].get("exec_close")); oldest = safe_float(rows[-1].get("exec_close"))
    highs = [safe_float(r.get("exec_high")) for r in rows]; lows = [safe_float(r.get("exec_low")) for r in rows]
    highs = [x for x in highs if x is not None]; lows = [x for x in lows if x is not None]
    if latest is None or oldest is None or oldest <= 0 or not highs or not lows: return "unknown"
    ret = (latest / oldest - 1.0) * 100.0
    rng = (max(highs)/min(lows)-1.0)*100.0 if min(lows)>0 else 0.0
    if ret >= BCO_SL_PCT: return "strong_favourable"
    if ret <= -BCO_SL_PCT: return "adverse"
    if abs(ret) <= BCO_SL_PCT*0.5 and rng >= BCO_SL_PCT*1.5: return "flat_choppy"
    return "normal"


def extension_decision(regime_name: str, return_pct: float, mfe_pct: float) -> Tuple[bool, List[str]]:
    ret_r = return_pct / BCO_SL_PCT; mfe_r = mfe_pct / BCO_SL_PCT
    if regime_name == "adverse": return False, ["directional_regime_adverse"]
    if regime_name == "flat_choppy": return False, ["directional_regime_flat_choppy"]
    if regime_name == "strong_favourable": min_ret, min_mfe, max_give = 0.0, 0.25, 0.65
    else: min_ret, min_mfe, max_give = 0.25, 0.75, 0.45
    reasons: List[str] = []
    if ret_r < min_ret: reasons.append("return_not_strong_enough")
    if mfe_r < min_mfe: reasons.append("mfe_not_strong_enough")
    if mfe_pct > 0 and (mfe_pct-return_pct)/mfe_pct > max_give: reasons.append("too_much_giveback")
    return (not reasons), reasons


def protect_fraction(hold: int) -> Tuple[float, str]:
    if hold >= 120: return BCO_PROTECT_120, "120h_plus_late_runner"
    if hold >= 96: return BCO_PROTECT_96, "96h_plus_mature_runner"
    if hold >= 72: return BCO_PROTECT_72, "72h_plus_runner"
    return BCO_PROTECT_48, "48h_plus_runner"


def mark_trade_closed(conn: DBConn, trade: Dict[str, Any], signal_time: str, exit_price: float, reason: str, rr: Optional[float] = None) -> float:
    entry = float(safe_float(trade.get("entry_price")) or 0.0)
    if rr is None:
        rr = (((exit_price-entry)/entry)*100.0)/BCO_SL_PCT if entry > 0 else 0.0
    pnl = rr * float(safe_float(trade.get("effective_risk_gbp")) or safe_float(trade.get("requested_risk_gbp")) or BCO_RISK_PER_TRADE_GBP)
    conn.execute("""
        UPDATE trades SET status='CLOSED',exit_time=?,exit_price=?,exit_reason=?,realized_R=?,realized_pnl_gbp=?,
            current_R=?,current_price=?,updated_at_utc=? WHERE trade_id=?
    """, (signal_time, exit_price, reason, rr, pnl, rr, exit_price, now_utc_iso(), trade.get("trade_id")))
    return rr


def execute_or_sim_close(conn: DBConn, trade: Dict[str, Any], signal_time: str, exit_price: float, reason: str, rr: Optional[float] = None) -> Tuple[bool, float]:
    broker_id = safe_str(trade.get("broker_trade_id"))
    if broker_id:
        # A broker-linked trade must never be marked locally closed unless this
        # service is explicitly authorised to manage it and OANDA confirms the close.
        if not BCO_AUTO_MANAGEMENT_ENABLED:
            return False, 0.0
        resp = close_broker_trade(broker_id, safe_str(trade.get("trade_id")), reason)
        if not resp.get("ok"):
            return False, 0.0
    realized = mark_trade_closed(conn, trade, signal_time, exit_price, reason, rr)
    return True, realized


def set_managed_stop(conn: DBConn, trade: Dict[str, Any], current: float, fraction: float, stage: str, signal_time: str, event_type: str) -> bool:
    entry = float(safe_float(trade.get("entry_price")) or 0.0); old = safe_float(trade.get("managed_stop_price"))
    if entry <= 0 or current <= entry: return False
    new = entry + (current-entry)*fraction
    new = min(new, current*0.9999)
    min_step = entry * BCO_MIN_STOP_STEP_PCT / 100.0
    if old is not None and new <= old + min_step: return False
    broker_id = safe_str(trade.get("broker_trade_id")); write_success = True
    if broker_id and not BCO_AUTO_MANAGEMENT_ENABLED:
        # Do not let a shadow/local protection state drift ahead of the actual
        # OANDA trade. Broker-linked protection requires explicit management enablement.
        return False
    if broker_id and BCO_AUTO_MANAGEMENT_ENABLED:
        wr = update_broker_stop(broker_id, new, safe_str(trade.get("trade_id")))
        write_success = bool(wr.get("ok"))
        if not write_success:
            conn.execute("""
                INSERT INTO managed_stop_events(created_at_utc,signal_time,cycle_id,trade_id,broker_trade_id,hold_candles,event_type,
                    rule_stage,old_stop_price,new_stop_price,protect_fraction,protected_R,broker_write_success,note)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (now_utc_iso(),signal_time,trade.get("cycle_id"),trade.get("trade_id"),broker_id,trade.get("hold_candles"),event_type,
                  stage,old,new,fraction,((new-entry)/entry*100.0)/BCO_SL_PCT,False,"Broker stop write failed; local stop NOT advanced."))
            return False
    conn.execute("UPDATE trades SET managed_stop_price=?,managed_stop_stage=?,updated_at_utc=? WHERE trade_id=?", (new,stage,now_utc_iso(),trade.get("trade_id")))
    conn.execute("""
        INSERT INTO managed_stop_events(created_at_utc,signal_time,cycle_id,trade_id,broker_trade_id,hold_candles,event_type,
            rule_stage,old_stop_price,new_stop_price,protect_fraction,protected_R,broker_write_success,note)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (now_utc_iso(),signal_time,trade.get("cycle_id"),trade.get("trade_id"),broker_id,trade.get("hold_candles"),event_type,
          stage,old,new,fraction,((new-entry)/entry*100.0)/BCO_SL_PCT,write_success,"Tighten only; never loosen existing protection."))
    return True


def update_trade_on_signal(conn: DBConn, trade: Dict[str, Any], signal: Dict[str, Any], support: Dict[str, Any]) -> Dict[str, Any]:
    signal_time = safe_str(signal.get("timestamp_readable")); current = safe_float(signal.get("exec_close")); hi = safe_float(signal.get("exec_high")); lo = safe_float(signal.get("exec_low"))
    entry = safe_float(trade.get("entry_price"))
    if current is None or entry is None or entry <= 0: return {"closed":False}
    hi = float(hi if hi is not None else current); lo = float(lo if lo is not None else current)
    hold = int(safe_float(trade.get("hold_candles")) or 0) + 1
    managed = safe_float(trade.get("managed_stop_price")); hard = float(safe_float(trade.get("hard_sl_price")) or entry*(1-BCO_SL_PCT/100.0))

    # In shadow/locked mode we can use candle lows for simulated stops. For broker-linked
    # trades, OANDA is authoritative; reconciliation will detect a broker-side close.
    if not safe_str(trade.get("broker_trade_id")):
        if managed is not None and lo <= managed:
            rr = ((managed-entry)/entry*100.0)/BCO_SL_PCT
            ok, _ = execute_or_sim_close(conn, trade, signal_time, managed, f"managed_stop:{safe_str(trade.get('managed_stop_stage'))}", rr)
            return {"closed":ok,"reason":"managed_stop","R":rr}
        if lo <= hard:
            ok, _ = execute_or_sim_close(conn, trade, signal_time, hard, "emergency_sl", -1.0)
            return {"closed":ok,"reason":"emergency_sl","R":-1.0}

    highest = max(float(safe_float(trade.get("highest_high")) or entry), hi)
    lowest = min(float(safe_float(trade.get("lowest_low")) or entry), lo)
    ret = (current-entry)/entry*100.0; mfe=max(0.0,(highest-entry)/entry*100.0); mae=min(0.0,(lowest-entry)/entry*100.0); rr=ret/BCO_SL_PCT
    d48=safe_str(trade.get("decision_48")); d72=safe_str(trade.get("decision_72")); exit_now=False; reason=""
    reg=regime(conn)
    if hold >= 48 and not d48:
        passed,reasons=extension_decision(reg,ret,mfe)
        if (not passed) and support.get("supported"):
            m=basket_metrics(conn); heavy=m["basket_R"]<=-2 and m["losing_pct"]>=60
            blocked=rr<0 or reg in {"adverse","flat_choppy"} or heavy or "too_much_giveback" in reasons
            if not blocked: passed=True; reasons=["candidate_supported_48h_extension_override"]
        d48="extend" if passed else "exit:"+",".join(reasons)
        if not passed: exit_now=True; reason="exit_48_no_extension:"+",".join(reasons)
    if hold >= 72 and d48 == "extend" and not d72 and not exit_now:
        passed,reasons=extension_decision(reg,ret,mfe); d72="extend" if passed else "exit:"+",".join(reasons)
        if not passed: exit_now=True; reason="exit_72_no_extension:"+",".join(reasons)
    conn.execute("""
        UPDATE trades SET current_price=?,hold_candles=?,highest_high=?,lowest_low=?,return_pct=?,mfe_pct=?,mae_pct=?,current_R=?,
            decision_48=?,decision_72=?,updated_at_utc=? WHERE trade_id=?
    """, (current,hold,highest,lowest,ret,mfe,mae,rr,d48,d72,now_utc_iso(),trade.get("trade_id")))
    if exit_now:
        refreshed=dict(trade); refreshed.update({"current_R":rr,"current_price":current,"hold_candles":hold,"decision_48":d48,"decision_72":d72})
        ok,_=execute_or_sim_close(conn,refreshed,signal_time,current,reason,rr)
        return {"closed":ok,"reason":reason,"R":rr}
    if hold >= BCO_MIN_HOLD_HOURS and rr > 0:
        fraction,stage=protect_fraction(hold)
        refreshed=dict(trade); refreshed.update({"current_price":current,"hold_candles":hold,"managed_stop_price":managed})
        set_managed_stop(conn,refreshed,float(current),fraction,stage,signal_time,"POST48_MANAGED_STOP")
    return {"closed":False,"R":rr,"hold":hold}


def create_trade(conn: DBConn, raw_signal_id: int, signal: Dict[str, Any], cycle_id: str) -> Optional[str]:
    metrics=basket_metrics(conn)
    if metrics["open_count"] >= BCO_MAX_OPEN_TRADES: return None
    entry=safe_float(signal.get("exec_close")); signal_time=safe_str(signal.get("timestamp_readable")); signal_id=safe_str(signal.get("signal_id"))
    if entry is None or entry <= 0: return None
    trade_id=f"BCO_LONG_{raw_signal_id}_{re.sub(r'[^0-9A-Za-z]','',signal_time)[-16:]}"
    hard=entry*(1-BCO_SL_PCT/100.0)
    conn.execute("""
        INSERT INTO trades(trade_id,status,direction,cycle_id,entry_raw_signal_id,entry_signal_id,entry_time,entry_price,
            requested_risk_gbp,effective_risk_gbp,sl_pct,hard_sl_price,current_price,highest_high,lowest_low,created_at_utc,updated_at_utc)
        VALUES(?, 'OPEN','long',?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (trade_id,cycle_id,raw_signal_id,signal_id,signal_time,entry,BCO_RISK_PER_TRADE_GBP,BCO_RISK_PER_TRADE_GBP,BCO_SL_PCT,hard,entry,entry,entry,now_utc_iso(),now_utc_iso()))
    if BCO_AUTO_ENTRY_ENABLED:
        result=open_bco_broker_trade(trade_id)
        if result.get("ok"):
            fill=result.get("fill") or {}; prev=result.get("preview") or {}
            broker_trade_id=safe_str(fill.get("broker_trade_id")); fill_price=safe_float(fill.get("price")) or entry
            effective=safe_float(prev.get("effective_risk_gbp")) or BCO_RISK_PER_TRADE_GBP
            broker_stop=fill_price*(1-BCO_SL_PCT/100.0)
            conn.execute("""
                UPDATE trades SET broker_trade_id=?,broker_instrument=?,broker_units=?,entry_price=?,current_price=?,highest_high=?,lowest_low=?,
                    effective_risk_gbp=?,hard_sl_price=?,updated_at_utc=? WHERE trade_id=?
            """, (broker_trade_id,BCO_OANDA_INSTRUMENT,fill.get("units"),fill_price,fill_price,fill_price,fill_price,effective,broker_stop,now_utc_iso(),trade_id))
        else:
            # Never pretend a live trade exists when broker entry failed.
            conn.execute("UPDATE trades SET status='ENTRY_FAILED',exit_reason=?,updated_at_utc=? WHERE trade_id=?", (safe_str(result.get("error")),now_utc_iso(),trade_id))
            return None
    return trade_id


def bank_sort_key(row: Dict[str,Any]) -> Tuple[Any,...]:
    hold=int(safe_float(row.get("hold_candles")) or 0); rr=float(safe_float(row.get("current_R")) or 0.0)
    mfe=float(safe_float(row.get("mfe_pct")) or 0.0); ret=float(safe_float(row.get("return_pct")) or 0.0); give=max(0.0,mfe-ret)
    protected=safe_float(row.get("managed_stop_price")) is not None
    if hold < BCO_MIN_HOLD_HOURS: return (0,BCO_MIN_HOLD_HOURS-hold,-give,-rr,safe_str(row.get("entry_time")))
    return (1,1 if protected else 0,-give,ret,-rr,safe_str(row.get("entry_time")))


def execute_protection(conn: DBConn, signal_time: str) -> Dict[str, Any]:
    metrics=basket_metrics(conn); state=ensure_cycle(conn,signal_time,metrics)
    if metrics["open_count"]<=0 or not safe_str(state.get("cycle_id")):
        return {"banked_R":0.0,"banked_trade_ids":[],"cohort_updates":0}
    cycle=safe_str(state.get("cycle_id")); br=float(metrics["basket_R"]); old_hwm=float(safe_float(state.get("high_water_R")) or 0.0); hwm=max(old_hwm,br)
    conn.execute("UPDATE basket_state SET high_water_R=?,high_water_seen_at=?,updated_at_utc=? WHERE singleton_key='BCO_LONG'",
                 (hwm,signal_time if hwm>old_hwm else state.get("high_water_seen_at"),now_utc_iso()))
    banked=0.0; bank_ids: List[str]=[]; cohort_updates=0
    for threshold,fraction in BCO_COHORT_LEVELS:
        if hwm < threshold: continue
        existing=fetchone_dict(conn.execute("SELECT id FROM protection_stages WHERE cycle_id=? AND stage_type='COHORT' AND threshold_R=?",(cycle,threshold)))
        if existing: continue
        candidates=[r for r in basket_metrics(conn)["rows"] if int(safe_float(r.get("hold_candles")) or 0)<48 and float(safe_float(r.get("current_R")) or 0)>0]
        ids=[]
        for t in candidates:
            current=float(safe_float(t.get("current_price")) or 0.0)
            if current>0 and set_managed_stop(conn,t,current,fraction,f"cohort_{int(threshold)}R",signal_time,"PRE48_COHORT_RATCHET"):
                cohort_updates+=1
            ids.append(safe_str(t.get("trade_id")))
        conn.execute("""
            INSERT INTO protection_stages(created_at_utc,updated_at_utc,cycle_id,stage_type,threshold_R,fraction,status,
                armed_at_signal_time,executed_at_signal_time,cohort_trade_ids,reason)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (now_utc_iso(),now_utc_iso(),cycle,"COHORT",threshold,fraction,"EXECUTED" if ids else "NO_ELIGIBLE",signal_time,signal_time,",".join(ids),"One-shot pre-48h cohort protection."))
    for threshold,fraction in BCO_BANK_LEVELS:
        if hwm < threshold: continue
        stage=fetchone_dict(conn.execute("SELECT * FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' AND threshold_R=?",(cycle,threshold)))
        if not stage:
            lower=fetchall_dict(conn.execute("SELECT target_bank_R,status FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' AND threshold_R<?",(cycle,threshold)))
            reserved=sum(float(safe_float(r.get("target_bank_R")) or 0.0) for r in lower if safe_str(r.get("status")).upper() not in {"EXECUTED","EXPIRED_FLAT"})
            base=max(0.0,br-reserved); target=base*fraction
            conn.execute("""
                INSERT INTO protection_stages(created_at_utc,updated_at_utc,cycle_id,stage_type,threshold_R,fraction,status,target_bank_R,armed_at_signal_time,reason)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """,(now_utc_iso(),now_utc_iso(),cycle,"BANK",threshold,fraction,"ARMED",target,signal_time,f"Fixed-at-arm target from {base:.2f}R base."))
            stage=fetchone_dict(conn.execute("SELECT * FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' AND threshold_R=?",(cycle,threshold))) or {}
        if safe_str(stage.get("status")).upper() in {"EXECUTED","NO_ELIGIBLE"}: continue
        target=float(safe_float(stage.get("target_bank_R")) or 0.0)
        eligible=[r for r in basket_metrics(conn)["rows"] if float(safe_float(r.get("current_R")) or 0.0)>0]
        ranked=sorted(eligible,key=bank_sort_key); selected=[]; running=0.0; remaining=list(ranked)
        while remaining and running+0.0001<target:
            need=target-running; finish=[r for r in remaining if float(safe_float(r.get("current_R")) or 0.0)+0.0001>=need]
            if finish:
                bucket=min(bank_sort_key(r)[:2] for r in finish); opts=[r for r in finish if bank_sort_key(r)[:2]==bucket]
                pick=min(opts,key=lambda r:(float(safe_float(r.get("current_R")) or 0.0)-need,bank_sort_key(r)))
            else: pick=remaining[0]
            selected.append(pick); running+=float(safe_float(pick.get("current_R")) or 0.0); remaining=[r for r in remaining if r.get("trade_id")!=pick.get("trade_id")]
        if not selected:
            conn.execute("UPDATE protection_stages SET status='ARMED_WAITING_PROFITABLE_POOL',updated_at_utc=? WHERE id=?",(now_utc_iso(),stage.get("id")))
            continue
        ids=[]; actual=0.0
        for t in selected:
            rr=float(safe_float(t.get("current_R")) or 0.0); px=float(safe_float(t.get("current_price")) or safe_float(t.get("entry_price")) or 0.0)
            ok,val=execute_or_sim_close(conn,t,signal_time,px,f"immediate_bank_{int(threshold)}R",rr)
            if ok: actual+=val; ids.append(safe_str(t.get("trade_id")))
        if ids:
            banked+=actual; bank_ids.extend(ids)
            conn.execute("UPDATE protection_stages SET status='EXECUTED',executed_R=?,executed_at_signal_time=?,selected_trade_ids=?,updated_at_utc=?,reason=? WHERE id=?",
                         (actual,signal_time,",".join(ids),now_utc_iso(),f"Banked whole trades against fixed target {target:.2f}R.",stage.get("id")))
    if banked:
        conn.execute("UPDATE basket_state SET banked_R_cycle=COALESCE(banked_R_cycle,0)+?,realized_R_cycle=COALESCE(realized_R_cycle,0)+?,updated_at_utc=? WHERE singleton_key='BCO_LONG'",
                     (banked,banked,now_utc_iso()))
    return {"banked_R":banked,"banked_trade_ids":bank_ids,"cohort_updates":cohort_updates}


def execute_defence(conn: DBConn, signal_time: str, current: float, action: str, pre_metrics: Dict[str,Any]) -> Dict[str,Any]:
    frac=action_close_fraction(action)
    if frac<=0 or not pre_metrics.get("rows"): return {"closed_count":0,"closed_trade_ids":[],"realized_R":0.0}
    stage=basket_execution_stage(pre_metrics.get("open_count")); maxf=float(stage.get("max_close_fraction") or 0.0)
    if maxf<=0: return {"closed_count":0,"closed_trade_ids":[],"realized_R":0.0,"blocked_by_stage":stage.get("stage")}
    if frac>=1 and not stage.get("full_close_eligible"): frac=maxf
    frac=min(frac,maxf); count=int(pre_metrics.get("open_count") or 0); n=count if frac>=1 else max(1,int(round(count*frac)))
    rows=sorted(list(pre_metrics.get("rows") or []),key=lambda r:float(safe_float(r.get("current_R")) or 0.0))[:n]
    ids=[]; realized=0.0
    for t in rows:
        rr=float(safe_float(t.get("current_R")) or 0.0); ok,val=execute_or_sim_close(conn,t,signal_time,current,f"basket_manager:{action}",rr)
        if ok: ids.append(safe_str(t.get("trade_id"))); realized+=val
    return {"closed_count":len(ids),"closed_trade_ids":ids,"realized_R":realized,"fraction":frac}


def latest_payload_flags(payload: Dict[str,Any]) -> Dict[str,Optional[bool]]:
    ctx=context_8h(payload)
    def maybe(obj: Dict[str,Any], key: str) -> Optional[bool]:
        return parse_bool(obj.get(key), False) if key in obj else None
    return {
        "close20": maybe(payload,"exec_close_gt_ema20"), "close50": maybe(payload,"exec_close_gt_ema50"),
        "hist_up": maybe(payload,"exec_hist_up"), "rsi_up": maybe(payload,"exec_rsi_up"),
        "ctx_bull": maybe(ctx,"ctx_bull_stack"), "d_bull": maybe(payload,"d_bull")
    }


def process_signal(raw_signal_id: int, payload: Dict[str,Any]) -> Dict[str,Any]:
    candidate=bco_long_candidate(payload); signal_time=safe_str(payload.get("timestamp") or payload.get("timestamp_readable") or payload.get("rule_entry_timestamp"))
    current=safe_float(payload.get("exec_close") or payload.get("rule_entry_price")); high=safe_float(payload.get("exec_high")); low=safe_float(payload.get("exec_low"))
    signal_id=safe_str(payload.get("signal_id"))
    if current is None:
        return {"ok":False,"error":"exec_close missing"}
    signal={"timestamp_readable":signal_time,"exec_close":current,"exec_high":high,"exec_low":low,"signal_id":signal_id}
    with _db_lock, get_conn() as conn:
        # idempotency: decision row means this raw signal was already fully processed.
        if fetchone_dict(conn.execute("SELECT id FROM basket_decisions WHERE raw_signal_id=?",(raw_signal_id,))):
            return {"ok":True,"duplicate":True,"raw_signal_id":raw_signal_id}
        support=candidate_support(conn)
        before=basket_metrics(conn)
        state=ensure_cycle(conn,signal_time,before)
        # Update existing trades before deciding defence/entry.
        for t in list(before["rows"]): update_trade_on_signal(conn,t,signal,support)
        mid=basket_metrics(conn); state=ensure_cycle(conn,signal_time,mid)
        old_hwm=float(safe_float(state.get("high_water_R")) or 0.0); hwm=max(old_hwm,float(mid["basket_R"]))
        flags=latest_payload_flags(payload)
        score,status,raw_action,reasons,giveback=calculate_tide_turn_status(candidate,int(support.get("candidate_true_last_3") or 0),hwm,float(mid["basket_R"]),float(mid["losing_pct"]),flags["close20"],flags["close50"],flags["hist_up"],flags["rsi_up"],flags["ctx_bull"],flags["d_bull"])
        action,detail=calculate_tiered_basket_defence(status,giveback,float(mid["losing_pct"]),float(mid["basket_R"]),int(mid["open_count"]),candidate,int(support.get("candidate_true_last_3") or 0))
        defence=execute_defence(conn,signal_time,float(current),action,mid) if BCO_AUTO_MANAGEMENT_ENABLED else {"closed_count":0,"closed_trade_ids":[],"realized_R":0.0}
        after_def=basket_metrics(conn)
        protection=execute_protection(conn,signal_time)
        after_prot=basket_metrics(conn)
        entry_block,block_reason=staged_entry_block(action,int(after_prot["open_count"]),float(after_prot["basket_R"]),status)
        entry_allowed=bool(candidate and not entry_block and after_prot["open_count"]<BCO_MAX_OPEN_TRADES)
        entry_created=False; new_trade_id=None
        if entry_allowed:
            state=ensure_cycle(conn,signal_time,after_prot)
            cycle=safe_str(state.get("cycle_id"))
            if not cycle:
                # First trade creates cycle using current signal time.
                temp_metrics=dict(after_prot); temp_metrics["open_count"]=1
                state=ensure_cycle(conn,signal_time,temp_metrics); cycle=safe_str(state.get("cycle_id"))
            new_trade_id=create_trade(conn,raw_signal_id,signal,cycle)
            entry_created=bool(new_trade_id)
        final=basket_metrics(conn)
        # If basket is now flat, close the cycle cleanly and expire waiting stages.
        if final["open_count"]<=0:
            cycle=safe_str(state.get("cycle_id"))
            if cycle:
                conn.execute("UPDATE protection_stages SET status='EXPIRED_FLAT',updated_at_utc=? WHERE cycle_id=? AND status LIKE 'ARMED%'",(now_utc_iso(),cycle))
            conn.execute("UPDATE basket_state SET status='FLAT',cycle_id=NULL,open_count=0,basket_R=0,basket_pnl_gbp=0,giveback_pct=0,losing_pct=0,basket_phase='FLAT',manager_action='NO_OPEN_BASKET',updated_at_utc=? WHERE singleton_key='BCO_LONG'",(now_utc_iso(),))
        else:
            state=ensure_cycle(conn,signal_time,final); hwm2=max(float(safe_float(state.get("high_water_R")) or 0.0),float(final["basket_R"])); give2=((hwm2-final["basket_R"])/hwm2*100.0) if hwm2>0 and final["basket_R"]<hwm2 else 0.0
            conn.execute("""
                UPDATE basket_state SET status='ACTIVE',last_signal_time=?,open_count=?,basket_R=?,basket_pnl_gbp=?,high_water_R=?,
                    high_water_seen_at=CASE WHEN ?>high_water_R THEN ? ELSE high_water_seen_at END,giveback_pct=?,losing_pct=?,basket_phase=?,
                    tide_score=?,tide_status=?,manager_action=?,manager_detail=?,updated_at_utc=? WHERE singleton_key='BCO_LONG'
            """,(signal_time,final["open_count"],final["basket_R"],final["basket_pnl_gbp"],hwm2,float(final["basket_R"]),signal_time,give2,final["losing_pct"],final["phase"],score,status,action,detail,now_utc_iso()))
        final_state=fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'")) or {}
        conn.execute("""
            INSERT INTO basket_decisions(created_at_utc,raw_signal_id,signal_time,cycle_id,candidate,entry_allowed,entry_created,
                open_before,open_after,basket_R_before,basket_R_after,high_water_R,giveback_pct,losing_pct,basket_phase,tide_score,
                tide_status,manager_action,manager_detail,defence_closed_count,defence_trade_ids,banked_R_this_hour,banked_trade_ids,note)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,(now_utc_iso(),raw_signal_id,signal_time,final_state.get("cycle_id"),candidate,entry_allowed,entry_created,before["open_count"],final["open_count"],before["basket_R"],final["basket_R"],final_state.get("high_water_R"),final_state.get("giveback_pct"),final["losing_pct"],final["phase"],score,status,action,detail,defence.get("closed_count"),",".join(defence.get("closed_trade_ids") or []),protection.get("banked_R"),",".join(protection.get("banked_trade_ids") or []),f"entry_block={block_reason}; raw_action={raw_action}; reasons={','.join(reasons)}"))
    return {"ok":True,"raw_signal_id":raw_signal_id,"candidate":candidate,"entry_allowed":entry_allowed,"entry_created":entry_created,"trade_id":new_trade_id,"basket":snapshot()}


# -----------------------------------------------------------------------------
# Reconciliation — only local BCO broker IDs; never touches foreign trades.
# -----------------------------------------------------------------------------
def reconcile_broker() -> Dict[str,Any]:
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok":False,"skipped":True,"reason":"OANDA not configured"}
    r=oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/openTrades")
    if not r.get("ok"): return {"ok":False,"error":r.get("error")}
    all_open=(r.get("data") or {}).get("trades",[]) or []
    owned=[t for t in all_open if safe_str(t.get("instrument")).upper()==BCO_OANDA_INSTRUMENT] if BCO_OANDA_INSTRUMENT else []
    by_id={safe_str(t.get("id")):t for t in owned}
    updates=[]
    with _db_lock,get_conn() as conn:
        locals=fetchall_dict(conn.execute("SELECT * FROM trades WHERE broker_trade_id IS NOT NULL AND broker_trade_id<>'' AND status='OPEN'"))
        for t in locals:
            bid=safe_str(t.get("broker_trade_id")); match=by_id.get(bid)
            if match:
                price=safe_float(match.get("price")); upl=safe_float(match.get("unrealizedPL"))
                updates.append({"trade_id":t.get("trade_id"),"broker_trade_id":bid,"status":"OPEN","unrealizedPL":upl,"price":price})
            else:
                # Broker is authoritative: mark local trade externally closed. We do not
                # guess final P&L here; transaction/financing sync can be added after smoke test.
                conn.execute("UPDATE trades SET status='BROKER_CLOSED',exit_reason='broker_reconciliation_not_open',exit_time=?,updated_at_utc=? WHERE trade_id=?",
                             (now_utc_iso(),now_utc_iso(),t.get("trade_id")))
                updates.append({"trade_id":t.get("trade_id"),"broker_trade_id":bid,"status":"BROKER_CLOSED"})
    return {"ok":True,"owned_open_count":len(owned),"account_open_count":len(all_open),"updates":updates,"time_utc":now_utc_iso()}


def snapshot() -> Dict[str,Any]:
    with get_conn() as conn:
        state=fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'")) or {}
        open_rows=fetchall_dict(conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time ASC,id ASC"))
        closed=fetchone_dict(conn.execute("SELECT COUNT(*) AS c, COALESCE(SUM(realized_R),0) AS r, COALESCE(SUM(realized_pnl_gbp),0) AS p FROM trades WHERE status IN ('CLOSED','BROKER_CLOSED')")) or {}
        latest=fetchone_dict(conn.execute("SELECT id,received_at_utc,signal_id,timestamp_readable,candidate_8h,signal_side,exec_close FROM raw_signals ORDER BY id DESC LIMIT 1")) or {}
        return {"status":"ok","app":APP_NAME,"policy_version":POLICY_VERSION,"strategy":{"asset":BCO_ASSET,"direction":BCO_DIRECTION,"risk_per_trade_gbp":BCO_RISK_PER_TRADE_GBP,"sl_pct":BCO_SL_PCT,"min_hold_hours":BCO_MIN_HOLD_HOURS,"execution_multiplier":BCO_EXECUTION_MULTIPLIER},"basket":state,"open_trades":open_rows,"closed_summary":closed,"latest_signal":latest,"broker_safety":safety_status(),"account":account_summary() if OANDA_ENABLED and OANDA_ACCOUNT_ID else {"ok":False,"error":"not configured"},"time_utc":now_utc_iso()}


def safety_status() -> Dict[str,Any]:
    allowed,reason=oanda_orders_allowed()
    return {"orders_allowed":allowed,"reason":reason,"oanda_enabled":OANDA_ENABLED,"oanda_env":OANDA_ENV,"read_only":BROKER_READ_ONLY,"execution_enabled":BROKER_EXECUTION_ENABLED,"kill_switch":BROKER_KILL_SWITCH,"live_execution_armed":BCO_LIVE_EXECUTION_ARMED,"auto_entry":BCO_AUTO_ENTRY_ENABLED,"auto_management":BCO_AUTO_MANAGEMENT_ENABLED,"configured_instrument":BCO_OANDA_INSTRUMENT,"exact_instrument_ownership":True,"direction":BCO_DIRECTION,"execution_multiplier":1.0}


def preflight() -> Dict[str,Any]:
    checks=[]
    def add(name:str,ok:bool,msg:str,data:Optional[Dict[str,Any]]=None): checks.append({"name":name,"ok":bool(ok),"message":msg,"data":data or {}})
    add("Database configured", bool(DATABASE_URL), "Railway production should provide DATABASE_URL from Postgres.", {"postgres":USE_POSTGRES})
    add("Direction locked LONG", BCO_DIRECTION=="long", "Initial live v1 is LONG only.")
    add("Execution multiplier locked", BCO_EXECUTION_MULTIPLIER==1.0, "Sizing multiplier is hard-locked at 1.00x.")
    add("OANDA credentials", bool(OANDA_ACCOUNT_ID and OANDA_API_TOKEN), "OANDA account/token present.")
    summary=account_summary() if OANDA_ACCOUNT_ID and OANDA_API_TOKEN else {"ok":False}
    add("OANDA read access", bool(summary.get("ok")), "Shared account NAV/balance can be read.", summary)
    discovery=discover_bco_instruments() if OANDA_ACCOUNT_ID and OANDA_API_TOKEN else {"ok":False,"matches":[]}
    add("BCO instrument configured", bool(BCO_OANDA_INSTRUMENT), "Exact Brent instrument must be discovered and set before writes.", {"configured":BCO_OANDA_INSTRUMENT,"matches":[{"name":m.get("name"),"displayName":m.get("displayName"),"minimumTradeSize":m.get("minimumTradeSize"),"tradeUnitsPrecision":m.get("tradeUnitsPrecision")} for m in discovery.get("matches",[])]})
    if BCO_OANDA_INSTRUMENT:
        rp=risk_preview(); add("Risk preview", bool(rp.get("ok")), "£5/3.5% sizing preview must succeed before practice smoke test.", rp)
    saf=safety_status(); add("Initial broker lock", not saf["orders_allowed"], "First deployment should remain locked/read-only.", saf)
    return {"status":"PASS" if all(c["ok"] for c in checks if c["name"] not in {"BCO instrument configured","Risk preview"}) else "CHECK","checks":checks,"time_utc":now_utc_iso()}


# -----------------------------------------------------------------------------
# Background reconcile worker
# -----------------------------------------------------------------------------
def _worker() -> None:
    while not _worker_stop.wait(BROKER_RECONCILE_INTERVAL_SECONDS):
        try:
            if OANDA_ENABLED and OANDA_ACCOUNT_ID:
                reconcile_broker()
        except Exception as e:
            log_event("reconcile_error", str(e))


def start_worker() -> None:
    global _worker_started
    if _worker_started: return
    _worker_started=True
    threading.Thread(target=_worker,name="bco-broker-reconcile",daemon=True).start()


@app.on_event("startup")
def startup_event() -> None:
    init_db(); start_worker(); log_event("startup", APP_NAME, {"safety":safety_status()})


# -----------------------------------------------------------------------------
# API / dashboard
# -----------------------------------------------------------------------------
def check_webhook(secret: str) -> None:
    if not WEBHOOK_SECRET or WEBHOOK_SECRET == "change-me" or secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid webhook secret")


def check_admin(x_admin_secret: Optional[str]) -> None:
    if not ADMIN_SECRET or ADMIN_SECRET == "change-me-too" or safe_str(x_admin_secret) != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="invalid admin secret")


@app.get("/health")
def health():
    db_ok=True; db_error=""
    try:
        with get_conn() as conn: fetchone_dict(conn.execute("SELECT 1 AS ok"))
    except Exception as e: db_ok=False; db_error=str(e)
    return {"status":"ok" if db_ok else "degraded","app":APP_NAME,"version":APP_VERSION,"database":{"ok":db_ok,"postgres":USE_POSTGRES,"error":db_error},"safety":safety_status(),"time_utc":now_utc_iso()}


@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request, secret: str = Query(default="")):
    check_webhook(secret)
    body=await request.json()
    if not isinstance(body,dict): raise HTTPException(status_code=400,detail="JSON object required")
    raw_id,payload,duplicate=store_signal(body)
    if duplicate: return {"status":"duplicate","raw_signal_id":raw_id}
    try:
        result=process_signal(raw_id,payload)
    except Exception as e:
        log_event("signal_processing_error",str(e),{"raw_signal_id":raw_id})
        raise
    return {"status":"ok","raw_signal_id":raw_id,"result":result}


@app.get("/snapshot")
def snapshot_endpoint(): return snapshot()


@app.get("/broker/preflight")
def preflight_endpoint(): return preflight()


@app.get("/broker/instruments/discover")
def discover_endpoint(): return discover_bco_instruments()


@app.get("/broker/risk-preview")
def risk_preview_endpoint(target_risk_gbp: float = Query(default=BCO_RISK_PER_TRADE_GBP, gt=0)): return risk_preview(target_risk_gbp)


@app.post("/admin/broker/practice-smoke-open")
def practice_smoke_open(x_admin_secret: Optional[str] = Header(default=None)):
    check_admin(x_admin_secret)
    if OANDA_ENV != "practice": raise HTTPException(status_code=400,detail="smoke-open is practice-only")
    if not BCO_PRACTICE_SMOKE_TEST_ENABLED: raise HTTPException(status_code=403,detail="BCO_PRACTICE_SMOKE_TEST_ENABLED=false")
    # This route still requires every normal broker safety lock to be deliberately open.
    local_id=f"SMOKE_BCO_{int(time.time())}"
    result=open_bco_broker_trade(local_id)
    return {"status":"ok" if result.get("ok") else "blocked_or_failed","result":result}


@app.post("/admin/broker/practice-smoke-close/{broker_trade_id}")
def practice_smoke_close(broker_trade_id: str, x_admin_secret: Optional[str] = Header(default=None)):
    check_admin(x_admin_secret)
    if OANDA_ENV != "practice": raise HTTPException(status_code=400,detail="smoke-close is practice-only")
    if not BCO_PRACTICE_SMOKE_TEST_ENABLED: raise HTTPException(status_code=403,detail="BCO_PRACTICE_SMOKE_TEST_ENABLED=false")
    return close_broker_trade(broker_trade_id, f"SMOKE_{broker_trade_id}", "practice_smoke_test")


@app.post("/admin/reconcile")
def reconcile_endpoint(x_admin_secret: Optional[str] = Header(default=None)):
    check_admin(x_admin_secret); return reconcile_broker()


def csv_response(rows: List[Dict[str,Any]], filename: str) -> Response:
    if not rows: return Response(content="",media_type="text/csv",headers={"Content-Disposition":f'attachment; filename="{filename}"'})
    out=io.StringIO(); fields=[]
    for r in rows:
        for k in r.keys():
            if k not in fields: fields.append(k)
    w=csv.DictWriter(out,fieldnames=fields,extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    return Response(content=out.getvalue(),media_type="text/csv",headers={"Content-Disposition":f'attachment; filename="{filename}"'})


@app.get("/export/{table}.csv")
def export_table(table: str):
    allowed={"raw-signals":"raw_signals","trades":"trades","basket-decisions":"basket_decisions","protection-stages":"protection_stages","managed-stops":"managed_stop_events","execution-audit":"execution_audit","system-events":"system_events"}
    if table not in allowed: raise HTTPException(status_code=404,detail="unknown export")
    with get_conn() as conn: rows=fetchall_dict(conn.execute(f"SELECT * FROM {allowed[table]} ORDER BY id ASC"))
    return csv_response(rows,f"bco-{table}.csv")


@app.get("/export/all.zip")
def export_all_zip():
    allowed={"raw-signals":"raw_signals","trades":"trades","basket-decisions":"basket_decisions","protection-stages":"protection_stages","managed-stops":"managed_stop_events","execution-audit":"execution_audit","system-events":"system_events"}
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        with get_conn() as conn:
            for name,table in allowed.items():
                rows=fetchall_dict(conn.execute(f"SELECT * FROM {table} ORDER BY id ASC"))
                sio=io.StringIO()
                if rows:
                    fields=[]
                    for r in rows:
                        for k in r.keys():
                            if k not in fields: fields.append(k)
                    w=csv.DictWriter(sio,fieldnames=fields); w.writeheader(); w.writerows(rows)
                z.writestr(f"bco-{name}.csv",sio.getvalue())
        z.writestr("bco-live-snapshot.json",json.dumps(snapshot(),indent=2,default=str))
        z.writestr("manifest.json",json.dumps({"app":APP_NAME,"version":APP_VERSION,"policy":POLICY_VERSION,"asset":"BCOUSD","direction":"long","risk_gbp":BCO_RISK_PER_TRADE_GBP,"sl_pct":BCO_SL_PCT,"generated_at_utc":now_utc_iso()},indent=2))
    return Response(content=buf.getvalue(),media_type="application/zip",headers={"Content-Disposition":'attachment; filename="bco-live-analysis.zip"'})


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    s=snapshot(); b=s.get("basket") or {}; safety=s.get("broker_safety") or {}; acct=s.get("account") or {}; latest=s.get("latest_signal") or {}
    open_rows=s.get("open_trades") or []
    rows="".join(f"<tr><td>{esc(t.get('trade_id'))}</td><td>{esc(t.get('entry_time'))}</td><td>{safe_float(t.get('entry_price')) or 0:.3f}</td><td>{int(safe_float(t.get('hold_candles')) or 0)}</td><td>{safe_float(t.get('current_R')) or 0:.2f}R</td><td>{esc(t.get('decision_48'))}</td><td>{esc(t.get('decision_72'))}</td><td>{esc(t.get('managed_stop_stage'))}</td><td>{esc(t.get('broker_trade_id'))}</td></tr>" for t in open_rows)
    if not rows: rows="<tr><td colspan='9'>No open BCO trades.</td></tr>"
    allowed="YES" if safety.get("orders_allowed") else "NO — LOCKED"
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='60'><title>{esc(APP_NAME)}</title><style>
    body{{background:#0b1220;color:#e5e7eb;font-family:Arial,sans-serif;margin:22px}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}} .card{{background:#111827;border:1px solid #243044;border-radius:10px;padding:14px}} .label{{color:#94a3b8;font-size:12px;text-transform:uppercase}} .value{{font-size:24px;font-weight:700;margin-top:5px}} .ok{{color:#86efac}} .bad{{color:#fca5a5}} table{{width:100%;border-collapse:collapse;background:#111827;margin-top:12px}} th,td{{padding:8px;border-bottom:1px solid #243044;text-align:left;font-size:13px}} a{{color:#7dd3fc}} code{{color:#fde68a}} .note{{background:#172033;padding:10px;border-radius:8px;margin:12px 0}}</style></head><body>
    <h1>BCO Live</h1><div class='note'>Production bootstrap. Shared OANDA account may be read, but this service owns <strong>BCO only</strong>. Initial live v1 is LONG-only, £{BCO_RISK_PER_TRADE_GBP:.2f}/R requested, {BCO_SL_PCT:.2f}% SL, 48h minimum then hourly management.</div>
    <div class='grid'>
      <div class='card'><div class='label'>Broker writes</div><div class='value {'ok' if safety.get('orders_allowed') else 'bad'}'>{allowed}</div><div>{esc(safety.get('reason'))}</div></div>
      <div class='card'><div class='label'>Shared OANDA NAV</div><div class='value'>£{safe_float(acct.get('NAV')) or 0:,.2f}</div><div>Margin available £{safe_float(acct.get('marginAvailable')) or 0:,.2f}</div></div>
      <div class='card'><div class='label'>BCO open basket</div><div class='value'>{int(safe_float(b.get('open_count')) or 0)} trades</div><div>{safe_float(b.get('basket_R')) or 0:.2f}R / £{safe_float(b.get('basket_pnl_gbp')) or 0:,.2f}</div></div>
      <div class='card'><div class='label'>High-water / giveback</div><div class='value'>{safe_float(b.get('high_water_R')) or 0:.2f}R</div><div>{safe_float(b.get('giveback_pct')) or 0:.1f}% giveback</div></div>
      <div class='card'><div class='label'>Basket manager</div><div class='value'>{esc(b.get('tide_status') or 'FLAT')}</div><div>{esc(b.get('manager_action'))}</div></div>
      <div class='card'><div class='label'>Latest signal</div><div class='value'>{'CANDIDATE' if parse_bool(latest.get('candidate_8h')) else 'NO TRADE'}</div><div>{esc(latest.get('timestamp_readable'))} @ {safe_float(latest.get('exec_close')) or 0:.3f}</div></div>
    </div>
    <h2>Open BCO Trades</h2><table><thead><tr><th>Trade</th><th>Entry</th><th>Price</th><th>Age</th><th>R</th><th>48h</th><th>72h</th><th>Protection</th><th>Broker ID</th></tr></thead><tbody>{rows}</tbody></table>
    <p><a href='/snapshot'>Snapshot JSON</a> | <a href='/broker/preflight'>Broker preflight</a> | <a href='/broker/instruments/discover'>Discover BCO instrument</a> | <a href='/broker/risk-preview'>Risk preview</a> | <a href='/export/all.zip'>Download live analysis ZIP</a></p>
    </body></html>"""


@app.get("/")
def root(): return {"app":APP_NAME,"dashboard":"/dashboard","health":"/health","preflight":"/broker/preflight"}
