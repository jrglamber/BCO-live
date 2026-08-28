# BCO Live v0.1.1 — instrument price precision fix
# Project Exit Plan
#
# Design sources:
# - BCO research/live-sim v10.1.08 for BCO candidate, 3.5% SL, 48h+ management,
#   runner protection and the original basket-banking implementation.
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
import queue
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

APP_NAME = "Project Exit Plan — BCO v0.8.4 — Manual New Basket Cycle + 50R Harvest + Exit Shadows"
APP_VERSION = "0.8.4"
POLICY_VERSION = "bco_v0.8.4_manual_economic_basket_reset_2026_08_28"

# v0.8.4 — manual economic basket-cycle reset.
# Adds the same explicit "Start new basket cycle / reset HWM" control used by
# Metals, inside Broker / OANDA / Accounting.
#
# This is FAMILY accounting/protection only:
# - archives the previous BCO family HWM and unfinished harvest stages;
# - rebases current-cycle HWM to current open-basket R (0 if negative);
# - creates a fresh economic cycle with 50R as the next harvest;
# - surviving trades keep their age, MFE/MAE, stops, Current Manager state,
#   OANDA trade IDs and MFE50/ATR2 forward-shadow rows.
# No OANDA order is sent by the reset.
#
# v0.8.3 — BCO basket-harvest simplification.
# The production trade manager remains unchanged. Basket harvesting is tightened
# to a coarse 50R-spaced ladder after a real ~50R BCO basket round-tripped:
#   50R  -> bank 20% of the remaining profitable basket
#   100R -> bank 20%
#   150R+ -> bank 25% at every additional +50R checkpoint
# The old separate pre-48 cohort ratchet is retired as redundant because normal
# checkpoint banking already permits profitable whole trades younger than 48h.
# Historical COHORT rows remain untouched for audit/research.
#
# v0.8.1 — operational repair only.
# Production strategy/broker/manager rules remain identical to v0.8.0.
# This build makes the forward exit-challenger research schema self-healing so
# an interrupted/deferred Railway bootstrap cannot leave the Research/Evidence
# Lab broken with UndefinedTable: bco_exit_challenger_shadow.


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


def _bco_zone(name: str):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone.utc


def bco_display_candle_time(value: Any) -> str:
    """Display TradingView candle timestamps in UK time, matching Metals.

    This is presentation-only. Stored timestamps are not altered.
    """
    s = safe_str(value)
    if not s:
        return ""

    source_tz = _bco_zone(BCO_SIGNAL_CANDLE_TIMEZONE)
    display_tz = _bco_zone(BCO_DISPLAY_TIMEZONE)

    for fmt in (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=source_tz)
            return dt.astimezone(display_tz).strftime("%Y-%m-%d %H:%M") + f" {BCO_DISPLAY_TIME_LABEL}"
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=source_tz)
        return dt.astimezone(display_tz).strftime("%Y-%m-%d %H:%M") + f" {BCO_DISPLAY_TIME_LABEL}"
    except Exception:
        return s


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

# v0.8.3 — simplified immediate basket harvesting.
# Bank percentages apply to the REMAINING PROFITABLE OPEN BASKET at the
# checkpoint. Selection still uses whole profitable trades and does not require
# a selected trade to be 48h old.
BCO_BANK_FIRST_LEVEL_R = 50.0
BCO_BANK_STEP_R = 50.0
BCO_BANK_50_FRACTION = 0.20
BCO_BANK_100_FRACTION = 0.20
BCO_BANK_150_PLUS_FRACTION = 0.25
BCO_BANK_MAX_LEVEL_R = max(
    300.0, min(float(os.getenv("BCO_BANK_MAX_LEVEL_R", "5000")), 100000.0)
)

# Compatibility/display seed only. Execution itself is generated dynamically
# every +50R and therefore does not stop at 300R.
BCO_BANK_LEVELS = [
    (50.0, BCO_BANK_50_FRACTION),
    (100.0, BCO_BANK_100_FRACTION),
    (150.0, BCO_BANK_150_PLUS_FRACTION),
    (200.0, BCO_BANK_150_PLUS_FRACTION),
    (250.0, BCO_BANK_150_PLUS_FRACTION),
    (300.0, BCO_BANK_150_PLUS_FRACTION),
]

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
BCO_ACTION_RETRY_MAX_ATTEMPTS = max(3, int(float(os.getenv("BCO_ACTION_RETRY_MAX_ATTEMPTS", "1000"))))
BCO_TRANSACTION_SYNC_ENABLED = env_bool("BCO_TRANSACTION_SYNC_ENABLED", True)
BCO_TRANSACTION_SYNC_PAGE_LIMIT = max(100, min(int(float(os.getenv("BCO_TRANSACTION_SYNC_PAGE_LIMIT", "1000"))), 5000))
BCO_HEALTH_SIGNAL_STALE_SECONDS = max(3600, int(float(os.getenv("BCO_HEALTH_SIGNAL_STALE_SECONDS", "10800"))))
BCO_HEALTH_RECONCILE_STALE_SECONDS = max(60, int(float(os.getenv("BCO_HEALTH_RECONCILE_STALE_SECONDS", "180"))))

# v0.7.2 — self-healing signal/manager recovery.
# A TradingView signal is durable as soon as it is inserted into raw_signals.
# If processing dies after that insert but before basket_decisions is written,
# the background worker safely replays the unprocessed tail in chronological order.
BCO_SIGNAL_RECOVERY_ENABLED = env_bool("BCO_SIGNAL_RECOVERY_ENABLED", True)
BCO_SIGNAL_RECOVERY_BATCH_LIMIT = max(
    1, min(int(float(os.getenv("BCO_SIGNAL_RECOVERY_BATCH_LIMIT", "12"))), 100)
)
BCO_SIGNAL_RECOVERY_INTERVAL_SECONDS = max(
    3.0,
    min(float(os.getenv("BCO_SIGNAL_RECOVERY_INTERVAL_SECONDS", "10")), 60.0),
)

# v0.7.6 — dashboard candle-time parity with Metals.
# Display-only: stored timestamps and all trading/management chronology remain unchanged.
BCO_SIGNAL_CANDLE_TIMEZONE = os.getenv("BCO_SIGNAL_CANDLE_TIMEZONE", "America/Chicago").strip()
BCO_DISPLAY_TIMEZONE = os.getenv("BCO_DISPLAY_TIMEZONE", "Europe/London").strip()
BCO_DISPLAY_TIME_LABEL = os.getenv("BCO_DISPLAY_TIME_LABEL", "UK").strip() or "UK"


# v0.8.0 — BCO EXIT CHALLENGERS, FORWARD SHADOW ONLY.
# These settings are NEVER consumed by live entry/exit/stop/banking/broker code.
BCO_EXIT_SHADOW_ENABLED = env_bool("BCO_EXIT_SHADOW_ENABLED", True)
BCO_EXIT_SHADOW_VERSION = "bco_exit_shadow_v1_mfe50_atr2_2026_08_27"
BCO_EXIT_SHADOW_MIN_HOLD_HOURS = 48
BCO_EXIT_SHADOW_MFE_GIVEBACK_FRACTION = 0.50
BCO_EXIT_SHADOW_ATR_MULTIPLIER = 2.0
BCO_EXIT_SHADOW_ATR_PERIOD = 14
BCO_EXIT_SHADOW_ATR_LOOKBACK_BARS = 500
BCO_EXIT_SHADOW_PAIR_TIE_R = 0.10
BCO_EXIT_SHADOW_REVERSAL_SAVE_DELTA_R = 0.50
BCO_EXIT_SHADOW_LARGE_WINNER_R = 2.00
BCO_EXIT_SHADOW_LARGE_WINNER_SACRIFICE_R = 0.75
BCO_EXIT_SHADOW_EXECUTION_AUTHORITY = False

# v0.8.2 — PostgreSQL boolean compatibility repair for exit-shadow schema.
# Postgres BOOLEAN columns require FALSE/TRUE defaults rather than integer 0/1,
# and boolean predicates cannot use COALESCE(boolean,0)=0. This repair changes
# only research-shadow schema/predicates; production BCO trading logic is untouched.
# v0.8.1 — dedicated self-healing schema state for the research-only exit shadow.
# This table is deliberately independent of production trading state. If Railway
# serves the dashboard before deferred bootstrap DDL finishes (or a prior DDL
# transaction was rolled back), any exit-shadow read/write can recreate and
# verify the table idempotently without touching trades/basket/broker logic.
_bco_exit_shadow_schema_lock = threading.RLock()
_bco_exit_shadow_schema_ready = False
_bco_exit_shadow_schema_last_error = ""
_bco_exit_shadow_schema_checked_at = ""


# Shared-account guard. This service may read account-wide NAV/margin, but all
# writes and strategy P&L are restricted to BCO-owned trades only.
FORBIDDEN_FOREIGN_INSTRUMENT_TOKENS = {
    "NAS100", "SPX500", "US500", "XAU", "XAG", "JP225", "NIKKEI"
}

app = FastAPI(title=APP_NAME, version=APP_VERSION)
_db_lock = threading.RLock()
_worker_stop = threading.Event()
_worker_started = False
_worker_thread: Optional[threading.Thread] = None
_signal_recovery_started = False
_signal_recovery_thread: Optional[threading.Thread] = None


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


_BCO_DB_COMPAT_ALIASES = {
    "current_r": "current_R",
    "realized_r": "realized_R",
    "basket_r": "basket_R",
    "high_water_r": "high_water_R",
    "realized_r_cycle": "realized_R_cycle",
    "banked_r_cycle": "banked_R_cycle",
    "threshold_r": "threshold_R",
    "target_bank_r": "target_bank_R",
    "executed_r": "executed_R",
    "protected_r": "protected_R",
    "basket_r_before": "basket_R_before",
    "basket_r_after": "basket_R_after",
}

def _bco_row_compat(row: Any) -> Dict[str, Any]:
    d = dict(row) if row is not None else {}
    for lower_key, legacy_key in _BCO_DB_COMPAT_ALIASES.items():
        if lower_key in d and legacy_key not in d:
            d[legacy_key] = d.get(lower_key)
        elif legacy_key in d and lower_key not in d:
            d[lower_key] = d.get(legacy_key)
    return d

def fetchone_dict(cur: Any) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    return _bco_row_compat(row) if row is not None else None

def fetchall_dict(cur: Any) -> List[Dict[str, Any]]:
    return [_bco_row_compat(r) for r in cur.fetchall()]


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
        bool_false_default = "FALSE" if conn.postgres else "0"
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

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS broker_action_queue (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                action_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                local_trade_id TEXT,
                broker_trade_id TEXT,
                reason TEXT,
                desired_stop_price DOUBLE PRECISION,
                attempts BIGINT DEFAULT 0,
                last_attempt_at_utc TEXT,
                last_error TEXT,
                broker_response_json TEXT,
                completed_at_utc TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_action_queue_status ON broker_action_queue(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_action_queue_trade ON broker_action_queue(local_trade_id)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS broker_transactions (
                id {id_type},
                synced_at_utc TEXT NOT NULL,
                transaction_id TEXT NOT NULL UNIQUE,
                transaction_type TEXT,
                transaction_time TEXT,
                account_balance DOUBLE PRECISION,
                pl_home DOUBLE PRECISION DEFAULT 0,
                financing_home DOUBLE PRECISION DEFAULT 0,
                capital_movement_home DOUBLE PRECISION DEFAULT 0,
                raw_json TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_broker_tx_time ON broker_transactions(transaction_time)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS fixed_48_outcomes (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                trade_id TEXT NOT NULL UNIQUE,
                broker_trade_id TEXT,
                cycle_id TEXT,
                signal_time TEXT,
                hold_candles BIGINT,
                entry_price DOUBLE PRECISION,
                control_exit_price DOUBLE PRECISION,
                fixed_48_R DOUBLE PRECISION,
                fixed_48_pnl_gbp DOUBLE PRECISION,
                mfe_pct DOUBLE PRECISION,
                mae_pct DOUBLE PRECISION,
                note TEXT
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS trade_manager_reviews (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                signal_time TEXT,
                raw_signal_id BIGINT,
                cycle_id TEXT,
                trade_id TEXT NOT NULL,
                broker_trade_id TEXT,
                hold_candles BIGINT,
                age_zone TEXT,
                current_price DOUBLE PRECISION,
                current_R DOUBLE PRECISION,
                mfe_pct DOUBLE PRECISION,
                mae_pct DOUBLE PRECISION,
                regime TEXT,
                candidate_supported {bool_type},
                decision_48 TEXT,
                decision_72 TEXT,
                manager_decision TEXT,
                manager_reason TEXT,
                managed_stop_price DOUBLE PRECISION,
                managed_stop_stage TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_mgr_reviews_trade ON trade_manager_reviews(trade_id,id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_mgr_reviews_signal ON trade_manager_reviews(raw_signal_id)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS basket_snapshots (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                raw_signal_id BIGINT,
                signal_time TEXT,
                cycle_id TEXT,
                open_count BIGINT,
                basket_R DOUBLE PRECISION,
                basket_pnl_gbp DOUBLE PRECISION,
                high_water_R DOUBLE PRECISION,
                giveback_pct DOUBLE PRECISION,
                losing_pct DOUBLE PRECISION,
                basket_phase TEXT,
                tide_score BIGINT,
                tide_status TEXT,
                manager_action TEXT,
                manager_detail TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_basket_snapshots_cycle ON basket_snapshots(cycle_id,id)")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS harvest_execution_outcomes (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                protection_stage_id BIGINT NOT NULL UNIQUE,
                cycle_id TEXT,
                threshold_R DOUBLE PRECISION,
                selected_trade_ids TEXT,
                model_realized_R DOUBLE PRECISION DEFAULT 0,
                broker_realized_pl_gbp DOUBLE PRECISION DEFAULT 0,
                financing_gbp DOUBLE PRECISION DEFAULT 0,
                net_realized_gbp DOUBLE PRECISION DEFAULT 0,
                sync_status TEXT,
                note TEXT
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS accounting_snapshots (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                account_nav DOUBLE PRECISION,
                account_balance DOUBLE PRECISION,
                bco_open_pl DOUBLE PRECISION,
                bco_realized_pl DOUBLE PRECISION,
                bco_financing DOUBLE PRECISION,
                capital_movements DOUBLE PRECISION,
                broker_last_transaction_id TEXT,
                note TEXT
            )
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS bco_exit_challenger_shadow (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                shadow_version TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                challenger TEXT NOT NULL,
                entry_raw_signal_id BIGINT,
                entry_signal_id TEXT,
                entry_time TEXT,
                entry_price DOUBLE PRECISION,
                sl_pct DOUBLE PRECISION,
                hard_stop_price DOUBLE PRECISION,
                status TEXT NOT NULL DEFAULT 'OPEN',
                last_raw_signal_id BIGINT,
                last_signal_time TEXT,
                hold_candles BIGINT DEFAULT 0,
                current_price DOUBLE PRECISION,
                current_R DOUBLE PRECISION DEFAULT 0,
                highest_high DOUBLE PRECISION,
                lowest_low DOUBLE PRECISION,
                mfe_pct DOUBLE PRECISION DEFAULT 0,
                mae_pct DOUBLE PRECISION DEFAULT 0,
                atr14 DOUBLE PRECISION,
                trail_price DOUBLE PRECISION,
                mfe_floor_price DOUBLE PRECISION,
                hypothetical_exit_time TEXT,
                hypothetical_exit_price DOUBLE PRECISION,
                hypothetical_exit_R DOUBLE PRECISION,
                hypothetical_exit_reason TEXT,
                actual_status TEXT,
                actual_exit_time TEXT,
                actual_exit_price DOUBLE PRECISION,
                actual_R DOUBLE PRECISION,
                actual_exit_reason TEXT,
                paired_complete {bool_type} DEFAULT {bool_false_default},
                challenger_minus_current_R DOUBLE PRECISION,
                paired_winner TEXT,
                saved_reversal {bool_type} DEFAULT {bool_false_default},
                killed_large_winner {bool_type} DEFAULT {bool_false_default},
                note TEXT,
                UNIQUE(trade_id, challenger)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_status ON bco_exit_challenger_shadow(status,paired_complete)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_trade ON bco_exit_challenger_shadow(trade_id,challenger)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_signal ON bco_exit_challenger_shadow(last_raw_signal_id)")


def _ensure_bco_exit_challenger_shadow_schema_on_conn(conn: DBConn) -> None:
    """Idempotently create/verify the v0.8 exit-challenger research table.

    This is intentionally isolated from the large main init_db transaction. It
    lets Railway recover if deferred startup DDL was interrupted or rolled back.
    No production trade/basket/broker table is modified by this helper.
    """
    id_type = "BIGSERIAL PRIMARY KEY" if conn.postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
    bool_type = "BOOLEAN" if conn.postgres else "INTEGER"
    bool_false_default = "FALSE" if conn.postgres else "0"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bco_exit_challenger_shadow (
            id {id_type},
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            shadow_version TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            challenger TEXT NOT NULL,
            entry_raw_signal_id BIGINT,
            entry_signal_id TEXT,
            entry_time TEXT,
            entry_price DOUBLE PRECISION,
            sl_pct DOUBLE PRECISION,
            hard_stop_price DOUBLE PRECISION,
            status TEXT NOT NULL DEFAULT 'OPEN',
            last_raw_signal_id BIGINT,
            last_signal_time TEXT,
            hold_candles BIGINT DEFAULT 0,
            current_price DOUBLE PRECISION,
            current_R DOUBLE PRECISION DEFAULT 0,
            highest_high DOUBLE PRECISION,
            lowest_low DOUBLE PRECISION,
            mfe_pct DOUBLE PRECISION DEFAULT 0,
            mae_pct DOUBLE PRECISION DEFAULT 0,
            atr14 DOUBLE PRECISION,
            trail_price DOUBLE PRECISION,
            mfe_floor_price DOUBLE PRECISION,
            hypothetical_exit_time TEXT,
            hypothetical_exit_price DOUBLE PRECISION,
            hypothetical_exit_R DOUBLE PRECISION,
            hypothetical_exit_reason TEXT,
            actual_status TEXT,
            actual_exit_time TEXT,
            actual_exit_price DOUBLE PRECISION,
            actual_R DOUBLE PRECISION,
            actual_exit_reason TEXT,
            paired_complete {bool_type} DEFAULT {bool_false_default},
            challenger_minus_current_R DOUBLE PRECISION,
            paired_winner TEXT,
            saved_reversal {bool_type} DEFAULT {bool_false_default},
            killed_large_winner {bool_type} DEFAULT {bool_false_default},
            note TEXT,
            UNIQUE(trade_id, challenger)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_status ON bco_exit_challenger_shadow(status,paired_complete)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_trade ON bco_exit_challenger_shadow(trade_id,challenger)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_exit_shadow_signal ON bco_exit_challenger_shadow(last_raw_signal_id)")
    # Verify the relation is queryable in the same transaction/connection.
    conn.execute("SELECT id FROM bco_exit_challenger_shadow LIMIT 1").fetchone()


def ensure_bco_exit_challenger_shadow_schema(force: bool = False) -> Dict[str, Any]:
    """Self-heal the forward exit-shadow schema on SQLite or Postgres.

    The first successful verification is cached per process. Failures are not
    cached, so a later dashboard/export/worker call can retry automatically.
    """
    global _bco_exit_shadow_schema_ready
    global _bco_exit_shadow_schema_last_error
    global _bco_exit_shadow_schema_checked_at

    if _bco_exit_shadow_schema_ready and not force:
        return {
            "ok": True,
            "ready": True,
            "cached": True,
            "checked_at_utc": _bco_exit_shadow_schema_checked_at,
            "last_error": "",
        }

    with _bco_exit_shadow_schema_lock:
        if _bco_exit_shadow_schema_ready and not force:
            return {
                "ok": True,
                "ready": True,
                "cached": True,
                "checked_at_utc": _bco_exit_shadow_schema_checked_at,
                "last_error": "",
            }

        last_exc: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                with get_conn() as conn:
                    _ensure_bco_exit_challenger_shadow_schema_on_conn(conn)
                _bco_exit_shadow_schema_ready = True
                _bco_exit_shadow_schema_last_error = ""
                _bco_exit_shadow_schema_checked_at = now_utc_iso()
                return {
                    "ok": True,
                    "ready": True,
                    "cached": False,
                    "attempt": attempt,
                    "checked_at_utc": _bco_exit_shadow_schema_checked_at,
                    "last_error": "",
                }
            except Exception as exc:
                last_exc = exc
                _bco_exit_shadow_schema_ready = False
                _bco_exit_shadow_schema_last_error = f"{type(exc).__name__}: {exc}"
                _bco_exit_shadow_schema_checked_at = now_utc_iso()
                if attempt < 3:
                    time.sleep(0.25 * attempt)

        raise RuntimeError(
            "BCO exit-challenger shadow schema could not be created/verified after 3 attempts: "
            + (_bco_exit_shadow_schema_last_error or safe_str(last_exc))
        )


def bco_exit_challenger_schema_status() -> Dict[str, Any]:
    """Read-only diagnostics for the research schema repair."""
    try:
        result = ensure_bco_exit_challenger_shadow_schema()
        result.update({
            "table": "bco_exit_challenger_shadow",
            "research_only": True,
            "execution_authority": False,
        })
        return result
    except Exception as exc:
        return {
            "ok": False,
            "ready": False,
            "table": "bco_exit_challenger_shadow",
            "checked_at_utc": _bco_exit_shadow_schema_checked_at,
            "last_error": f"{type(exc).__name__}: {exc}",
            "research_only": True,
            "execution_authority": False,
        }


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
    high = safe_float(payload.get("exec_high") or payload.get("high")); low = safe_float(payload.get("exec_low") or payload.get("low"))
    candidate = bco_long_candidate(payload); side = directional_side(payload.get("signal_side")); model = safe_str(payload.get("model_name") or payload.get("model_version")); now = now_utc_iso()

    for attempt in range(6):
        try:
            with _db_lock, get_conn() as conn:
                existing = fetchone_dict(conn.execute("SELECT id FROM raw_signals WHERE signal_id=?", (signal_id,)))
                if existing: return int(existing["id"]), payload, True
                raw_id = db_insert_id(conn, """
                    INSERT INTO raw_signals(received_at_utc,pair,signal_id,timestamp_readable,exec_close,exec_high,exec_low,forward_test_candidate,candidate_8h,signal_side,model_name,raw_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """, (now,pair,signal_id,timestamp,close,high,low,bool(candidate),bool(candidate),side,model,json.dumps(body)))
                return raw_id,payload,False
        except Exception as exc:
            msg=safe_str(exc).lower(); transient=any(x in msg for x in ['database is locked','deadlock','could not serialize','connection reset','timeout'])
            if transient and attempt<5:
                time.sleep(0.35*(attempt+1)); continue
            raise
    raise RuntimeError('BCO raw signal insert retries exhausted')


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
        "currency": safe_str(acct.get("currency")), "lastTransactionID": safe_str(acct.get("lastTransactionID") or (r.get("data") or {}).get("lastTransactionID")),
        "error": r.get("error")
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



def runtime_get(conn: DBConn, key: str, default: str = "") -> str:
    row = fetchone_dict(conn.execute("SELECT value FROM runtime_state WHERE key=? LIMIT 1", (key,)))
    return safe_str((row or {}).get("value")) or default


def runtime_set(conn: DBConn, key: str, value: Any) -> None:
    val = safe_str(value)
    if conn.postgres:
        conn.execute("""
            INSERT INTO runtime_state(key,value,updated_at_utc) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at_utc=EXCLUDED.updated_at_utc
        """, (key, val, now_utc_iso()))
    else:
        conn.execute("""
            INSERT INTO runtime_state(key,value,updated_at_utc) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at_utc=excluded.updated_at_utc
        """, (key, val, now_utc_iso()))


def _iso_age_seconds(value: Any) -> Optional[float]:
    s = safe_str(value)
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def _broker_close_fill(resp: Dict[str, Any]) -> Dict[str, Any]:
    data = resp.get("data") or {}
    fill = data.get("orderFillTransaction") or {}
    closed = fill.get("tradesClosed") or []
    reduced = fill.get("tradeReduced")
    if isinstance(reduced, dict):
        closed = list(closed) + [reduced]
    tc = fill.get("tradeClosed")
    if isinstance(tc, dict):
        closed = list(closed) + [tc]
    pl = safe_float(fill.get("pl"))
    financing = safe_float(fill.get("financing"))
    if pl is None:
        pl = sum(float(safe_float(x.get("realizedPL")) or safe_float(x.get("pl")) or 0.0) for x in closed if isinstance(x, dict))
    if financing is None:
        financing = sum(float(safe_float(x.get("financing")) or 0.0) for x in closed if isinstance(x, dict))
    return {
        "transaction_id": safe_str(fill.get("id")),
        "price": safe_float(fill.get("price")),
        "pl_home": float(pl or 0.0),
        "financing_home": float(financing or 0.0),
        "account_balance": safe_float(fill.get("accountBalance")),
        "raw_fill": fill,
    }


def mark_trade_closed_from_broker(
    conn: DBConn,
    trade: Dict[str, Any],
    signal_time: str,
    reason: str,
    broker_close: Dict[str, Any],
) -> float:
    effective = float(
        safe_float(trade.get("effective_risk_gbp"))
        or safe_float(trade.get("requested_risk_gbp"))
        or BCO_RISK_PER_TRADE_GBP
    )
    broker_pl = float(safe_float(broker_close.get("pl_home")) or 0.0)
    financing = float(safe_float(broker_close.get("financing_home")) or 0.0)
    net = broker_pl + financing
    rr = (net / effective) if effective > 0 else 0.0
    exit_price = safe_float(broker_close.get("price")) or safe_float(trade.get("current_price")) or safe_float(trade.get("entry_price")) or 0.0
    conn.execute("""
        UPDATE trades SET
            status='CLOSED',exit_time=?,exit_price=?,exit_reason=?,
            realized_R=?,realized_pnl_gbp=?,broker_realized_pl_home=?,financing_home=?,
            current_R=?,current_price=?,updated_at_utc=?
        WHERE trade_id=?
    """, (
        signal_time or now_utc_iso(), exit_price, reason,
        rr, net, broker_pl, financing,
        rr, exit_price, now_utc_iso(), trade.get("trade_id")
    ))
    audit(
        "BROKER_CLOSE_ACCOUNTED", True,
        trade_id=trade.get("trade_id"),
        broker_trade_id=trade.get("broker_trade_id"),
        instrument=BCO_OANDA_INSTRUMENT,
        actual_price=exit_price,
        effective_risk_gbp=effective,
        message=f"Broker authoritative close: pl={broker_pl:.6f}, financing={financing:.6f}, net={net:.6f}, R={rr:.6f}",
        raw=broker_close.get("raw_fill") or broker_close,
    )
    return rr


def enqueue_broker_action(
    conn: DBConn,
    action_type: str,
    trade: Dict[str, Any],
    reason: str,
    desired_stop_price: Optional[float] = None,
    error: str = "",
) -> int:
    action_type = safe_str(action_type).upper()
    local_id = safe_str(trade.get("trade_id"))
    broker_id = safe_str(trade.get("broker_trade_id"))
    existing = fetchone_dict(conn.execute("""
        SELECT * FROM broker_action_queue
        WHERE action_type=? AND local_trade_id=? AND status IN ('PENDING','RETRY')
        ORDER BY id DESC LIMIT 1
    """, (action_type, local_id)))
    if existing:
        conn.execute("""
            UPDATE broker_action_queue
            SET updated_at_utc=?,reason=?,desired_stop_price=COALESCE(?,desired_stop_price),
                last_error=CASE WHEN ?<>'' THEN ? ELSE last_error END
            WHERE id=?
        """, (now_utc_iso(), reason, desired_stop_price, error, error, existing.get("id")))
        return int(existing.get("id") or 0)
    return db_insert_id(conn, """
        INSERT INTO broker_action_queue(
            created_at_utc,updated_at_utc,action_type,status,local_trade_id,broker_trade_id,
            reason,desired_stop_price,attempts,last_error
        ) VALUES(?,?,?,'PENDING',?,?,?,?,0,?)
    """, (
        now_utc_iso(), now_utc_iso(), action_type, local_id, broker_id,
        reason, desired_stop_price, error
    ))


def record_fixed_48_outcome(
    conn: DBConn,
    trade: Dict[str, Any],
    signal_time: str,
    current_price: float,
    rr: float,
    mfe_pct: float,
    mae_pct: float,
    hold: int,
) -> None:
    if hold < 48:
        return
    existing = fetchone_dict(conn.execute(
        "SELECT id FROM fixed_48_outcomes WHERE trade_id=? LIMIT 1",
        (trade.get("trade_id"),)
    ))
    if existing:
        return
    risk = float(
        safe_float(trade.get("effective_risk_gbp"))
        or safe_float(trade.get("requested_risk_gbp"))
        or BCO_RISK_PER_TRADE_GBP
    )
    conn.execute("""
        INSERT INTO fixed_48_outcomes(
            created_at_utc,trade_id,broker_trade_id,cycle_id,signal_time,hold_candles,
            entry_price,control_exit_price,fixed_48_R,fixed_48_pnl_gbp,mfe_pct,mae_pct,note
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_utc_iso(), trade.get("trade_id"), trade.get("broker_trade_id"), trade.get("cycle_id"),
        signal_time, hold, trade.get("entry_price"), current_price, rr, rr*risk, mfe_pct, mae_pct,
        "Frozen 48h control. Managed trade may continue independently."
    ))



def _bco_age_zone(hold: Any) -> str:
    h = int(safe_float(hold) or 0)
    if h < 48:
        return "YOUNG"
    if h < 72:
        return "48–72 EARLY"
    if h < 96:
        return "72–96 STRONG"
    if h < 120:
        return "96–120 MATURE"
    return "120+ LATE"


def record_manager_review(
    conn: DBConn,
    raw_signal_id: Optional[int],
    trade: Dict[str, Any],
    signal_time: str,
    hold: int,
    current_price: float,
    rr: float,
    mfe_pct: float,
    mae_pct: float,
    regime_name: str,
    support: Dict[str, Any],
    manager_decision: str,
    manager_reason: str,
    d48: str,
    d72: str,
) -> None:
    if hold < 48:
        return
    conn.execute("""
        INSERT INTO trade_manager_reviews(
            created_at_utc,signal_time,raw_signal_id,cycle_id,trade_id,broker_trade_id,
            hold_candles,age_zone,current_price,current_R,mfe_pct,mae_pct,regime,
            candidate_supported,decision_48,decision_72,manager_decision,manager_reason,
            managed_stop_price,managed_stop_stage
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_utc_iso(), signal_time, raw_signal_id, trade.get("cycle_id"), trade.get("trade_id"),
        trade.get("broker_trade_id"), hold, _bco_age_zone(hold), current_price, rr, mfe_pct, mae_pct,
        regime_name, bool(support.get("supported")), d48, d72, manager_decision, manager_reason,
        trade.get("managed_stop_price"), trade.get("managed_stop_stage")
    ))


def record_basket_snapshot(
    conn: DBConn,
    raw_signal_id: int,
    signal_time: str,
    state: Dict[str, Any],
    metrics: Dict[str, Any],
) -> None:
    conn.execute("""
        INSERT INTO basket_snapshots(
            created_at_utc,raw_signal_id,signal_time,cycle_id,open_count,basket_R,basket_pnl_gbp,
            high_water_R,giveback_pct,losing_pct,basket_phase,tide_score,tide_status,
            manager_action,manager_detail
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_utc_iso(), raw_signal_id, signal_time, state.get("cycle_id"), metrics.get("open_count"),
        metrics.get("basket_R"), metrics.get("basket_pnl_gbp"), state.get("high_water_R"),
        state.get("giveback_pct"), metrics.get("losing_pct"), metrics.get("phase"),
        state.get("tide_score"), state.get("tide_status"), state.get("manager_action"),
        state.get("manager_detail")
    ))


def finalize_harvest_stage(conn: DBConn, stage_id: int, signal_time: str = "") -> Dict[str, Any]:
    stage = fetchone_dict(conn.execute("SELECT * FROM protection_stages WHERE id=? LIMIT 1", (stage_id,))) or {}
    ids = [x for x in safe_str(stage.get("selected_trade_ids")).split(",") if x]
    if not ids:
        return {"ok": False, "reason": "no_selected_trade_ids"}
    closed_rows = []
    all_closed = True
    for trade_id in ids:
        tr = fetchone_dict(conn.execute("SELECT * FROM trades WHERE trade_id=? LIMIT 1", (trade_id,))) or {}
        if safe_str(tr.get("status")).upper() not in {"CLOSED","BROKER_CLOSED"}:
            all_closed = False
        else:
            closed_rows.append(tr)
    model_r = sum(float(safe_float(t.get("realized_R")) or 0.0) for t in closed_rows)
    broker_pl = sum(float(safe_float(t.get("broker_realized_pl_home")) or 0.0) for t in closed_rows)
    financing = sum(float(safe_float(t.get("financing_home")) or 0.0) for t in closed_rows)
    net = sum(float(safe_float(t.get("realized_pnl_gbp")) or 0.0) for t in closed_rows)
    status = "EXECUTED" if all_closed and len(closed_rows) == len(ids) else "EXECUTING_RETRY"
    conn.execute("""
        UPDATE protection_stages SET status=?,executed_R=?,executed_at_signal_time=CASE WHEN ?='EXECUTED' THEN ? ELSE executed_at_signal_time END,
            updated_at_utc=?,reason=?
        WHERE id=?
    """, (
        status, model_r, status, signal_time or now_utc_iso(), now_utc_iso(),
        f"Selected {len(ids)} whole trade(s); broker-net GBP {net:.6f}; financing {financing:.6f}.",
        stage_id
    ))
    if conn.postgres:
        conn.execute("""
            INSERT INTO harvest_execution_outcomes(
                created_at_utc,updated_at_utc,protection_stage_id,cycle_id,threshold_R,selected_trade_ids,
                model_realized_R,broker_realized_pl_gbp,financing_gbp,net_realized_gbp,sync_status,note
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(protection_stage_id) DO UPDATE SET
                updated_at_utc=EXCLUDED.updated_at_utc,model_realized_R=EXCLUDED.model_realized_R,
                broker_realized_pl_gbp=EXCLUDED.broker_realized_pl_gbp,financing_gbp=EXCLUDED.financing_gbp,
                net_realized_gbp=EXCLUDED.net_realized_gbp,sync_status=EXCLUDED.sync_status,note=EXCLUDED.note
        """, (
            now_utc_iso(), now_utc_iso(), stage_id, stage.get("cycle_id"), stage.get("threshold_R"),
            ",".join(ids), model_r, broker_pl, financing, net, status,
            "Authoritative GBP comes from broker-close accounting where available."
        ))
    else:
        conn.execute("""
            INSERT INTO harvest_execution_outcomes(
                created_at_utc,updated_at_utc,protection_stage_id,cycle_id,threshold_R,selected_trade_ids,
                model_realized_R,broker_realized_pl_gbp,financing_gbp,net_realized_gbp,sync_status,note
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(protection_stage_id) DO UPDATE SET
                updated_at_utc=excluded.updated_at_utc,model_realized_R=excluded.model_realized_R,
                broker_realized_pl_gbp=excluded.broker_realized_pl_gbp,financing_gbp=excluded.financing_gbp,
                net_realized_gbp=excluded.net_realized_gbp,sync_status=excluded.sync_status,note=excluded.note
        """, (
            now_utc_iso(), now_utc_iso(), stage_id, stage.get("cycle_id"), stage.get("threshold_R"),
            ",".join(ids), model_r, broker_pl, financing, net, status,
            "Authoritative GBP comes from broker-close accounting where available."
        ))
    return {"ok": True, "status": status, "model_R": model_r, "net_realized_gbp": net}


def finalize_pending_harvest_stages(conn: DBConn) -> None:
    stages = fetchall_dict(conn.execute("""
        SELECT id FROM protection_stages
        WHERE stage_type='BANK' AND status IN ('EXECUTING','EXECUTING_RETRY')
        ORDER BY id ASC
    """))
    for st in stages:
        finalize_harvest_stage(conn, int(st.get("id") or 0), now_utc_iso())


def sync_broker_transactions() -> Dict[str, Any]:
    """
    Incremental OANDA transaction sync.
    Captures close fills, daily financing, and explicit capital movements without
    importing unrelated historical strategy performance.
    """
    if not BCO_TRANSACTION_SYNC_ENABLED or not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok": False, "skipped": True, "reason": "transaction sync disabled/unconfigured"}

    with _db_lock, get_conn() as conn:
        cursor = runtime_get(conn, "broker_transaction_cursor", "")
        if not cursor:
            summary = account_summary()
            last_id = safe_str(summary.get("lastTransactionID"))
            if not last_id:
                return {"ok": False, "error": "unable to initialize transaction cursor"}
            try:
                # Small recent lookback catches closes/financing around an upgrade
                # without importing the account's full historic transaction ledger.
                cursor = str(max(0, int(float(last_id)) - 500))
            except Exception:
                cursor = last_id
            runtime_set(conn, "broker_transaction_cursor", cursor)

    resp = oanda_request(
        f"/v3/accounts/{OANDA_ACCOUNT_ID}/transactions/sinceid",
        "GET",
        params={"id": cursor}
    )
    if not resp.get("ok"):
        return {"ok": False, "error": resp.get("error"), "cursor": cursor}

    data = resp.get("data") or {}
    transactions = data.get("transactions") or []
    processed = 0
    matched_closes = 0
    financing_updates = 0
    capital_movements = 0.0

    with _db_lock, get_conn() as conn:
        for tx in transactions[:BCO_TRANSACTION_SYNC_PAGE_LIMIT]:
            txid = safe_str(tx.get("id"))
            if not txid:
                continue
            tx_type = safe_str(tx.get("type")).upper()
            tx_time = safe_str(tx.get("time"))
            pl = float(safe_float(tx.get("pl")) or 0.0)
            financing = float(safe_float(tx.get("financing")) or 0.0)
            account_balance = safe_float(tx.get("accountBalance"))
            capital = 0.0
            if tx_type in {"TRANSFER_FUNDS","DIVIDEND_ADJUSTMENT"}:
                capital = float(safe_float(tx.get("amount")) or 0.0)
                capital_movements += capital

            # Store transaction idempotently.
            try:
                conn.execute("""
                    INSERT INTO broker_transactions(
                        synced_at_utc,transaction_id,transaction_type,transaction_time,account_balance,
                        pl_home,financing_home,capital_movement_home,raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                """, (
                    now_utc_iso(), txid, tx_type, tx_time, account_balance,
                    pl, financing, capital, json.dumps(tx)[:50000]
                ))
            except Exception:
                # Already synced transaction.
                pass

            # Daily financing may contain per-trade financing records.
            if tx_type == "DAILY_FINANCING":
                for pos in (tx.get("positionFinancings") or []):
                    for tf in (pos.get("tradeFinancings") or []):
                        bid = safe_str(tf.get("tradeID"))
                        fin = float(safe_float(tf.get("financing")) or 0.0)
                        if bid and fin:
                            conn.execute("""
                                UPDATE trades
                                SET financing_home=COALESCE(financing_home,0)+?,updated_at_utc=?
                                WHERE broker_trade_id=?
                            """, (fin, now_utc_iso(), bid))
                            financing_updates += 1

            # ORDER_FILL closure components.
            if tx_type == "ORDER_FILL":
                components = []
                if isinstance(tx.get("tradeClosed"), dict):
                    components.append(tx.get("tradeClosed"))
                components.extend([x for x in (tx.get("tradesClosed") or []) if isinstance(x, dict)])
                if isinstance(tx.get("tradeReduced"), dict):
                    components.append(tx.get("tradeReduced"))
                for comp in components:
                    bid = safe_str(comp.get("tradeID"))
                    if not bid:
                        continue
                    tr = fetchone_dict(conn.execute(
                        "SELECT * FROM trades WHERE broker_trade_id=? LIMIT 1",
                        (bid,)
                    ))
                    if not tr:
                        continue
                    cpl = float(safe_float(comp.get("realizedPL")) or safe_float(comp.get("pl")) or pl or 0.0)
                    cfin = float(safe_float(comp.get("financing")) or financing or 0.0)
                    close_info = {
                        "transaction_id": txid,
                        "price": safe_float(tx.get("price")) or safe_float(comp.get("price")),
                        "pl_home": cpl,
                        "financing_home": cfin,
                        "account_balance": account_balance,
                        "raw_fill": tx,
                    }
                    if safe_str(tr.get("status")).upper() not in {"CLOSED"} or safe_float(tr.get("broker_realized_pl_home")) is None:
                        mark_trade_closed_from_broker(
                            conn, tr, tx_time or now_utc_iso(),
                            safe_str(tr.get("exit_reason") or "broker_transaction_sync"),
                            close_info
                        )
                        matched_closes += 1

            processed += 1

        new_cursor = safe_str(data.get("lastTransactionID"))
        if not new_cursor and transactions:
            new_cursor = safe_str(transactions[-1].get("id"))
        if new_cursor:
            runtime_set(conn, "broker_transaction_cursor", new_cursor)
        runtime_set(conn, "broker_transaction_sync_at", now_utc_iso())
        runtime_set(conn, "broker_capital_movements_total", str(
            float(runtime_get(conn, "broker_capital_movements_total", "0") or 0.0) + capital_movements
        ))
        finalize_pending_harvest_stages(conn)

    return {
        "ok": True, "processed": processed, "matched_closes": matched_closes,
        "financing_updates": financing_updates, "capital_movements": capital_movements,
        "cursor": new_cursor or cursor, "time_utc": now_utc_iso()
    }


def process_broker_action_queue(limit: int = 50) -> Dict[str, Any]:
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok": False, "skipped": True, "reason": "OANDA unavailable"}
    processed = success = failed = 0
    with _db_lock, get_conn() as conn:
        rows = fetchall_dict(conn.execute("""
            SELECT * FROM broker_action_queue
            WHERE status IN ('PENDING','RETRY')
            ORDER BY id ASC LIMIT ?
        """, (max(1, min(int(limit), 500)),)))
        for row in rows:
            qid = int(row.get("id") or 0)
            trade = fetchone_dict(conn.execute(
                "SELECT * FROM trades WHERE trade_id=? LIMIT 1",
                (row.get("local_trade_id"),)
            )) or {}
            if not trade:
                conn.execute("""
                    UPDATE broker_action_queue SET status='FAILED_FINAL',updated_at_utc=?,completed_at_utc=?,
                        last_error='local trade missing' WHERE id=?
                """, (now_utc_iso(), now_utc_iso(), qid))
                failed += 1
                continue
            if safe_str(trade.get("status")).upper() != "OPEN":
                conn.execute("""
                    UPDATE broker_action_queue SET status='DONE',updated_at_utc=?,completed_at_utc=?,
                        last_error='' WHERE id=?
                """, (now_utc_iso(), now_utc_iso(), qid))
                success += 1
                continue

            attempts = int(safe_float(row.get("attempts")) or 0) + 1
            action = safe_str(row.get("action_type")).upper()
            resp = {}
            if action == "CLOSE":
                resp = close_broker_trade(
                    safe_str(trade.get("broker_trade_id")),
                    safe_str(trade.get("trade_id")),
                    safe_str(row.get("reason") or "queued_close")
                )
                if resp.get("ok"):
                    info = _broker_close_fill(resp)
                    mark_trade_closed_from_broker(
                        conn, trade, now_utc_iso(),
                        safe_str(row.get("reason") or "queued_close"), info
                    )
            elif action == "UPDATE_STOP":
                desired = safe_float(row.get("desired_stop_price"))
                if desired is None:
                    resp = {"ok": False, "error": "missing desired stop price"}
                else:
                    resp = update_broker_stop(
                        safe_str(trade.get("broker_trade_id")),
                        desired, safe_str(trade.get("trade_id"))
                    )
                    if resp.get("ok"):
                        conn.execute("""
                            UPDATE trades SET managed_stop_price=?,updated_at_utc=? WHERE trade_id=?
                        """, (desired, now_utc_iso(), trade.get("trade_id")))
            else:
                resp = {"ok": False, "error": f"unsupported action {action}"}

            if resp.get("ok"):
                conn.execute("""
                    UPDATE broker_action_queue
                    SET status='DONE',attempts=?,last_attempt_at_utc=?,last_error='',
                        broker_response_json=?,updated_at_utc=?,completed_at_utc=?
                    WHERE id=?
                """, (
                    attempts, now_utc_iso(), json.dumps(resp.get("data") or resp)[:50000],
                    now_utc_iso(), now_utc_iso(), qid
                ))
                success += 1
            else:
                final = attempts >= BCO_ACTION_RETRY_MAX_ATTEMPTS
                conn.execute("""
                    UPDATE broker_action_queue
                    SET status=?,attempts=?,last_attempt_at_utc=?,last_error=?,
                        broker_response_json=?,updated_at_utc=?
                    WHERE id=?
                """, (
                    "FAILED_FINAL" if final else "RETRY", attempts, now_utc_iso(),
                    safe_str(resp.get("error") or resp.get("response") or "broker action failed")[:3000],
                    json.dumps(resp.get("data") or resp)[:50000], now_utc_iso(), qid
                ))
                failed += 1
            processed += 1

        finalize_pending_harvest_stages(conn)
        runtime_set(conn, "broker_action_queue_last_run", now_utc_iso())
    return {"ok": True, "processed": processed, "success": success, "failed": failed, "time_utc": now_utc_iso()}

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
def candidate_support(
    conn: DBConn,
    limit: int = 3,
    max_raw_signal_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Candidate support using only information available at that raw signal."""
    if max_raw_signal_id is not None and int(max_raw_signal_id or 0) > 0:
        rows = fetchall_dict(conn.execute(
            "SELECT candidate_8h FROM raw_signals WHERE id<=? ORDER BY id DESC LIMIT ?",
            (int(max_raw_signal_id), int(limit)),
        ))
    else:
        rows = fetchall_dict(conn.execute(
            "SELECT candidate_8h FROM raw_signals ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ))
    vals = [parse_bool(r.get("candidate_8h"), False) for r in rows]
    n = sum(1 for v in vals if v)
    return {
        "latest_candidate": bool(vals[0]) if vals else False,
        "candidate_true_last_3": n,
        "supported": bool((vals and vals[0]) or n >= 2),
    }


def basket_metrics(conn: DBConn) -> Dict[str, Any]:
    rows = fetchall_dict(conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time ASC,id ASC"))
    count = len(rows)
    br = sum(float(safe_float(r.get("current_R")) or 0.0) for r in rows)
    losing = sum(1 for r in rows if float(safe_float(r.get("current_R")) or 0.0) < 0)
    return {"rows":rows,"open_count":count,"basket_R":br,"basket_pnl_gbp":br*BCO_RISK_PER_TRADE_GBP,
            "losing_pct":losing/count*100.0 if count else 0.0,"phase":basket_phase_from_count(count)}



def reset_flat_bco_basket_state(
    conn: DBConn,
    reason: str,
    observed_at: str = "",
) -> Dict[str, Any]:
    """Reset CURRENT basket state after the BCO basket is genuinely flat.

    Historical basket_snapshots / research tables are intentionally untouched,
    so the completed cycle's ~20R high-water remains available for analysis.
    """
    open_row = fetchone_dict(conn.execute(
        "SELECT COUNT(*) AS c FROM trades WHERE status='OPEN'"
    )) or {}
    local_open = int(safe_float(open_row.get("c")) or 0)
    if local_open > 0:
        return {
            "reset": False,
            "reason": "local_open_trades_remain",
            "local_open": local_open,
        }

    state = fetchone_dict(conn.execute(
        "SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1"
    )) or {}
    previous = {
        "cycle_id": safe_str(state.get("cycle_id")),
        "high_water_R": safe_float(state.get("high_water_R")) or 0.0,
        "high_water_seen_at": safe_str(state.get("high_water_seen_at")),
        "banked_R_cycle": safe_float(state.get("banked_R_cycle")) or 0.0,
        "realized_R_cycle": safe_float(state.get("realized_R_cycle")) or 0.0,
        "status": safe_str(state.get("status")),
    }

    conn.execute("""
        UPDATE basket_state
        SET status='FLAT',
            cycle_id=NULL,
            cycle_started_at=NULL,
            open_count=0,
            basket_R=0,
            basket_pnl_gbp=0,
            high_water_R=0,
            high_water_seen_at=NULL,
            giveback_pct=0,
            losing_pct=0,
            basket_phase='FLAT',
            tide_score=0,
            tide_status='FLAT',
            manager_action='NO_OPEN_BASKET',
            manager_detail=?,
            realized_R_cycle=0,
            banked_R_cycle=0,
            updated_at_utc=?
        WHERE singleton_key='BCO_LONG'
    """, (
        f"Flat basket state reset: {safe_str(reason)}",
        observed_at or now_utc_iso(),
    ))

    return {
        "reset": True,
        "reason": safe_str(reason),
        "previous": previous,
        "local_open": 0,
    }


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


def regime(conn: DBConn, max_raw_signal_id: Optional[int] = None) -> str:
    """Regime using only rows known at the signal being processed/recovered."""
    if max_raw_signal_id is not None and int(max_raw_signal_id or 0) > 0:
        rows = fetchall_dict(conn.execute(
            "SELECT exec_close,exec_high,exec_low FROM raw_signals "
            "WHERE exec_close IS NOT NULL AND id<=? ORDER BY id DESC LIMIT 121",
            (int(max_raw_signal_id),),
        ))
    else:
        rows = fetchall_dict(conn.execute(
            "SELECT exec_close,exec_high,exec_low FROM raw_signals "
            "WHERE exec_close IS NOT NULL ORDER BY id DESC LIMIT 121"
        ))
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



def candidate_supported_extension_override(
    conn: DBConn,
    support: Dict[str, Any],
    regime_name: str,
    rr: float,
    reasons: List[str],
) -> Tuple[bool, List[str]]:
    """
    Mirrors the Live Indices candidate-supported extension rule.

    A current/recent same-direction BCO candidate may override only SOFT
    post-48 extension failures. It must never suppress genuine safety exits:
    - trade currently negative;
    - adverse or flat/choppy regime;
    - heavy basket pressure;
    - excessive giveback.
    """
    if not support.get("supported"):
        return False, reasons

    metrics = basket_metrics(conn)
    heavy_pressure = (
        float(metrics.get("basket_R") or 0.0) <= -2.0
        and float(metrics.get("losing_pct") or 0.0) >= 60.0
    )

    blockers = []
    if rr < 0:
        blockers.append("blocked_trade_negative")
    if regime_name in {"adverse", "flat_choppy"}:
        blockers.append(f"blocked_live_regime_{regime_name}")
    if heavy_pressure:
        blockers.append("blocked_heavy_basket_pressure")
    if "too_much_giveback" in reasons:
        blockers.append("blocked_too_much_giveback")

    if blockers:
        return False, list(reasons) + blockers

    return True, [
        "candidate_supported_extension_override",
        "same_direction_bco_candidate_support_present",
    ]


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
        if not BCO_AUTO_MANAGEMENT_ENABLED:
            enqueue_broker_action(conn, "CLOSE", trade, reason, error="auto management disabled")
            return False, 0.0
        resp = close_broker_trade(broker_id, safe_str(trade.get("trade_id")), reason)
        if not resp.get("ok"):
            enqueue_broker_action(conn, "CLOSE", trade, reason, error=safe_str(resp.get("error") or "close failed"))
            return False, 0.0
        info = _broker_close_fill(resp)
        realized = mark_trade_closed_from_broker(conn, trade, signal_time, reason, info)
        return True, realized
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
                  stage,old,new,fraction,((new-entry)/entry*100.0)/BCO_SL_PCT,False,"Broker stop write failed; queued for durable retry; local stop NOT advanced."))
            enqueue_broker_action(conn, "UPDATE_STOP", trade, f"{event_type}:{stage}", desired_stop_price=new,
                                  error=safe_str(wr.get("error") or "stop write failed"))
            return False
    conn.execute("UPDATE trades SET managed_stop_price=?,managed_stop_stage=?,updated_at_utc=? WHERE trade_id=?", (new,stage,now_utc_iso(),trade.get("trade_id")))
    conn.execute("""
        INSERT INTO managed_stop_events(created_at_utc,signal_time,cycle_id,trade_id,broker_trade_id,hold_candles,event_type,
            rule_stage,old_stop_price,new_stop_price,protect_fraction,protected_R,broker_write_success,note)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (now_utc_iso(),signal_time,trade.get("cycle_id"),trade.get("trade_id"),broker_id,trade.get("hold_candles"),event_type,
          stage,old,new,fraction,((new-entry)/entry*100.0)/BCO_SL_PCT,write_success,"Tighten only; never loosen existing protection."))
    return True


def update_trade_on_signal(conn: DBConn, trade: Dict[str, Any], signal: Dict[str, Any], support: Dict[str, Any], raw_signal_id: Optional[int] = None) -> Dict[str, Any]:
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
    record_fixed_48_outcome(conn, trade, signal_time, float(current), rr, mfe, mae, hold)
    d48=safe_str(trade.get("decision_48")); d72=safe_str(trade.get("decision_72")); exit_now=False; reason=""
    reg=regime(conn, max_raw_signal_id=raw_signal_id)
    if hold >= 48 and not d48:
        passed,reasons=extension_decision(reg,ret,mfe)
        if not passed:
            override,override_reasons=candidate_supported_extension_override(conn,support,reg,rr,reasons)
            if override:
                passed=True
                reasons=["candidate_supported_48h_extension_override"]+override_reasons
            else:
                reasons=override_reasons
        d48="extend" if passed else "exit:"+",".join(reasons)
        if not passed:
            exit_now=True
            reason="exit_48_no_extension:"+",".join(reasons)

    if hold >= 72 and d48 == "extend" and not d72 and not exit_now:
        passed,reasons=extension_decision(reg,ret,mfe)
        if not passed:
            override,override_reasons=candidate_supported_extension_override(conn,support,reg,rr,reasons)
            if override:
                passed=True
                reasons=["candidate_supported_72h_extension_override"]+override_reasons
            else:
                reasons=override_reasons
        d72="extend" if passed else "exit:"+",".join(reasons)
        if not passed:
            exit_now=True
            reason="exit_72_no_extension:"+",".join(reasons)
    conn.execute("""
        UPDATE trades SET current_price=?,hold_candles=?,highest_high=?,lowest_low=?,return_pct=?,mfe_pct=?,mae_pct=?,current_R=?,
            decision_48=?,decision_72=?,updated_at_utc=? WHERE trade_id=?
    """, (current,hold,highest,lowest,ret,mfe,mae,rr,d48,d72,now_utc_iso(),trade.get("trade_id")))
    if exit_now:
        refreshed=dict(trade); refreshed.update({"current_R":rr,"current_price":current,"hold_candles":hold,"decision_48":d48,"decision_72":d72})
        record_manager_review(conn, raw_signal_id, refreshed, signal_time, hold, float(current), rr, mfe, mae, reg, support,
                              "CLOSE_REQUESTED", reason, d48, d72)
        ok,_=execute_or_sim_close(conn,refreshed,signal_time,current,reason,rr)
        return {"closed":ok,"reason":reason,"R":rr}
    if hold >= BCO_MIN_HOLD_HOURS and rr > 0:
        fraction,stage=protect_fraction(hold)
        refreshed=dict(trade); refreshed.update({"current_price":current,"hold_candles":hold,"managed_stop_price":managed})
        set_managed_stop(conn,refreshed,float(current),fraction,stage,signal_time,"POST48_MANAGED_STOP")
    if hold >= 48:
        refreshed=dict(trade); refreshed.update({"current_R":rr,"current_price":current,"hold_candles":hold,
                                                "decision_48":d48,"decision_72":d72})
        record_manager_review(conn, raw_signal_id, refreshed, signal_time, hold, float(current), rr, mfe, mae, reg, support,
                              "EXTEND" if not exit_now else "CLOSE_REQUESTED",
                              "hourly_post48_review", d48, d72)
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



def bco_bank_fraction_for_level(level_r: Any) -> float:
    level = float(safe_float(level_r) or 0.0)
    if level <= 50.0:
        return float(BCO_BANK_50_FRACTION)
    if level <= 100.0:
        return float(BCO_BANK_100_FRACTION)
    return float(BCO_BANK_150_PLUS_FRACTION)


def bco_bank_levels_up_to(high_water_r: Any) -> List[Tuple[float, float]]:
    """Generate every crossed/displayed +50R BCO harvest checkpoint."""
    top = min(
        max(0.0, float(safe_float(high_water_r) or 0.0)),
        float(BCO_BANK_MAX_LEVEL_R),
    )
    out: List[Tuple[float, float]] = []
    level = float(BCO_BANK_FIRST_LEVEL_R)
    while level <= top + 1e-9:
        out.append((level, bco_bank_fraction_for_level(level)))
        level += float(BCO_BANK_STEP_R)
    return out


def bco_profitable_open_pool_r(conn: DBConn) -> float:
    """Current positive-R open pool used to freeze each checkpoint target."""
    rows = fetchall_dict(conn.execute(
        "SELECT current_R FROM trades WHERE status='OPEN' AND COALESCE(current_R,0)>0"
    ))
    return sum(float(safe_float(r.get("current_R")) or 0.0) for r in rows)


def bank_sort_key(row: Dict[str,Any]) -> Tuple[Any,...]:
    hold=int(safe_float(row.get("hold_candles")) or 0); rr=float(safe_float(row.get("current_R")) or 0.0)
    mfe=float(safe_float(row.get("mfe_pct")) or 0.0); ret=float(safe_float(row.get("return_pct")) or 0.0); give=max(0.0,mfe-ret)
    protected=safe_float(row.get("managed_stop_price")) is not None
    if hold < BCO_MIN_HOLD_HOURS: return (0,BCO_MIN_HOLD_HOURS-hold,-give,-rr,safe_str(row.get("entry_time")))
    return (1,1 if protected else 0,-give,ret,-rr,safe_str(row.get("entry_time")))


def execute_protection(conn: DBConn, signal_time: str) -> Dict[str, Any]:
    """v0.8.3 family-level BCO harvesting on a coarse 50R ladder."""
    metrics = basket_metrics(conn)
    state = ensure_cycle(conn, signal_time, metrics)
    if metrics["open_count"] <= 0 or not safe_str(state.get("cycle_id")):
        return {"banked_R": 0.0, "banked_trade_ids": [], "bank_stages_completed": 0}

    cycle = safe_str(state.get("cycle_id"))
    br = float(metrics["basket_R"])
    old_hwm = float(safe_float(state.get("high_water_R")) or 0.0)
    hwm = max(old_hwm, br)
    conn.execute(
        "UPDATE basket_state SET high_water_R=?,high_water_seen_at=?,updated_at_utc=? WHERE singleton_key='BCO_LONG'",
        (hwm, signal_time if hwm > old_hwm else state.get("high_water_seen_at"), now_utc_iso()),
    )

    banked = 0.0
    bank_ids: List[str] = []
    stages_completed = 0

    for threshold, fraction in bco_bank_levels_up_to(hwm):
        stage = fetchone_dict(conn.execute(
            "SELECT * FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' AND threshold_R=? LIMIT 1",
            (cycle, threshold),
        ))

        if not stage:
            # Freeze against the remaining PROFITABLE pool at this checkpoint.
            # Negative open trades do not reduce the agreed amount to bank.
            pool_r = bco_profitable_open_pool_r(conn)
            target = max(0.0, pool_r * float(fraction))
            status = "ARMED" if target > 0 else "ARMED_WAITING_PROFITABLE_POOL"
            conn.execute("""
                INSERT INTO protection_stages(
                    created_at_utc,updated_at_utc,cycle_id,stage_type,threshold_R,
                    fraction,status,target_bank_R,armed_at_signal_time,reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (
                now_utc_iso(), now_utc_iso(), cycle, "BANK", threshold, fraction,
                status, target, signal_time,
                f"v0.8.3 target = {fraction*100:.0f}% of remaining profitable pool {pool_r:.2f}R.",
            ))
            stage = fetchone_dict(conn.execute(
                "SELECT * FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' AND threshold_R=? LIMIT 1",
                (cycle, threshold),
            )) or {}

        status = safe_str(stage.get("status")).upper()
        if status in {"EXECUTED", "NO_ELIGIBLE", "EXPIRED_FLAT"}:
            continue

        target = float(safe_float(stage.get("target_bank_R")) or 0.0)
        if target <= 0:
            pool_r = bco_profitable_open_pool_r(conn)
            if pool_r <= 0:
                conn.execute(
                    "UPDATE protection_stages SET status='ARMED_WAITING_PROFITABLE_POOL',updated_at_utc=?,reason=? WHERE id=?",
                    (now_utc_iso(), f"{threshold:.0f}R crossed; waiting for a positive open pool.", stage.get("id")),
                )
                continue
            target = pool_r * float(fraction)
            conn.execute(
                "UPDATE protection_stages SET target_bank_R=?,status='ARMED',updated_at_utc=?,reason=? WHERE id=?",
                (target, now_utc_iso(), f"v0.8.3 target frozen at {fraction*100:.0f}% of {pool_r:.2f}R positive pool.", stage.get("id")),
            )

        eligible = [
            r for r in basket_metrics(conn)["rows"]
            if float(safe_float(r.get("current_R")) or 0.0) > 0
        ]
        ranked = sorted(eligible, key=bank_sort_key)
        selected: List[Dict[str, Any]] = []
        running = 0.0
        remaining = list(ranked)

        while remaining and running + 0.0001 < target:
            need = target - running
            finish = [
                r for r in remaining
                if float(safe_float(r.get("current_R")) or 0.0) + 0.0001 >= need
            ]
            if finish:
                bucket = min(bank_sort_key(r)[:2] for r in finish)
                opts = [r for r in finish if bank_sort_key(r)[:2] == bucket]
                pick = min(
                    opts,
                    key=lambda r: (
                        float(safe_float(r.get("current_R")) or 0.0) - need,
                        bank_sort_key(r),
                    ),
                )
            else:
                pick = remaining[0]
            selected.append(pick)
            running += float(safe_float(pick.get("current_R")) or 0.0)
            remaining = [r for r in remaining if r.get("trade_id") != pick.get("trade_id")]

        if not selected:
            conn.execute(
                "UPDATE protection_stages SET status='ARMED_WAITING_PROFITABLE_POOL',updated_at_utc=?,reason=? WHERE id=?",
                (now_utc_iso(), f"Fixed target {target:.2f}R remains armed; no profitable whole trade available.", stage.get("id")),
            )
            continue

        selected_ids = [safe_str(t.get("trade_id")) for t in selected]
        conn.execute("""
            UPDATE protection_stages
            SET status='EXECUTING',selected_trade_ids=?,updated_at_utc=?,reason=?
            WHERE id=?
        """, (
            ",".join(selected_ids), now_utc_iso(),
            f"v0.8.3 immediate {threshold:.0f}R bank against fixed {target:.2f}R target; durable retry active.",
            stage.get("id"),
        ))

        ids: List[str] = []
        actual = 0.0
        for t in selected:
            rr = float(safe_float(t.get("current_R")) or 0.0)
            px = float(safe_float(t.get("current_price")) or safe_float(t.get("entry_price")) or 0.0)
            ok, val = execute_or_sim_close(conn, t, signal_time, px, f"immediate_bank_{int(threshold)}R", rr)
            if ok:
                actual += val
                ids.append(safe_str(t.get("trade_id")))

        result = finalize_harvest_stage(conn, int(stage.get("id") or 0), signal_time)
        if safe_str(result.get("status")).upper() == "EXECUTED":
            stages_completed += 1
        if ids:
            banked += actual
            bank_ids.extend(ids)

    if banked:
        conn.execute(
            "UPDATE basket_state SET banked_R_cycle=COALESCE(banked_R_cycle,0)+?,realized_R_cycle=COALESCE(realized_R_cycle,0)+?,updated_at_utc=? WHERE singleton_key='BCO_LONG'",
            (banked, banked, now_utc_iso()),
        )

    return {
        "banked_R": banked,
        "banked_trade_ids": bank_ids,
        "bank_stages_completed": stages_completed,
    }


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
        support=candidate_support(conn, max_raw_signal_id=raw_signal_id)
        before=basket_metrics(conn)
        state=ensure_cycle(conn,signal_time,before)
        # Update existing production trades before deciding defence/entry.
        for t in list(before["rows"]):
            update_trade_on_signal(conn,t,signal,support,raw_signal_id=raw_signal_id)

        # v0.8.0 research-only exit challengers advance on the same immutable
        # hourly candle. This has zero execution authority and never feeds back
        # into production state.
        exit_shadow = update_bco_exit_challenger_shadows(
            conn, raw_signal_id, signal
        )

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
            if entry_created:
                # Forward-only: challenger rows are born only alongside NEW
                # production trades after this deployment. No backfill exists.
                start_bco_exit_challenger_shadows(
                    conn, new_trade_id, raw_signal_id
                )
        final=basket_metrics(conn)
        # If basket is now flat, close the cycle cleanly and expire waiting stages.
        if final["open_count"]<=0:
            cycle=safe_str(state.get("cycle_id"))
            if cycle:
                conn.execute("UPDATE protection_stages SET status='EXPIRED_FLAT',updated_at_utc=? WHERE cycle_id=? AND status LIKE 'ARMED%'",(now_utc_iso(),cycle))
            flat_reset = reset_flat_bco_basket_state(
                conn,
                reason="process_signal_final_open_count_zero",
                observed_at=signal_time or now_utc_iso(),
            )
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
        record_basket_snapshot(conn, raw_signal_id, signal_time, final_state, final)
    return {
        "ok":True,
        "raw_signal_id":raw_signal_id,
        "candidate":candidate,
        "entry_allowed":entry_allowed,
        "entry_created":entry_created,
        "trade_id":new_trade_id,
        "basket":snapshot(),
        "exit_challenger_shadow":exit_shadow,
    }


def recover_unprocessed_bco_signals(limit: Optional[int] = None) -> Dict[str, Any]:
    """Recover only FRESH durable BCO signals missing a basket_decision.

    v0.7.9 safety change:
    - scan genuine orphan rows using LEFT JOIN, so a hole below the newest
      decision is detectable;
    - only rows received within BCO_FRESH_SIGNAL_MAX_AGE_SECONDS are eligible
      for deterministic replay;
    - older orphan rows are classified as legacy audit gaps and are NEVER
      replayed into entry/management logic.

    This prevents historical gaps (including old candidate=true rows) from
    reopening trades long after their original candle.
    """
    if not BCO_SIGNAL_RECOVERY_ENABLED:
        return {
            "ok": True,
            "enabled": False,
            "recovered": 0,
            "pending_fresh": 0,
            "legacy_unprocessed": 0,
        }

    batch = max(1, min(int(limit or BCO_SIGNAL_RECOVERY_BATCH_LIMIT), 100))
    now_dt = datetime.now(timezone.utc)
    cutoff = (now_dt - timedelta(seconds=BCO_FRESH_SIGNAL_MAX_AGE_SECONDS)).isoformat()

    with _db_lock, get_conn() as conn:
        fresh_pending = fetchall_dict(conn.execute("""
            SELECT r.*
            FROM raw_signals r
            LEFT JOIN basket_decisions d ON d.raw_signal_id=r.id
            WHERE d.id IS NULL
              AND r.received_at_utc>=?
            ORDER BY r.id ASC
            LIMIT ?
        """, (cutoff, batch)))

        legacy_row = fetchone_dict(conn.execute("""
            SELECT COUNT(*) AS c
            FROM raw_signals r
            LEFT JOIN basket_decisions d ON d.raw_signal_id=r.id
            WHERE d.id IS NULL
              AND r.received_at_utc<?
        """, (cutoff,))) or {}
        legacy_unprocessed = int(safe_float(legacy_row.get("c")) or 0)

    recovered = 0
    duplicates = 0
    errors: List[Dict[str, Any]] = []
    recovered_ids: List[int] = []

    for row in fresh_pending:
        raw_id = int(safe_float(row.get("id")) or 0)
        if raw_id <= 0:
            continue

        with _db_lock, get_conn() as conn:
            done = fetchone_dict(conn.execute(
                "SELECT id FROM basket_decisions WHERE raw_signal_id=? LIMIT 1",
                (raw_id,),
            ))
        if done:
            duplicates += 1
            continue

        try:
            raw_body = json.loads(safe_str(row.get("raw_json")) or "{}")
            if not isinstance(raw_body, dict):
                raise ValueError("raw_json is not a JSON object")
            payload = extract_payload(raw_body)
            if not isinstance(payload, dict):
                raise ValueError("stored payload is not a JSON object")

            result = process_signal(raw_id, payload)
            if result.get("duplicate"):
                duplicates += 1
            elif result.get("ok"):
                recovered += 1
                recovered_ids.append(raw_id)
                research_fn = globals().get("record_bco_focused_research")
                if callable(research_fn):
                    try:
                        research_fn(raw_id)
                    except Exception as research_exc:
                        log_event(
                            "signal_recovery_research_warning",
                            f"raw_signal_id={raw_id}: {research_exc}",
                        )
                log_event(
                    "signal_recovered",
                    f"Recovered fresh stored BCO raw signal {raw_id} into deterministic processing.",
                    {
                        "raw_signal_id": raw_id,
                        "fresh_signal_max_age_seconds": BCO_FRESH_SIGNAL_MAX_AGE_SECONDS,
                    },
                )
            else:
                raise RuntimeError(
                    safe_str(result.get("error") or "process_signal returned ok=false")
                )
        except Exception as exc:
            errors.append({
                "raw_signal_id": raw_id,
                "error": f"{type(exc).__name__}: {exc}",
            })
            log_event(
                "signal_recovery_error",
                f"raw_signal_id={raw_id}: {type(exc).__name__}: {exc}",
                {"raw_signal_id": raw_id},
            )
            # Do not continue past a fresh failed orphan: preserve chronology
            # among the recoverable tail.
            break

    with _db_lock, get_conn() as conn:
        remaining_fresh_row = fetchone_dict(conn.execute("""
            SELECT COUNT(*) AS c
            FROM raw_signals r
            LEFT JOIN basket_decisions d ON d.raw_signal_id=r.id
            WHERE d.id IS NULL
              AND r.received_at_utc>=?
        """, (cutoff,))) or {}
        remaining_fresh = int(safe_float(remaining_fresh_row.get("c")) or 0)

        runtime_set(conn, "signal_recovery_last_at", now_utc_iso())
        runtime_set(conn, "signal_recovery_last_recovered", str(recovered))
        runtime_set(conn, "signal_recovery_last_errors", str(len(errors)))
        runtime_set(conn, "signal_recovery_legacy_unprocessed", str(legacy_unprocessed))
        runtime_set(conn, "signal_recovery_fresh_pending", str(remaining_fresh))

    return {
        "ok": not errors,
        "enabled": True,
        "fresh_signal_max_age_seconds": BCO_FRESH_SIGNAL_MAX_AGE_SECONDS,
        "pending_fresh_scanned": len(fresh_pending),
        "pending_fresh_remaining": remaining_fresh,
        "legacy_unprocessed": legacy_unprocessed,
        "recovered": recovered,
        "recovered_ids": recovered_ids,
        "duplicates": duplicates,
        "errors": errors,
        "time_utc": now_utc_iso(),
    }



# ============================================================
# v0.8.0 — BCO MFE + ATR2 EXIT CHALLENGER FORWARD SHADOW
# Research-only. Zero broker / execution authority.
# ============================================================

def _bco_exit_shadow_wilder_atr14(
    conn: DBConn,
    raw_signal_id: int,
    period: int = BCO_EXIT_SHADOW_ATR_PERIOD,
) -> Optional[float]:
    """Point-in-time Wilder ATR from BCO hourly raw signals.

    Historical raw bars may be used only to seed the indicator. Shadow TRADES are
    never backfilled: challenger rows are created exclusively when a new
    production trade is created after deployment.
    """
    period = max(2, int(period))
    rows = fetchall_dict(conn.execute("""
        SELECT id,exec_close,exec_high,exec_low,raw_json
        FROM raw_signals
        WHERE id<=? AND exec_close IS NOT NULL
        ORDER BY id DESC
        LIMIT ?
    """, (int(raw_signal_id), max(period + 2, int(BCO_EXIT_SHADOW_ATR_LOOKBACK_BARS)))))
    rows = list(reversed(rows))
    if len(rows) < period + 1:
        return None

    # Prefer a genuinely exported ATR if present on the current point-in-time payload.
    try:
        latest_raw = json.loads(safe_str(rows[-1].get("raw_json")) or "{}")
        latest_payload = extract_payload(latest_raw) if isinstance(latest_raw, dict) else {}
        for key in ("atr14", "atr", "exec_atr"):
            v = safe_float(latest_payload.get(key))
            if v is not None and v > 0:
                return float(v)
    except Exception:
        pass

    trs: List[float] = []
    prev_close: Optional[float] = None
    for row in rows:
        close = safe_float(row.get("exec_close"))
        high = safe_float(row.get("exec_high"))
        low = safe_float(row.get("exec_low"))
        if close is None:
            continue
        high = float(high if high is not None else close)
        low = float(low if low is not None else close)
        if prev_close is None:
            tr = max(0.0, high - low)
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(float(tr))
        prev_close = float(close)

    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / float(period)
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + tr) / float(period)
    return float(atr) if atr > 0 else None


def _bco_exit_shadow_classification(actual_r: Optional[float], challenger_r: Optional[float]) -> Dict[str, Any]:
    if actual_r is None or challenger_r is None:
        return {
            "paired_complete": False,
            "delta_r": None,
            "winner": "",
            "saved_reversal": False,
            "killed_large_winner": False,
        }
    actual = float(actual_r)
    challenger = float(challenger_r)
    delta = challenger - actual
    if delta > BCO_EXIT_SHADOW_PAIR_TIE_R:
        winner = "CHALLENGER"
    elif delta < -BCO_EXIT_SHADOW_PAIR_TIE_R:
        winner = "CURRENT"
    else:
        winner = "TIE"
    return {
        "paired_complete": True,
        "delta_r": delta,
        "winner": winner,
        "saved_reversal": bool(
            actual < 0
            and challenger > actual + BCO_EXIT_SHADOW_REVERSAL_SAVE_DELTA_R
        ),
        "killed_large_winner": bool(
            actual >= BCO_EXIT_SHADOW_LARGE_WINNER_R
            and challenger < actual - BCO_EXIT_SHADOW_LARGE_WINNER_SACRIFICE_R
        ),
    }


def _bco_exit_shadow_sync_actual(conn: DBConn, shadow: Dict[str, Any]) -> Dict[str, Any]:
    """Copy actual/current-manager outcome into the research row.

    Reads the production trade table only. Never writes production state.
    """
    trade = fetchone_dict(conn.execute(
        "SELECT * FROM trades WHERE trade_id=? LIMIT 1",
        (shadow.get("trade_id"),),
    )) or {}
    if not trade:
        return shadow

    actual_status = safe_str(trade.get("status")).upper() or "UNKNOWN"
    actual_exit_time = safe_str(trade.get("exit_time"))
    actual_exit_price = safe_float(trade.get("exit_price"))
    actual_r = safe_float(trade.get("realized_R"))
    actual_reason = safe_str(trade.get("exit_reason"))

    if actual_status in {"CLOSED", "BROKER_CLOSED"} and actual_r is None:
        entry = safe_float(shadow.get("entry_price")) or safe_float(trade.get("entry_price"))
        if entry and actual_exit_price:
            actual_r = (((float(actual_exit_price) - float(entry)) / float(entry)) * 100.0) / float(BCO_SL_PCT)

    shadow_closed = safe_str(shadow.get("status")).upper() == "CLOSED"
    pair = _bco_exit_shadow_classification(
        actual_r if actual_status in {"CLOSED", "BROKER_CLOSED"} else None,
        safe_float(shadow.get("hypothetical_exit_R")) if shadow_closed else None,
    )

    conn.execute("""
        UPDATE bco_exit_challenger_shadow SET
            actual_status=?,actual_exit_time=?,actual_exit_price=?,actual_R=?,actual_exit_reason=?,
            paired_complete=?,challenger_minus_current_R=?,paired_winner=?,
            saved_reversal=?,killed_large_winner=?,updated_at_utc=?
        WHERE id=?
    """, (
        actual_status,
        actual_exit_time or None,
        actual_exit_price,
        actual_r,
        actual_reason,
        bool(pair["paired_complete"]),
        pair["delta_r"],
        pair["winner"],
        bool(pair["saved_reversal"]),
        bool(pair["killed_large_winner"]),
        now_utc_iso(),
        shadow.get("id"),
    ))
    return fetchone_dict(conn.execute(
        "SELECT * FROM bco_exit_challenger_shadow WHERE id=? LIMIT 1",
        (shadow.get("id"),),
    )) or shadow


def start_bco_exit_challenger_shadows(
    conn: DBConn,
    trade_id: str,
    entry_raw_signal_id: int,
) -> Dict[str, Any]:
    """Create MFE and ATR2 research rows for a NEW production trade only.

    There is intentionally no historical/backfill loop anywhere in the app.
    """
    if not BCO_EXIT_SHADOW_ENABLED:
        return {"ok": True, "enabled": False, "created": 0}
    _ensure_bco_exit_challenger_shadow_schema_on_conn(conn)
    trade = fetchone_dict(conn.execute(
        "SELECT * FROM trades WHERE trade_id=? LIMIT 1",
        (safe_str(trade_id),),
    )) or {}
    if not trade or safe_str(trade.get("status")).upper() != "OPEN":
        return {"ok": False, "created": 0, "reason": "production_trade_not_open"}

    entry = safe_float(trade.get("entry_price"))
    if entry is None or entry <= 0:
        return {"ok": False, "created": 0, "reason": "entry_price_missing"}

    entry_signal_id = safe_str(trade.get("entry_signal_id"))
    entry_time = safe_str(trade.get("entry_time"))
    hard = safe_float(trade.get("hard_sl_price")) or float(entry) * (1.0 - BCO_SL_PCT / 100.0)
    created = 0

    for challenger in ("MFE_GIVEBACK_50", "ATR2_CHANDELIER"):
        existing = fetchone_dict(conn.execute("""
            SELECT id FROM bco_exit_challenger_shadow
            WHERE trade_id=? AND challenger=? LIMIT 1
        """, (trade_id, challenger)))
        if existing:
            continue
        conn.execute("""
            INSERT INTO bco_exit_challenger_shadow(
                created_at_utc,updated_at_utc,shadow_version,trade_id,challenger,
                entry_raw_signal_id,entry_signal_id,entry_time,entry_price,sl_pct,
                hard_stop_price,status,last_raw_signal_id,last_signal_time,hold_candles,
                current_price,current_R,highest_high,lowest_low,mfe_pct,mae_pct,
                actual_status,note
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_utc_iso(), now_utc_iso(), BCO_EXIT_SHADOW_VERSION, trade_id, challenger,
            int(entry_raw_signal_id), entry_signal_id, entry_time, float(entry), BCO_SL_PCT,
            float(hard), int(entry_raw_signal_id), entry_time, 0,
            float(entry), 0.0, float(entry), float(entry), 0.0, 0.0,
            safe_str(trade.get("status")).upper(),
            "Forward-only research shadow created from new production-accepted BCO trade. Zero execution authority.",
        ))
        created += 1
    return {"ok": True, "enabled": True, "created": created, "trade_id": trade_id}


def _bco_exit_shadow_close(
    conn: DBConn,
    shadow: Dict[str, Any],
    signal_time: str,
    exit_price: float,
    reason: str,
    current_price: float,
    highest: float,
    lowest: float,
    hold: int,
    atr14: Optional[float],
    trail_price: Optional[float],
    mfe_floor: Optional[float],
) -> None:
    entry = float(safe_float(shadow.get("entry_price")) or 0.0)
    exit_r = ((((float(exit_price) - entry) / entry) * 100.0) / float(BCO_SL_PCT)) if entry > 0 else 0.0
    current_r = ((((float(current_price) - entry) / entry) * 100.0) / float(BCO_SL_PCT)) if entry > 0 else 0.0
    mfe_pct = max(0.0, ((highest - entry) / entry) * 100.0) if entry > 0 else 0.0
    mae_pct = min(0.0, ((lowest - entry) / entry) * 100.0) if entry > 0 else 0.0
    conn.execute("""
        UPDATE bco_exit_challenger_shadow SET
            status='CLOSED',last_signal_time=?,hold_candles=?,current_price=?,current_R=?,
            highest_high=?,lowest_low=?,mfe_pct=?,mae_pct=?,atr14=?,
            trail_price=?,mfe_floor_price=?,
            hypothetical_exit_time=?,hypothetical_exit_price=?,hypothetical_exit_R=?,
            hypothetical_exit_reason=?,updated_at_utc=?
        WHERE id=?
    """, (
        signal_time, hold, current_price, current_r,
        highest, lowest, mfe_pct, mae_pct, atr14,
        trail_price, mfe_floor,
        signal_time, float(exit_price), exit_r, reason, now_utc_iso(), shadow.get("id"),
    ))


def _bco_exit_shadow_incomplete_sql(conn: DBConn) -> str:
    """Cross-engine SQL predicate for an incomplete paired research result."""
    return "COALESCE(paired_complete,FALSE)=FALSE" if conn.postgres else "COALESCE(paired_complete,0)=0"


def update_bco_exit_challenger_shadows(
    conn: DBConn,
    raw_signal_id: int,
    signal: Dict[str, Any],
) -> Dict[str, Any]:
    """Advance every active/incomplete forward shadow on the current BCO candle."""
    if not BCO_EXIT_SHADOW_ENABLED:
        return {"ok": True, "enabled": False, "updated": 0, "closed": 0}
    _ensure_bco_exit_challenger_shadow_schema_on_conn(conn)

    current = safe_float(signal.get("exec_close"))
    high = safe_float(signal.get("exec_high"))
    low = safe_float(signal.get("exec_low"))
    signal_time = safe_str(signal.get("timestamp_readable"))
    if current is None:
        return {"ok": False, "enabled": True, "reason": "exec_close_missing"}

    current = float(current)
    high = float(high if high is not None else current)
    low = float(low if low is not None else current)
    atr14 = _bco_exit_shadow_wilder_atr14(conn, int(raw_signal_id))

    incomplete_sql = _bco_exit_shadow_incomplete_sql(conn)
    shadows = fetchall_dict(conn.execute(f"""
        SELECT * FROM bco_exit_challenger_shadow
        WHERE status='OPEN' OR {incomplete_sql}
        ORDER BY id ASC
    """))

    updated = 0
    closed = 0
    for shadow in shadows:
        # Always sync Current outcome, even after challenger has already exited.
        shadow = _bco_exit_shadow_sync_actual(conn, shadow)
        if safe_str(shadow.get("status")).upper() != "OPEN":
            updated += 1
            continue

        last_id = int(safe_float(shadow.get("last_raw_signal_id")) or 0)
        if int(raw_signal_id) <= last_id:
            continue  # recovery/idempotency guard

        entry = float(safe_float(shadow.get("entry_price")) or 0.0)
        if entry <= 0:
            continue
        hard = float(
            safe_float(shadow.get("hard_stop_price"))
            or entry * (1.0 - BCO_SL_PCT / 100.0)
        )
        hold = int(safe_float(shadow.get("hold_candles")) or 0) + 1
        highest = max(float(safe_float(shadow.get("highest_high")) or entry), high)
        lowest = min(float(safe_float(shadow.get("lowest_low")) or entry), low)
        current_r = (((current - entry) / entry) * 100.0) / float(BCO_SL_PCT)
        mfe_pct = max(0.0, ((highest - entry) / entry) * 100.0)
        mae_pct = min(0.0, ((lowest - entry) / entry) * 100.0)
        challenger = safe_str(shadow.get("challenger")).upper()

        trail: Optional[float] = safe_float(shadow.get("trail_price"))
        mfe_floor: Optional[float] = safe_float(shadow.get("mfe_floor_price"))
        exit_price: Optional[float] = None
        exit_reason = ""

        if hold < BCO_EXIT_SHADOW_MIN_HOLD_HOURS:
            if low <= hard:
                exit_price = hard
                exit_reason = "HARD_STOP_PRE48"
        else:
            if challenger == "ATR2_CHANDELIER":
                if atr14 is not None and atr14 > 0:
                    candidate_trail = highest - (BCO_EXIT_SHADOW_ATR_MULTIPLIER * float(atr14))
                    trail = max(
                        hard,
                        float(trail) if trail is not None else hard,
                        candidate_trail,
                    )
                    if low <= trail:
                        exit_price = trail
                        exit_reason = "ATR2_CHANDELIER"
                elif low <= hard:
                    exit_price = hard
                    exit_reason = "HARD_STOP_ATR_UNAVAILABLE"

            elif challenger == "MFE_GIVEBACK_50":
                if highest > entry:
                    retained_fraction = 1.0 - BCO_EXIT_SHADOW_MFE_GIVEBACK_FRACTION
                    candidate_floor = entry + ((highest - entry) * retained_fraction)
                    mfe_floor = max(
                        hard,
                        float(mfe_floor) if mfe_floor is not None else hard,
                        candidate_floor,
                    )
                    if low <= mfe_floor:
                        exit_price = mfe_floor
                        exit_reason = "MFE_GIVEBACK_50"
                elif low <= hard:
                    exit_price = hard
                    exit_reason = "HARD_STOP_NO_POSITIVE_MFE"
            else:
                continue

        if exit_price is not None:
            _bco_exit_shadow_close(
                conn, shadow, signal_time, float(exit_price), exit_reason,
                current, highest, lowest, hold, atr14, trail, mfe_floor,
            )
            refreshed = fetchone_dict(conn.execute(
                "SELECT * FROM bco_exit_challenger_shadow WHERE id=? LIMIT 1",
                (shadow.get("id"),),
            )) or shadow
            _bco_exit_shadow_sync_actual(conn, refreshed)
            closed += 1
        else:
            conn.execute("""
                UPDATE bco_exit_challenger_shadow SET
                    last_raw_signal_id=?,last_signal_time=?,hold_candles=?,
                    current_price=?,current_R=?,highest_high=?,lowest_low=?,
                    mfe_pct=?,mae_pct=?,atr14=?,trail_price=?,mfe_floor_price=?,
                    updated_at_utc=?
                WHERE id=?
            """, (
                int(raw_signal_id), signal_time, hold,
                current, current_r, highest, lowest,
                mfe_pct, mae_pct, atr14, trail, mfe_floor,
                now_utc_iso(), shadow.get("id"),
            ))
        updated += 1

    return {
        "ok": True,
        "enabled": True,
        "updated": updated,
        "closed": closed,
        "atr14": atr14,
        "raw_signal_id": int(raw_signal_id),
        "research_only": True,
        "execution_authority": False,
    }


def sync_bco_exit_challenger_actual_outcomes(conn: DBConn) -> Dict[str, Any]:
    """Refresh paired Current outcomes after broker reconciliation/transaction sync."""
    if not BCO_EXIT_SHADOW_ENABLED:
        return {"ok": True, "enabled": False, "updated": 0}
    _ensure_bco_exit_challenger_shadow_schema_on_conn(conn)
    incomplete_sql = _bco_exit_shadow_incomplete_sql(conn)
    rows = fetchall_dict(conn.execute(f"""
        SELECT * FROM bco_exit_challenger_shadow
        WHERE {incomplete_sql}
        ORDER BY id ASC
    """))
    for row in rows:
        _bco_exit_shadow_sync_actual(conn, row)
    return {"ok": True, "enabled": True, "updated": len(rows)}


def bco_exit_challenger_shadow_summary() -> Dict[str, Any]:
    schema_status = ensure_bco_exit_challenger_shadow_schema()
    with get_conn() as conn:
        rows = fetchall_dict(conn.execute("""
            SELECT * FROM bco_exit_challenger_shadow
            ORDER BY id DESC
        """))
    out: Dict[str, Any] = {
        "ok": True,
        "enabled": BCO_EXIT_SHADOW_ENABLED,
        "shadow_version": BCO_EXIT_SHADOW_VERSION,
        "research_only": True,
        "execution_authority": False,
        "forward_only_no_backfill": True,
        "schema_status": schema_status,
        "challengers": {},
        "trade_count": len({safe_str(r.get("trade_id")) for r in rows if safe_str(r.get("trade_id"))}),
        "row_count": len(rows),
    }
    for challenger in ("MFE_GIVEBACK_50", "ATR2_CHANDELIER"):
        rr = [r for r in rows if safe_str(r.get("challenger")).upper() == challenger]
        pairs = [r for r in rr if parse_bool(r.get("paired_complete"), False)]
        deltas = [safe_float(r.get("challenger_minus_current_R")) for r in pairs]
        deltas = [float(x) for x in deltas if x is not None]
        out["challengers"][challenger] = {
            "rows": len(rr),
            "open": sum(1 for r in rr if safe_str(r.get("status")).upper() == "OPEN"),
            "shadow_closed": sum(1 for r in rr if safe_str(r.get("status")).upper() == "CLOSED"),
            "paired_complete": len(pairs),
            "avg_delta_R": (sum(deltas) / len(deltas)) if deltas else None,
            "challenger_wins": sum(1 for r in pairs if safe_str(r.get("paired_winner")).upper() == "CHALLENGER"),
            "current_wins": sum(1 for r in pairs if safe_str(r.get("paired_winner")).upper() == "CURRENT"),
            "ties": sum(1 for r in pairs if safe_str(r.get("paired_winner")).upper() == "TIE"),
            "saved_reversals": sum(1 for r in pairs if parse_bool(r.get("saved_reversal"), False)),
            "large_winners_killed": sum(1 for r in pairs if parse_bool(r.get("killed_large_winner"), False)),
        }
    return out


@app.get("/bco-exit-challenger-shadow/status")
def bco_exit_challenger_shadow_status_endpoint():
    return bco_exit_challenger_shadow_summary()


@app.get("/bco-exit-challenger-shadow/schema-status")
def bco_exit_challenger_shadow_schema_status_endpoint():
    return bco_exit_challenger_schema_status()


@app.get("/export/bco-exit-challenger-shadow.csv")
def export_bco_exit_challenger_shadow_csv(limit: int = 50000):
    ensure_bco_exit_challenger_shadow_schema()
    limit = max(1, min(int(limit), 100000))
    with get_conn() as conn:
        rows = fetchall_dict(conn.execute("""
            SELECT * FROM bco_exit_challenger_shadow
            ORDER BY id DESC LIMIT ?
        """, (limit,)))
    return csv_response(rows, "bco-exit-challenger-shadow.csv")


def build_bco_exit_challenger_shadow_html() -> str:
    summary = bco_exit_challenger_shadow_summary()
    with get_conn() as conn:
        rows = fetchall_dict(conn.execute("""
            SELECT * FROM bco_exit_challenger_shadow
            ORDER BY id DESC LIMIT 300
        """))

    def _fmt_r(v: Any) -> str:
        n = safe_float(v)
        return "—" if n is None else f"{n:.2f}R"

    cards = ""
    for key, label in (
        ("MFE_GIVEBACK_50", "MFE 50% Giveback"),
        ("ATR2_CHANDELIER", "ATR2 Chandelier"),
    ):
        s = (summary.get("challengers") or {}).get(key) or {}
        avg = safe_float(s.get("avg_delta_R"))
        cards += f"""
          <div class="mini-card">
            <div class="k">{esc(label)}</div>
            <div class="v">{int(s.get('paired_complete') or 0)} paired</div>
            <div class="small">
              Open {int(s.get('open') or 0)} · Shadow closed {int(s.get('shadow_closed') or 0)} ·
              Avg Δ {'—' if avg is None else f'{avg:+.2f}R'}<br>
              Challenger wins {int(s.get('challenger_wins') or 0)} · Current wins {int(s.get('current_wins') or 0)} ·
              Saved reversals {int(s.get('saved_reversals') or 0)} · Killed large winners {int(s.get('large_winners_killed') or 0)}
            </div>
          </div>
        """

    # Pivot latest rows by trade so Current/MFE/ATR2 are easy to compare.
    by_trade: Dict[str, Dict[str, Dict[str, Any]]] = {}
    order: List[str] = []
    for r in rows:
        tid = safe_str(r.get("trade_id"))
        if not tid:
            continue
        if tid not in by_trade:
            by_trade[tid] = {}
            order.append(tid)
        by_trade[tid][safe_str(r.get("challenger")).upper()] = r

    trs = ""
    for tid in order[:80]:
        d = by_trade.get(tid) or {}
        mfe = d.get("MFE_GIVEBACK_50") or {}
        atr = d.get("ATR2_CHANDELIER") or {}
        basis = mfe or atr
        actual_r = safe_float(basis.get("actual_R"))
        trs += f"""
        <tr>
          <td>{esc(tid)}</td>
          <td>{esc(basis.get('entry_time'))}</td>
          <td>{_fmt_r(actual_r)}</td>
          <td>{esc(mfe.get('status') or '—')}</td>
          <td>{_fmt_r(mfe.get('hypothetical_exit_R') if safe_str(mfe.get('status')).upper()=='CLOSED' else mfe.get('current_R'))}</td>
          <td>{_fmt_r(mfe.get('challenger_minus_current_R'))}</td>
          <td>{esc(mfe.get('hypothetical_exit_reason') or '—')}</td>
          <td>{esc(f"{safe_float(mfe.get('mfe_floor_price')):.3f}" if safe_float(mfe.get('mfe_floor_price')) is not None else '—')}</td>
          <td>{esc(atr.get('status') or '—')}</td>
          <td>{_fmt_r(atr.get('hypothetical_exit_R') if safe_str(atr.get('status')).upper()=='CLOSED' else atr.get('current_R'))}</td>
          <td>{_fmt_r(atr.get('challenger_minus_current_R'))}</td>
          <td>{esc(atr.get('hypothetical_exit_reason') or '—')}</td>
          <td>{esc(f"{safe_float(atr.get('trail_price')):.3f}" if safe_float(atr.get('trail_price')) is not None else '—')}</td>
          <td>{esc(f"{safe_float(atr.get('atr14')):.3f}" if safe_float(atr.get('atr14')) is not None else '—')}</td>
        </tr>
        """
    if not trs:
        trs = '<tr><td colspan="14">No forward-shadow trades yet. This is intentional: existing/historical BCO trades are not backfilled. The first new production-accepted trade after deployment will create both challenger rows.</td></tr>'

    return f"""
      <div class="section-note small">
        <strong>Forward shadow only — ZERO broker authority.</strong>
        New production-accepted BCO trades create two independent research copies:
        <strong>MFE 50% giveback after 48h</strong> and <strong>2ATR Chandelier after 48h</strong>.
        Both retain the normal {BCO_SL_PCT:.1f}% hard-stop basis. Either challenger may exit while
        Current continues, or Current may exit while the challenger continues. No historical trade is backfilled.
      </div>
      <div class="metric-grid">
        <div class="mini-card"><div class="k">Forward Trades Shadowed</div><div class="v">{int(summary.get('trade_count') or 0)}</div><div class="small">{esc(BCO_EXIT_SHADOW_VERSION)}</div></div>
        {cards}
        <div class="mini-card"><div class="k">Execution Authority</div><div class="v pos">NONE</div><div class="small">Research tables are never read by production entry, exit, stop, banking or broker code.</div></div>
      </div>
      <div class="table-scroll"><table>
        <thead><tr>
          <th>Production Trade</th><th>Entry</th><th>Current Exit R</th>
          <th>MFE State</th><th>MFE R</th><th>MFE Δ</th><th>MFE Exit</th><th>MFE Floor</th>
          <th>ATR2 State</th><th>ATR2 R</th><th>ATR2 Δ</th><th>ATR2 Exit</th><th>ATR2 Trail</th><th>ATR14</th>
        </tr></thead>
        <tbody>{trs}</tbody>
      </table></div>
      <div class="section-note small">
        <a href="/export/bco-exit-challenger-shadow.csv">Exit Challenger Shadow CSV</a> ·
        <a href="/bco-exit-challenger-shadow/status">Shadow status JSON</a>.
        Paired classifications include challenger-minus-Current R, reversal saved and large-winner killed.
      </div>
    """


# -----------------------------------------------------------------------------
# Reconciliation — only local BCO broker IDs; never touches foreign trades.
# -----------------------------------------------------------------------------

def bco_broker_live_snapshot() -> Dict[str, Any]:
    """Fresh read-only BCO-only broker view for dashboard/accounting."""
    account = account_summary() if OANDA_ENABLED and OANDA_ACCOUNT_ID else {"ok": False}
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok":False,"account":account,"owned_open_trades":[],"owned_open_count":0,
                "owned_unrealized_pl":0.0,"owned_margin_used":0.0,"account_open_count":0,
                "error":"OANDA not configured"}
    resp = oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/openTrades")
    if not resp.get("ok"):
        return {"ok":False,"account":account,"owned_open_trades":[],"owned_open_count":0,
                "owned_unrealized_pl":0.0,"owned_margin_used":0.0,"account_open_count":0,
                "error":resp.get("error")}
    all_trades=(resp.get("data") or {}).get("trades",[]) or []
    owned=[t for t in all_trades if safe_str(t.get("instrument")).upper()==safe_str(BCO_OANDA_INSTRUMENT).upper()]
    return {
        "ok":True,"account":account,"owned_open_trades":owned,"owned_open_count":len(owned),
        "owned_unrealized_pl":sum(float(safe_float(t.get("unrealizedPL")) or 0.0) for t in owned),
        "owned_margin_used":sum(float(safe_float(t.get("marginUsed")) or 0.0) for t in owned),
        "account_open_count":len(all_trades),"time_utc":now_utc_iso()
    }

def reconcile_broker() -> Dict[str,Any]:
    if not OANDA_ENABLED or not OANDA_ACCOUNT_ID:
        return {"ok":False,"skipped":True,"reason":"OANDA not configured"}
    live=bco_broker_live_snapshot()
    if not live.get("ok"):
        return {"ok":False,"error":live.get("error")}
    owned=live.get("owned_open_trades") or []
    by_id={safe_str(t.get("id")):t for t in owned}
    updates=[]; local_only_cleaned=[]
    with _db_lock,get_conn() as conn:
        locals_linked=fetchall_dict(conn.execute(
            "SELECT * FROM trades WHERE broker_trade_id IS NOT NULL AND broker_trade_id<>'' AND status='OPEN'"
        ))
        local_ids={safe_str(t.get("broker_trade_id")) for t in locals_linked}
        for t in locals_linked:
            bid=safe_str(t.get("broker_trade_id")); match=by_id.get(bid)
            if match:
                updates.append({"trade_id":t.get("trade_id"),"broker_trade_id":bid,"status":"OPEN",
                                "unrealizedPL":safe_float(match.get("unrealizedPL")),
                                "broker_entry_price":safe_float(match.get("price")),
                                "currentUnits":safe_float(match.get("currentUnits"))})
            else:
                # Do not invent close economics. Transaction sync is authoritative.
                conn.execute("""UPDATE trades SET exit_reason=CASE WHEN COALESCE(exit_reason,'')='' THEN 'awaiting_broker_close_transaction' ELSE exit_reason END,
                              updated_at_utc=? WHERE trade_id=?""",(now_utc_iso(),t.get("trade_id")))
                updates.append({"trade_id":t.get("trade_id"),"broker_trade_id":bid,"status":"AWAITING_CLOSE_TRANSACTION"})

        if BCO_AUTO_ENTRY_ENABLED and oanda_orders_allowed()[0]:
            local_only=fetchall_dict(conn.execute(
                "SELECT * FROM trades WHERE status='OPEN' AND (broker_trade_id IS NULL OR broker_trade_id='')"
            ))
            for t in local_only:
                conn.execute("""UPDATE trades SET status='ENTRY_FAILED',
                              exit_reason='reconcile_local_open_without_broker_link',updated_at_utc=?
                              WHERE trade_id=?""",(now_utc_iso(),t.get("trade_id")))
                local_only_cleaned.append(safe_str(t.get("trade_id")))

        unlinked_broker=[
            {"broker_trade_id":safe_str(t.get("id")),"instrument":safe_str(t.get("instrument")),
             "units":safe_float(t.get("currentUnits")),"price":safe_float(t.get("price")),
             "unrealizedPL":safe_float(t.get("unrealizedPL"))}
            for t in owned if safe_str(t.get("id")) not in local_ids
        ]
    with _db_lock,get_conn() as _rc:
        runtime_set(_rc,"broker_reconcile_last_at",now_utc_iso())
    tx_sync=sync_broker_transactions() if BCO_TRANSACTION_SYNC_ENABLED else {"ok":False,"skipped":True}

    # Research-only paired-outcome refresh after broker transaction accounting.
    with _db_lock, get_conn() as _shadow_conn:
        exit_shadow_sync = sync_bco_exit_challenger_actual_outcomes(_shadow_conn)

    flat_reset={"reset":False,"reason":"not_flat"}
    if len(owned) == 0:
        with _db_lock,get_conn() as _flat_conn:
            _open_now=fetchone_dict(_flat_conn.execute(
                "SELECT COUNT(*) AS c FROM trades WHERE status='OPEN'"
            )) or {}
            if int(safe_float(_open_now.get("c")) or 0) == 0:
                flat_reset=reset_flat_bco_basket_state(
                    _flat_conn,
                    reason="broker_and_local_flat_after_reconcile",
                    observed_at=now_utc_iso(),
                )
        if flat_reset.get("reset"):
            log_event(
                "basket_cycle_flat_reset",
                "BCO broker and local basket are flat; current basket HWM/giveback reset to zero.",
                flat_reset,
            )

    return {"ok":True,"owned_open_count":len(owned),
            "owned_unrealized_pl":live.get("owned_unrealized_pl"),
            "owned_margin_used":live.get("owned_margin_used"),
            "account_open_count":live.get("account_open_count"),
            "updates":updates,"local_only_cleaned":local_only_cleaned,
            "unlinked_broker_trades":unlinked_broker,"transaction_sync":tx_sync,
            "exit_challenger_shadow_sync":exit_shadow_sync,
            "flat_basket_reset":flat_reset,"time_utc":now_utc_iso()}


def snapshot() -> Dict[str,Any]:
    with get_conn() as conn:
        state=fetchone_dict(conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'")) or {}
        open_rows=fetchall_dict(conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY entry_time ASC,id ASC"))
        closed=fetchone_dict(conn.execute("""SELECT COUNT(*) AS c,COALESCE(SUM(realized_R),0) AS r,
                                         COALESCE(SUM(realized_pnl_gbp),0) AS p
                                         FROM trades WHERE status IN ('CLOSED','BROKER_CLOSED')""")) or {}
        latest=fetchone_dict(conn.execute("""SELECT id,received_at_utc,signal_id,timestamp_readable,
                                         candidate_8h,signal_side,exec_close
                                         FROM raw_signals ORDER BY id DESC LIMIT 1""")) or {}
    local_basket_r=sum(float(safe_float(t.get("current_R")) or 0.0) for t in open_rows)
    local_pnl=sum(
        float(safe_float(t.get("current_R")) or 0.0) *
        float(safe_float(t.get("effective_risk_gbp")) or safe_float(t.get("requested_risk_gbp")) or BCO_RISK_PER_TRADE_GBP)
        for t in open_rows
    )
    broker=bco_broker_live_snapshot()
    return {
        "status":"ok","app":APP_NAME,"policy_version":POLICY_VERSION,
        "strategy":{"asset":BCO_ASSET,"direction":BCO_DIRECTION,"risk_per_trade_gbp":BCO_RISK_PER_TRADE_GBP,
                    "sl_pct":BCO_SL_PCT,"min_hold_hours":BCO_MIN_HOLD_HOURS,
                    "execution_multiplier":BCO_EXECUTION_MULTIPLIER},
        "basket":state,
        "live_local_basket":{"open_count":len(open_rows),"basket_R":local_basket_r,"basket_pnl_gbp":local_pnl},
        "open_trades":open_rows,"closed_summary":closed,"latest_signal":latest,
        "broker_safety":safety_status(),"account":broker.get("account") or {},
        "broker_live":broker,"time_utc":now_utc_iso()
    }


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
# Background workers
# -----------------------------------------------------------------------------
def _signal_recovery_worker() -> None:
    """Dedicated deterministic signal recovery, independent of broker/OANDA I/O."""
    while not _worker_stop.is_set():
        try:
            recover_unprocessed_bco_signals()
        except Exception as e:
            log_event("signal_recovery_worker_error", str(e))
        if _worker_stop.wait(BCO_SIGNAL_RECOVERY_INTERVAL_SECONDS):
            break


def start_signal_recovery_worker() -> None:
    global _signal_recovery_started, _signal_recovery_thread
    if not BCO_SIGNAL_RECOVERY_ENABLED:
        return
    if _signal_recovery_thread is not None and _signal_recovery_thread.is_alive():
        _signal_recovery_started = True
        return
    _signal_recovery_started = True
    _signal_recovery_thread = threading.Thread(
        target=_signal_recovery_worker,
        name="bco-deterministic-signal-recovery",
        daemon=True,
    )
    _signal_recovery_thread.start()


def _worker() -> None:
    while not _worker_stop.wait(BROKER_RECONCILE_INTERVAL_SECONDS):
        try:
            if OANDA_ENABLED and OANDA_ACCOUNT_ID:
                process_broker_action_queue()
                reconcile_broker()
                record_accounting_snapshot()
        except Exception as e:
            log_event("reconcile_error", str(e))


def start_worker() -> None:
    global _worker_started, _worker_thread
    if _worker_thread is not None and _worker_thread.is_alive():
        _worker_started = True
        return
    _worker_started = True
    _worker_thread = threading.Thread(
        target=_worker,
        name="bco-broker-reconcile-and-manager-recovery",
        daemon=True,
    )
    _worker_thread.start()


_bootstrap_started = False
_bootstrap_lock = threading.Lock()
_bootstrap_state = {
    "status": "NOT_STARTED",
    "started_at_utc": "",
    "completed_at_utc": "",
    "error": "",
}

def _background_bootstrap() -> None:
    _bootstrap_state["status"] = "INITIALIZING_DATABASE"
    _bootstrap_state["started_at_utc"] = now_utc_iso()
    try:
        # v0.8.1: create the research-only exit-shadow relation independently
        # before the large bootstrap transaction. A failure here must NEVER stop
        # production BCO recovery/manager startup; later reads/writes will retry.
        try:
            ensure_bco_exit_challenger_shadow_schema(force=True)
        except Exception as shadow_schema_exc:
            try:
                print(f"BCO exit-shadow pre-bootstrap schema repair warning: {shadow_schema_exc}", flush=True)
            except Exception:
                pass

        init_db()

        # Verify again after the main migration. This also repairs the edge case
        # where the first attempt was blocked by a transient Postgres DDL lock.
        try:
            ensure_bco_exit_challenger_shadow_schema(force=True)
        except Exception as shadow_schema_exc:
            try:
                log_event("exit_shadow_schema_warning", str(shadow_schema_exc), bco_exit_challenger_schema_status())
            except Exception:
                pass

        if BCO_SIGNAL_RECOVERY_ENABLED:
            _bootstrap_state["status"] = "RECOVERING_STORED_SIGNALS"
            recovery = recover_unprocessed_bco_signals()
            try:
                log_event("startup_signal_recovery", "BCO startup signal recovery completed.", recovery)
            except Exception:
                pass
        _bootstrap_state["status"] = "STARTING_WORKER"
        start_signal_recovery_worker()
        start_worker()
        try:
            log_event("startup", APP_NAME, {"safety": safety_status()})
        except Exception:
            pass
        _bootstrap_state["status"] = "READY"
        _bootstrap_state["completed_at_utc"] = now_utc_iso()
    except Exception as exc:
        _bootstrap_state["status"] = "FAILED"
        _bootstrap_state["error"] = str(exc)
        _bootstrap_state["completed_at_utc"] = now_utc_iso()
        try:
            print(f"BCO background bootstrap failed: {exc}", flush=True)
        except Exception:
            pass

@app.on_event("startup")
def startup_event() -> None:
    """
    Railway/Uvicorn must be allowed to bind immediately.
    Database migrations and the broker worker are intentionally deferred to
    a daemon thread so slow Postgres DDL can never block the platform healthcheck.
    """
    global _bootstrap_started
    with _bootstrap_lock:
        if _bootstrap_started:
            return
        _bootstrap_started = True
        threading.Thread(
            target=_background_bootstrap,
            name="bco-background-bootstrap",
            daemon=True,
        ).start()



# ============================================================
# BCO — EVENT-DRIVEN AI REGIME OBSERVER
# Mirrors the Live Indices research architecture.
# ZERO execution/broker authority.
# ============================================================
AI_SHADOW_CAPTURE_ENABLED = os.getenv("AI_SHADOW_CAPTURE_ENABLED", "true").strip().lower() == "true"
AI_SHADOW_ENABLED = os.getenv("AI_SHADOW_ENABLED", "false").strip().lower() == "true"
AI_SHADOW_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
AI_SHADOW_OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").strip().rstrip("/")
AI_SHADOW_MODEL = os.getenv("AI_SHADOW_MODEL", "gpt-5.6-terra").strip() or "gpt-5.6-terra"
AI_SHADOW_PROMPT_VERSION = "ai_regime_observer_family_v1_2026_08_21"
AI_SHADOW_OBSERVER_VERSION = "ai_regime_observer_v1_event_driven_2026_08_21"
AI_SHADOW_EVENT_DRIVEN_ONLY = os.getenv("AI_SHADOW_EVENT_DRIVEN_ONLY", "true").strip().lower() == "true"
AI_SHADOW_TIMEOUT_SECONDS = max(10.0, min(float(os.getenv("AI_SHADOW_TIMEOUT_SECONDS", "45")), 120.0))
AI_SHADOW_MAX_OUTPUT_TOKENS = max(250, min(int(float(os.getenv("AI_SHADOW_MAX_OUTPUT_TOKENS", "700"))), 2000))
AI_SHADOW_QUEUE_MAXSIZE = max(50, min(int(float(os.getenv("AI_SHADOW_QUEUE_MAXSIZE", "1000"))), 10000))
AI_SHADOW_GIVEBACK_BANDS_PCT = (25.0, 50.0, 75.0)
# Includes earlier levels than Indices because BCO/Metals basket throughput is
# still being learned prospectively. These are CALL TRIGGERS ONLY.
AI_SHADOW_HIGH_WATER_LEVELS_R = (25.0, 50.0, 75.0, 100.0, 200.0, 300.0)

AI_REGIME_SYSTEM_PROMPT = 'You are the Project Exit Plan AI Regime Observer.\nYou are research-only and have ZERO authority over live or demo trading.\n\nYou receive one immutable point-in-time snapshot captured before the deterministic\ntrading worker acts on that signal. Use ONLY that snapshot. Do not use web\nknowledge, remembered market history, later candles, hidden information, or\nassumptions about what happened next.\n\nThis project trades BCO/Brent long-only using deterministic rules. BCO is a\nsingle-market strategy with stacked entries. Existing positions use a 48h minimum\nnormal hold with hourly mature-runner review thereafter. Your role is to classify\nthe current Brent regime and independently describe whether a fresh deterministic\nlong slice looks supported and whether existing exposure appears HOLD / PROTECT /\nREDUCE. You do not make or alter trading decisions.\n\nregime TREND / CHOP / TRANSITION / EXHAUSTION describes the current state only.\nBe conservative about certainty. Candidate state is evidence, not an instruction.'

AI_REGIME_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "entry_view": {"type": "string", "enum": ["ENTER", "HOLD", "AVOID"]},
        "management_view": {"type": "string", "enum": ["HOLD", "PROTECT", "REDUCE"]},
        "regime": {"type": "string", "enum": ["TREND", "CHOP", "TRANSITION", "EXHAUSTION"]},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "live_rule_assessment": {
            "type": "string",
            "enum": ["AGREE", "AI_MORE_PERMISSIVE", "AI_MORE_DEFENSIVE",
                     "MANAGEMENT_DIFFERS", "INSUFFICIENT_CONTEXT"],
        },
        "reason_codes": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "maxItems": 6,
        },
        "short_reason": {"type": "string"},
    },
    "required": [
        "entry_view","management_view","regime","confidence",
        "live_rule_assessment","reason_codes","short_reason"
    ],
    "additionalProperties": False,
}


_ai_regime_queue = queue.Queue(maxsize=AI_SHADOW_QUEUE_MAXSIZE)
_ai_regime_worker_started = False
_ai_regime_worker_thread = None
_ai_regime_last_heartbeat_utc = ""
_ai_regime_last_raw_signal_id = None


def ensure_ai_regime_observer_table() -> None:
    with get_conn() as conn:
        id_type = "BIGSERIAL PRIMARY KEY" if getattr(conn, "postgres", False) else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS ai_regime_observer (
                id {id_type},
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                raw_signal_id BIGINT NOT NULL UNIQUE,
                asset TEXT,
                signal_time TEXT,
                live_candidate INTEGER DEFAULT 0,
                candidate_side TEXT,
                trigger_reason TEXT,
                api_eligible INTEGER DEFAULT 0,
                status TEXT,
                snapshot_json TEXT,
                model TEXT,
                prompt_version TEXT,
                observer_version TEXT,
                entry_view TEXT,
                management_view TEXT,
                regime TEXT,
                confidence INTEGER,
                live_rule_assessment TEXT,
                reason_codes TEXT,
                short_reason TEXT,
                response_id TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                request_started_at_utc TEXT,
                completed_at_utc TEXT,
                error TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_regime_status ON ai_regime_observer(status, created_at_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_regime_asset ON ai_regime_observer(asset, raw_signal_id)")
        conn.commit()


def _aiobs_bool(v):
    if isinstance(v, bool):
        return v
    return safe_str(v).lower() in {"1","true","yes","on","y","candidate","long","short"}


def _aiobs_band(value, levels):
    v = float(safe_float(value) or 0.0)
    band = 0
    for level in levels:
        if v >= float(level):
            band = int(level)
    return band


def _aiobs_scalar_features(raw):
    """Small deterministic feature subset; secrets/non-scalars are excluded."""
    if not isinstance(raw, dict):
        return {}
    preferred = [
        "pair","ticker","symbol","timestamp","timestamp_readable","timeframe",
        "context_tf","execution_tf","exec_close","exec_high","exec_low",
        "forward_test_candidate","take_trade","signal_side","model_version",
        "model_name","atr_pct","ctx_atr_pct","context_atr_pct","rsi","ctx_rsi",
        "daily_rsi","macd_hist","macd_hist_delta","ema20","ema50","ema200",
        "trend","exec_trend","context_trend","ctx_regime","daily_trend",
        "return_4h","return_8h","return_24h","momentum","breakout","pullback",
    ]
    out = {}
    lower = {safe_str(k).lower(): k for k in raw.keys()}
    for want in preferred:
        key = lower.get(want.lower())
        if key is not None and isinstance(raw.get(key), (str,int,float,bool,type(None))):
            out[want] = raw.get(key)
    # Retain other useful scalar technical fields without dumping arbitrary JSON.
    useful_tokens = ("trend","atr","rsi","macd","ema","moment","return","break","pull","vol","candidate","regime")
    for key,val in raw.items():
        if len(out) >= 60:
            break
        lk = safe_str(key).lower()
        if any(tok in lk for tok in useful_tokens) and isinstance(val,(str,int,float,bool,type(None))):
            out.setdefault(safe_str(key), val)
    return out


def _aiobs_extract_text(data):
    direct = data.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    bits = []
    for item in data.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, dict):
                txt = content.get("text")
                if isinstance(txt, str) and txt.strip():
                    bits.append(txt.strip())
    return "\n".join(bits).strip()


def _aiobs_openai_call(model_input, raw_signal_id):
    body = {
        "model": AI_SHADOW_MODEL,
        "instructions": AI_REGIME_SYSTEM_PROMPT,
        "input": json.dumps(model_input, separators=(",",":"), default=str),
        "store": False,
        "max_output_tokens": AI_SHADOW_MAX_OUTPUT_TOKENS,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "project_exit_plan_regime_observer",
                "strict": True,
                "schema": AI_REGIME_OUTPUT_SCHEMA,
            }
        },
    }
    req = urllib.request.Request(
        AI_SHADOW_OPENAI_API_BASE + "/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + AI_SHADOW_OPENAI_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    last_error = ""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=AI_SHADOW_TIMEOUT_SECONDS) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            txt = _aiobs_extract_text(data)
            decision = json.loads(txt) if txt else {}
            required = {"entry_view","management_view","regime","confidence","live_rule_assessment","reason_codes","short_reason"}
            if not isinstance(decision, dict) or not required.issubset(decision):
                raise ValueError("structured regime decision missing required fields")
            usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
            return {
                "ok": True,
                "decision": decision,
                "response_id": safe_str(data.get("id")),
                "input_tokens": int(safe_float(usage.get("input_tokens")) or 0),
                "output_tokens": int(safe_float(usage.get("output_tokens")) or 0),
            }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
    return {"ok": False, "error": last_error or "OpenAI request failed"}


def process_ai_regime_observer(raw_signal_id):
    if not AI_SHADOW_ENABLED or not AI_SHADOW_OPENAI_API_KEY:
        return {"ok":True,"skipped":True,"reason":"AI observer API inactive"}
    ensure_ai_regime_observer_table()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM ai_regime_observer WHERE raw_signal_id=? LIMIT 1",(int(raw_signal_id),)).fetchone()
        if not row:
            return {"ok":False,"reason":"snapshot_missing"}
        row = dict(row)
        if safe_str(row.get("status")).upper() == "COMPLETE":
            return {"ok":True,"skipped":True,"reason":"already_complete"}
        if not int(safe_float(row.get("api_eligible")) or 0):
            return {"ok":True,"skipped":True,"reason":"capture_only"}
        snap = json.loads(safe_str(row.get("snapshot_json")) or "{}")
        model_input = snap.get("model_input") or {}
        started = now_utc_iso()
        conn.execute("""UPDATE ai_regime_observer
                       SET status='RUNNING',updated_at_utc=?,request_started_at_utc=?,model=?,error=''
                       WHERE raw_signal_id=?""",
                    (started,started,AI_SHADOW_MODEL,int(raw_signal_id)))
        conn.commit()

    api = _aiobs_openai_call(model_input, int(raw_signal_id))
    now = now_utc_iso()
    with get_conn() as conn:
        if not api.get("ok"):
            conn.execute("""UPDATE ai_regime_observer SET status='ERROR',updated_at_utc=?,
                            completed_at_utc=?,error=? WHERE raw_signal_id=?""",
                         (now,now,safe_str(api.get("error"))[:4000],int(raw_signal_id)))
            conn.commit()
            return api
        d = api["decision"]
        conn.execute("""UPDATE ai_regime_observer SET
                        status='COMPLETE',updated_at_utc=?,completed_at_utc=?,
                        entry_view=?,management_view=?,regime=?,confidence=?,
                        live_rule_assessment=?,reason_codes=?,short_reason=?,
                        response_id=?,input_tokens=?,output_tokens=?,model=?,error=''
                        WHERE raw_signal_id=?""",
                     (now,now,safe_str(d.get("entry_view")),safe_str(d.get("management_view")),
                      safe_str(d.get("regime")),int(safe_float(d.get("confidence")) or 0),
                      safe_str(d.get("live_rule_assessment")),json.dumps(d.get("reason_codes") or []),
                      safe_str(d.get("short_reason"))[:1500],safe_str(api.get("response_id")),
                      int(api.get("input_tokens") or 0),int(api.get("output_tokens") or 0),
                      AI_SHADOW_MODEL,int(raw_signal_id)))
        conn.commit()
    return {"ok":True,"decision":d,"research_only":True}


def _aiobs_worker_loop():
    global _ai_regime_last_heartbeat_utc, _ai_regime_last_raw_signal_id
    while True:
        _ai_regime_last_heartbeat_utc = now_utc_iso()
        try:
            rid = _ai_regime_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            _ai_regime_last_raw_signal_id = int(rid)
            process_ai_regime_observer(int(rid))
        finally:
            _ai_regime_last_heartbeat_utc = now_utc_iso()
            try:
                _ai_regime_queue.task_done()
            except Exception:
                pass


def _aiobs_ensure_worker():
    global _ai_regime_worker_started, _ai_regime_worker_thread
    if not AI_SHADOW_ENABLED or not AI_SHADOW_OPENAI_API_KEY:
        return
    if _ai_regime_worker_thread is not None and _ai_regime_worker_thread.is_alive():
        _ai_regime_worker_started = True
        return
    _ai_regime_worker_started = True
    _ai_regime_worker_thread = threading.Thread(target=_aiobs_worker_loop,name="bco-ai-regime-observer",daemon=True)
    _ai_regime_worker_thread.start()


def enqueue_ai_regime_observer(raw_signal_id):
    if not AI_SHADOW_ENABLED:
        return {"queued":False,"reason":"AI_SHADOW_ENABLED=false"}
    if not AI_SHADOW_OPENAI_API_KEY:
        return {"queued":False,"reason":"OPENAI_API_KEY missing"}
    _aiobs_ensure_worker()
    try:
        _ai_regime_queue.put_nowait(int(raw_signal_id))
        return {"queued":True}
    except queue.Full:
        return {"queued":False,"reason":"observer_queue_full"}


def ai_regime_observer_status():
    ensure_ai_regime_observer_table()
    with get_conn() as conn:
        rows = conn.execute("SELECT status,COUNT(*) AS c FROM ai_regime_observer GROUP BY status").fetchall()
    return {
        "ok":True,
        "research_only":True,
        "execution_authority":False,
        "capture_enabled":AI_SHADOW_CAPTURE_ENABLED,
        "ai_enabled":AI_SHADOW_ENABLED,
        "api_key_configured":bool(AI_SHADOW_OPENAI_API_KEY),
        "model":AI_SHADOW_MODEL,
        "prompt_version":AI_SHADOW_PROMPT_VERSION,
        "observer_version":AI_SHADOW_OBSERVER_VERSION,
        "event_driven_only":AI_SHADOW_EVENT_DRIVEN_ONLY,
        "counts":{safe_str(r["status"]):int(r["c"] or 0) for r in rows},
        "worker_started":_ai_regime_worker_started,
        "thread_alive":bool(_ai_regime_worker_thread and _ai_regime_worker_thread.is_alive()),
        "queue_size":_ai_regime_queue.qsize(),
        "last_heartbeat_utc":_ai_regime_last_heartbeat_utc,
        "last_raw_signal_id":_ai_regime_last_raw_signal_id,
    }


@app.get("/ai-regime-observer/status")
def ai_regime_observer_status_endpoint():
    return ai_regime_observer_status()


@app.get("/export/ai-regime-observer.csv")
def export_ai_regime_observer_csv(limit: int = 5000):
    ensure_ai_regime_observer_table()
    limit=max(1,min(int(limit),50000))
    with get_conn() as conn:
        rows=[dict(r) for r in conn.execute(
            "SELECT * FROM ai_regime_observer ORDER BY id DESC LIMIT ?",(limit,)
        ).fetchall()]
    out=io.StringIO()
    if rows:
        fields=[]
        for r in rows:
            for k in r:
                if k not in fields: fields.append(k)
        w=csv.DictWriter(out,fieldnames=fields,extrasaction="ignore")
        w.writeheader();w.writerows(rows)
    else:
        out.write("note\nNo AI regime-observer rows yet\n")
    return Response(content=out.getvalue(),media_type="text/csv",
                    headers={"Content-Disposition":'attachment; filename="ai-regime-observer.csv"'})


def build_ai_regime_observer_html():
    status=ai_regime_observer_status()
    with get_conn() as conn:
        rows=[dict(r) for r in conn.execute(
            "SELECT * FROM ai_regime_observer ORDER BY id DESC LIMIT 300"
        ).fetchall()]
    complete=[r for r in rows if safe_str(r.get("status")).upper()=="COMPLETE"]
    visible=[r for r in rows if int(safe_float(r.get("api_eligible")) or 0) or safe_str(r.get("status")).upper()=="COMPLETE"][:50]
    counts={x:sum(1 for r in complete if safe_str(r.get("regime")).upper()==x)
             for x in ("TREND","TRANSITION","CHOP","EXHAUSTION")}
    latest=complete[0] if complete else None
    if status["ai_enabled"] and status["api_key_configured"]:
        mode="ACTIVE EVENT-DRIVEN REGIME OBSERVER";cls="pos"
    elif status["capture_enabled"]:
        mode="CAPTURE ONLY — API INACTIVE";cls="warn"
    else:
        mode="DISABLED";cls="flat"
    latest_txt=(f"{safe_str(latest.get('asset'))} {safe_str(latest.get('regime'))} "
                f"({int(safe_float(latest.get('confidence')) or 0)}%)") if latest else "No completed label yet"
    trs=""
    for r in visible:
        try:
            codes=json.loads(safe_str(r.get("reason_codes")) or "[]")
        except Exception:
            codes=[]
        trs+=f"""<tr>
          <td>{esc(r.get('signal_time'))}</td><td><strong>{esc(r.get('asset'))}</strong></td>
          <td>{esc(r.get('trigger_reason') or '—')}</td>
          <td>{'TRUE' if int(safe_float(r.get('live_candidate')) or 0) else 'FALSE'}</td>
          <td>{esc(r.get('candidate_side') or '—')}</td>
          <td><strong>{esc(r.get('regime') or '—')}</strong></td>
          <td>{esc(r.get('confidence') if r.get('confidence') is not None else '—')}</td>
          <td>{esc(', '.join(str(x) for x in codes) or '—')}</td>
          <td>{esc(r.get('short_reason') or r.get('error') or '—')}</td>
          <td>{esc(r.get('status'))}</td>
        </tr>"""
    if not trs:
        trs='<tr><td colspan="10">No AI regime-observer events yet. First post-deploy production signal will create a point-in-time observation.</td></tr>'
    return f"""
      <div class="section-note small">
        <strong>Research only — zero broker authority.</strong>
        Same event-driven regime-observer architecture and output taxonomy as Live Indices.
        Every signal snapshot is frozen before deterministic processing; paid calls occur only
        on meaningful state changes. No observer field is consumed by entry, exit, stop,
        sizing, harvesting or basket-management code.
      </div>
      <div class="cards three">
        <div class="card"><div class="label">AI Regime Observer</div><div class="value {cls}" style="font-size:18px">{esc(mode)}</div><div class="small">Model: {esc(AI_SHADOW_MODEL)} · {esc(AI_SHADOW_PROMPT_VERSION)}</div></div>
        <div class="card"><div class="label">Latest Regime Label</div><div class="value flat" style="font-size:18px">{esc(latest_txt)}</div><div class="small">TREND {counts['TREND']} · TRANSITION {counts['TRANSITION']} · CHOP {counts['CHOP']} · EXHAUSTION {counts['EXHAUSTION']}</div></div>
        <div class="card"><div class="label">API Spend Control</div><div class="value pos">EVENT DRIVEN</div><div class="small">Captured {len(rows)} recent · paid-eligible {sum(1 for r in rows if int(safe_float(r.get('api_eligible')) or 0))}</div></div>
      </div>
      <div class="table-scroll"><table>
        <thead><tr><th>Candle</th><th>Asset</th><th>Why AI Was Called</th><th>Candidate</th><th>Side</th><th>Regime</th><th>Confidence</th><th>Reason Codes</th><th>Observer Reason / Error</th><th>Status</th></tr></thead>
        <tbody>{trs}</tbody>
      </table></div>
      <div class="section-note small">
        Export: <a href="/export/ai-regime-observer.csv">AI Regime Observer CSV</a> ·
        status: <a href="/ai-regime-observer/status">observer JSON</a>.
      </div>
    """


def _bco_aiobs_state(conn, raw_signal_id, payload):
    """
    Point-in-time BCO observer state using the SAME basket state basis as the
    production manager/dashboard, rather than rebuilding from stale trade rows.
    """
    sig=conn.execute("SELECT * FROM raw_signals WHERE id=? LIMIT 1",(int(raw_signal_id),)).fetchone()
    sig=dict(sig) if sig else {}
    state=conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1").fetchone()
    state=dict(state) if state else {}

    rows=conn.execute("SELECT * FROM trades WHERE status='OPEN' ORDER BY id").fetchall()
    open_rows=[dict(r) for r in rows]

    # Use actual wall-clock/manager age where available for maturity count.
    mature=0
    for t in open_rows:
        hold=int(safe_float(t.get("hold_candles")) or 0)
        if hold >= 48:
            mature += 1

    candidate=bool(sig.get("candidate_8h"))

    # Production basket_state is the authoritative manager state.
    # Fallback only if older DB rows do not have the expected columns populated.
    basket_r=(
        safe_float(state.get("current_basket_R"))
        if safe_float(state.get("current_basket_R")) is not None
        else safe_float(state.get("basket_R"))
    )
    if basket_r is None:
        basket_r=sum(float(safe_float(t.get("current_R")) or 0.0) for t in open_rows)
    basket_r=float(basket_r or 0.0)

    hwm=(
        safe_float(state.get("high_water_R"))
        if safe_float(state.get("high_water_R")) is not None
        else safe_float(state.get("high_water_r"))
    )
    hwm=float(hwm or 0.0)

    give=(
        safe_float(state.get("giveback_pct"))
        if safe_float(state.get("giveback_pct")) is not None
        else 0.0
    )
    give=float(give or 0.0)

    # If giveback percentage is absent/stale but R HWM is available, derive
    # an R-basis fallback for observer context only.
    if hwm > 0 and give <= 0 and basket_r < hwm:
        give=max(0.0,(hwm-basket_r)/hwm*100.0)

    consumed=0
    cycle=safe_str(state.get("cycle_id"))
    if cycle:
        row=conn.execute("""SELECT COUNT(*) AS c FROM protection_stages
                            WHERE cycle_id=? AND stage_type='BANK'
                              AND UPPER(COALESCE(status,'')) IN
                            ('EXECUTED','CONSUMED','DONE','BANKED')""",(cycle,)).fetchone()
        consumed=int(row["c"] or 0) if row else 0

    return {
        "asset":"BCO",
        "candidate":candidate,
        "side":"long" if candidate else "",
        "open_count":len(open_rows),
        "mature_48h_plus":mature,

        # Explicit provenance for future audit/export.
        "basket_state_source":"basket_state:BCO_LONG",
        "basket_r":basket_r,
        "high_water_r":hwm,
        "giveback_pct":give,

        "giveback_band":_aiobs_band(give,AI_SHADOW_GIVEBACK_BANDS_PCT),
        "high_water_band":_aiobs_band(hwm,AI_SHADOW_HIGH_WATER_LEVELS_R),
        "consumed_bank_stages":consumed,
        "cycle_id":cycle,
        "tide_status":safe_str(
            state.get("tide_status")
            or state.get("status")
            or state.get("phase")
        ),
        "manager_action":safe_str(
            state.get("manager_action")
            or state.get("recommended_action")
            or state.get("action")
        ),
    }


def capture_ai_regime_snapshot(raw_signal_id, payload):
    if not AI_SHADOW_CAPTURE_ENABLED:
        return {"captured":False,"reason":"AI_SHADOW_CAPTURE_ENABLED=false"}
    ensure_ai_regime_observer_table()
    with get_conn() as conn:
        existing=conn.execute("SELECT * FROM ai_regime_observer WHERE raw_signal_id=? LIMIT 1",(int(raw_signal_id),)).fetchone()
        if existing:
            return {"captured":True,"existing":True,"api_eligible":bool(existing["api_eligible"]),"trigger_reason":existing["trigger_reason"]}
        sig=conn.execute("SELECT * FROM raw_signals WHERE id=? LIMIT 1",(int(raw_signal_id),)).fetchone()
        sig=dict(sig) if sig else {}
        current=_bco_aiobs_state(conn,raw_signal_id,payload)
        previous_row=conn.execute("SELECT snapshot_json FROM ai_regime_observer ORDER BY raw_signal_id DESC LIMIT 1").fetchone()
        previous={}
        if previous_row:
            try: previous=(json.loads(previous_row["snapshot_json"]) or {}).get("event_state") or {}
            except Exception: previous={}
        reasons=[]
        if not previous:
            reasons.append("FIRST_OBSERVATION")
        else:
            if bool(current["candidate"]) != bool(previous.get("candidate")):
                reasons.append("CANDIDATE_FLIP_TO_TRUE" if current["candidate"] else "CANDIDATE_FLIP_TO_FALSE")
            if int(previous.get("mature_48h_plus") or 0)<=0<int(current["mature_48h_plus"]):
                reasons.append("MATURE_EXPOSURE_ON")
            elif int(previous.get("mature_48h_plus") or 0)>0>=int(current["mature_48h_plus"]):
                reasons.append("MATURE_EXPOSURE_OFF")
            if int(current["giveback_band"])>int(previous.get("giveback_band") or 0):
                reasons.append(f"GIVEBACK_BAND_UP_{int(previous.get('giveback_band') or 0)}_TO_{current['giveback_band']}")
            if int(current["high_water_band"])>int(previous.get("high_water_band") or 0):
                reasons.append(f"HIGH_WATER_BAND_UP_{int(previous.get('high_water_band') or 0)}_TO_{current['high_water_band']}")
            if int(current["consumed_bank_stages"])>int(previous.get("consumed_bank_stages") or 0):
                reasons.append("BANK_STAGE_CONSUMED")
        api_eligible=(not AI_SHADOW_EVENT_DRIVEN_ONLY) or bool(reasons)
        model_input={
            "snapshot_version":"ai_regime_observer_point_in_time_v1",
            "project_family":"BCO",
            "current_asset":"BCO",
            "current_signal":_aiobs_scalar_features(payload),
            "event_state":current,
            "recent_history":[],
            "research_note":"Single-asset Brent long-only strategy. Snapshot captured before deterministic processing. Basket R/HWM/giveback come from production basket_state:BCO_LONG.",
        }
        recent=conn.execute("""SELECT timestamp_readable,candidate_8h,exec_close,signal_side
                               FROM raw_signals WHERE id<=? ORDER BY id DESC LIMIT 6""",(int(raw_signal_id),)).fetchall()
        model_input["recent_history"]=[dict(r) for r in recent]
        snapshot={"captured_at_utc":now_utc_iso(),"event_state":current,"model_input":model_input}
        status="CAPTURED" if api_eligible else "CAPTURED_NO_CALL"
        trigger="|".join(reasons)
        conn.execute("""INSERT INTO ai_regime_observer(
            created_at_utc,updated_at_utc,raw_signal_id,asset,signal_time,
            live_candidate,candidate_side,trigger_reason,api_eligible,status,
            snapshot_json,model,prompt_version,observer_version)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now_utc_iso(),now_utc_iso(),int(raw_signal_id),"BCO",safe_str(sig.get("timestamp_readable")),
             1 if current["candidate"] else 0,current["side"],trigger,1 if api_eligible else 0,status,
             json.dumps(snapshot,default=str),AI_SHADOW_MODEL,AI_SHADOW_PROMPT_VERSION,AI_SHADOW_OBSERVER_VERSION))
        conn.commit()
    q=enqueue_ai_regime_observer(raw_signal_id) if api_eligible else {"queued":False,"reason":"event_driven_capture_only"}
    return {"captured":True,"api_eligible":api_eligible,"trigger_reason":trigger,"queue":q,"research_only":True}


# -----------------------------------------------------------------------------
# API / dashboard
# -----------------------------------------------------------------------------
def check_webhook(secret: str) -> None:
    if not WEBHOOK_SECRET or WEBHOOK_SECRET == "change-me" or secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="invalid webhook secret")


def check_admin(x_admin_secret: Optional[str]) -> None:
    if not ADMIN_SECRET or ADMIN_SECRET == "change-me-too" or safe_str(x_admin_secret) != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="invalid admin secret")



def record_accounting_snapshot() -> Dict[str, Any]:
    live = bco_broker_live_snapshot()
    if not live.get("ok"):
        return {"ok": False, "error": live.get("error")}
    acct = live.get("account") or {}
    with _db_lock, get_conn() as conn:
        realized = fetchone_dict(conn.execute("""
            SELECT COALESCE(SUM(broker_realized_pl_home),0) AS pl,
                   COALESCE(SUM(financing_home),0) AS fin
            FROM trades WHERE status='CLOSED'
        """)) or {}
        capital = float(runtime_get(conn, "broker_capital_movements_total", "0") or 0.0)
        cursor = runtime_get(conn, "broker_transaction_cursor", "")
        conn.execute("""
            INSERT INTO accounting_snapshots(
                created_at_utc,account_nav,account_balance,bco_open_pl,bco_realized_pl,
                bco_financing,capital_movements,broker_last_transaction_id,note
            ) VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            now_utc_iso(), safe_float(acct.get("NAV")), safe_float(acct.get("balance")),
            safe_float(live.get("owned_unrealized_pl")) or 0.0,
            safe_float(realized.get("pl")) or 0.0, safe_float(realized.get("fin")) or 0.0,
            capital, cursor,
            "Shared account NAV/balance; BCO open/realized/financing isolated by owned trades."
        ))
        runtime_set(conn, "accounting_snapshot_last_at", now_utc_iso())
    return {"ok": True}


def operational_health() -> Dict[str, Any]:
    db_ok = True
    db_error = ""
    signal_time = ""
    reconcile_time = ""
    queue_last = ""
    tx_sync_at = ""
    queue_pending = queue_failed = local_open = 0
    broker_open = None
    local_broker_linked = 0
    latest_raw_id = 0
    latest_decision_raw_id = 0
    latest_processing_error = ""
    try:
        with get_conn() as conn:
            fetchone_dict(conn.execute("SELECT 1 AS ok"))
            latest = fetchone_dict(conn.execute(
                "SELECT id,received_at_utc,timestamp_readable FROM raw_signals ORDER BY id DESC LIMIT 1"
            )) or {}
            signal_time = safe_str(latest.get("received_at_utc") or latest.get("timestamp_readable"))
            latest_raw_id = int(safe_float(latest.get("id")) or 0)
            _dec = fetchone_dict(conn.execute(
                "SELECT raw_signal_id FROM basket_decisions ORDER BY id DESC LIMIT 1"
            )) or {}
            latest_decision_raw_id = int(safe_float(_dec.get("raw_signal_id")) or 0)
            _err = fetchone_dict(conn.execute(
                """SELECT message FROM system_events
                   WHERE event_type='signal_processing_error'
                   ORDER BY id DESC LIMIT 1"""
            )) or {}
            latest_processing_error = safe_str(_err.get("message"))
            reconcile_time = runtime_get(conn, "broker_reconcile_last_at", "")
            queue_last = runtime_get(conn, "broker_action_queue_last_run", "")
            tx_sync_at = runtime_get(conn, "broker_transaction_sync_at", "")
            q = fetchone_dict(conn.execute("""
                SELECT
                    SUM(CASE WHEN status IN ('PENDING','RETRY') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status='FAILED_FINAL' THEN 1 ELSE 0 END) AS failed
                FROM broker_action_queue
            """)) or {}
            queue_pending = int(safe_float(q.get("pending")) or 0)
            queue_failed = int(safe_float(q.get("failed")) or 0)
            tr = fetchone_dict(conn.execute("""
                SELECT COUNT(*) AS c,
                       SUM(CASE WHEN broker_trade_id IS NOT NULL AND broker_trade_id<>'' THEN 1 ELSE 0 END) AS linked
                FROM trades WHERE status='OPEN'
            """)) or {}
            local_open = int(safe_float(tr.get("c")) or 0)
            local_broker_linked = int(safe_float(tr.get("linked")) or 0)
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    broker = bco_broker_live_snapshot() if OANDA_ENABLED and OANDA_ACCOUNT_ID else {"ok": False}
    if broker.get("ok"):
        broker_open = int(broker.get("owned_open_count") or 0)

    signal_age = _iso_age_seconds(signal_time)
    reconcile_age = _iso_age_seconds(reconcile_time)
    tx_age = _iso_age_seconds(tx_sync_at)

    worker_alive = bool(_worker_thread is not None and _worker_thread.is_alive())
    recovery_worker_alive = bool(
        _signal_recovery_thread is not None and _signal_recovery_thread.is_alive()
    )
    recovery_last_at = ""
    recovery_last_count = 0
    try:
        with get_conn() as conn:
            recovery_last_at = runtime_get(conn, "signal_recovery_last_at", "")
            recovery_last_count = int(float(runtime_get(conn, "signal_recovery_last_recovered", "0") or 0))
    except Exception:
        pass

    checks = {
        "database": {"ok": db_ok, "error": db_error},
        "manager_recovery_worker": {
            "ok": (not BCO_SIGNAL_RECOVERY_ENABLED) or recovery_worker_alive,
            "enabled": BCO_SIGNAL_RECOVERY_ENABLED,
            "thread_alive": recovery_worker_alive,
            "interval_seconds": BCO_SIGNAL_RECOVERY_INTERVAL_SECONDS,
            "last_recovery_at": recovery_last_at,
            "last_recovered_count": recovery_last_count,
            "broker_worker_alive": worker_alive,
        },
        "signal_freshness": {
            "ok": signal_age is None or signal_age <= BCO_HEALTH_SIGNAL_STALE_SECONDS,
            "age_seconds": signal_age, "latest": signal_time,
        },
        "signal_processing": {
            "ok": latest_raw_id == 0 or latest_decision_raw_id >= latest_raw_id,
            "latest_raw_signal_id": latest_raw_id,
            "latest_decision_raw_signal_id": latest_decision_raw_id,
            "lag": max(0, latest_raw_id-latest_decision_raw_id),
            "last_processing_error": latest_processing_error,
        },
        "broker_read": {"ok": bool(broker.get("ok")), "owned_open_count": broker_open, "error": broker.get("error")},
        "local_broker_parity": {
            "ok": broker_open is None or (local_broker_linked == broker_open and local_open == local_broker_linked),
            "local_open": local_open, "local_linked": local_broker_linked, "broker_open": broker_open,
        },
        "action_queue": {
            "ok": queue_failed == 0, "pending": queue_pending, "failed_final": queue_failed, "last_run": queue_last,
        },
        "transaction_sync": {
            "ok": (not BCO_TRANSACTION_SYNC_ENABLED) or (tx_sync_at != "" and (tx_age is None or tx_age <= max(BCO_HEALTH_RECONCILE_STALE_SECONDS*3, 600))),
            "enabled": BCO_TRANSACTION_SYNC_ENABLED, "last_sync": tx_sync_at, "age_seconds": tx_age,
        },
        "reconciliation": {
            "ok": reconcile_time == "" or reconcile_age is None or reconcile_age <= max(BCO_HEALTH_RECONCILE_STALE_SECONDS*3, 600),
            "last_reconcile": reconcile_time, "age_seconds": reconcile_age,
        },
    }
    overall = "ok" if all(bool(v.get("ok")) for v in checks.values()) else "degraded"
    return {"status": overall, "checks": checks, "time_utc": now_utc_iso()}

@app.get("/health")
def health():
    """
    Railway liveness only. Must always return immediately once Uvicorn is bound.
    Database/broker readiness is deliberately NOT part of platform liveness.
    """
    return {
        "status": "ok",
        "app": APP_NAME,
        "version": APP_VERSION,
        "bootstrap": dict(_bootstrap_state),
        "time_utc": now_utc_iso(),
    }


@app.get("/ready")
def ready():
    """Application readiness separate from Railway liveness."""
    status = safe_str(_bootstrap_state.get("status")).upper()
    return {
        "status": "ok" if status == "READY" else "initializing" if status not in {"FAILED"} else "degraded",
        "ready": status == "READY",
        "bootstrap": dict(_bootstrap_state),
        "time_utc": now_utc_iso(),
    }


# -----------------------------------------------------------------------------
# Webhook ingress audit / delivery hardening
# -----------------------------------------------------------------------------
def ensure_webhook_ingress_audit_table() -> None:
    with _db_lock, get_conn() as conn:
        id_type = "BIGSERIAL PRIMARY KEY" if conn.postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS webhook_ingress_audit (
                id {id_type}, received_at_utc TEXT NOT NULL, completed_at_utc TEXT,
                status TEXT NOT NULL, http_content_type TEXT, body_size BIGINT,
                body_preview_redacted TEXT, pair TEXT, signal_id TEXT, signal_time TEXT,
                raw_signal_id BIGINT, detail TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bco_webhook_ingress_time ON webhook_ingress_audit(received_at_utc)")
        conn.commit()


def _bco_redact_webhook_preview(raw_text: str) -> str:
    s=safe_str(raw_text)[:12000]
    s=re.sub(r'(?i)("secret"\s*:\s*")[^"]*(")', r'\1***REDACTED***\2', s)
    s=re.sub(r"(?i)('secret'\s*:\s*')[^']*(')", r'\1***REDACTED***\2', s)
    return s


def _bco_ingress_begin(content_type: str, raw_text: str) -> Optional[int]:
    try:
        ensure_webhook_ingress_audit_table()
        with _db_lock,get_conn() as conn:
            rid=db_insert_id(conn,"""INSERT INTO webhook_ingress_audit(received_at_utc,status,http_content_type,body_size,body_preview_redacted,detail) VALUES(?,?,?,?,?,?)""",
                (now_utc_iso(),'RECEIVED',safe_str(content_type),len(raw_text.encode('utf-8',errors='ignore')),_bco_redact_webhook_preview(raw_text),'HTTP request reached Railway'))
            return int(rid)
    except Exception:
        return None


def _bco_ingress_finish(receipt_id: Optional[int],status: str,*,pair: str='',signal_id: str='',signal_time: str='',raw_signal_id: Any=None,detail: str='') -> None:
    if not receipt_id: return
    try:
        with _db_lock,get_conn() as conn:
            conn.execute("UPDATE webhook_ingress_audit SET completed_at_utc=?,status=?,pair=?,signal_id=?,signal_time=?,raw_signal_id=?,detail=? WHERE id=?",
                (now_utc_iso(),safe_str(status),safe_str(pair),safe_str(signal_id),safe_str(signal_time),int(raw_signal_id) if raw_signal_id is not None else None,safe_str(detail),int(receipt_id)))
    except Exception: pass


def bco_webhook_ingress_health(hours: int=48) -> Dict[str,Any]:
    ensure_webhook_ingress_audit_table(); hours=max(1,min(int(hours),168)); cutoff=(datetime.now(timezone.utc)-timedelta(hours=hours)).isoformat()
    with _db_lock,get_conn() as conn:
        rows=fetchall_dict(conn.execute("SELECT * FROM webhook_ingress_audit WHERE received_at_utc>=? ORDER BY id DESC LIMIT 1000",(cutoff,)))
    counts={}
    for r in rows:
        st=safe_str(r.get('status') or 'UNKNOWN').upper(); counts[st]=counts.get(st,0)+1
    failures=sum(v for k,v in counts.items() if k in {'INVALID_JSON','BAD_SECRET','WRONG_ASSET','DB_ERROR','STORE_FAILED'})
    return {'ok':failures==0,'hours':hours,'receipt_count':len(rows),'status_counts':counts,'latest':rows[:50],
            'note':'RECEIVED proves HTTP reached Railway; STORED proves raw_signals insert succeeded.'}


@app.get('/webhook/ingress-health')
def bco_webhook_ingress_health_route(hours: int=48): return bco_webhook_ingress_health(hours)


@app.get('/export/webhook-ingress.csv')
def bco_export_webhook_ingress_csv(limit: int=5000):
    ensure_webhook_ingress_audit_table(); limit=max(1,min(int(limit),50000))
    with _db_lock,get_conn() as conn:
        rows=fetchall_dict(conn.execute('SELECT * FROM webhook_ingress_audit ORDER BY id DESC LIMIT ?',(limit,)))
    out=io.StringIO(); fields=['id','received_at_utc','completed_at_utc','status','http_content_type','body_size','pair','signal_id','signal_time','raw_signal_id','detail','body_preview_redacted']
    w=csv.DictWriter(out,fieldnames=fields,extrasaction='ignore'); w.writeheader(); w.writerows(rows)
    return Response(content=out.getvalue(),media_type='text/csv',headers={'Content-Disposition':'attachment; filename="webhook-ingress.csv"'})


def _bco_webhook_ingress_html() -> str:
    h=bco_webhook_ingress_health(48); counts=h.get('status_counts') or {}; bad=sum(v for k,v in counts.items() if k in {'INVALID_JSON','BAD_SECRET','WRONG_ASSET','DB_ERROR','STORE_FAILED'})
    trs=''
    for r in (h.get('latest') or [])[:30]:
        trs+=f"<tr><td>{esc(r.get('received_at_utc'))}</td><td>{esc(r.get('signal_time') or '—')}</td><td><strong>{esc(r.get('status'))}</strong></td><td>{esc(r.get('raw_signal_id') or '—')}</td><td>{esc(r.get('detail') or '—')}</td></tr>"
    if not trs: trs='<tr><td colspan="5">No ingress receipts yet; logging begins with the next webhook hit.</td></tr>'
    return f"""<div class='metric-grid'><div class='mini-card'><div class='k'>HTTP Receipts · 48h</div><div class='v'>{int(h.get('receipt_count') or 0)}</div><div class='small'>Every BCO webhook that reached Railway</div></div><div class='mini-card'><div class='k'>Ingress Errors</div><div class='v'>{bad}</div><div class='small'>{esc(counts)}</div></div></div><div class='section-note small'><strong>Delivery audit.</strong> RECEIVED = HTTP reached Railway; STORED = raw signal safely persisted.</div><div class='table-scroll'><table><thead><tr><th>Received</th><th>Signal Time</th><th>Status</th><th>Raw ID</th><th>Detail</th></tr></thead><tbody>{trs}</tbody></table></div><div class='section-note small'><a href='/webhook/ingress-health'>Ingress health JSON</a> · <a href='/export/webhook-ingress.csv'>Ingress audit CSV</a></div>"""


@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request, secret: str = Query(default="")):
    raw_bytes=await request.body(); raw_text=raw_bytes.decode('utf-8',errors='replace'); receipt_id=_bco_ingress_begin(request.headers.get('content-type',''),raw_text)
    if not WEBHOOK_SECRET or WEBHOOK_SECRET == "change-me" or secret != WEBHOOK_SECRET:
        _bco_ingress_finish(receipt_id,'BAD_SECRET',detail='Request reached Railway but webhook secret did not match')
        raise HTTPException(status_code=401,detail="Invalid webhook secret")
    try:
        body=json.loads(raw_text)
    except Exception as exc:
        _bco_ingress_finish(receipt_id,'INVALID_JSON',detail=f'{type(exc).__name__}: {exc}')
        raise HTTPException(status_code=400,detail='Invalid JSON body')
    if not isinstance(body,dict):
        _bco_ingress_finish(receipt_id,'INVALID_JSON',detail='JSON object required')
        raise HTTPException(status_code=400,detail='JSON object required')
    payload=extract_payload(body); pair=normalise_pair(payload.get('pair') or payload.get('ticker') or payload.get('symbol')); signal_id=safe_str(payload.get('signal_id')); signal_time=safe_str(payload.get('timestamp') or payload.get('timestamp_readable') or payload.get('rule_entry_timestamp'))
    if pair != BCO_ASSET:
        _bco_ingress_finish(receipt_id,'WRONG_ASSET',pair=pair,signal_id=signal_id,signal_time=signal_time,detail='BCO service owns BCO only')
        raise HTTPException(status_code=400,detail=f"BCO Live accepts BCO only; received {pair or 'unknown'}")
    try:
        raw_id,payload,duplicate=store_signal(body)
    except Exception as exc:
        _bco_ingress_finish(receipt_id,'DB_ERROR',pair=pair,signal_id=signal_id,signal_time=signal_time,detail=f'{type(exc).__name__}: {exc}')
        raise
    if duplicate:
        _bco_ingress_finish(receipt_id,'DUPLICATE',pair=pair,signal_id=signal_id,signal_time=signal_time,raw_signal_id=raw_id,detail='Signal already stored')
        return {"status":"duplicate","raw_signal_id":raw_id,"ingress_receipt_id":receipt_id}
    _bco_ingress_finish(receipt_id,'STORED',pair=pair,signal_id=signal_id,signal_time=signal_time,raw_signal_id=raw_id,detail='HTTP received, validated and stored in raw_signals')
    try:
        ai_regime_observer=capture_ai_regime_snapshot(raw_id,payload)
    except Exception as _ai_exc:
        ai_regime_observer={"captured":False,"research_only":True,"error":f"{type(_ai_exc).__name__}: {_ai_exc}"}
    try:
        result=process_signal(raw_id,payload); focused_research=record_bco_focused_research(raw_id)
    except Exception as e:
        log_event("signal_processing_error",str(e),{"raw_signal_id":raw_id}); raise
    return {"status":"ok","raw_signal_id":raw_id,"ingress_receipt_id":receipt_id,"result":result,"focused_research":focused_research,"ai_regime_observer":ai_regime_observer}


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


@app.post("/admin/signals/recover-pending")
def recover_pending_signals_endpoint(
    x_admin_secret: Optional[str] = Header(default=None),
    limit: int = Query(default=100, ge=1, le=100),
):
    """Manual safe recovery trigger for FRESH orphan signals only.

    Historical orphan rows older than BCO_FRESH_SIGNAL_MAX_AGE_SECONDS are
    intentionally never replayed into current entry/management logic.
    """
    check_admin(x_admin_secret)
    return recover_unprocessed_bco_signals(limit=limit)


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
    allowed={
        "raw-signals":"raw_signals",
        "trades":"trades",
        "basket-decisions":"basket_decisions",
        "protection-stages":"protection_stages",
        "managed-stops":"managed_stop_events",
        "execution-audit":"execution_audit",
        "system-events":"system_events",
        "broker-action-queue":"broker_action_queue",
        "broker-transactions":"broker_transactions",
        "fixed-48-outcomes":"fixed_48_outcomes",
        "trade-manager-reviews":"trade_manager_reviews",
        "basket-snapshots":"basket_snapshots",
        "harvest-execution-outcomes":"harvest_execution_outcomes",
        "accounting-snapshots":"accounting_snapshots",
        "exit-challenger-shadow":"bco_exit_challenger_shadow",
    }
    if table not in allowed: raise HTTPException(status_code=404,detail="unknown export")
    if allowed[table] == "bco_exit_challenger_shadow":
        ensure_bco_exit_challenger_shadow_schema()
    with get_conn() as conn: rows=fetchall_dict(conn.execute(f"SELECT * FROM {allowed[table]} ORDER BY id ASC"))
    return csv_response(rows,f"bco-{table}.csv")


@app.get("/export/all.zip")
def export_all_zip():
    allowed={
        "raw-signals":"raw_signals",
        "trades":"trades",
        "basket-decisions":"basket_decisions",
        "protection-stages":"protection_stages",
        "managed-stops":"managed_stop_events",
        "execution-audit":"execution_audit",
        "system-events":"system_events",
        "broker-action-queue":"broker_action_queue",
        "broker-transactions":"broker_transactions",
        "fixed-48-outcomes":"fixed_48_outcomes",
        "trade-manager-reviews":"trade_manager_reviews",
        "basket-snapshots":"basket_snapshots",
        "harvest-execution-outcomes":"harvest_execution_outcomes",
        "accounting-snapshots":"accounting_snapshots",
        "exit-challenger-shadow":"bco_exit_challenger_shadow",
    }
    ensure_bco_exit_challenger_shadow_schema()
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
        z.writestr("manifest.json",json.dumps({
            "app":APP_NAME,
            "version":APP_VERSION,
            "policy":POLICY_VERSION,
            "asset":"BCOUSD",
            "direction":"long",
            "requested_risk_gbp":BCO_RISK_PER_TRADE_GBP,
            "sl_pct":BCO_SL_PCT,
            "signal_recovery_enabled":BCO_SIGNAL_RECOVERY_ENABLED,
            "exit_challenger_shadow":{
                "enabled":BCO_EXIT_SHADOW_ENABLED,
                "version":BCO_EXIT_SHADOW_VERSION,
                "research_only":True,
                "execution_authority":False,
                "forward_only_no_backfill":True,
                "mfe_giveback_fraction":BCO_EXIT_SHADOW_MFE_GIVEBACK_FRACTION,
                "atr_multiplier":BCO_EXIT_SHADOW_ATR_MULTIPLIER,
                "min_hold_hours":BCO_EXIT_SHADOW_MIN_HOLD_HOURS,
            },
            "analysis_tables":sorted(list(allowed.keys())),
            "generated_at_utc":now_utc_iso(),
        },indent=2))
    return Response(content=buf.getvalue(),media_type="application/zip",headers={"Content-Disposition":'attachment; filename="bco-live-analysis.zip"'})


@app.get("/dashboard-full", response_class=HTMLResponse)
def dashboard_full():
    s=snapshot(); b=s.get("basket") or {}; safety=s.get("broker_safety") or {}; acct=s.get("account") or {}; latest=s.get("latest_signal") or {}
    open_rows=s.get("open_trades") or []
    rows="".join(f"<tr><td>{esc(t.get('trade_id'))}</td><td>{esc(t.get('entry_time'))}</td><td>{safe_float(t.get('entry_price')) or 0:.3f}</td><td>{int(safe_float(t.get('hold_candles')) or 0)}</td><td>{safe_float(t.get('current_R')) or 0:.2f}R</td><td>{esc(t.get('decision_48'))}</td><td>{esc(t.get('decision_72'))}</td><td>{esc(t.get('managed_stop_stage'))}</td><td>{esc(t.get('broker_trade_id'))}</td></tr>" for t in open_rows)
    if not rows: rows="<tr><td colspan='9'>No open BCO trades.</td></tr>"
    allowed="YES" if safety.get("orders_allowed") else "NO — LOCKED"
    return f"""<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='60'><title>{esc(APP_NAME)}</title><style>
    body{{background:#0b1220;color:#e5e7eb;font-family:Arial,sans-serif;margin:22px}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}} .card{{background:#111827;border:1px solid #243044;border-radius:10px;padding:14px}} .label{{color:#94a3b8;font-size:12px;text-transform:uppercase}} .value{{font-size:24px;font-weight:700;margin-top:5px}} .ok{{color:#86efac}} .bad{{color:#fca5a5}} table{{width:100%;border-collapse:collapse;background:#111827;margin-top:12px}} th,td{{padding:8px;border-bottom:1px solid #243044;text-align:left;font-size:13px}} a{{color:#7dd3fc}} code{{color:#fde68a}} .note{{background:#172033;padding:10px;border-radius:8px;margin:12px 0}}.research-inner{{margin:7px 10px}}.research-inner>summary{{background:#11161d;border-left-color:#315f39}}.research-inner-body{{padding:0}}</style></head><body>
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


# ============================================================
# v0.3.0 FOCUSED BCO RESEARCH — master v10.1.35 parity
# Research-only. Never consumed by execution/management.
# ============================================================
BCO_FOCUSED_THRESHOLDS=[40,60,75,100,150,200,300,400,500,600]
BCO_FOCUSED_HORIZONS=[6,12,24,48]
BCO_FOCUSED_EFFICIENCY_LOOKBACKS=[8,12,24]

def ensure_bco_focused_research_tables():
    with get_conn() as conn:
        idt="BIGSERIAL PRIMARY KEY" if conn.postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
        conn.execute(f"""CREATE TABLE IF NOT EXISTS bco_focused_efficiency (
            id {idt}, created_at_utc TEXT NOT NULL, raw_signal_id BIGINT, signal_time TEXT, lookback_candles BIGINT,
            candles_found BIGINT, net_move_pct DOUBLE PRECISION, path_travelled_pct DOUBLE PRECISION,
            efficiency DOUBLE PRECISION, state TEXT, UNIQUE(raw_signal_id,lookback_candles))""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS bco_focused_alignment (
            id {idt}, created_at_utc TEXT NOT NULL, raw_signal_id BIGINT UNIQUE, signal_time TEXT, state TEXT,
            return_4h DOUBLE PRECISION, return_8h DOUBLE PRECISION, return_24h DOUBLE PRECISION, candidate BIGINT)""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS bco_focused_recovery (
            id {idt}, created_at_utc TEXT NOT NULL, raw_signal_id BIGINT UNIQUE, cycle_id TEXT, trigger_signal_time TEXT,
            trigger_status TEXT, trigger_action TEXT, trigger_open_count BIGINT, trigger_r DOUBLE PRECISION,
            trigger_hwm_r DOUBLE PRECISION, trigger_giveback_pct DOUBLE PRECISION,
            outcome_6_r DOUBLE PRECISION,outcome_12_r DOUBLE PRECISION,outcome_24_r DOUBLE PRECISION,outcome_48_r DOUBLE PRECISION,
            completed_48 BIGINT DEFAULT 0,updated_at_utc TEXT)""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS bco_focused_highwater (
            id {idt}, created_at_utc TEXT NOT NULL, cycle_id TEXT, threshold_r DOUBLE PRECISION,
            trigger_raw_signal_id BIGINT, trigger_signal_time TEXT, trigger_r DOUBLE PRECISION,
            trigger_hwm_r DOUBLE PRECISION,trigger_giveback_pct DOUBLE PRECISION,trigger_banked_r DOUBLE PRECISION,
            outcome_6_r DOUBLE PRECISION,outcome_12_r DOUBLE PRECISION,outcome_24_r DOUBLE PRECISION,outcome_48_r DOUBLE PRECISION,
            completed_48 BIGINT DEFAULT 0,updated_at_utc TEXT,UNIQUE(cycle_id,threshold_r))""")

def _bf_return(conn,raw_id,lookback):
    rows=conn.execute("SELECT exec_close FROM raw_signals WHERE exec_close IS NOT NULL AND id<=? ORDER BY id DESC LIMIT ?",(int(raw_id),int(lookback)+1)).fetchall()
    closes=[safe_float(r["exec_close"]) for r in reversed(rows)];closes=[float(x) for x in closes if x is not None]
    if len(closes)<2 or not closes[0]:return None
    return (closes[-1]/closes[0]-1)*100

def _bf_state():
    with get_conn() as conn:row=conn.execute("SELECT * FROM basket_state WHERE singleton_key='BCO_LONG'").fetchone()
    d=dict(row) if row else {}
    return {"cycle_id":safe_str(d.get("cycle_id")),"open_count":int(safe_float(d.get("open_count")) or 0),
            "r":safe_float(d.get("basket_R")) or 0.0,"hwm":safe_float(d.get("high_water_R")) or 0.0,
            "giveback":safe_float(d.get("giveback_pct")) or 0.0,"status":safe_str(d.get("tide_status") or d.get("status") or "FLAT").upper(),
            "action":safe_str(d.get("manager_action") or ""),"banked_r":safe_float(d.get("banked_R_cycle")) or 0.0}

def _bf_elapsed(conn,raw_id):
    row=conn.execute("SELECT COUNT(DISTINCT timestamp_readable) AS c FROM raw_signals WHERE id>? AND timestamp_readable IS NOT NULL AND timestamp_readable!=''",(int(raw_id),)).fetchone()
    return int(row["c"] if row else 0)

def _bf_eff_state(x):
    x=safe_float(x)
    if x is None:return "INSUFFICIENT_DATA"
    if x<0.25:return "CHOPPY"
    if x<0.40:return "MIXED"
    if x<0.60:return "TRENDING"
    return "CLEAN_TREND"

def record_bco_focused_research(raw_signal_id):
    try:
        ensure_bco_focused_research_tables();raw_signal_id=int(raw_signal_id or 0)
        with get_conn() as conn:
            raw=conn.execute("SELECT * FROM raw_signals WHERE id=? LIMIT 1",(raw_signal_id,)).fetchone()
            if not raw:return {"ok":False,"research_only":True,"reason":"raw_signal_not_found"}
            d=dict(raw);sig=safe_str(d.get("timestamp_readable"))
            for lb in BCO_FOCUSED_EFFICIENCY_LOOKBACKS:
                rows=conn.execute("SELECT exec_close FROM raw_signals WHERE exec_close IS NOT NULL AND id<=? ORDER BY id DESC LIMIT ?",(raw_signal_id,lb+1)).fetchall()
                closes=[safe_float(r["exec_close"]) for r in reversed(rows)];closes=[float(x) for x in closes if x is not None]
                eff=net=path=None
                if len(closes)>=2 and closes[0]:
                    net_abs=abs(closes[-1]-closes[0]);path_abs=sum(abs(b-a) for a,b in zip(closes[:-1],closes[1:]))
                    net=abs((closes[-1]/closes[0]-1)*100);path=sum(abs((b/a-1)*100) for a,b in zip(closes[:-1],closes[1:]) if a)
                    eff=net_abs/path_abs if path_abs else None
                conn.execute("""INSERT INTO bco_focused_efficiency
                    (created_at_utc,raw_signal_id,signal_time,lookback_candles,candles_found,net_move_pct,path_travelled_pct,efficiency,state)
                    VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(raw_signal_id,lookback_candles) DO NOTHING""",
                    (now_utc_iso(),raw_signal_id,sig,lb,len(closes),net,path,eff,_bf_eff_state(eff)))
            r4=_bf_return(conn,raw_signal_id,4);r8=_bf_return(conn,raw_signal_id,8);r24=_bf_return(conn,raw_signal_id,24)
            vals=[x for x in (r4,r8,r24) if x is not None]
            al="INSUFFICIENT_DATA" if len(vals)<2 else ("ALIGNED_UP" if all(x>0 for x in vals) else "ALIGNED_DOWN" if all(x<0 for x in vals) else "DIVERGENT_TIMEFRAMES")
            conn.execute("""INSERT INTO bco_focused_alignment
                (created_at_utc,raw_signal_id,signal_time,state,return_4h,return_8h,return_24h,candidate)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(raw_signal_id) DO NOTHING""",
                (now_utc_iso(),raw_signal_id,sig,al,r4,r8,r24,1 if parse_bool(d.get("candidate_8h"),False) else 0))
            st=_bf_state()
            for table in ("bco_focused_recovery","bco_focused_highwater"):
                for pr in conn.execute(f"SELECT * FROM {table} WHERE COALESCE(completed_48,0)=0 ORDER BY id ASC LIMIT 500").fetchall():
                    pd=dict(pr);tid=int(pd.get("raw_signal_id") or pd.get("trigger_raw_signal_id") or 0);elapsed=_bf_elapsed(conn,tid);sets=[];vals2=[]
                    for h in BCO_FOCUSED_HORIZONS:
                        col=f"outcome_{h}_r"
                        if elapsed>=h and safe_float(pd.get(col)) is None:
                            sets.append(f"{col}=?");vals2.append(st["r"])
                            if h==48:sets.append("completed_48=?");vals2.append(1)
                    if sets:
                        sets.append("updated_at_utc=?");vals2.extend([now_utc_iso(),int(pd["id"])])
                        conn.execute(f"UPDATE {table} SET {', '.join(sets)} WHERE id=?",tuple(vals2))
            warn=st["open_count"]>0 and (st["status"] in {"AMBER","RED","CRITICAL"} or st["giveback"]>=40 or st["r"]<0 or any(k in st["action"].upper() for k in ("PAUSE","CLOSE","REDUCE","DEFENCE","DEFENSE")))
            if warn:
                conn.execute("""INSERT INTO bco_focused_recovery
                    (created_at_utc,raw_signal_id,cycle_id,trigger_signal_time,trigger_status,trigger_action,trigger_open_count,trigger_r,trigger_hwm_r,trigger_giveback_pct,updated_at_utc)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(raw_signal_id) DO NOTHING""",
                    (now_utc_iso(),raw_signal_id,st["cycle_id"],sig,st["status"],st["action"],st["open_count"],st["r"],st["hwm"],st["giveback"],now_utc_iso()))
            if st["cycle_id"] and st["open_count"]>0:
                for th in BCO_FOCUSED_THRESHOLDS:
                    if st["hwm"]>=th:
                        conn.execute("""INSERT INTO bco_focused_highwater
                            (created_at_utc,cycle_id,threshold_r,trigger_raw_signal_id,trigger_signal_time,trigger_r,trigger_hwm_r,trigger_giveback_pct,trigger_banked_r,updated_at_utc)
                            VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(cycle_id,threshold_r) DO NOTHING""",
                            (now_utc_iso(),st["cycle_id"],th,raw_signal_id,sig,st["r"],st["hwm"],st["giveback"],st["banked_r"],now_utc_iso()))
        return {"ok":True,"research_only":True}
    except Exception as exc:return {"ok":False,"research_only":True,"error":f"{type(exc).__name__}: {exc}"}

def _bf_rows(table,limit=5000):
    ensure_bco_focused_research_tables()
    with get_conn() as conn:return [dict(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?",(max(1,min(int(limit),100000)),)).fetchall()]

def _bf_table(title,rows,cols):
    body="".join("<tr>"+"".join(f"<td>{esc(r.get(c))}</td>" for c in cols)+"</tr>" for r in rows[:50]) or f'<tr><td colspan="{len(cols)}">No research rows yet.</td></tr>'
    head="".join(f"<th>{esc(c.replace('_',' ').title())}</th>" for c in cols)
    return f'<details class="research-inner"><summary>{esc(title)}</summary><div class="research-inner-body"><div class="table-scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></div></details>'

def build_bco_focused_research_html():
    return '<div class="section-note small"><strong>Focused BCO research.</strong> Same evidence themes as the Indices master plus forward exit challengers and the event-driven AI Regime Observer. All research layers have zero execution authority.</div>' + \
      '<details class="research-inner"><summary>MFE + ATR2 Exit Challenger — Forward Shadow</summary><div class="research-inner-body">' + build_bco_exit_challenger_shadow_html() + '</div></details>' + \
      '<details class="research-inner"><summary>AI Regime Observer — Event-Driven Point-in-Time Labels</summary><div class="research-inner-body">' + build_ai_regime_observer_html() + '</div></details>' + \
      _bf_table("Live High-Water / Banking Outcomes",_bf_rows("bco_focused_highwater",100),["threshold_r","trigger_signal_time","trigger_r","trigger_hwm_r","trigger_banked_r","outcome_6_r","outcome_12_r","outcome_24_r","outcome_48_r"]) + \
      _bf_table("BCO Multi-Horizon Alignment / Divergence",_bf_rows("bco_focused_alignment",100),["signal_time","state","return_4h","return_8h","return_24h","candidate"]) + \
      _bf_table("Trend Efficiency / Chop Research",_bf_rows("bco_focused_efficiency",150),["signal_time","lookback_candles","efficiency","state","net_move_pct","path_travelled_pct"]) + \
      _bf_table("Basket Recovery / Red-State Outcomes",_bf_rows("bco_focused_recovery",100),["trigger_signal_time","trigger_status","trigger_action","trigger_open_count","trigger_r","trigger_hwm_r","trigger_giveback_pct","outcome_6_r","outcome_12_r","outcome_24_r","outcome_48_r"]) + \
      '<details class="research-inner"><summary>Strategy Model Evidence</summary><div class="section-note small"><a href="/export/raw-signals.csv">Raw signals CSV</a> · <a href="/export/trades.csv">Trades CSV</a> · <a href="/export/basket-decisions.csv">Basket decisions CSV</a> · <a href="/export/protection-stages.csv">Protection stages CSV</a></div></details>'

@app.get("/export/bco-focused-research.zip")
def export_bco_focused_research_zip(limit:int=25000):
    ensure_bco_focused_research_tables();limit=max(1,min(int(limit),100000));buf=io.BytesIO()
    tables={"highwater-banking-research.csv":"bco_focused_highwater","alignment-research.csv":"bco_focused_alignment","trend-efficiency-research.csv":"bco_focused_efficiency","basket-recovery-research.csv":"bco_focused_recovery"}
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for fn,tbl in tables.items():
            rows=_bf_rows(tbl,limit);out=io.StringIO()
            if rows:
                fields=[]
                for r in rows:
                    for k in r:
                        if k not in fields:fields.append(k)
                w=csv.DictWriter(out,fieldnames=fields);w.writeheader();w.writerows(rows)
            z.writestr(fn,out.getvalue())
        ensure_ai_regime_observer_table()
        with get_conn() as _aic:
            _airows=[dict(r) for r in _aic.execute("SELECT * FROM ai_regime_observer ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]
        _aio=io.StringIO()
        if _airows:
            _fields=[]
            for _r in _airows:
                for _k in _r:
                    if _k not in _fields:_fields.append(_k)
            _w=csv.DictWriter(_aio,fieldnames=_fields,extrasaction="ignore");_w.writeheader();_w.writerows(_airows)
        z.writestr("ai-regime-observer.csv",_aio.getvalue())

        ensure_bco_exit_challenger_shadow_schema()
        with get_conn() as _esc:
            _esrows=fetchall_dict(_esc.execute(
                "SELECT * FROM bco_exit_challenger_shadow ORDER BY id DESC LIMIT ?",
                (limit,)
            ))
        _eso=io.StringIO()
        if _esrows:
            _esfields=[]
            for _r in _esrows:
                for _k in _r:
                    if _k not in _esfields:_esfields.append(_k)
            _esw=csv.DictWriter(_eso,fieldnames=_esfields,extrasaction="ignore")
            _esw.writeheader();_esw.writerows(_esrows)
        z.writestr("bco-exit-challenger-shadow.csv",_eso.getvalue())

        z.writestr("manifest.json",json.dumps({
            "project":"BCO",
            "research_only":True,
            "generated_at_utc":now_utc_iso(),
            "streams":list(tables)+["ai-regime-observer.csv","bco-exit-challenger-shadow.csv"],
            "exit_challenger_shadow":{
                "version":BCO_EXIT_SHADOW_VERSION,
                "forward_only_no_backfill":True,
                "execution_authority":False,
                "challengers":["MFE_GIVEBACK_50","ATR2_CHANDELIER"],
                "min_hold_hours":BCO_EXIT_SHADOW_MIN_HOLD_HOURS,
                "mfe_giveback_fraction":BCO_EXIT_SHADOW_MFE_GIVEBACK_FRACTION,
                "atr_multiplier":BCO_EXIT_SHADOW_ATR_MULTIPLIER,
            },
        },indent=2))
    return Response(content=buf.getvalue(),media_type="application/zip",headers={"Content-Disposition":'attachment; filename="bco-focused-research.zip"'})



def _bco_export_table_csv(table: str, limit: int = 50000) -> Response:
    limit = max(1, min(int(limit), 100000))
    with get_conn() as conn:
        rows = fetchall_dict(conn.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,)))
    out = io.StringIO()
    if rows:
        fields = []
        for row in rows:
            for key in row.keys():
                if key not in fields:
                    fields.append(key)
        writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    else:
        out.write("note\nNo rows yet\n")
    return Response(content=out.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{table}.csv"'})


@app.get("/export/broker-action-queue.csv")
def export_broker_action_queue(limit: int = 50000):
    return _bco_export_table_csv("broker_action_queue", limit)


@app.get("/export/broker-transactions.csv")
def export_broker_transactions(limit: int = 50000):
    return _bco_export_table_csv("broker_transactions", limit)


@app.get("/export/fixed-48-outcomes.csv")
def export_fixed_48_outcomes(limit: int = 50000):
    return _bco_export_table_csv("fixed_48_outcomes", limit)


@app.get("/export/trade-manager-reviews.csv")
def export_trade_manager_reviews(limit: int = 50000):
    return _bco_export_table_csv("trade_manager_reviews", limit)


@app.get("/export/basket-snapshots.csv")
def export_basket_snapshots(limit: int = 50000):
    return _bco_export_table_csv("basket_snapshots", limit)


@app.get("/export/harvest-execution-outcomes.csv")
def export_harvest_execution_outcomes(limit: int = 50000):
    return _bco_export_table_csv("harvest_execution_outcomes", limit)


@app.get("/export/accounting-snapshots.csv")
def export_accounting_snapshots(limit: int = 50000):
    return _bco_export_table_csv("accounting_snapshots", limit)


# ============================================================
# BCO v0.2.0 — PROJECT EXIT PLAN STANDARD DASHBOARD
# ============================================================

_BCO_STD_TOP_CACHE = {"payload": None, "expires_at": 0.0}
_BCO_STD_TOP_LOCK = threading.Lock()
BCO_STANDARD_TOP_CACHE_SECONDS = max(5.0, min(float(os.getenv("BCO_STANDARD_TOP_CACHE_SECONDS", "20")), 120.0))

def _money(v):
    n = safe_float(v)
    if n is None:
        return "n/a"
    return ("-" if n < 0 else "") + f"£{abs(n):,.2f}"

def _pnl_class(v):
    n = safe_float(v)
    if n is None or abs(n) < 1e-12:
        return ""
    return "pos" if n > 0 else "neg"

def _bco_standard_top_uncached():
    s=snapshot();basket=s.get("basket") or {};lm=s.get("live_local_basket") or {}
    acct=s.get("account") or {};broker=s.get("broker_live") or {};safety=s.get("broker_safety") or {}
    latest=s.get("latest_signal") or {};rows=s.get("open_trades") or [];closed=s.get("closed_summary") or {}
    local_open=len(rows);broker_open=int(broker.get("owned_open_count") or 0)
    mature=sum(1 for r in rows if int(safe_float(r.get("hold_candles")) or 0)>=BCO_MIN_HOLD_HOURS)
    oldest=max([int(safe_float(r.get("hold_candles")) or 0) for r in rows] or [0])
    basket_r=safe_float(lm.get("basket_R")) or 0.0
    model_open=safe_float(lm.get("basket_pnl_gbp")) or 0.0
    broker_open_pnl=safe_float(broker.get("owned_unrealized_pl")) or 0.0
    hwm=safe_float(basket.get("high_water_R")) or 0.0
    hwm_time=safe_str(basket.get("high_water_seen_at"))

    # v0.7.7: current basket HWM/giveback belongs to the ACTIVE basket only.
    # Once both OANDA and local BCO exposure are flat, show zero immediately;
    # completed-cycle HWM remains preserved in basket_snapshots/research.
    authoritative_flat = (local_open == 0 and broker_open == 0)
    if authoritative_flat:
        hwm = 0.0
        hwm_time = ""

    hwm_gbp=None
    if hwm > 0:
        current_cycle=safe_str(basket.get("cycle_id"))
        with get_conn() as _hwm_conn:
            _hrow=fetchone_dict(_hwm_conn.execute("""
                SELECT basket_pnl_gbp,signal_time,created_at_utc
                FROM basket_snapshots
                WHERE cycle_id=?
                  AND high_water_R>=?
                  AND basket_R>=?
                ORDER BY id ASC LIMIT 1
            """,(current_cycle,hwm-0.000001,hwm-0.000001))) or {}
        hwm_gbp=safe_float(_hrow.get("basket_pnl_gbp"))
        if not hwm_time:
            hwm_time=safe_str(_hrow.get("signal_time") or _hrow.get("created_at_utc"))
    if hwm_gbp is None:
        hwm_gbp=hwm*float(BCO_RISK_PER_TRADE_GBP)
    give=((hwm-basket_r)/hwm*100.0) if hwm>0 and basket_r<hwm else 0.0
    current_open_basket_gbp=safe_float(lm.get("basket_pnl_gbp")) or 0.0
    realized_pnl=safe_float(closed.get("p")) or 0.0
    realized_r=safe_float(closed.get("r")) or 0.0

    # Match Live Indices:
    # cash giveback = cash high-water minus CURRENT UNREALISED BROKER P&L.
    # Realised historical P&L remains accounting context only.
    current_giveback_basis_gbp=broker_open_pnl

    # R giveback = open-basket R high-water minus current open-basket R.
    current_giveback_basis_r=basket_r

    giveback_r=max(0.0,hwm-current_giveback_basis_r)
    giveback_gbp=max(0.0,float(hwm_gbp or 0.0)-current_giveback_basis_gbp)
    giveback_cash_pct=(
        (giveback_gbp/float(hwm_gbp)*100.0)
        if hwm_gbp is not None and float(hwm_gbp)>0
        else 0.0
    )
    now=datetime.now(timezone.utc);ws=(now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0);ms=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    with get_conn() as conn:
        wk=conn.execute("SELECT COALESCE(SUM(realized_pnl_gbp),0) AS p FROM trades WHERE exit_time>=?",(ws.isoformat(),)).fetchone()
        mo=conn.execute("SELECT COALESCE(SUM(realized_pnl_gbp),0) AS p FROM trades WHERE exit_time>=?",(ms.isoformat(),)).fetchone()
        _raw_latest=fetchone_dict(conn.execute(
            "SELECT id FROM raw_signals ORDER BY id DESC LIMIT 1"
        )) or {}
        _dec_latest=fetchone_dict(conn.execute(
            "SELECT raw_signal_id FROM basket_decisions ORDER BY id DESC LIMIT 1"
        )) or {}
        _latest_raw_id=int(safe_float(_raw_latest.get("id")) or 0)
        _latest_decision_id=int(safe_float(_dec_latest.get("raw_signal_id")) or 0)

        # v0.7.9: live Signal Health is concerned only with FRESH,
        # recoverable processing gaps. Old historical orphan rows are retained
        # as audit gaps but cannot keep the live dashboard in RECOVERING or be
        # replayed into current trading.
        _health_cutoff=(
            datetime.now(timezone.utc)
            - timedelta(seconds=BCO_FRESH_SIGNAL_MAX_AGE_SECONDS)
        ).isoformat()

        _pending_rows=fetchall_dict(conn.execute("""
            SELECT r.id, r.candidate_8h
            FROM raw_signals r
            LEFT JOIN basket_decisions d ON d.raw_signal_id=r.id
            WHERE d.id IS NULL
              AND r.received_at_utc>=?
            ORDER BY r.id ASC
        """, (_health_cutoff,)))
        _pending_count=len(_pending_rows)
        _pending_candidate_count=sum(
            1 for _pending_row in _pending_rows
            if parse_bool(_pending_row.get("candidate_8h"), False)
        )

        _legacy_pending_row=fetchone_dict(conn.execute("""
            SELECT COUNT(*) AS c
            FROM raw_signals r
            LEFT JOIN basket_decisions d ON d.raw_signal_id=r.id
            WHERE d.id IS NULL
              AND r.received_at_utc<?
        """, (_health_cutoff,))) or {}
        _legacy_pending_count=int(
            safe_float(_legacy_pending_row.get("c")) or 0
        )

        _latest_processed=True
        if _latest_raw_id>0:
            _latest_done=fetchone_dict(conn.execute(
                "SELECT id FROM basket_decisions WHERE raw_signal_id=? LIMIT 1",
                (_latest_raw_id,),
            ))
            _latest_processed=bool(_latest_done)

        _processor_ok=bool(_latest_processed and _pending_count==0)
    return {
      "status":"ok","project":"BCO","mode":safe_str(OANDA_ENV).upper(),"time_utc":now_utc_iso(),
      "account":{"nav":safe_float(acct.get("NAV")),"balance":safe_float(acct.get("balance")),
                 "margin_available":safe_float(acct.get("marginAvailable")),"currency":safe_str(acct.get("currency"))},
      "accounting":{"week_pnl":safe_float(wk["p"] if wk else 0) or 0.0,"week_label":"Realised this week",
                    "month_pnl":safe_float(mo["p"] if mo else 0) or 0.0,"month_label":"Realised this month"},
      "strategy":{"open_pnl":broker_open_pnl,"headline_pnl":broker_open_pnl,
                  "model_open_pnl":model_open,"realized_pnl":realized_pnl,
                  "realized_r":realized_r,"total_pnl":broker_open_pnl+realized_pnl,
                  "open_trades":broker_open,"local_open_trades":local_open,
                  "mature_48h_plus":mature,"oldest_hold":oldest,"basket_r":basket_r,
                  "high_water_r":hwm,"high_water_gbp":hwm_gbp,"high_water_time":hwm_time,"giveback_basis_r":current_giveback_basis_r,
                  "giveback_basis_gbp":current_giveback_basis_gbp,
                  "giveback_r":giveback_r,"giveback_gbp":giveback_gbp,
                  "giveback_pct":giveback_cash_pct,
                  "giveback_r_pct":((giveback_r/hwm*100.0) if hwm>0 else 0.0),
                  "basket_phase":safe_str(basket.get("basket_phase") or "FLAT"),
                  "tide_status":safe_str(basket.get("tide_status") or "FLAT"),
                  "manager_action":safe_str(basket.get("manager_action") or "NO_OPEN_BASKET"),
                  "orders_allowed":bool(safety.get("orders_allowed")),
                  "auto_entry":bool(safety.get("auto_entry")),"auto_management":bool(safety.get("auto_management")),
                  "banked_r_cycle":safe_float(basket.get("banked_R_cycle")) or 0.0,
                  "broker_margin_used":safe_float(broker.get("owned_margin_used")) or 0.0,
                  "broker_account_open_count":int(broker.get("account_open_count") or 0)},
      "signals":{"candidate":parse_bool(latest.get("candidate_8h"),False),
                 "latest_time":safe_str(latest.get("timestamp_readable")),
                 "latest_time_display":bco_display_candle_time(latest.get("timestamp_readable")),
                 "latest_price":safe_float(latest.get("exec_close")),
                 "latest_signal_id":safe_str(latest.get("signal_id")),
                 "received_assets":1 if safe_str(latest.get("signal_id")) else 0,"expected_assets":1,
                 "processor_ok":_processor_ok,
                 "latest_raw_processed":_latest_processed,
                 "latest_raw_id":_latest_raw_id,
                 "latest_decision_raw_id":_latest_decision_id,
                 "processing_lag":_pending_count,
                 "pending_candidate_count":_pending_candidate_count,
                 "legacy_unprocessed_count":_legacy_pending_count,
                 "fresh_signal_max_age_seconds":BCO_FRESH_SIGNAL_MAX_AGE_SECONDS,
                 "processing_health_source":"fresh unprocessed raw_signals missing basket_decisions",
                 "recovery_interval_seconds":BCO_SIGNAL_RECOVERY_INTERVAL_SECONDS},
      "config":{"risk_per_trade_gbp":BCO_RISK_PER_TRADE_GBP,"sl_pct":BCO_SL_PCT,
                "min_hold_hours":BCO_MIN_HOLD_HOURS,"instrument":BCO_OANDA_INSTRUMENT,"direction":BCO_DIRECTION}}

def bco_standard_top_snapshot(force=False):
    now_ts = time.time()
    with _BCO_STD_TOP_LOCK:
        if (not force and _BCO_STD_TOP_CACHE.get("payload") is not None and float(_BCO_STD_TOP_CACHE.get("expires_at") or 0.0) > now_ts):
            out = dict(_BCO_STD_TOP_CACHE["payload"])
            out["cached"] = True
            return out
    out = _bco_standard_top_uncached()
    with _BCO_STD_TOP_LOCK:
        _BCO_STD_TOP_CACHE["payload"] = dict(out)
        _BCO_STD_TOP_CACHE["expires_at"] = now_ts + BCO_STANDARD_TOP_CACHE_SECONDS
    out["cached"] = False
    return out

@app.get("/dashboard/top")
def bco_standard_top_route(force: bool = False):
    return bco_standard_top_snapshot(force=force)


def _bco_standard_signal_html():
    s = snapshot()
    latest = s.get("latest_signal") or {}
    with get_conn() as conn:
        recent = fetchall_dict(conn.execute(
            """SELECT id,timestamp_readable,signal_id,candidate_8h,signal_side,
                      exec_close,model_name
               FROM raw_signals ORDER BY id DESC LIMIT 25"""
        ))
    rows = "".join(
        f'<tr><td>{esc(r.get("timestamp_readable"))}</td>'
        f'<td>{esc(r.get("signal_id"))}</td>'
        f'<td>{"TRUE" if parse_bool(r.get("candidate_8h")) else "FALSE"}</td>'
        f'<td>{esc(r.get("signal_side") or "-")}</td>'
        f'<td>{esc(r.get("exec_close"))}</td>'
        f'<td>{esc(r.get("model_name") or "-")}</td></tr>'
        for r in recent
    )
    if not rows:
        rows = "<tr><td colspan='6'>Waiting for BCO signals.</td></tr>"
    return f"""
      <div class="metric-grid">
        <div class="mini-card"><div class="k">Latest Candidate</div>
          <div class="v {'pos' if parse_bool(latest.get('candidate_8h')) else 'neg'}">
            {'CANDIDATE' if parse_bool(latest.get('candidate_8h')) else 'NO TRADE'}
          </div></div>
        <div class="mini-card"><div class="k">Latest Signal</div>
          <div class="v small">{esc(latest.get('timestamp_readable') or '-')}</div></div>
        <div class="mini-card"><div class="k">Latest Price</div>
          <div class="v">{safe_float(latest.get('exec_close')) or 0:.3f}</div></div>
        <div class="mini-card"><div class="k">Direction</div>
          <div class="v">{esc(BCO_DIRECTION.upper())}</div></div>
      </div>
      <div class="table-scroll"><table>
        <thead><tr><th>Time</th><th>Signal ID</th><th>Candidate</th>
        <th>Side</th><th>Price</th><th>Model</th></tr></thead>
        <tbody>{rows}</tbody>
      </table></div>
    """


def _bco_standard_open_trades_html():
    s=snapshot()
    local=s.get("open_trades") or []
    live=s.get("broker_live") or {}
    broker_map={safe_str(t.get("id")):t for t in (live.get("owned_open_trades") or [])}
    local_bids={safe_str(t.get("broker_trade_id")) for t in local if safe_str(t.get("broker_trade_id"))}
    rows=""
    linked=0
    local_only=0

    for t in local:
        bid=safe_str(t.get("broker_trade_id"))
        bt=broker_map.get(bid) if bid else None
        if bt:
            linked+=1
        else:
            local_only+=1
        rr=safe_float(t.get("current_R")) or 0.0
        eff=(
            safe_float(t.get("effective_risk_gbp"))
            or safe_float(t.get("requested_risk_gbp"))
            or BCO_RISK_PER_TRADE_GBP
        )
        model_pnl=rr*eff
        upl=safe_float((bt or {}).get("unrealizedPL"))
        rows+=f"""<tr>
          <td>{esc(t.get('trade_id'))}</td>
          <td>{esc(bid or 'LOCAL ONLY')}</td>
          <td>{esc(t.get('entry_time'))}</td>
          <td>{safe_float(t.get('entry_price')) or 0:.3f}</td>
          <td>{safe_float(t.get('current_price')) or 0:.3f}</td>
          <td>{int(safe_float(t.get('hold_candles')) or 0)}h</td>
          <td>{esc(_bco_age_zone(t.get('hold_candles')))}</td>
          <td class="{_pnl_class(rr)}">{rr:.3f}R</td>
          <td class="{_pnl_class(model_pnl)}">{_money(model_pnl)}</td>
          <td class="{_pnl_class(upl)}">{_money(upl)}</td>
          <td>{safe_float(t.get('mfe_pct')) or 0:.3f}%</td>
          <td>{safe_float(t.get('mae_pct')) or 0:.3f}%</td>
          <td>{safe_float(t.get('hard_sl_price')) or 0:.3f}</td>
          <td>{esc(t.get('managed_stop_stage') or '-')}</td>
          <td>{_money(eff)}</td>
        </tr>"""

    broker_only=[
        t for t in (live.get("owned_open_trades") or [])
        if safe_str(t.get("id")) not in local_bids
    ]
    broker_only_rows="".join(
        f"""<tr>
          <td>{esc(t.get('id'))}</td>
          <td>{esc(t.get('instrument'))}</td>
          <td>{esc(t.get('currentUnits'))}</td>
          <td>{safe_float(t.get('price')) or 0:.3f}</td>
          <td class="{_pnl_class(t.get('unrealizedPL'))}">{_money(t.get('unrealizedPL'))}</td>
          <td>{_money(t.get('marginUsed'))}</td>
          <td>{esc(t.get('openTime'))}</td>
        </tr>"""
        for t in broker_only
    )

    return f"""
      <div class="metric-grid">
        <div class="mini-card"><div class="k">OANDA BCO Trades</div>
          <div class="v">{int(live.get('owned_open_count') or 0)}</div>
          <div class="small">Actual broker positions</div></div>
        <div class="mini-card"><div class="k">Linked Local Trades</div>
          <div class="v {'pos' if linked==int(live.get('owned_open_count') or 0) else 'warn'}">{linked}</div>
          <div class="small">Local ↔ broker IDs matched</div></div>
        <div class="mini-card"><div class="k">Local-Only OPEN</div>
          <div class="v {'neg' if local_only else 'pos'}">{local_only}</div>
          <div class="small">Should be zero in auto-entry mode</div></div>
        <div class="mini-card"><div class="k">BCO Broker UPL</div>
          <div class="v {_pnl_class(live.get('owned_unrealized_pl'))}">{_money(live.get('owned_unrealized_pl'))}</div>
          <div class="small">Fresh OANDA unrealised P&amp;L</div></div>
      </div>

      <h3>Open BCO Trades — Local Manager + OANDA</h3>
      <div class="table-scroll"><table>
        <thead><tr>
          <th>Local Trade</th><th>Broker ID</th><th>Entry Time</th><th>Entry</th>
          <th>Current</th><th>Age</th><th>Zone</th><th>Model R</th><th>Model £</th>
          <th>OANDA UPL</th><th>MFE</th><th>MAE</th><th>Hard SL</th>
          <th>Protection</th><th>Effective Risk</th>
        </tr></thead>
        <tbody>{rows or '<tr><td colspan="15">No local open BCO trades.</td></tr>'}</tbody>
      </table></div>

      {f'<h3>Broker-Only BCO Trades — Attention Required</h3><div class="table-scroll"><table><thead><tr><th>Broker ID</th><th>Instrument</th><th>Units</th><th>Entry</th><th>UPL</th><th>Margin</th><th>Open Time</th></tr></thead><tbody>{broker_only_rows}</tbody></table></div>' if broker_only_rows else ''}
    """


def _bco_standard_basket_manager_html():
    s = snapshot()
    b = s.get("basket") or {}
    rows = s.get("open_trades") or []
    trade_rows = ""
    for t in rows:
        trade_rows += f'''
        <tr>
          <td>{esc(t.get("trade_id"))}</td>
          <td>{esc(t.get("entry_time"))}</td>
          <td>{safe_float(t.get("entry_price")) or 0:.3f}</td>
          <td>{int(safe_float(t.get("hold_candles")) or 0)}</td>
          <td class="{_pnl_class(t.get("current_R"))}">{safe_float(t.get("current_R")) or 0:.2f}R</td>
          <td>{esc(t.get("decision_48") or "-")}</td>
          <td>{esc(t.get("decision_72") or "-")}</td>
          <td>{esc(t.get("managed_stop_stage") or "-")}</td>
          <td>{esc(t.get("broker_trade_id") or "-")}</td>
        </tr>'''
    if not trade_rows:
        trade_rows = "<tr><td colspan='9'>No open BCO trades.</td></tr>"
    return f'''
      <div class="section-note">Same Project Exit Plan manager contract: 48h minimum normal hold, hourly post-48h review, staged defence, and tighten-only protection.</div>
      <div class="metric-grid">
        <div class="mini-card"><div class="k">Basket Phase</div><div class="v">{esc(b.get("basket_phase") or "FLAT")}</div></div>
        <div class="mini-card"><div class="k">Tide</div><div class="v">{esc(b.get("tide_status") or "FLAT")}</div></div>
        <div class="mini-card"><div class="k">Manager Action</div><div class="v small">{esc(b.get("manager_action") or "NO_OPEN_BASKET")}</div></div>
        <div class="mini-card"><div class="k">Basket</div><div class="v {_pnl_class(b.get("basket_R"))}">{safe_float(b.get("basket_R")) or 0:.2f}R</div><div class="small">{_money(b.get("basket_pnl_gbp"))}</div></div>
      </div>
      <div class="table-scroll"><table><thead><tr><th>Trade</th><th>Entry</th><th>Price</th><th>Age</th><th>R</th><th>48h</th><th>72h</th><th>Protection</th><th>Broker ID</th></tr></thead><tbody>{trade_rows}</tbody></table></div>
    '''


def _bco_latest_30_signals_html(limit: int = 30):
    limit=max(1,min(int(limit or 30),100))
    with get_conn() as conn:
        rows=fetchall_dict(conn.execute("SELECT id,received_at_utc,pair,signal_id,timestamp_readable,exec_close,forward_test_candidate,candidate_8h,signal_side,model_name,raw_json FROM raw_signals ORDER BY id DESC LIMIT ?",(limit,)))
        decisions=fetchall_dict(conn.execute("SELECT raw_signal_id,entry_allowed,entry_created,manager_action,note FROM basket_decisions WHERE raw_signal_id IS NOT NULL ORDER BY id DESC LIMIT ?",(limit*3,)))
    by_raw={int(safe_float(x.get('raw_signal_id')) or 0):x for x in decisions}; body=[]
    for r in rows:
        rid=int(safe_float(r.get('id')) or 0); d=by_raw.get(rid,{})
        cand=parse_bool(r.get('candidate_8h'),parse_bool(r.get('forward_test_candidate'),False))
        try:
            raw=json.loads(safe_str(r.get('raw_json')) or '{}'); raw=raw.get('payload',raw) if isinstance(raw,dict) else {}
        except Exception: raw={}
        ctx=context_8h(raw); trend=safe_str(ctx.get('ctx_trend_state') or '-'); atr=safe_float(ctx.get('ctx_atr_pct') or raw.get('ctx_atr_pct')); price=safe_float(r.get('exec_close'))
        reason=safe_str(d.get('note') or d.get('manager_action') or ('candidate' if cand else 'not candidate'))
        body.append(f'''<tr><td>{rid}</td><td>{esc(r.get('timestamp_readable'))}</td><td><strong>BCO</strong></td><td class="{'pos' if cand else 'neg'}">{'TRUE' if cand else 'FALSE'}</td><td>{esc(r.get('signal_side') or '-')}</td><td>{esc(trend)}</td><td>{'—' if atr is None else format(atr,'.3f')+'%'}</td><td>{'—' if price is None else format(price,'.3f')}</td><td>{'YES' if parse_bool(d.get('entry_allowed')) else 'NO'}</td><td>{'YES' if parse_bool(d.get('entry_created')) else 'NO'}</td><td>{esc(reason[:500])}</td><td>{esc(r.get('signal_id'))}</td><td>{esc(r.get('received_at_utc'))}</td></tr>''')
    if not body: return '<div class="section-note">No BCO signals stored yet.</div>'
    return f'''<div class="section-note small"><strong>Latest 30 BCO Signals.</strong> Candidate state plus deterministic basket decision.</div><div class="table-scroll"><table><thead><tr><th>ID</th><th>Candle</th><th>Asset</th><th>Candidate</th><th>Side</th><th>8H Regime</th><th>8H ATR</th><th>Price</th><th>Entry Allowed</th><th>Entry Created</th><th>Decision / Reason</th><th>Signal ID</th><th>Received UTC</th></tr></thead><tbody>{''.join(body)}</tbody></table></div>'''


def _bco_recently_closed_trades_html(limit: int = 30):
    limit=max(1,min(int(limit or 30),100))
    with get_conn() as conn:
        rows=fetchall_dict(conn.execute("SELECT * FROM trades WHERE UPPER(COALESCE(status,'')) IN ('CLOSED','BROKER_CLOSED') ORDER BY COALESCE(exit_time,updated_at_utc,created_at_utc) DESC,id DESC LIMIT ?",(limit,)))
    body=[]
    for r in rows:
        rr=safe_float(r.get('realized_R')); pnl=safe_float(r.get('realized_pnl_gbp'))
        body.append(f'''<tr><td>{esc(r.get('exit_time'))}</td><td>{esc(r.get('trade_id'))}</td><td>{esc(r.get('broker_trade_id') or '-')}</td><td>{esc(safe_str(r.get('direction')).upper())}</td><td>{esc(r.get('entry_time'))}</td><td>{int(safe_float(r.get('hold_candles')) or 0)}h</td><td>{esc(r.get('exit_reason') or '-')}</td><td class="{_pnl_class(rr)}">{_fmt_metric(rr,'R',2)}</td><td class="{_pnl_class(pnl)}">{_money(pnl)}</td><td>{_money(r.get('broker_realized_pl_home'))}</td><td>{_money(r.get('financing_home'))}</td><td>{_money(r.get('effective_risk_gbp'))}</td><td>{_fmt_metric(r.get('mfe_pct'),'%',2)}</td><td>{_fmt_metric(r.get('mae_pct'),'%',2)}</td><td>{esc(r.get('managed_stop_stage') or '-')}</td></tr>''')
    if not body: return '<div class="section-note">No closed BCO trades yet.</div>'
    return f'''<div class="section-note small"><strong>Recently Closed BCO Trades.</strong> Latest 30 closures with persisted close reason and broker/accounting result.</div><div class="table-scroll"><table><thead><tr><th>Exit</th><th>Trade</th><th>Broker ID</th><th>Side</th><th>Entry</th><th>Age</th><th>Why Closed</th><th>Realised R</th><th>Net P&amp;L</th><th>Broker P/L</th><th>Financing</th><th>Risk</th><th>MFE</th><th>MAE</th><th>Protection</th></tr></thead><tbody>{''.join(body)}</tbody></table></div>'''

def _bco_standard_profit_harvesting_html():
    s = snapshot()
    b = s.get("basket") or {}
    cycle = safe_str(b.get("cycle_id"))
    hwm = float(safe_float(b.get("high_water_R")) or 0.0)

    with get_conn() as conn:
        rows = fetchall_dict(conn.execute(
            "SELECT * FROM protection_stages WHERE cycle_id=? AND stage_type='BANK' ORDER BY threshold_R",
            (cycle,),
        )) if cycle else []

    # Show at least through 300R and two checkpoints above the current HWM.
    display_top = max(300.0, hwm + (2.0 * BCO_BANK_STEP_R))
    levels = bco_bank_levels_up_to(display_top)
    banks = []
    for threshold, fraction in levels:
        m = next(
            (r for r in rows if abs((safe_float(r.get("threshold_R")) or 0.0) - threshold) < 1e-9),
            {},
        )
        target = safe_float(m.get("target_bank_R"))
        executed = safe_float(m.get("executed_R"))
        status = safe_str(m.get("status") or "NOT_ARMED")
        banks.append(f'''
        <tr>
          <td>{threshold:.0f}R</td><td>{esc(status)}</td><td>{fraction*100:.0f}%</td>
          <td>{_fmt_metric(target,"R",2)}</td><td class="{_pnl_class(executed)}">{_fmt_metric(executed,"R",2)}</td>
          <td>{_money((executed or 0.0)*BCO_RISK_PER_TRADE_GBP) if executed is not None else "—"}</td>
          <td>{esc(m.get("executed_at_signal_time") or "—")}</td><td>{esc(m.get("selected_trade_ids") or "waiting")}</td>
        </tr>''')

    executed_levels = {
        int(round(float(safe_float(r.get("threshold_R")) or 0.0)))
        for r in rows if safe_str(r.get("status")).upper() == "EXECUTED"
    }
    next_level = float(BCO_BANK_FIRST_LEVEL_R)
    while int(round(next_level)) in executed_levels:
        next_level += float(BCO_BANK_STEP_R)
    next_fraction = bco_bank_fraction_for_level(next_level)

    return f'''
      <div class="section-note">
        <strong>Simplified BCO basket harvesting.</strong> First checkpoint is 50R, then every additional +50R.
        Bank 20% at 50R and 100R; bank 25% from 150R onward. The percentage is frozen against
        the <strong>remaining profitable open BCO pool</strong> at that checkpoint. Profitable whole trades
        can be banked immediately and do not need to be 48h old. Surviving trades continue under the normal Current Manager.
      </div>
      <div class="section-note small">
        The former exceptional pre-48 cohort ratchet has been retired as redundant. Historical COHORT rows
        remain in database exports for audit only; production creates and consumes BANK stages only.
      </div>
      <div class="metric-grid">
        <div class="mini-card"><div class="k">Current Basket</div><div class="v {_pnl_class(b.get("basket_R"))}">{safe_float(b.get("basket_R")) or 0:.2f}R</div></div>
        <div class="mini-card"><div class="k">High-Water</div><div class="v {_pnl_class(b.get("high_water_R"))}">{safe_float(b.get("high_water_R")) or 0:.2f}R</div></div>
        <div class="mini-card"><div class="k">Giveback</div><div class="v">{safe_float(b.get("giveback_pct")) or 0:.1f}%</div></div>
        <div class="mini-card"><div class="k">Next Harvest</div><div class="v pos">{next_level:.0f}R</div><div class="small">Bank {next_fraction*100:.0f}% of remaining profitable pool</div></div>
      </div>
      <h3>BCO Cash-Banking Ladder</h3>
      <div class="table-scroll"><table><thead><tr><th>Level</th><th>Status</th><th>Bank %</th><th>Target at Trigger</th><th>Actually Banked</th><th>Approx £ Banked</th><th>Executed At</th><th>Trade IDs</th></tr></thead><tbody>{"".join(banks)}</tbody></table></div>
    '''


def bco_live_readiness() -> Dict[str, Any]:
    """Production-readiness view. Read-only: never unlocks broker writes."""
    safety = safety_status()
    acct = account_summary()
    rec = reconcile_broker()
    pf = preflight()

    with get_conn() as conn:
        pending = int((conn.execute("""
            SELECT COUNT(*) AS c FROM broker_action_queue
            WHERE UPPER(COALESCE(status,'')) IN ('PENDING','RETRY','WAITING_MARKET_REOPEN','WAITING_RECONCILIATION')
        """).fetchone() or {"c":0})["c"] or 0)
        failed = int((conn.execute("""
            SELECT COUNT(*) AS c FROM broker_action_queue
            WHERE UPPER(COALESCE(status,'')) IN ('FAILED','FAILED_FINAL','EXECUTION_FAILED')
        """).fetchone() or {"c":0})["c"] or 0)
        tx = fetchone_dict(conn.execute("""
            SELECT COUNT(*) AS rows,
                   COALESCE(SUM(pl_home),0) AS pl,
                   COALESCE(SUM(financing_home),0) AS financing,
                   MAX(transaction_time) AS latest_time
            FROM broker_transactions
        """)) or {}
        last_acct = fetchone_dict(conn.execute(
            "SELECT * FROM accounting_snapshots ORDER BY id DESC LIMIT 1"
        )) or {}

    checks = [
        {"name":"Environment", "ok": OANDA_ENV in {"practice","live"}, "detail": OANDA_ENV},
        {"name":"OANDA read access", "ok": bool(acct.get("ok")), "detail": safe_str(acct.get("error") or "account readable")},
        {"name":"GBP account", "ok": safe_str(acct.get("currency")).upper()=="GBP", "detail": safe_str(acct.get("currency") or "unknown")},
        {"name":"Exact BCO ownership", "ok": bool(BCO_OANDA_INSTRUMENT) and not any(tok in safe_str(BCO_OANDA_INSTRUMENT).upper() for tok in FORBIDDEN_FOREIGN_INSTRUMENT_TOKENS), "detail": BCO_OANDA_INSTRUMENT},
        {"name":"Risk preview", "ok": bool(risk_preview().get("ok")) if BCO_OANDA_INSTRUMENT else False, "detail": "risk-sized order preview"},
        {"name":"Reconciliation", "ok": bool(rec.get("ok")), "detail": f"{int(rec.get('owned_open_count') or 0)} owned broker trades"},
        {"name":"Durable queue", "ok": failed==0, "detail": f"{pending} pending / {failed} failed"},
        {"name":"Transaction accounting", "ok": int(tx.get("rows") or 0)>0 or OANDA_ENV=="practice", "detail": f"{int(tx.get('rows') or 0)} broker transaction rows"},
        {"name":"Auto management configured", "ok": bool(BCO_AUTO_MANAGEMENT_ENABLED), "detail": f"48h+ manager {'ON' if BCO_AUTO_MANAGEMENT_ENABLED else 'OFF'}"},
    ]

    # Promotion readiness does NOT require writes to be unlocked now.
    promotion_ready = all(c["ok"] for c in checks)
    return {
        "status":"READY" if promotion_ready else "CHECK",
        "promotion_ready":promotion_ready,
        "current_environment":OANDA_ENV,
        "switch_to_live_requires":[
            "Set OANDA_ENV=live",
            "Use live OANDA API base/account/token",
            "Confirm GBP live account",
            "Keep BCO_OANDA_INSTRUMENT exact",
            "Run /broker/live-readiness while writes remain locked",
            "Only then explicitly arm BROKER_READ_ONLY=false, BROKER_EXECUTION_ENABLED=true, BROKER_KILL_SWITCH=false and BCO_LIVE_EXECUTION_ARMED=true",
        ],
        "checks":checks,
        "broker_safety":safety,
        "account":acct,
        "reconciliation":rec,
        "transactions":tx,
        "latest_accounting_snapshot":last_acct,
        "preflight":pf,
        "time_utc":now_utc_iso(),
    }



def _bco_exact_open_reconciliation() -> Dict[str, Any]:
    """
    Read-only proof that the current local OPEN BCO set exactly matches OANDA.

    The manual economic-cycle reset is accounting-only, but we still require
    exact ownership before changing the family protection cycle.
    """
    live = bco_broker_live_snapshot()
    if not live.get("ok"):
        return {
            "ok": False,
            "reason": f"fresh OANDA BCO read failed: {safe_str(live.get('error'))}",
            "broker": live,
        }

    owned = list(live.get("owned_open_trades") or [])
    broker_ids = {safe_str(t.get("id")) for t in owned if safe_str(t.get("id"))}

    with get_conn() as conn:
        local_open = fetchall_dict(conn.execute("""
            SELECT *
            FROM trades
            WHERE status='OPEN'
            ORDER BY id
        """))

    local_linked_ids = {
        safe_str(t.get("broker_trade_id"))
        for t in local_open
        if safe_str(t.get("broker_trade_id"))
    }
    local_unlinked = [
        t for t in local_open
        if not safe_str(t.get("broker_trade_id"))
    ]
    local_missing = [
        t for t in local_open
        if safe_str(t.get("broker_trade_id"))
        and safe_str(t.get("broker_trade_id")) not in broker_ids
    ]
    broker_only = [
        t for t in owned
        if safe_str(t.get("id")) not in local_linked_ids
    ]

    ok = bool(
        not local_unlinked
        and not local_missing
        and not broker_only
        and len(local_open) == len(owned)
    )
    reasons = []
    if local_unlinked:
        reasons.append(f"{len(local_unlinked)} local OPEN trade(s) have no broker ID")
    if local_missing:
        reasons.append(f"{len(local_missing)} local OPEN trade(s) are absent from OANDA")
    if broker_only:
        reasons.append(f"{len(broker_only)} OANDA BCO trade(s) have no local OPEN row")
    if len(local_open) != len(owned):
        reasons.append(f"open-count mismatch local={len(local_open)} broker={len(owned)}")

    return {
        "ok": ok,
        "reason": "EXACT" if ok else "; ".join(reasons),
        "local_open_count": len(local_open),
        "broker_open_count": len(owned),
        "local_unlinked_trade_ids": [safe_str(t.get("trade_id")) for t in local_unlinked],
        "local_missing_broker_ids": [safe_str(t.get("broker_trade_id")) for t in local_missing],
        "broker_only_trade_ids": [safe_str(t.get("id")) for t in broker_only],
        "broker": live,
    }


def bco_manual_start_new_basket_cycle_impl() -> Dict[str, Any]:
    """
    Start a fresh BCO ECONOMIC/FAMILY basket cycle without touching trades.

    This deliberately does NOT:
      - close or open an OANDA position;
      - alter trade age / hold_candles;
      - alter MFE / MAE / current_R;
      - alter hard or managed stops;
      - alter Current Manager decisions/history;
      - alter MFE50 or ATR2 challenger rows.

    It DOES:
      - archive unfinished BANK stages from the previous economic cycle;
      - retain all historical basket_snapshots;
      - rebase basket_state HWM to current open basket R (0 if negative);
      - create a new cycle_id and reassign currently-open trades to that family
        cycle for FUTURE review/snapshot bookkeeping;
      - reset cycle banked/realized counters;
      - make 50R the next harvest checkpoint again.
    """
    init_db()

    # Let normal reconciliation/transaction sync run first, then require exact
    # ownership. The reset itself has zero broker-write authority.
    try:
        reconcile_result = reconcile_broker()
    except Exception as exc:
        reconcile_result = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }

    exact = _bco_exact_open_reconciliation()
    if not exact.get("ok"):
        raise HTTPException(
            status_code=409,
            detail=(
                "Cannot start a new BCO basket cycle until OANDA/local ownership is exact. "
                + safe_str(exact.get("reason"))
            ),
        )

    with _db_lock, get_conn() as conn:
        metrics = basket_metrics(conn)
        if int(metrics.get("open_count") or 0) <= 0:
            raise HTTPException(
                status_code=409,
                detail="BCO basket is already flat; the normal flat-cycle reset already applies.",
            )

        current_r = float(safe_float(metrics.get("basket_R")) or 0.0)
        current_gbp = float(safe_float(metrics.get("basket_pnl_gbp")) or 0.0)

        # Same safety as Metals: a manual economic reset should not be used to
        # jump over an already-earned first harvest checkpoint.
        if current_r >= float(BCO_BANK_FIRST_LEVEL_R):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Current BCO basket is already {current_r:.2f}R. "
                    "Manual new-cycle reset is only allowed below the first 50R harvest checkpoint."
                ),
            )

        state = fetchone_dict(conn.execute(
            "SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1"
        )) or {}

        previous = {
            "cycle_id": safe_str(state.get("cycle_id")),
            "high_water_R": float(safe_float(state.get("high_water_R")) or 0.0),
            "high_water_seen_at": safe_str(state.get("high_water_seen_at")),
            "basket_R": float(safe_float(state.get("basket_R")) or 0.0),
            "basket_pnl_gbp": float(safe_float(state.get("basket_pnl_gbp")) or 0.0),
            "banked_R_cycle": float(safe_float(state.get("banked_R_cycle")) or 0.0),
            "realized_R_cycle": float(safe_float(state.get("realized_R_cycle")) or 0.0),
            "status": safe_str(state.get("status")),
        }

        observed_at = now_utc_iso()
        clean = re.sub(r"[^0-9A-Za-z]", "", observed_at)[-20:]
        new_cycle = f"BCO_LONG_MANUAL_{clean}"
        new_hwm = max(0.0, current_r)
        new_hwm_seen_at = observed_at if new_hwm > 0 else None

        old_cycle = safe_str(previous.get("cycle_id"))
        if old_cycle:
            conn.execute("""
                UPDATE protection_stages
                SET status=CASE
                        WHEN status IN ('EXECUTED','NO_ELIGIBLE','EXPIRED_FLAT')
                            THEN status
                        ELSE 'MANUAL_ECONOMIC_RESET_ARCHIVED'
                    END,
                    updated_at_utc=?,
                    reason=CASE
                        WHEN status IN ('EXECUTED','NO_ELIGIBLE','EXPIRED_FLAT')
                            THEN reason
                        ELSE COALESCE(reason,'') || ' | archived by manual BCO economic basket-cycle reset'
                    END
                WHERE cycle_id=?
            """, (observed_at, old_cycle))

        # New family state. Current basket R is preserved; only the cycle HWM
        # and cycle-level counters are rebased.
        conn.execute("""
            UPDATE basket_state
            SET status='ACTIVE',
                cycle_id=?,
                cycle_started_at=?,
                open_count=?,
                basket_R=?,
                basket_pnl_gbp=?,
                high_water_R=?,
                high_water_seen_at=?,
                giveback_pct=0,
                realized_R_cycle=0,
                banked_R_cycle=0,
                manager_detail=?,
                updated_at_utc=?
            WHERE singleton_key='BCO_LONG'
        """, (
            new_cycle,
            observed_at,
            int(metrics.get("open_count") or 0),
            current_r,
            current_gbp,
            new_hwm,
            new_hwm_seen_at,
            (
                "MANUAL_ECONOMIC_CYCLE_RESET: previous family HWM/harvest cycle archived; "
                "surviving trades unchanged; next family harvest starts again at 50R."
            ),
            observed_at,
        ))

        # Open trades continue unchanged, but future reviews/snapshots need to
        # belong to the newly-started FAMILY cycle.
        conn.execute("""
            UPDATE trades
            SET cycle_id=?,
                updated_at_utc=?
            WHERE status='OPEN'
        """, (new_cycle, observed_at))

        # Permanent audit boundary in basket snapshots. Old snapshots remain
        # untouched and therefore retain the previous HWM for research.
        latest_signal = fetchone_dict(conn.execute(
            "SELECT id,timestamp_readable FROM raw_signals ORDER BY id DESC LIMIT 1"
        )) or {}
        latest_raw_id = int(safe_float(latest_signal.get("id")) or 0)
        signal_time = safe_str(latest_signal.get("timestamp_readable")) or observed_at

        conn.execute("""
            INSERT INTO basket_snapshots(
                created_at_utc,raw_signal_id,signal_time,cycle_id,open_count,
                basket_R,basket_pnl_gbp,high_water_R,giveback_pct,losing_pct,
                basket_phase,tide_score,tide_status,manager_action,manager_detail
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            observed_at,
            latest_raw_id,
            signal_time,
            new_cycle,
            int(metrics.get("open_count") or 0),
            current_r,
            current_gbp,
            new_hwm,
            0.0,
            float(safe_float(metrics.get("losing_pct")) or 0.0),
            safe_str(metrics.get("phase")),
            safe_float(state.get("tide_score")) or 0.0,
            safe_str(state.get("tide_status")),
            safe_str(state.get("manager_action")),
            (
                "MANUAL_ECONOMIC_CYCLE_RESET boundary. Previous cycle "
                f"{old_cycle or 'none'} archived at HWM "
                f"{float(previous.get('high_water_R') or 0.0):.2f}R. "
                "No OANDA trade/stop/age/exit-shadow state changed."
            ),
        ))

        conn.commit()

    log_event(
        "manual_economic_basket_cycle_reset",
        "Manual BCO economic basket-cycle/HWM reset from Broker/OANDA/Accounting.",
        {
            "previous": previous,
            "new_cycle_id": new_cycle,
            "new_hwm_R": new_hwm,
            "current_basket_R": current_r,
            "current_basket_pnl_gbp": current_gbp,
            "open_count": int(metrics.get("open_count") or 0),
            "next_harvest_R": BCO_BANK_FIRST_LEVEL_R,
            "reconciliation": exact,
            "reconcile_result": reconcile_result,
            "broker_write_authority": False,
            "individual_trade_state_unchanged": True,
            "exit_shadow_state_unchanged": True,
        },
    )

    # Make the reset visible in the top tiles immediately.
    try:
        with _BCO_STD_TOP_LOCK:
            _BCO_STD_TOP_CACHE["payload"] = None
            _BCO_STD_TOP_CACHE["expires_at"] = 0.0
    except Exception:
        pass

    # Verify persisted state after commit.
    with get_conn() as conn:
        verify = fetchone_dict(conn.execute(
            "SELECT * FROM basket_state WHERE singleton_key='BCO_LONG' LIMIT 1"
        )) or {}
        next_stage = fetchone_dict(conn.execute("""
            SELECT *
            FROM protection_stages
            WHERE cycle_id=? AND stage_type='BANK'
            ORDER BY threshold_R ASC LIMIT 1
        """, (new_cycle,))) or {}

    return {
        "ok": True,
        "status": "NEW_ECONOMIC_BASKET_STARTED",
        "message": (
            "New BCO economic basket cycle started. No broker orders were sent; "
            "surviving trades retain their age, stops, manager state and exit shadows."
        ),
        "previous_cycle_id": previous.get("cycle_id"),
        "previous_hwm_R": previous.get("high_water_R"),
        "new_cycle_id": new_cycle,
        "current_basket_R": current_r,
        "current_basket_pnl_gbp": current_gbp,
        "new_hwm_R": safe_float(verify.get("high_water_R")) or 0.0,
        "new_hwm_seen_at": safe_str(verify.get("high_water_seen_at")),
        "next_harvest_R": BCO_BANK_FIRST_LEVEL_R,
        "new_cycle_stage_rows": 1 if next_stage else 0,
        "open_count": int(metrics.get("open_count") or 0),
        "reconciliation": exact,
        "broker_write_authority": False,
        "individual_trade_state_unchanged": True,
        "exit_shadow_state_unchanged": True,
        "time_utc": now_utc_iso(),
    }


@app.post("/broker/start-new-basket-cycle")
async def bco_manual_start_new_basket_cycle(
    request: Request,
    x_webhook_secret: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}

    body_secret = safe_str(body.get("webhook_secret") or body.get("secret"))
    query_secret = safe_str(request.query_params.get("secret") or "")

    if (
        x_webhook_secret != WEBHOOK_SECRET
        and body_secret != WEBHOOK_SECRET
        and query_secret != WEBHOOK_SECRET
    ):
        raise HTTPException(status_code=401, detail="Invalid WEBHOOK_SECRET")

    if safe_str(body.get("confirm")) != "START_NEW_BCO_BASKET":
        raise HTTPException(
            status_code=400,
            detail="Missing confirm=START_NEW_BCO_BASKET",
        )

    return bco_manual_start_new_basket_cycle_impl()


@app.get("/broker/live-readiness")
def bco_live_readiness_endpoint():
    return bco_live_readiness()


def _bco_standard_broker_html():
    s = snapshot()
    safety = s.get("broker_safety") or {}
    acct = s.get("account") or {}
    broker = s.get("broker_live") or {}
    preview = risk_preview()
    ready = bco_live_readiness()

    with get_conn() as conn:
        txs = fetchall_dict(conn.execute("""
            SELECT * FROM broker_transactions
            ORDER BY id DESC LIMIT 30
        """))
        queue = fetchall_dict(conn.execute("""
            SELECT * FROM broker_action_queue
            ORDER BY id DESC LIMIT 30
        """))
        audits = fetchall_dict(conn.execute("""
            SELECT * FROM execution_audit
            ORDER BY id DESC LIMIT 30
        """))
        accounting = fetchall_dict(conn.execute("""
            SELECT * FROM accounting_snapshots
            ORDER BY id DESC LIMIT 20
        """))

    owned = broker.get("owned_open_trades") or []
    local_open = s.get("open_trades") or []
    broker_ids = {safe_str(x.get("id")) for x in owned}
    local_ids = {safe_str(x.get("broker_trade_id")) for x in local_open if safe_str(x.get("broker_trade_id"))}
    local_missing = [x for x in local_open if safe_str(x.get("broker_trade_id")) and safe_str(x.get("broker_trade_id")) not in broker_ids]
    broker_only = [x for x in owned if safe_str(x.get("id")) not in local_ids]

    pending = sum(1 for q in queue if safe_str(q.get("status")).upper() in {"PENDING","RETRY","WAITING_MARKET_REOPEN","WAITING_RECONCILIATION"})
    failed = sum(1 for q in queue if safe_str(q.get("status")).upper() in {"FAILED","FAILED_FINAL","EXECUTION_FAILED"})

    tx_pl = sum(float(safe_float(x.get("pl_home")) or 0) for x in txs)
    tx_fin = sum(float(safe_float(x.get("financing_home")) or 0) for x in txs)

    owned_rows = "".join(
        f"<tr><td>{esc(t.get('id'))}</td><td>{esc(t.get('instrument'))}</td><td>{esc(t.get('currentUnits'))}</td>"
        f"<td>{_fmt_metric(t.get('price'),3)}</td><td class='{_pnl_class(t.get('unrealizedPL'))}'>{_money(t.get('unrealizedPL'))}</td>"
        f"<td>{_money(t.get('marginUsed'))}</td><td>{esc(t.get('openTime'))}</td></tr>"
        for t in owned
    )
    tx_rows = "".join(
        f"<tr><td>{esc(t.get('transaction_time'))}</td><td>{esc(t.get('transaction_id'))}</td><td>{esc(t.get('transaction_type'))}</td>"
        f"<td class='{_pnl_class(t.get('pl_home'))}'>{_money(t.get('pl_home'))}</td><td>{_money(t.get('financing_home'))}</td>"
        f"<td>{_money(t.get('account_balance'))}</td></tr>"
        for t in txs
    )
    queue_rows = "".join(
        f"<tr><td>{esc(q.get('created_at_utc'))}</td><td>{esc(q.get('action_type'))}</td><td>{esc(q.get('status'))}</td>"
        f"<td>{esc(q.get('local_trade_id'))}</td><td>{esc(q.get('broker_trade_id'))}</td><td>{esc(q.get('attempts'))}</td>"
        f"<td>{esc(q.get('last_error'))}</td></tr>"
        for q in queue
    )
    audit_rows = "".join(
        f"<tr><td>{esc(a.get('created_at_utc'))}</td><td>{esc(a.get('action'))}</td><td>{esc(a.get('success'))}</td>"
        f"<td>{esc(a.get('trade_id'))}</td><td>{esc(a.get('broker_trade_id'))}</td><td>{esc(a.get('message'))}</td></tr>"
        for a in audits
    )
    acct_rows = "".join(
        f"<tr><td>{esc(a.get('created_at_utc'))}</td><td>{_money(a.get('account_nav'))}</td><td>{_money(a.get('account_balance'))}</td>"
        f"<td class='{_pnl_class(a.get('bco_open_pl'))}'>{_money(a.get('bco_open_pl'))}</td>"
        f"<td>{_money(a.get('bco_realized_pl'))}</td><td>{_money(a.get('bco_financing'))}</td></tr>"
        for a in accounting
    )
    readiness_rows = "".join(
        f"<tr><td>{esc(c.get('name'))}</td><td class='{'pos' if c.get('ok') else 'neg'}'>{'PASS' if c.get('ok') else 'CHECK'}</td><td>{esc(c.get('detail'))}</td></tr>"
        for c in ready.get("checks") or []
    )

    return f"""
      <div class="section-note {'warn' if OANDA_ENV!='live' else 'neg'}">
        <strong>{'LIVE' if OANDA_ENV=='live' else 'DEMO / PRACTICE'} BROKER LANE.</strong>
        Exact BCO ownership is enforced. Demo and live use the same execution/reconciliation/accounting architecture; promotion is an environment/credential/safety-gate change, not a new engine.
      </div>

      <div class="metric-grid">
        <div class="mini-card"><div class="k">Live Readiness</div><div class="v {'pos' if ready.get('promotion_ready') else 'warn'}">{esc(ready.get('status'))}</div><div class="small">Read-only readiness test</div></div>
        <div class="mini-card"><div class="k">Broker Writes</div><div class="v {'pos' if safety.get('orders_allowed') else 'warn'}">{'ENABLED' if safety.get('orders_allowed') else 'LOCKED'}</div><div class="small">{esc(safety.get('reason'))}</div></div>
        <div class="mini-card"><div class="k">Account NAV</div><div class="v">{_money(acct.get('NAV'))}</div><div class="small">Balance {_money(acct.get('balance'))}</div></div>
        <div class="mini-card"><div class="k">Margin Available</div><div class="v">{_money(acct.get('marginAvailable'))}</div><div class="small">Currency {esc(acct.get('currency') or '-')}</div></div>
        <div class="mini-card"><div class="k">BCO Open P&amp;L</div><div class="v {_pnl_class(broker.get('owned_unrealized_pl'))}">{_money(broker.get('owned_unrealized_pl'))}</div><div class="small">{int(broker.get('owned_open_count') or 0)} owned broker trades</div></div>
        <div class="mini-card"><div class="k">Reconciliation</div><div class="v {'pos' if not local_missing and not broker_only else 'neg'}">{'SAFE' if not local_missing and not broker_only else 'CHECK'}</div><div class="small">Local missing {len(local_missing)} · broker-only {len(broker_only)}</div></div>
        <div class="mini-card"><div class="k">Broker Queue</div><div class="v {'warn' if pending else 'pos'}">{pending}</div><div class="small">{failed} failed final</div></div>
        <div class="mini-card"><div class="k">Risk / Trade</div><div class="v">£{BCO_RISK_PER_TRADE_GBP:.2f}</div><div class="small">{BCO_SL_PCT:.2f}% SL · 1.00x locked</div></div>
      </div>

      <h3>Live Promotion Readiness</h3>
      <div class="table-scroll"><table><thead><tr><th>Check</th><th>State</th><th>Detail</th></tr></thead>
      <tbody>{readiness_rows}</tbody></table></div>

      <h3>Risk / Order Preview</h3>
      <div class="table-scroll"><table><thead><tr><th>Instrument</th><th>Requested Risk</th><th>Effective Risk</th><th>Units</th><th>Entry</th><th>SL</th><th>State</th></tr></thead>
      <tbody><tr><td>{esc(preview.get('instrument') or BCO_OANDA_INSTRUMENT)}</td><td>£{BCO_RISK_PER_TRADE_GBP:.2f}</td>
      <td>{_money(preview.get('effective_risk_gbp'))}</td><td>{esc(preview.get('units') or preview.get('order_units') or '-')}</td>
      <td>{_fmt_metric(preview.get('entry_price'),3)}</td><td>{_fmt_metric(preview.get('sl_price'),3)}</td>
      <td class='{'pos' if preview.get('ok') else 'warn'}'>{'OK' if preview.get('ok') else 'BLOCKED'}</td></tr></tbody></table></div>

      <h3>Manual Economic Basket Cycle</h3>
      <div class="section-note">
        <strong>Start New Basket Cycle / Reset HWM</strong><br>
        Use this when the previous BCO family campaign has economically ended but
        some trades remain open. It archives the old family HWM/harvest cycle,
        rebases HWM to the current basket and makes <strong>50R the next harvest again</strong>.
        <br><strong>No trades are closed or modified.</strong> Existing trade ages,
        MFE/MAE, hard/managed stops, Current Manager state and MFE50/ATR2 shadows
        remain unchanged.
      </div>
      <div style="padding:12px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
        <button type="button"
          onclick="startNewBCOBasketCycle()"
          style="padding:10px 14px;border:1px solid #58a6ff;border-radius:8px;background:#0f172a;color:#d8ecff;cursor:pointer;font-weight:800;">
          Start new basket cycle / reset HWM
        </button>
        <span class="small">Requires exact BCO OANDA ↔ local reconciliation and WEBHOOK_SECRET confirmation.</span>
      </div>
      <div id="bco-new-cycle-result" class="section-note small" style="display:none;"></div>

      <h3>Actual OANDA BCO Open Trades</h3>
      <div class="table-scroll"><table><thead><tr><th>Broker ID</th><th>Instrument</th><th>Units</th><th>Entry</th><th>UPL</th><th>Margin</th><th>Open Time</th></tr></thead>
      <tbody>{owned_rows or '<tr><td colspan="7">No OANDA BCO trades open.</td></tr>'}</tbody></table></div>

      <h3>Broker-Authoritative Realised P&amp;L / Financing</h3>
      <div class="section-note small">Recent transaction rows shown below. Full ledger is persisted independently of local strategy trades.</div>
      <div class="table-scroll"><table><thead><tr><th>Time</th><th>Transaction</th><th>Type</th><th>P/L</th><th>Financing</th><th>Account Balance</th></tr></thead>
      <tbody>{tx_rows or '<tr><td colspan="6">No broker transaction rows yet.</td></tr>'}</tbody></table></div>

      <h3>Durable Broker Action Queue</h3>
      <div class="table-scroll"><table><thead><tr><th>Created</th><th>Action</th><th>Status</th><th>Local Trade</th><th>Broker ID</th><th>Attempts</th><th>Error</th></tr></thead>
      <tbody>{queue_rows or '<tr><td colspan="7">No queued broker actions.</td></tr>'}</tbody></table></div>

      <h3>Accounting Snapshots</h3>
      <div class="table-scroll"><table><thead><tr><th>Time</th><th>NAV</th><th>Balance</th><th>BCO Open P/L</th><th>Realised</th><th>Financing</th></tr></thead>
      <tbody>{acct_rows or '<tr><td colspan="6">No accounting snapshots yet.</td></tr>'}</tbody></table></div>

      <h3>Recent Execution Audit</h3>
      <div class="table-scroll"><table><thead><tr><th>Time</th><th>Action</th><th>Success</th><th>Local Trade</th><th>Broker ID</th><th>Message</th></tr></thead>
      <tbody>{audit_rows or '<tr><td colspan="6">No execution audit rows.</td></tr>'}</tbody></table></div>

      <div class="section-note small">
        <a href="/broker/live-readiness">live-readiness JSON</a> ·
        <a href="/broker/preflight">execution preflight</a> ·
        <a href="/broker/risk-preview">risk preview</a> ·
        <a href="/broker/instruments/discover">instrument discovery</a> ·
        <a href="/export/broker-transactions.csv">broker transactions</a> ·
        <a href="/export/broker-action-queue.csv">action queue</a> ·
        <a href="/export/accounting-snapshots.csv">accounting snapshots</a>
      </div>
    """


def _fmt_metric(value: Any, suffix: str = "", decimals: int = 2) -> str:
    v = safe_float(value)
    if v is None:
        return "—"
    return f"{v:.{max(0,int(decimals))}f}{suffix}"


def _bco_standard_execution_html():
    rec = reconcile_broker()
    with get_conn() as conn:
        audits = fetchall_dict(conn.execute("SELECT * FROM execution_audit ORDER BY id DESC LIMIT 30"))
        stops = fetchall_dict(conn.execute("SELECT * FROM managed_stop_events ORDER BY id DESC LIMIT 30"))
        events = fetchall_dict(conn.execute("SELECT * FROM system_events ORDER BY id DESC LIMIT 30"))
        queue = fetchall_dict(conn.execute("SELECT * FROM broker_action_queue ORDER BY id DESC LIMIT 30"))
        reviews = fetchall_dict(conn.execute("SELECT * FROM trade_manager_reviews ORDER BY id DESC LIMIT 30"))
        fixed = fetchall_dict(conn.execute("SELECT * FROM fixed_48_outcomes ORDER BY id DESC LIMIT 30"))
        harvest = fetchall_dict(conn.execute("SELECT * FROM harvest_execution_outcomes ORDER BY id DESC LIMIT 20"))

    pending = sum(1 for q in queue if safe_str(q.get("status")).upper() in {"PENDING","RETRY"})
    failed_final = sum(1 for q in queue if safe_str(q.get("status")).upper() == "FAILED_FINAL")
    audit_rows = "".join(
        f"<tr><td>{esc(a.get('created_at_utc'))}</td><td>{esc(a.get('action'))}</td><td>{esc(a.get('success'))}</td><td>{esc(a.get('trade_id'))}</td><td>{esc(a.get('broker_trade_id'))}</td><td>{esc(a.get('message'))}</td></tr>"
        for a in audits
    )
    queue_rows = "".join(
        f"<tr><td>{esc(q.get('created_at_utc'))}</td><td>{esc(q.get('action_type'))}</td><td>{esc(q.get('status'))}</td><td>{esc(q.get('local_trade_id'))}</td><td>{esc(q.get('broker_trade_id'))}</td><td>{esc(q.get('attempts'))}</td><td>{esc(q.get('last_error'))}</td></tr>"
        for q in queue
    )
    review_rows = "".join(
        f"<tr><td>{esc(r.get('signal_time'))}</td><td>{esc(r.get('trade_id'))}</td><td>{esc(r.get('hold_candles'))}</td><td>{_fmt_metric(r.get('current_R'),'R',2)}</td><td>{_fmt_metric(r.get('mfe_pct'),'%',2)}</td><td>{_fmt_metric(r.get('mae_pct'),'%',2)}</td><td>{esc(r.get('regime'))}</td><td>{esc(r.get('manager_decision'))}</td><td>{esc(r.get('manager_reason'))}</td></tr>"
        for r in reviews
    )
    fixed_rows = "".join(
        f"<tr><td>{esc(r.get('trade_id'))}</td><td>{esc(r.get('signal_time'))}</td><td>{_fmt_metric(r.get('fixed_48_R'),'R',2)}</td><td>{_money(r.get('fixed_48_pnl_gbp'))}</td><td>{_fmt_metric(r.get('mfe_pct'),'%',2)}</td><td>{_fmt_metric(r.get('mae_pct'),'%',2)}</td></tr>"
        for r in fixed
    )
    harvest_rows = "".join(
        f"<tr><td>{esc(r.get('threshold_R'))}R</td><td>{esc(r.get('selected_trade_ids'))}</td><td>{_fmt_metric(r.get('model_realized_R'),'R',2)}</td><td>{_money(r.get('broker_realized_pl_gbp'))}</td><td>{_money(r.get('financing_gbp'))}</td><td>{_money(r.get('net_realized_gbp'))}</td><td>{esc(r.get('sync_status'))}</td></tr>"
        for r in harvest
    )

    return f"""
      <div class="metric-grid">
        <div class="mini-card"><div class="k">Reconciliation</div><div class="v {'pos' if rec.get('ok') else 'neg'}">{'SAFE' if rec.get('ok') else 'ERROR'}</div><div class="small">{int(rec.get('owned_open_count') or 0)} OANDA BCO trades</div></div>
        <div class="mini-card"><div class="k">Queued Broker Actions</div><div class="v {'warn' if pending else 'pos'}">{pending}</div><div class="small">Durable close/stop retries</div></div>
        <div class="mini-card"><div class="k">Failed Final Actions</div><div class="v {'neg' if failed_final else 'pos'}">{failed_final}</div></div>
        <div class="mini-card"><div class="k">Managed Stop Events</div><div class="v">{len(stops)}</div></div>
      </div>

      <h3>Durable Broker Action Queue</h3>
      <div class="table-scroll"><table><thead><tr><th>Created</th><th>Action</th><th>Status</th><th>Local Trade</th><th>Broker ID</th><th>Attempts</th><th>Last Error</th></tr></thead>
      <tbody>{queue_rows or '<tr><td colspan="7">No queued actions.</td></tr>'}</tbody></table></div>

      <h3>Recent Post-48 Manager Reviews</h3>
      <div class="table-scroll"><table><thead><tr><th>Signal</th><th>Trade</th><th>Age</th><th>R</th><th>MFE</th><th>MAE</th><th>Regime</th><th>Decision</th><th>Reason</th></tr></thead>
      <tbody>{review_rows or '<tr><td colspan="9">No 48h+ reviews yet.</td></tr>'}</tbody></table></div>

      <h3>Fixed-48h Control Outcomes</h3>
      <div class="table-scroll"><table><thead><tr><th>Trade</th><th>48h Signal</th><th>48h R</th><th>48h £</th><th>MFE</th><th>MAE</th></tr></thead>
      <tbody>{fixed_rows or '<tr><td colspan="6">No matured 48h controls yet.</td></tr>'}</tbody></table></div>

      <h3>Harvest Execution — Broker GBP</h3>
      <div class="table-scroll"><table><thead><tr><th>Threshold</th><th>Trades</th><th>Realised R</th><th>Broker P/L</th><th>Financing</th><th>Net £</th><th>Sync</th></tr></thead>
      <tbody>{harvest_rows or '<tr><td colspan="7">No executed harvest stages yet.</td></tr>'}</tbody></table></div>

      <h3>Recent Execution Audit</h3>
      <div class="table-scroll"><table><thead><tr><th>Time</th><th>Action</th><th>Success</th><th>Local Trade</th><th>Broker ID</th><th>Message</th></tr></thead>
      <tbody>{audit_rows or '<tr><td colspan="6">No audit rows.</td></tr>'}</tbody></table></div>

      <div class="section-note small">
        <a href="/export/broker-action-queue.csv">broker action queue</a> ·
        <a href="/export/broker-transactions.csv">broker transactions</a> ·
        <a href="/export/fixed-48-outcomes.csv">fixed-48 outcomes</a> ·
        <a href="/export/trade-manager-reviews.csv">manager reviews</a> ·
        <a href="/export/basket-snapshots.csv">basket snapshots</a> ·
        <a href="/export/harvest-execution-outcomes.csv">harvest GBP outcomes</a> ·
        <a href="/export/accounting-snapshots.csv">accounting snapshots</a>
      </div>
    """


@app.get("/operational-health")
def operational_health_endpoint():
    if safe_str(_bootstrap_state.get("status")).upper() != "READY":
        return {
            "status": "initializing" if safe_str(_bootstrap_state.get("status")).upper() != "FAILED" else "degraded",
            "bootstrap": dict(_bootstrap_state),
            "time_utc": now_utc_iso(),
        }
    return operational_health()


def _bco_standard_health_html():
    h = operational_health()
    checks = h.get("checks") or {}
    cards = ""
    for name, item in checks.items():
        ok = bool(item.get("ok"))
        detail = " · ".join(f"{k}={v}" for k,v in item.items() if k != "ok" and v not in (None,""))
        cards += f'<div class="mini-card"><div class="k">{esc(name.replace("_"," ").title())}</div><div class="v {"pos" if ok else "neg"}>{"OK" if ok else "CHECK"}</div><div class="small">{esc(detail)}</div></div>'
    return f"""
      <div class="section-note"><strong>Operational Health.</strong> Signal freshness, OANDA read access, local↔broker parity, durable retry queue, transaction sync and reconciliation freshness.</div>
      <div class="metric-grid">{cards}</div>
      <div class="section-note small"><a href="/operational-health">full operational health JSON</a> · <a href="/ready">application readiness</a> · <a href="/health">Railway liveness</a></div>
    """



def _bco_standard_latest_signals_combined_html():
    return _bco_latest_30_signals_html() + """
      <details><summary>Webhook Ingress / Delivery Audit</summary>
      <div class="research-inner-body">""" + _bco_webhook_ingress_html() + """</div></details>
      <details><summary>Current Signal State / Recent Signal Detail</summary>
      <div class="research-inner-body">""" + _bco_standard_signal_html() + """</div></details>"""


def _bco_standard_broker_combined_html():
    return _bco_standard_broker_html() + """
      <details><summary>Execution / Reconciliation</summary>
      <div class="research-inner-body">""" + _bco_standard_execution_html() + """</div></details>
      <details><summary>Operational Health / Pipeline</summary>
      <div class="research-inner-body">""" + _bco_standard_health_html() + """</div></details>"""


def _bco_standard_manager_protection_html():
    return _bco_standard_basket_manager_html() + """
      <details open><summary>Profit Harvesting / Protection Plan</summary>
      <div class="research-inner-body">""" + _bco_standard_profit_harvesting_html() + """</div></details>"""


_BCO_STD_SECTIONS = {
    "latest-signals": ("Latest 30 BCO Signals", _bco_standard_latest_signals_combined_html),
    "recent-closed": ("Recently Closed BCO Trades", _bco_recently_closed_trades_html),
    "open-trades": ("Open Trades / Positions", _bco_standard_open_trades_html),
    "broker": ("Broker / OANDA / Accounting", _bco_standard_broker_combined_html),
    "manager-protection": ("Basket Manager / Profit Protection", _bco_standard_manager_protection_html),
    "research": ("BCO Research / Evidence Lab", build_bco_focused_research_html),
    "basket-manager": ("Basket Manager", _bco_standard_basket_manager_html),
    "profit-harvesting": ("Profit Harvesting / Protection Plan", _bco_standard_profit_harvesting_html),
    "execution": ("Execution / Reconciliation", _bco_standard_execution_html),
    "operational-health": ("Operational Health / Pipeline", _bco_standard_health_html),
    "signals": ("Signal State", _bco_standard_signal_html),
}
@app.get("/dashboard/section/{section_key}", response_class=HTMLResponse)
def bco_standard_section(section_key: str):
    item = _BCO_STD_SECTIONS.get(safe_str(section_key))
    if not item:
        return HTMLResponse('<div class="lazy-error">Unknown section.</div>', status_code=404)
    title, fn = item
    started = time.perf_counter()
    try:
        body = fn()
        return f'<div class="lazy-meta">{esc(title)} loaded in {time.perf_counter()-started:.2f}s</div>{body}'
    except Exception as exc:
        return f'<div class="lazy-error"><strong>{esc(title)} failed.</strong><br>{esc(type(exc).__name__)}: {esc(exc)}</div>'

def _bco_std_placeholder(key, title, note=""):
    return f'<details class="lazy-section" data-section="{esc(key)}"><summary>{esc(title)}</summary><div class="lazy-body"><div class="lazy-placeholder"><strong>Not loaded yet.</strong> {esc(note)}<br>Open this section to fetch it.</div></div></details>'

@app.get("/dashboard", response_class=HTMLResponse)
def bco_standard_dashboard():
    sections = "".join([
        _bco_std_placeholder("latest-signals", "Latest 30 BCO Signals", "Current BCO candidate state plus detailed signal context."),
        _bco_std_placeholder("recent-closed", "Recently Closed BCO Trades", "Latest BCO closures with exact reason, realised R, broker P&L and financing."),
        _bco_std_placeholder("open-trades", "Open Trades / Positions", "Actual OANDA BCO positions with local R, MFE/MAE, age, stops and effective risk."),
        _bco_std_placeholder("broker", "Broker / OANDA / Accounting", "BCO OANDA lane, accounting, execution/reconciliation and operational health."),
        _bco_std_placeholder("manager-protection", "Basket Manager / Profit Protection", "48h+ manager state plus persisted harvesting/protection stages."),
        _bco_std_placeholder("research", "BCO Research / Evidence Lab", "MFE/ATR2 exit challengers, AI/regime evidence, high-water outcomes, alignment, trend efficiency and basket recovery."),
    ])
    env_label = "LIVE" if OANDA_ENV == "live" else "DEMO / PRACTICE"
    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Project Exit Plan — BCO</title>
<style>
:root{{--bg:#0d1117;--panel:#161b22;--border:#30363d;--text:#f3f4f6;--muted:#aab2bf;--green:#54d98c;--amber:#f7c65d;--red:#ff7b72;--blue:#58a6ff;--summary:#2a1114;--summary2:#3b1519}}
*{{box-sizing:border-box}}body{{font-family:Arial,sans-serif;margin:0;padding:12px;background:var(--bg);color:var(--text)}}.page{{max-width:1900px;margin:auto}}h1{{margin:0 0 2px;font-size:clamp(28px,3.2vw,44px)}}h2{{margin:18px 0 10px}}h3{{padding:0 12px}}
.sub{{color:var(--muted);margin-bottom:10px;font-size:14px}}.banner{{padding:9px 12px;border-radius:9px;background:#12351f;border:1px solid #275c37;margin:8px 0;color:#d8ffe4;font-size:13px}}.top-status{{padding:9px 12px;border-radius:9px;background:#10263b;border:1px solid #1f4e73;margin:8px 0 12px;color:#d8ecff;font-size:14px}}
.cards{{display:grid;gap:6px;margin-bottom:6px}}.cards.four{{grid-template-columns:repeat(4,minmax(0,1fr))}}.cards.three{{grid-template-columns:repeat(3,minmax(0,1fr))}}.card{{background:var(--panel);border:1px solid var(--border);padding:9px 10px;border-radius:9px;min-height:75px;overflow:hidden}}.label,.k{{color:var(--muted);font-size:12px}}.value,.v{{font-size:clamp(18px,1.8vw,27px);font-weight:800;margin-top:4px}}.small{{color:var(--muted);font-size:11px;line-height:1.35;margin-top:3px}}.pos{{color:var(--green)!important;font-weight:800}}.neg{{color:var(--red)!important;font-weight:800}}.warn{{color:var(--amber)!important;font-weight:800}}
details{{background:var(--panel);border:1px solid var(--border);border-radius:10px;margin-bottom:9px;overflow:hidden}}details>summary{{cursor:pointer;padding:11px 13px;font-weight:800;font-size:15px;background:var(--summary);color:white;border-left:5px solid #6f1d27}}details>summary:hover{{background:var(--summary2)}}.lazy-placeholder,.section-note{{padding:12px;color:var(--muted);background:#11161d;line-height:1.5;border-bottom:1px solid var(--border)}}.lazy-loading{{padding:14px;color:#8ecbff;font-weight:700}}.lazy-error{{margin:10px;padding:12px;background:#2b1113;border:1px solid #5f2329;border-radius:8px;color:#ffb4ad}}.lazy-meta{{padding:7px 12px;background:#11161d;border-bottom:1px solid var(--border);color:var(--muted);font-size:11px}}
.metric-grid{{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:8px;padding:12px}}.mini-card{{background:#11161d;border:1px solid var(--border);border-radius:8px;padding:9px}}table{{width:100%;border-collapse:collapse;background:var(--panel)}}th,td{{padding:8px;border-bottom:1px solid var(--border);font-size:12px;text-align:left;vertical-align:top}}th{{background:#0f141a;color:white}}.table-scroll{{width:100%;overflow-x:auto}}a{{color:var(--blue);text-decoration:none}}.links{{margin:9px 0 14px;font-size:12px}}
@media(max-width:1000px){{.cards.four{{grid-template-columns:repeat(2,minmax(0,1fr))}}.cards.three{{grid-template-columns:repeat(3,minmax(0,1fr))}}}}@media(max-width:650px){{body{{padding:8px}}h1{{font-size:34px}}.cards{{gap:6px;margin-bottom:6px}}.cards.four{{grid-template-columns:repeat(2,minmax(0,1fr))}}.cards.three{{grid-template-columns:repeat(3,minmax(0,1fr))}}.card{{min-height:76px;padding:8px 9px}}.label,.k{{font-size:10px}}.value,.v{{font-size:18px}}.small{{font-size:9px}}details>summary{{font-size:13px;padding:9px 10px}}}}
</style></head><body><div class="page">
<h1>Project Exit Plan — BCO</h1><div class="sub">{esc(APP_VERSION)} Dashboard Parity · BCO LONG · {esc(env_label)}</div><div class="banner"><strong>{esc(env_label)}.</strong> Standalone BCO project. This service owns BCO only; indices and metals remain outside its management scope.</div>
<div id="topStatus" class="top-status">Loading top tiles…</div><div id="topTiles"><div class="cards four"><div class="card"><div class="label">Account NAV</div><div class="value">…</div></div><div class="card"><div class="label">BCO P&amp;L</div><div class="value">…</div></div><div class="card"><div class="label">Basket High-Water</div><div class="value">…</div></div><div class="card"><div class="label">Giveback</div><div class="value">…</div></div></div></div>
<div class="links"><a href="/dashboard-full">Full legacy dashboard</a><a href="/health">Health</a><a href="/snapshot">Broker control JSON</a></div><div class="export-actions"><a class="export-btn" href="/export/all.zip">⬇ BCO Analysis ZIP</a><a class="export-btn research" href="/export/bco-focused-research.zip">⬇ BCO Research ZIP</a></div><h2>Details</h2>{sections}</div>
<script>
function eh(v){{return String(v??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;')}}function money(v){{const n=Number(v);if(!Number.isFinite(n))return'n/a';return(n<0?'-':'')+'£'+Math.abs(n).toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}}function cls(v){{const n=Number(v);return!Number.isFinite(n)||n===0?'':(n>0?'pos':'neg')}}function card(l,v,s='',c=''){{return`<div class="card"><div class="label">${{eh(l)}}</div><div class="value ${{c}}">${{v}}</div><div class="small">${{s}}</div></div>`}}function localTime(iso){{if(!iso)return'';const d=new Date(iso);if(Number.isNaN(d.getTime()))return eh(iso);return new Intl.DateTimeFormat('en-GB',{{timeZone:'Europe/London',day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false,timeZoneName:'short'}}).format(d)}}
async function loadTop(force=false){{const st=document.getElementById('topStatus'),t0=performance.now();try{{const r=await fetch('/dashboard/top'+(force?'?force=true':''),{{cache:'no-store'}});const d=await r.json();if(!r.ok||d.status!=='ok')throw new Error(d.error||`HTTP ${{r.status}}`);const a=d.account||{{}},s=d.strategy||{{}},g=d.signals||{{}},c=d.config||{{}},ac=d.accounting||{{}};const gb=Number(s.giveback_pct||0),gbc=gb>=70?'neg':gb>=40?'warn':'pos';document.getElementById('topTiles').innerHTML=`
<div class="cards four">
${{card('NAV',money(a.nav),`Bal ${{money(a.balance)}} · Margin ${{money(a.margin_available)}}`)}}
${{card('Broker P&L',money(s.headline_pnl),`Open broker P&L · Realised ${{money(s.realized_pnl)}}`,cls(s.headline_pnl))}}
${{card('High-Water',money(s.high_water_gbp),`${{Number(s.high_water_r||0).toFixed(2)}}R · ${{s.high_water_time?localTime(s.high_water_time):'time not recorded'}}`,cls(s.high_water_gbp))}}
${{card('Giveback',`${{money(s.giveback_gbp)}} · ${{Number(s.giveback_pct||0).toFixed(1)}}%`,`${{Number(s.giveback_r||0).toFixed(2)}}R`,Number(s.giveback_pct||0)>=50?'neg':Number(s.giveback_pct||0)>=25?'warn':'pos')}}</div>
<div class="cards four">
${{card('This Week',money(ac.week_pnl),eh(ac.week_label||''),cls(ac.week_pnl))}}
${{card('This Month',money(ac.month_pnl),eh(ac.month_label||''),cls(ac.month_pnl))}}
${{card('Open Trades',eh(s.open_trades||0),`OANDA BCO · local ${{eh(s.local_open_trades||0)}}`)}}
${{card('48h+ Trades',eh(s.mature_48h_plus||0),`Oldest ${{eh(s.oldest_hold||0)}}h`)}}</div>
<div class="cards three">
${{card('Signal Health',g.processor_ok&&Number(g.received_assets||0)===1?'OK':'RECOVERING',g.processor_ok?(g.latest_time_display?`Latest BCO candle · ${{eh(g.latest_time_display)}}${{Number(g.legacy_unprocessed_count||0)>0?' · legacy audit gaps '+eh(g.legacy_unprocessed_count)+' (non-executable)':''}}`:'Waiting for BCO signal'):`Fresh pending ${{eh(g.processing_lag||0)}} · candidate pending ${{eh(g.pending_candidate_count||0)}} · auto-retry every ${{eh(g.recovery_interval_seconds||10)}}s${{g.latest_time_display?' · latest '+eh(g.latest_time_display):''}}`,g.processor_ok&&Number(g.received_assets||0)===1?'pos':'warn')}}
${{card('Signals',`${{eh(g.received_assets||0)}}/${{eh(g.expected_assets||1)}}`,Number(g.received_assets||0)===1?(g.latest_time_display?`Latest candle · ${{eh(g.latest_time_display)}}`:'Signal received'):(g.latest_time_display?`Waiting · latest ${{eh(g.latest_time_display)}}`:'Waiting'))}}
${{card('Candidate Support',g.candidate?'1/1':'0/1','BCO',g.candidate?'pos':'neg')}}</div>`;st.innerHTML=`<strong>Updated ${{localTime(d.time_utc)}} · loaded in ${{((performance.now()-t0)/1000).toFixed(2)}}s</strong>`}}catch(e){{st.innerHTML=`<span class="neg"><strong>Top tile load failed:</strong> ${{eh(e.message||e)}}</span>`}}}}
async function loadSection(d){{if(d.dataset.loaded==='1'||d.dataset.loading==='1')return;d.dataset.loading='1';const b=d.querySelector('.lazy-body');b.innerHTML='<div class="lazy-loading">Loading this section…</div>';try{{const r=await fetch('/dashboard/section/'+encodeURIComponent(d.dataset.section),{{cache:'no-store'}});const h=await r.text();if(!r.ok)throw new Error(h);b.innerHTML=h;d.dataset.loaded='1'}}catch(e){{b.innerHTML=`<div class="lazy-error">${{eh(e.message||e)}}</div>`}}finally{{d.dataset.loading='0'}}}}

async function startNewBCOBasketCycle(){{
 const box=document.getElementById('bco-new-cycle-result');
 const msg='Start a NEW BCO economic basket cycle?\\n\\nThis archives the old family HWM/harvest cycle and makes 50R the next harvest again.\\n\\nNO OANDA trades, stops, ages, Current Manager state or exit shadows will be changed.';
 if(!window.confirm(msg))return;
 const secret=window.prompt('Enter WEBHOOK_SECRET to confirm the BCO economic basket-cycle reset:','');
 if(!secret){{if(box){{box.style.display='block';box.textContent='Cancelled: WEBHOOK_SECRET not supplied.';}}return;}}
 if(box){{box.style.display='block';box.textContent='Starting new BCO economic basket cycle…';}}
 try{{
   const r=await fetch('/broker/start-new-basket-cycle',{{
     method:'POST',
     headers:{{'Content-Type':'application/json','x-webhook-secret':secret}},
     body:JSON.stringify({{confirm:'START_NEW_BCO_BASKET'}}),
     cache:'no-store'
   }});
   const txt=await r.text();
   let d={{}};try{{d=JSON.parse(txt)}}catch(_e){{}}
   if(!r.ok)throw new Error(d.detail||d.error||txt||`HTTP ${{r.status}}`);
   if(box){{
     box.innerHTML='<strong>New BCO basket cycle started.</strong> Previous HWM '
       +Number(d.previous_hwm_R||0).toFixed(2)+'R → new HWM '
       +Number(d.new_hwm_R||0).toFixed(2)+'R. Next harvest: '
       +Number(d.next_harvest_R||50).toFixed(0)+'R. No broker orders sent.';
   }}
   await loadTop(true);
   for(const key of ['broker','manager-protection']){{
     const el=document.querySelector(`details.lazy-section[data-section="${{key}}"]`);
     if(el){{el.dataset.loaded='0';if(el.open)await loadSection(el);}}
   }}
 }}catch(e){{
   if(box){{box.style.display='block';box.innerHTML='<span class="neg"><strong>Reset blocked:</strong> '+eh(e.message||e)+'</span>';}}
 }}
}}

document.querySelectorAll('details.lazy-section').forEach(d=>d.addEventListener('toggle',()=>{{if(d.open)loadSection(d)}}));loadTop(false);setInterval(()=>loadTop(true),60000);
</script></body></html>'''

@app.get("/dashboard-standard-status")
def bco_standard_status():
    return {
        "status":"ok","version":APP_VERSION,"project_standard":True,"project":"BCO",
        "environment":OANDA_ENV,"dashboard_mode":"dark_compact_lazy","legacy_dashboard":"/dashboard-full",
        "trading_logic_changed":True,
        "manager_contract":{
            "minimum_hold_hours":BCO_MIN_HOLD_HOURS,
            "hourly_post_48h_review":True,
            "immediate_banking_policy":{
                "first_level_R":BCO_BANK_FIRST_LEVEL_R,
                "step_R":BCO_BANK_STEP_R,
                "50R_fraction":BCO_BANK_50_FRACTION,
                "100R_fraction":BCO_BANK_100_FRACTION,
                "150R_plus_fraction":BCO_BANK_150_PLUS_FRACTION,
                "continues_every_50R":True,
                "target_basis":"remaining_profitable_open_pool",
                "selected_trades_need_48h":False,
            },
            "exceptional_pre48_cohort_layer":False,
            "manual_economic_basket_cycle_reset":True,
            "staged_defence":True,
            "exact_instrument_ownership":True,
            "exit_shadows":{
                "MFE_GIVEBACK_50":BCO_EXIT_SHADOW_ENABLED,
                "ATR2_CHANDELIER":BCO_EXIT_SHADOW_ENABLED,
                "execution_authority":False,
            },
        },
        "time_utc":now_utc_iso(),
    }
