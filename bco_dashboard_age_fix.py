"""BCO dashboard presentation repairs.

1) The BCO headline Open Trades count already comes from live OANDA, but the
48h+ count and Oldest age in the top dashboard were calculated from local
`trades.hold_candles`. That can remain zero/stale even while broker trades are
open, so those fields are repaired from actual OANDA `openTime`.

2) The latest-signals table is a signal/decision audit, not a trade ledger. On a
narrow mobile screen the decisive `Entry Created` column sits off-screen, which
made a green Candidate=TRUE row look like a new trade. Add an always-visible
execution key and latest-signal execution banner so Candidate TRUE can no longer
be mistaken for a broker fill.

Both repairs are presentation-only. Execution, stacking-brake, ATR2, harvest,
risk and AI logic are untouched.
"""
from datetime import datetime, timezone
from typing import Any, Optional

import live_promotions as _live

core = _live.core
_original_top_snapshot = core.bco_standard_top_snapshot
_original_latest_signals_combined_html = core._bco_standard_latest_signals_combined_html


def _parse_oanda_time(value: Any) -> Optional[datetime]:
    s = core.safe_str(value)
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _broker_age_hours(open_time: Any) -> Optional[int]:
    dt = _parse_oanda_time(open_time)
    if dt is None:
        return None
    seconds = (datetime.now(timezone.utc) - dt).total_seconds()
    if seconds < 0:
        return 0
    return int(seconds // 3600)


def _bco_top_snapshot_with_true_broker_age(force: bool = False):
    out = _original_top_snapshot(force=force)
    try:
        if not isinstance(out, dict) or out.get("status") != "ok":
            return out
        strategy = out.get("strategy")
        if not isinstance(strategy, dict):
            return out

        live = core.bco_owned_open_trades_snapshot()
        if not live.get("ok"):
            # Fail safe: retain the original dashboard fields if OANDA is
            # temporarily unreadable rather than inventing an age.
            strategy["trade_age_source"] = "local_hold_candles_fallback"
            strategy["trade_age_source_error"] = core.safe_str(live.get("error"))
            return out

        ages = []
        for trade in (live.get("owned_open_trades") or []):
            age = _broker_age_hours(trade.get("openTime"))
            if age is not None:
                ages.append(age)

        # Broker exposure is authoritative. If BCO is flat, both values are 0.
        # If open trades exist but one somehow lacks a usable openTime, only the
        # parseable broker trades contribute rather than falling back to stale
        # local hold_candles for the whole basket.
        strategy["mature_48h_plus"] = sum(1 for age in ages if age >= 48)
        strategy["oldest_hold"] = max(ages) if ages else 0
        strategy["trade_age_source"] = "OANDA_openTime"
        strategy["trade_age_broker_open_count"] = int(live.get("owned_open_count") or 0)
        strategy["trade_age_parseable_count"] = len(ages)
    except Exception as exc:
        # Presentation-only repair must never impair the trading service.
        try:
            strategy = out.get("strategy") if isinstance(out, dict) else None
            if isinstance(strategy, dict):
                strategy["trade_age_source"] = "local_hold_candles_fallback"
                strategy["trade_age_source_error"] = f"{type(exc).__name__}: {exc}"
        except Exception:
            pass
    return out


def _latest_signal_execution_banner() -> str:
    """Mobile-visible distinction between a candidate signal and a real entry."""
    try:
        with core.get_conn() as conn:
            row = core.fetchone_dict(conn.execute(
                "SELECT id,timestamp_readable,forward_test_candidate,candidate_8h,signal_side "
                "FROM raw_signals ORDER BY id DESC LIMIT 1"
            )) or {}
            raw_id = int(core.safe_float(row.get("id")) or 0)
            decision = core.fetchone_dict(conn.execute(
                "SELECT entry_allowed,entry_created,manager_action,note "
                "FROM basket_decisions WHERE raw_signal_id=? ORDER BY id DESC LIMIT 1",
                (raw_id,),
            )) or {}

        # Match the existing signal table's candidate semantics.
        candidate = core.parse_bool(row.get("forward_test_candidate")) or core.parse_bool(row.get("candidate_8h"))
        allowed = core.parse_bool(decision.get("entry_allowed"))
        created = core.parse_bool(decision.get("entry_created"))
        signal_time = core.safe_str(row.get("timestamp_readable")) or "latest"
        side = (core.safe_str(row.get("signal_side")) or "-").upper()
        reason = core.safe_str(decision.get("note") or decision.get("manager_action") or "")
        if len(reason) > 220:
            reason = reason[:217] + "..."

        candidate_text = "TRUE" if candidate else "FALSE"
        allowed_text = "YES" if allowed else "NO"
        created_text = "YES — BROKER ENTRY CREATED" if created else "NO — NO NEW TRADE"
        created_class = "pos" if created else "neg"
        reason_html = f"<div class='small'>{core.esc(reason)}</div>" if reason else ""

        return f"""
        <div class='section-note small'>
          <strong>Execution key:</strong> Candidate TRUE means the signal qualifies; it does <strong>not</strong> mean a trade opened.
          A new BCO trade exists only when <strong>Entry Created = YES</strong>. Portfolio Hub “Last opened” is the latest broker-confirmed fill.
        </div>
        <div class='metric-grid'>
          <div class='mini-card'><div class='k'>Latest Signal</div><div class='v small'>{core.esc(signal_time)}</div><div class='small'>{core.esc(side)}</div></div>
          <div class='mini-card'><div class='k'>Candidate Signal</div><div class='v'>{candidate_text}</div><div class='small'>Research/entry qualification</div></div>
          <div class='mini-card'><div class='k'>Entry Allowed</div><div class='v'>{allowed_text}</div><div class='small'>Pre-execution decision</div></div>
          <div class='mini-card'><div class='k'>Trade Opened?</div><div class='v {created_class}'>{created_text}</div>{reason_html}</div>
        </div>
        """
    except Exception as exc:
        # Never let a presentation aid interfere with dashboard availability.
        return (
            "<div class='section-note small'><strong>Execution key:</strong> "
            "Candidate TRUE is a signal only. Confirm a real trade with "
            "<strong>Entry Created = YES</strong> or the broker-backed Open Trades section. "
            f"<span class='small'>Banner detail unavailable: {core.esc(type(exc).__name__)}</span></div>"
        )


def _latest_signals_with_execution_key():
    return _latest_signal_execution_banner() + _original_latest_signals_combined_html()


core.bco_standard_top_snapshot = _bco_top_snapshot_with_true_broker_age

# The lazy-section endpoint resolves its renderer from this map at request time,
# so update that renderer without touching any execution route.
try:
    title, _old_renderer = core._BCO_STD_SECTIONS["latest-signals"]
    core._BCO_STD_SECTIONS["latest-signals"] = (title, _latest_signals_with_execution_key)
except Exception:
    pass

# Preserve the existing v0.8.33 promotion wrapper/routes unchanged.
app = _live.app
