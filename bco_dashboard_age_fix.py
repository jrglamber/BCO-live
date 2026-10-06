"""BCO dashboard age-source repair.

The BCO headline Open Trades count already comes from live OANDA, but the
48h+ count and Oldest age in the top dashboard are calculated from local
`trades.hold_candles`.  That can remain zero/stale even while broker trades are
open, which is the same display bug recently fixed on Indices.

This wrapper leaves all execution, stacking-brake, ATR2, harvest, risk and AI
logic untouched.  It only corrects the top-dashboard age fields using the
actual OANDA `openTime` for currently owned BCO trades.
"""
from datetime import datetime, timezone
from typing import Any, Optional

import live_promotions as _live

core = _live.core
_original_top_snapshot = core.bco_standard_top_snapshot


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

        # Broker exposure is authoritative.  If BCO is flat, both values are 0.
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


core.bco_standard_top_snapshot = _bco_top_snapshot_with_true_broker_age

# Preserve the existing v0.8.33 promotion wrapper/routes unchanged.
app = _live.app
