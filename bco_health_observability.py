"""BCO observability cleanup layered on the repaired runtime wrapper.

The runtime/schema repair can leave an old `signal_processing_error` event as
"last_processing_error" forever even after the missing research relation has
been created and current signals are fully processed. This module changes only
that health presentation and closes the historical alert once the repaired
schema is proven ready and signal lag is zero.

No trading, entry, stacking, exit, harvest, sizing, broker or AI rule changes.
"""

import threading
import time

import bco_dashboard_age_fix as repaired

core = repaired.core
_original_operational_health = core.operational_health

print("BCO health observability wrapper active", flush=True)


def _operational_health_with_resolved_schema_error():
    out = _original_operational_health()
    try:
        checks = out.get("checks") if isinstance(out, dict) else None
        signal = checks.get("signal_processing") if isinstance(checks, dict) else None
        if not isinstance(signal, dict):
            return out

        err = core.safe_str(signal.get("last_processing_error"))
        if not err or "bco_exit_challenger_shadow" not in err:
            return out

        schema = core.bco_exit_challenger_schema_status()
        lag = int(core.safe_float(signal.get("lag")) or 0)
        if bool(schema.get("ready")) and lag == 0:
            signal["last_processing_error_resolved"] = err
            signal["last_processing_error"] = ""
            signal["last_processing_error_state"] = "RESOLVED_SCHEMA_READY"
            signal["exit_shadow_schema_ready"] = True
            signal["exit_shadow_schema_checked_at_utc"] = schema.get("checked_at_utc")
    except Exception:
        # Observability cleanup must never affect service availability.
        pass
    return out


core.operational_health = _operational_health_with_resolved_schema_error

# Reuse the already-repaired app and its startup worker forwarding unchanged.
app = repaired.app


def _close_resolved_historical_schema_alert() -> None:
    """Write one empty current error marker only after the old fault is proven fixed.

    Core health historically reports the newest `signal_processing_error` event,
    so a repaired relation can otherwise leave an obsolete error visible forever.
    A future genuine processing error will naturally become the newest event and
    surface again.
    """
    for _ in range(360):
        try:
            if core.safe_str((getattr(core, "_bootstrap_state", {}) or {}).get("status")).upper() == "READY":
                break
        except Exception:
            pass
        time.sleep(0.5)

    try:
        raw = _original_operational_health()
        signal = ((raw.get("checks") or {}).get("signal_processing") or {}) if isinstance(raw, dict) else {}
        err = core.safe_str(signal.get("last_processing_error"))
        lag = int(core.safe_float(signal.get("lag")) or 0)
        schema = core.bco_exit_challenger_schema_status()
        if err and "bco_exit_challenger_shadow" in err and lag == 0 and bool(schema.get("ready")):
            core.log_event("signal_processing_error", "")
            core.log_event(
                "signal_processing_error_resolved",
                "Historical missing bco_exit_challenger_shadow relation alert cleared after schema-ready and zero-lag verification.",
            )
    except Exception as exc:
        try:
            core.log_event("observability_cleanup_error", str(exc))
        except Exception:
            pass


@app.on_event("startup")
def _start_observability_cleanup() -> None:
    threading.Thread(
        target=_close_resolved_historical_schema_alert,
        name="bco-observability-cleanup",
        daemon=True,
    ).start()
