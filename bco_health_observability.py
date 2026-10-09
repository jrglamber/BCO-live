"""BCO observability cleanup layered on the repaired runtime wrapper.

The runtime/schema repair can leave an old `signal_processing_error` event as
"last_processing_error" forever even after the missing research relation has
been created and current signals are fully processed. This module changes only
that health presentation: when the exit-shadow schema is demonstrably ready and
signal lag is zero, the old missing-relation error is marked resolved instead of
being shown as a live fault.

No trading, entry, stacking, exit, harvest, sizing, broker or AI rule changes.
"""

import bco_dashboard_age_fix as repaired

core = repaired.core
_original_operational_health = core.operational_health


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


# The registered core endpoint resolves this module-global function at request
# time, so replacing it here also fixes the dashboard health tile without
# touching any execution route.
core.operational_health = _operational_health_with_resolved_schema_error

# Reuse the already-repaired app and its startup worker forwarding unchanged.
app = repaired.app
