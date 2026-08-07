"""Offline smoke test for GitHub/Railway package safety and shadow basket creation."""
import json
import os
import tempfile

os.environ.setdefault("OANDA_ENABLED", "false")
os.environ.setdefault("WEBHOOK_SECRET", "self-test")
os.environ.setdefault("ADMIN_SECRET", "self-test-admin")
os.environ["SQLITE_DEV_PATH"] = os.path.join(tempfile.gettempdir(), "bco_live_self_test.sqlite")
try:
    os.remove(os.environ["SQLITE_DEV_PATH"])
except FileNotFoundError:
    pass

import app

app.init_db()
assert app.safety_status()["orders_allowed"] is False

payload = {
    "timestamp": "2026-08-07 10:00",
    "pair": "BCOUSD",
    "signal_id": "BCO_SELF_TEST_1",
    "signal_side": "long",
    "exec_close": 80.0,
    "exec_high": 80.2,
    "exec_low": 79.8,
    "exec_close_gt_ema20": True,
    "exec_close_gt_ema50": True,
    "exec_hist_up": True,
    "exec_rsi_up": True,
    "d_bull": True,
    "contexts": [{
        "context_tf": "8H",
        "forward_test_candidate": True,
        "rule_trend_long_v1": True,
        "ctx_bull_stack": True,
    }],
}
rid, parsed, dup = app.store_signal(payload)
assert not dup
result = app.process_signal(rid, parsed)
assert result["entry_created"] is True
snap = app.snapshot()
assert len(snap["open_trades"]) == 1
assert snap["broker_safety"]["orders_allowed"] is False
print(json.dumps({
    "ok": True,
    "app": app.APP_NAME,
    "shadow_open_trades": len(snap["open_trades"]),
    "broker_orders_allowed": snap["broker_safety"]["orders_allowed"],
}, indent=2))
