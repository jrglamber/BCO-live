"""BCO dashboard + runtime wrapper repairs.

Presentation repairs:
1) Use broker openTime for dashboard trade ages.
2) Make Candidate-vs-Trade-Opened explicit on mobile.

Runtime repairs:
3) The v0.8.33 outer FastAPI wrapper mounted the core app but did not forward
   the mounted app's startup event. That left schema bootstrap, broker recovery,
   reconciliation, live-HWM monitoring and accounting workers stopped.
4) Durable OANDA transaction sync must advance its cursor only to the last
   transaction actually processed. Advancing to OANDA's response-wide
   lastTransactionID after slicing a backlog can permanently skip close fills.
   This wrapper also repairs an already-jumped cursor from the durable ledger
   and drains the backlog once bootstrap is ready.

No entry, stacking-brake, ATR2, harvest, sizing, risk or AI decision rule is
changed here. The runtime repair restores the workers and authoritative broker
accounting the core already intended to run.
"""
from datetime import datetime, timezone
import json
import threading
import time
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
            strategy["trade_age_source"] = "local_hold_candles_fallback"
            strategy["trade_age_source_error"] = core.safe_str(live.get("error"))
            return out

        ages = []
        for trade in (live.get("owned_open_trades") or []):
            age = _broker_age_hours(trade.get("openTime"))
            if age is not None:
                ages.append(age)

        strategy["mature_48h_plus"] = sum(1 for age in ages if age >= 48)
        strategy["oldest_hold"] = max(ages) if ages else 0
        strategy["trade_age_source"] = "OANDA_openTime"
        strategy["trade_age_broker_open_count"] = int(live.get("owned_open_count") or 0)
        strategy["trade_age_parseable_count"] = len(ages)
    except Exception as exc:
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
        return (
            "<div class='section-note small'><strong>Execution key:</strong> "
            "Candidate TRUE is a signal only. Confirm a real trade with "
            "<strong>Entry Created = YES</strong> or the broker-backed Open Trades section. "
            f"<span class='small'>Banner detail unavailable: {core.esc(type(exc).__name__)}</span></div>"
        )


def _latest_signals_with_execution_key():
    return _latest_signal_execution_banner() + _original_latest_signals_combined_html()


# -----------------------------------------------------------------------------
# Durable broker transaction-sync repair
# -----------------------------------------------------------------------------
def _safe_sync_broker_transactions():
    """Incremental OANDA sync that never skips an unprocessed backlog page."""
    if not core.BCO_TRANSACTION_SYNC_ENABLED or not core.OANDA_ENABLED or not core.OANDA_ACCOUNT_ID:
        return {"ok": False, "skipped": True, "reason": "transaction sync disabled/unconfigured"}

    with core._db_lock, core.get_conn() as conn:
        cursor = core.runtime_get(conn, "broker_transaction_cursor", "")
        if not cursor:
            summary = core.account_summary()
            last_id = core.safe_str(summary.get("lastTransactionID"))
            if not last_id:
                return {"ok": False, "error": "unable to initialize transaction cursor"}
            try:
                cursor = str(max(0, int(float(last_id)) - 500))
            except Exception:
                cursor = last_id
            core.runtime_set(conn, "broker_transaction_cursor", cursor)

    resp = core.oanda_request(
        f"/v3/accounts/{core.OANDA_ACCOUNT_ID}/transactions/sinceid",
        "GET",
        params={"id": cursor},
    )
    if not resp.get("ok"):
        return {"ok": False, "error": resp.get("error"), "cursor": cursor}

    data = resp.get("data") or {}
    transactions = data.get("transactions") or []
    batch = transactions[:core.BCO_TRANSACTION_SYNC_PAGE_LIMIT]
    processed = 0
    matched_closes = 0
    financing_updates = 0
    capital_movements = 0.0
    last_processed = ""

    with core._db_lock, core.get_conn() as conn:
        for tx in batch:
            txid = core.safe_str(tx.get("id"))
            if not txid:
                continue
            tx_type = core.safe_str(tx.get("type")).upper()
            tx_time = core.safe_str(tx.get("time"))
            pl = float(core.safe_float(tx.get("pl")) or 0.0)
            financing = float(core.safe_float(tx.get("financing")) or 0.0)
            account_balance = core.safe_float(tx.get("accountBalance"))
            capital = 0.0
            if tx_type in {"TRANSFER_FUNDS", "DIVIDEND_ADJUSTMENT"}:
                capital = float(core.safe_float(tx.get("amount")) or 0.0)

            # Avoid replay side effects. Every transaction is durable/unique;
            # financing and capital are applied only when this tx is first stored.
            already = core.fetchone_dict(conn.execute(
                "SELECT id FROM broker_transactions WHERE transaction_id=? LIMIT 1",
                (txid,),
            )) or {}
            inserted = not bool(already)
            if inserted:
                conn.execute("""
                    INSERT INTO broker_transactions(
                        synced_at_utc,transaction_id,transaction_type,transaction_time,account_balance,
                        pl_home,financing_home,capital_movement_home,raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                """, (
                    core.now_utc_iso(), txid, tx_type, tx_time, account_balance,
                    pl, financing, capital, json.dumps(tx)[:50000],
                ))
                if capital:
                    capital_movements += capital

            if inserted and tx_type == "DAILY_FINANCING":
                for pos in (tx.get("positionFinancings") or []):
                    for tf in (pos.get("tradeFinancings") or []):
                        bid = core.safe_str(tf.get("tradeID"))
                        fin = float(core.safe_float(tf.get("financing")) or 0.0)
                        if bid and fin:
                            conn.execute("""
                                UPDATE trades
                                SET financing_home=COALESCE(financing_home,0)+?,updated_at_utc=?
                                WHERE broker_trade_id=?
                            """, (fin, core.now_utc_iso(), bid))
                            financing_updates += 1

            # Always allow an ORDER_FILL replay to heal a local trade that was
            # missed when the cursor jumped. The closed-state check makes this
            # idempotent for trades already accounted.
            if tx_type == "ORDER_FILL":
                components = []
                if isinstance(tx.get("tradeClosed"), dict):
                    components.append(tx.get("tradeClosed"))
                components.extend([x for x in (tx.get("tradesClosed") or []) if isinstance(x, dict)])
                if isinstance(tx.get("tradeReduced"), dict):
                    components.append(tx.get("tradeReduced"))
                for comp in components:
                    bid = core.safe_str(comp.get("tradeID"))
                    if not bid:
                        continue
                    tr = core.fetchone_dict(conn.execute(
                        "SELECT * FROM trades WHERE broker_trade_id=? LIMIT 1",
                        (bid,),
                    )) or {}
                    if not tr:
                        continue
                    status = core.safe_str(tr.get("status")).upper()
                    if status in {"CLOSED", "BROKER_CLOSED"} and core.safe_float(tr.get("broker_realized_pl_home")) is not None:
                        continue
                    cpl = float(core.safe_float(comp.get("realizedPL")) or core.safe_float(comp.get("pl")) or pl or 0.0)
                    cfin = float(core.safe_float(comp.get("financing")) or financing or 0.0)
                    close_info = {
                        "transaction_id": txid,
                        "price": core.safe_float(tx.get("price")) or core.safe_float(comp.get("price")),
                        "pl_home": cpl,
                        "financing_home": cfin,
                        "account_balance": account_balance,
                        "raw_fill": tx,
                    }
                    core.mark_trade_closed_from_broker(
                        conn,
                        tr,
                        tx_time or core.now_utc_iso(),
                        core.safe_str(tr.get("exit_reason") or "broker_transaction_sync"),
                        close_info,
                    )
                    matched_closes += 1

            processed += 1
            last_processed = txid

        response_last = core.safe_str(data.get("lastTransactionID"))
        # Critical repair: if OANDA returned more rows than our bounded batch,
        # advance only to the final row ACTUALLY processed. The next pass then
        # starts exactly where this one stopped rather than jumping over fills.
        if last_processed:
            new_cursor = last_processed
        elif response_last:
            new_cursor = response_last
        else:
            new_cursor = cursor
        if new_cursor:
            core.runtime_set(conn, "broker_transaction_cursor", new_cursor)
        core.runtime_set(conn, "broker_transaction_sync_at", core.now_utc_iso())
        if capital_movements:
            core.runtime_set(
                conn,
                "broker_capital_movements_total",
                str(float(core.runtime_get(conn, "broker_capital_movements_total", "0") or 0.0) + capital_movements),
            )
        core.finalize_pending_harvest_stages(conn)

    backlog_remaining = bool(response_last and new_cursor and str(new_cursor) != str(response_last))
    return {
        "ok": True,
        "processed": processed,
        "matched_closes": matched_closes,
        "financing_updates": financing_updates,
        "capital_movements": capital_movements,
        "cursor": new_cursor or cursor,
        "response_last_transaction_id": response_last or None,
        "backlog_remaining": backlog_remaining,
        "time_utc": core.now_utc_iso(),
    }


def _repair_jumped_transaction_cursor():
    """Rewind only an impossible cursor: ahead of this account's stored ledger."""
    if not core.OANDA_ACCOUNT_ID:
        return {"ok": True, "repaired": False, "reason": "no_account_id"}
    with core._db_lock, core.get_conn() as conn:
        cursor_text = core.runtime_get(conn, "broker_transaction_cursor", "")
        try:
            cursor_num = int(float(cursor_text))
        except Exception:
            return {"ok": True, "repaired": False, "reason": "cursor_not_numeric", "cursor": cursor_text}

        rows = core.fetchall_dict(conn.execute(
            "SELECT transaction_id FROM broker_transactions WHERE raw_json LIKE ?",
            (f"%{core.OANDA_ACCOUNT_ID}%",),
        ))
        stored = []
        for row in rows:
            try:
                stored.append(int(float(core.safe_str(row.get("transaction_id")))))
            except Exception:
                pass
        if not stored:
            return {"ok": True, "repaired": False, "reason": "no_stored_transactions", "cursor": cursor_num}
        max_stored = max(stored)
        if cursor_num <= max_stored:
            return {"ok": True, "repaired": False, "cursor": cursor_num, "max_stored": max_stored}

        core.runtime_set(conn, "broker_transaction_cursor", str(max_stored))
        result = {
            "ok": True,
            "repaired": True,
            "previous_cursor": cursor_num,
            "new_cursor": max_stored,
            "reason": "cursor_ahead_of_last_durable_transaction",
        }
    try:
        core.log_event(
            "broker_transaction_cursor_gap_repaired",
            "Rewound OANDA transaction cursor to the last transaction actually stored so skipped fills can replay.",
            result,
        )
    except Exception:
        pass
    return result


def _post_bootstrap_broker_recovery():
    """Drain any repaired backlog once migrations/workers have started."""
    for _ in range(360):
        status = core.safe_str((getattr(core, "_bootstrap_state", {}) or {}).get("status")).upper()
        if status in {"READY", "FAILED"}:
            break
        time.sleep(0.5)
    if core.safe_str((getattr(core, "_bootstrap_state", {}) or {}).get("status")).upper() != "READY":
        return

    try:
        repair = _repair_jumped_transaction_cursor()
        pages = 0
        matched = 0
        for _ in range(40):
            out = _safe_sync_broker_transactions()
            if not out.get("ok"):
                break
            pages += 1
            matched += int(out.get("matched_closes") or 0)
            if not out.get("backlog_remaining"):
                break
            time.sleep(0.1)

        # Reconcile exposure after close fills are accounted, then clear any
        # queued stop/close action whose local trade is no longer OPEN.
        reconcile = core.reconcile_broker()
        queue = core.process_broker_action_queue()
        try:
            core.record_accounting_snapshot()
        except Exception:
            pass
        try:
            core.log_event(
                "startup_broker_backlog_recovery",
                "BCO startup repaired and drained durable broker accounting backlog.",
                {"repair": repair, "pages": pages, "matched_closes": matched,
                 "reconcile": reconcile, "queue": queue},
            )
        except Exception:
            pass
    except Exception as exc:
        try:
            core.log_event("startup_broker_backlog_recovery_error", str(exc))
        except Exception:
            pass


# Install the safe sync before core bootstrap/workers are started.
core.sync_broker_transactions = _safe_sync_broker_transactions
core.bco_standard_top_snapshot = _bco_top_snapshot_with_true_broker_age

# The lazy-section endpoint resolves its renderer from this map at request time.
try:
    title, _old_renderer = core._BCO_STD_SECTIONS["latest-signals"]
    core._BCO_STD_SECTIONS["latest-signals"] = (title, _latest_signals_with_execution_key)
except Exception:
    pass

# Preserve the existing v0.8.33 promotion wrapper/routes.
app = _live.app


@app.on_event("startup")
def _start_mounted_core_runtime_and_recover_broker_state() -> None:
    """Forward startup into the mounted core app, then heal any skipped backlog."""
    try:
        _repair_jumped_transaction_cursor()
    except Exception:
        pass
    # core.startup_event is internally idempotent, so this remains safe if the
    # wrapper topology changes later and the core startup event also fires.
    core.startup_event()
    threading.Thread(
        target=_post_bootstrap_broker_recovery,
        name="bco-wrapper-broker-recovery",
        daemon=True,
    ).start()
