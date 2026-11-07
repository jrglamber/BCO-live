"""Research-only broker-unit feasibility for BCO partial banking.

No broker requests or execution authority. Integer-unit assumption must be
validated against the instrument's current broker minimum and increment.
"""
from math import floor, isfinite


def partial_close_feasibility(units, fraction, min_units=1, increment=1):
    """Return a conservative close size, or explain why a partial is impossible.

    Never round up: doing so can exceed the intended fraction of risk.
    A full close is not classified as a partial bank.
    """
    try:
        units, fraction = float(units), float(fraction)
        min_units, increment = float(min_units), float(increment)
    except (ValueError, TypeError):
        return {"feasible": False, "reason": "invalid_input", "close_units": 0}
    if not all(isfinite(v) for v in (units, fraction, min_units, increment)) or units <= 0 or not 0 < fraction < 1 or min_units <= 0 or increment <= 0:
        return {"feasible": False, "reason": "invalid_input", "close_units": 0}
    target = units * fraction
    steps = floor((target - min_units) / increment + 1e-10)
    close_units = min_units + steps * increment if steps >= 0 else 0
    if close_units < min_units - 1e-9:
        return {"feasible": False, "reason": "below_minimum_close", "close_units": 0}
    if units - close_units < min_units - 1e-9:
        return {"feasible": False, "reason": "insufficient_remaining_units", "close_units": 0}
    return {"feasible": True, "reason": "partial_close_possible", "close_units": close_units,
            "effective_fraction": close_units / units}


def whole_trade_basket_bank(trades, fraction):
    """Research-only subset of profitable trades, without fractional broker units.

    trades: broker trade IDs, units, and currently *unrealised* net GBP P&L.
    Uses a deterministic greedy subset, not an optimal profit maximiser.
    A trade must be profitable and fully closable; no live orders are sent.
    """
    try:
        fraction=float(fraction)
        if not isfinite(fraction) or not 0<fraction<1: raise ValueError()
    except (TypeError, ValueError):
        return {"feasible":False,"reason":"invalid_fraction","selected":[]}
    eligible=[]; seen=set()
    for t in trades:
        try:
            tid=str(t["broker_trade_id"])
            units=float(t["units"]); pnl=float(t["unrealised_net_gbp"])
            if not tid.strip() or tid.lower() in {"none", "null"} or tid in seen or not all(isfinite(x) for x in (units,pnl)) or units<=0:
                return {"feasible":False,"reason":"invalid_or_duplicate_trade","selected":[]}
            seen.add(tid)
            if pnl>0: eligible.append((tid,units,pnl))
        except (KeyError,TypeError,ValueError):
            return {"feasible":False,"reason":"invalid_trade","selected":[]}
    target=sum(x[2] for x in eligible)*fraction
    if target<=0: return {"feasible":False,"reason":"no_profitable_trades","selected":[]}
    # Smallest profitable positions first to avoid oversized profit banking.
    selected=[]; banked=0.0
    for tid,units,pnl in sorted(eligible,key=lambda x:(x[2],x[0])):
        if banked+pnl<=target+1e-9:
            selected.append({"broker_trade_id":tid,"units":units,"unrealised_net_gbp":pnl})
            banked+=pnl
    return {"feasible":bool(selected),"reason":"whole_trade_subset" if selected else "target_below_smallest_profitable_trade",
            "selected":selected,"target_gbp":target,"banked_gbp_before_costs":banked,
            "effective_fraction":banked/sum(x[2] for x in eligible)}
