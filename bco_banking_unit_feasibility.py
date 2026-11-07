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
