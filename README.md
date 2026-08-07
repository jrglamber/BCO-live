# BCO Live

Separate production service for Project Exit Plan Brent/BCO execution.

## Architecture

- **BCO Research Hub** remains independent and continues research + forward live-sim.
- **BCO Live** receives the same fresh TradingView BCO hourly alert directly.
- This service may share the same OANDA account/NAV as the index service.
- It owns **BCO only**. Every broker write is hard-checked against the exact configured BCO instrument.
- Initial strategy is **LONG only**.

## Live v1 policy

- Requested risk: **£5/R**
- Emergency SL: **3.5%**
- Minimum normal hold: **48h**
- Review every new hourly signal after 48h
- 48/72 extension gates; 96/120 are runner-protection milestones, not forced exits
- Managed protection: 25% / 50% / 65% / 75%
- Basket phases: 1–9 tiny, 10–24 early, 25–49 developing, 50–99 mature, 100+ heavy
- Immediate basket bank stages: 100R / 200R / 300R
- Pre-48 cohort ratchets: 150R / 200R / 300R
- Execution multiplier hard-locked at **1.00x**
- Fresh signals only; no historical/backlog import into the live book

## First deployment — keep broker locked

Create a Railway project from this GitHub repo and add a Railway Postgres database.
Copy `.env.example` into Railway variables, replacing placeholders only in Railway.

Initial broker flags must remain:

```text
BROKER_READ_ONLY=true
BROKER_EXECUTION_ENABLED=false
BROKER_KILL_SWITCH=true
BCO_LIVE_EXECUTION_ARMED=false
BCO_AUTO_ENTRY_ENABLED=false
BCO_AUTO_MANAGEMENT_ENABLED=false
BCO_PRACTICE_SMOKE_TEST_ENABLED=false
```

After deployment:

1. `/health`
2. `/broker/preflight`
3. `/broker/instruments/discover`
4. Set `BCO_OANDA_INSTRUMENT` to the exact OANDA Brent instrument returned by discovery.
5. `/broker/risk-preview`
6. Only after the sizing preview is sensible do the one-off **practice** smoke test.

## TradingView webhook

Create a separate BCO TradingView alert for this service:

```text
https://YOUR-BCO-LIVE.up.railway.app/webhook/tradingview?secret=YOUR_WEBHOOK_SECRET
```

The app stores all BCO hourly observations for management, but opens only the existing 8H LONG candidate:

- 8H `forward_test_candidate=true`
- `rule_trend_long_v1=true` when supplied
- `signal_side=long` (or blank legacy side)

## Practice smoke test

Do **not** use these routes until the exact BCO instrument and risk preview are confirmed.
The route requires both the admin header and all normal broker safety gates.

Enable temporarily on practice only:

```text
OANDA_ENV=practice
BROKER_READ_ONLY=false
BROKER_EXECUTION_ENABLED=true
BROKER_KILL_SWITCH=false
BCO_LIVE_EXECUTION_ARMED=true
BCO_PRACTICE_SMOKE_TEST_ENABLED=true
```

Open:

```text
POST /admin/broker/practice-smoke-open
X-Admin-Secret: <ADMIN_SECRET>
```

Close using the returned broker trade ID:

```text
POST /admin/broker/practice-smoke-close/<BROKER_TRADE_ID>
X-Admin-Secret: <ADMIN_SECRET>
```

Immediately relock after testing.

## Promotion to real execution

Real auto-entry is still a deliberate second gate. It requires all broker locks open **and**:

```text
BCO_AUTO_ENTRY_ENABLED=true
BCO_AUTO_MANAGEMENT_ENABLED=true
```

Before doing that, confirm:

- BCO research live-sim has produced real 48h+ manager cycles.
- Practice BCO open / SL / close works.
- Effective GBP risk is acceptable at OANDA minimum size.
- Reconciliation sees only BCO-owned trades.
- No duplicate TradingView entries.

## Exports

`/export/all.zip` includes live BCO signals, trades, decisions, managed-stop events, banking/protection stages, execution audit and system events.
