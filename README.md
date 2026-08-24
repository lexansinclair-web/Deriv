# MomentumMaster Digit — Strict Over 3

MomentumMaster Digit is a Streamlit terminal for Deriv digit contracts. It supports manual selection from the live market catalogue, rolling last-digit reviews, and automated **Over 3** entries for one- or two-tick contracts.

## Strategy behavior

The bot collects raw quotes and extracts the final displayed digit using the selected symbol’s quote precision. Every minute it reviews fast, medium, and slow rolling windows. The default windows are 20, 50, and 200 ticks.

A review arms the Over 3 setup only when every enabled window passes all strict gates: digits 4–9 must have a high combined share, digits 0–3 must remain below the corresponding maximum share, and the average frequency per digit for 4–9 must exceed the average frequency per digit for 0–3 by the configured minimum gap. The conservative defaults are 75% / 72% / 68% minimum 4–9 share for fast / medium / slow, 25% / 28% / 32% maximum 0–3 share, and a 3 percentage-point per-digit advantage. These defaults are deliberately selective because Over 3 has a larger winning digit region and normally offers lower returns.

The strategy does not infer an edge from payout. It waits for the rolling digit evidence and then applies account, proposal, and settlement safeguards. The lower digits 0–3 are used only as the post-review entry-timing confirmation. With the default one confirmation, the first qualifying 0–3 tick after the review boundary queues a `DIGITOVER` proposal with barrier `3`. Any 4–9 digit before the required confirmation completes kills the signal for that review window by default.

## Markets and controls

The sidebar loads active derived indices only from Deriv when credentials and an account are available. Forex, commodities, stocks, and crypto are excluded. A local indices-only catalogue remains available as a fallback. You can manually select the market, one- or two-tick duration, lower-confirmation count, minimum 4–9 share, maximum 0–3 share, per-digit advantage, starting stake, recovery multiplier, maximum recovery steps, and a positive session take-profit target.

The dashboard exposes the strictness settings per rolling window. Disabled windows are ignored. At least one window must remain enabled. The journal records the exact counts, high/low group percentages, per-digit averages, rejection reason, review boundary, confirmation evidence, proposal ask/payout, and settlement outcome.

## Safety behavior

Demo accounts are allowed to trade virtual funds. Real accounts remain blocked unless the account is recognized as real and `LIVE` is typed exactly in the sidebar. Unknown account types remain monitoring-only. If a buy or settlement cannot be confirmed, the trade is classified as unknown and the engine stops for manual statement verification.

## Setup

Install the dependencies in `requirements.txt`, provide `DERIV_APP_ID` and `DERIV_API_TOKEN` through Streamlit Secrets or the project environment, and start the dashboard with:

```bash
streamlit run dashboard.py
```

Use a demo account, fixed stake, and a substantial journal sample before considering recovery sizing or real orders. This software is experimental and does not guarantee profit. A high recent 4–9 percentage is a hypothesis about the recent tape, not a promise about the next contract.

## Project structure

| File | Purpose |
|---|---|
| `dashboard.py` | Streamlit controls, market selector, strict digit review display, trade ledger, journal download, and safety controls. |
| `src/digit_strategy.py` | Rolling digit counts, strict 4–9 versus 0–3 qualification, lower-tick confirmation, and signal state machine. |
| `src/trading_engine.py` | Deriv connection, tick subscription, proposal/buy flow, settlement monitoring, coordination, and reconnection. |
| `src/coordination.py` | Cross-session per-account/market reservation, cooldown, daily-cap, recovery, and unknown-settlement coordination. |
| `src/api_client.py` | Current Deriv account, market, tick, proposal, buy, and open-contract requests. |
| `src/state_manager.py` | Thread-safe runtime state, trade accounting, cooldowns, and recovery state. |
| `src/journal.py` | Append-only decision and outcome journal with digit-specific fields. |
| `EXPECTED_BEHAVIOR.md` | Exact operational behavior and entry-state sequence. |
