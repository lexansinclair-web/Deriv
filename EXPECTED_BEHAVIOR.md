# Expected Behavior — Strict Over 3

## Startup

When the dashboard opens, it loads the live Deriv account list if `DERIV_APP_ID` and `DERIV_API_TOKEN` are available. After an account is selected, it attempts to load active derived indices only from Deriv. Forex, commodities, stocks, and crypto are excluded. If the live catalogue cannot be loaded, the sidebar uses the local indices-only market list. Selecting a market does not place an order.

The dashboard shows numeric digit counts and rolling 4–9 versus 0–3 percentages. It does not render a predictive chart and does not fetch candle charts for the digit strategy.

## Review and entry sequence

The bot subscribes to raw ticks for the selected market and seeds its rolling buffer from recent tick history. It reviews the buffer once per minute. The default windows are 20, 50, and 200 ticks.

A review arms the Over 3 setup only if every enabled window passes the following gates:

| Gate | Default behavior |
|---|---|
| Minimum 4–9 share | 75% fast, 72% medium, and 68% slow. |
| Maximum 0–3 share | 25% fast, 28% medium, and 32% slow. |
| Per-digit advantage | Average 4–9 frequency per digit must exceed average 0–3 frequency per digit by at least 3 percentage points in every enabled window. |
| Review cadence | One evaluation per minute bucket. Each qualifying review resets the lower-confirmation sequence and establishes the boundary at the actual timestamp when that review executes. |
| Lower confirmation | Only ticks with an epoch strictly greater than the review boundary count toward the configured number of consecutive digits from 0 through 3. The default is one lower digit. |
| Higher-digit behavior | Any digit from 4 through 9 before completion kills the signal for that review window by default. Reset mode can be selected if the user deliberately wants a less restrictive timing gate. |
| Entry tick | The final required lower digit itself queues the Over 3 entry immediately; there is no extra gap. |

The default contract request is `DIGITOVER` with barrier `3`, duration `1` tick. The user can select `2` ticks. A two-tick contract settles on the last digit of its final expiry tick; it does not provide two separate chances.

The lower digits are only an entry-timing trigger. They do not replace the strict 4–9 concentration condition. After a signal is consumed, the strategy is reserved for that execution and cannot queue another signal until the trade result is finalized. After a contract finishes, it re-arms for a new confirmation sequence only when the last minute-reviewed condition is still valid. A later invalid review disarms it.

## Quote, order, and risk sequence

After a signal is queued, the engine requests a fresh Deriv proposal for the actual stake and validates the proposal ID, ask price, payout, and contract response. Ask price and payout are recorded for audit and used to submit the actual buy; they are not used as a forecast of profitability. A missing, invalid, unsupported, or rejected proposal prevents the buy and is recorded as a cancellation.

The digit profile defaults to a 1.10 recovery multiplier with a maximum of 10 recovery steps. Recovery never opens a second trade while the first is unresolved, and a loss result is applied before any later stake is read. The default session take-profit target is controlled globally. Cooldowns, daily caps, same-minute attempt protection, one-trade-at-a-time reservation, ambiguous-settlement stop, and real-account confirmation remain active.

## Account safety and journaling

Demo accounts can place virtual-fund orders. Real accounts remain blocked unless the account is recognized as real and `LIVE` is typed exactly. Unknown account types are monitoring-only. A connection error, invalid proposal, unsupported digit contract, or quote rejection does not silently become a buy.

If a buy receipt or contract settlement cannot be confirmed, the position is classified as `UNKNOWN`, the recovery plan is not advanced, and the engine stops for manual statement verification. Every review is recorded, including stand-asides. Digit records include the selected index, strategy mode, barrier, tick duration, quote precision, exact rolling counts, 4–9 and 0–3 percentages, per-digit averages, strictness thresholds, review boundary, confirmation evidence, proposal ask/payout, and rejection reason.

This guide describes implemented behavior; it is not a promise of profitability. The first real-world run should use a demo account, fixed stake, and a sufficiently long journal sample before enabling recovery sizing or real orders.
