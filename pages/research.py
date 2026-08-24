"""pages/research.py — Digit Research Lab with full bot-lifecycle backtest.

Read-only research page.
Places no trades.
Does not modify live strategy behavior.
Can run alongside the live dashboard bot.
"""
from __future__ import annotations

import calendar
import html
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)

if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from config import (
    AVAILABLE_MARKETS,
    DEFAULT_MARKET_DISPLAY,
    DERIV_APP_ID,
    DERIV_API_TOKEN,
)
from src import research_lab as rl
from src.journal import get_journal
from src.persistence import (
    export_archive_csv_bytes,
    export_merged_json_bytes,
    import_journal,
)

st.set_page_config(
    page_title="Digit Research Lab",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    html, body, .stApp {
        background: #060912;
        color: #c7d2e0;
        font-family: Inter, system-ui, sans-serif;
    }

    [data-testid="stSidebar"] {
        background: #0a0f1c;
        border-right: 1px solid #1b2740;
    }

    [data-testid="stMainBlockContainer"] {
        max-width: 1520px;
        padding-top: 1.1rem;
    }

    .rl-panel {
        background: linear-gradient(160deg, #0c1322, #0a101d);
        border: 1px solid #18233a;
        border-radius: 15px;
        padding: 16px 18px;
        margin-bottom: 14px;
    }

    .rl-h {
        font-size: .68rem;
        font-weight: 800;
        letter-spacing: .16em;
        text-transform: uppercase;
        color: #8294b0;
        margin-bottom: 10px;
    }

    .rl-kpis {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 12px;
        margin: 4px 0 16px 0;
    }

    @media (max-width: 1100px) {
        .rl-kpis {
            grid-template-columns: repeat(3, 1fr);
        }
    }

    .rl-kpi {
        background: linear-gradient(160deg, #0c1322, #0a101d);
        border: 1px solid #18233a;
        border-radius: 14px;
        padding: 12px 14px;
        border-left: 3px solid var(--ac, #33507e);
    }

    .rl-kpi-l {
        font-size: .58rem;
        font-weight: 700;
        letter-spacing: .14em;
        text-transform: uppercase;
        color: #6b7c97;
    }

    .rl-kpi-v {
        font-family: monospace;
        font-weight: 800;
        font-size: 1.3rem;
        color: #eef3fb;
        margin-top: 7px;
    }

    .rl-kpi-s {
        font-family: monospace;
        font-size: .64rem;
        color: #6b7c97;
        margin-top: 4px;
    }

    .pos { color: #4ade80; }
    .neg { color: #fb7185; }
    .mut { color: #6b7c97; }

    .cal {
        width: 100%;
        border-collapse: collapse;
        font-family: monospace;
        table-layout: fixed;
    }

    .cal th {
        color: #6b7c97;
        font-size: .65rem;
        padding: 6px;
        text-transform: uppercase;
        border-bottom: 1px solid #18233a;
    }

    .cal td {
        border: 1px solid #18233a;
        height: 70px;
        vertical-align: top;
        padding: 6px;
        background: #0a101d;
    }

    .cal td.empty {
        background: transparent;
        border: none;
    }

    .cal .day {
        font-weight: 700;
        color: #8294b0;
    }

    .cal .cellpnl {
        font-size: .7rem;
        margin-top: 6px;
    }

    .pos-day {
        background: rgba(34, 197, 94, 0.12);
        border-left: 3px solid #22c55e;
    }

    .neg-day {
        background: rgba(239, 68, 68, 0.12);
        border-left: 3px solid #ef4444;
    }

    .rev-day {
        background: rgba(56, 132, 255, 0.08);
        border-left: 3px solid #3884ff;
    }

    .none {
        color: #42506b;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _kpi(label: str, value: str, sub: str, accent: str) -> str:
    return (
        f'<div class="rl-kpi" style="--ac:{html.escape(accent)}">'
        f'<div class="rl-kpi-l">{html.escape(label)}</div>'
        f'<div class="rl-kpi-v">{html.escape(value)}</div>'
        f'<div class="rl-kpi-s">{html.escape(sub)}</div>'
        f"</div>"
    )


def _table(df: pd.DataFrame, height: int = 320) -> None:
    if df is None or df.empty:
        st.info("No rows yet.")
        return

    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


def _render_month_calendar(year: int, month: int, days: dict) -> str:
    cal = calendar.Calendar(firstweekday=0)
    parts = []

    parts.append('<table class="cal"><thead><tr>')

    for day_name in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"):
        parts.append(f"<th>{day_name}</th>")

    parts.append("</tr></thead><tbody>")

    for week in cal.monthdayscalendar(year, month):
        parts.append("<tr>")

        for day in week:
            if day == 0:
                parts.append('<td class="empty"></td>')
                continue

            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            row = days.get(date_str)

            if row and int(row.get("closed", 0)) > 0:
                pnl = float(row.get("pnl", 0.0))
                css = "pos-day" if pnl >= 0 else "neg-day"

                title = (
                    f"P&L {pnl:+.2f} | trades {row.get('closed', 0)} | "
                    f"win rate {row.get('win_rate_pct', 0)}%"
                )

                parts.append(
                    f'<td class="{css}" title="{html.escape(title, quote=True)}">'
                    f'<div class="day">{day}</div>'
                    f'<div class="cellpnl">{pnl:+.2f}</div>'
                    f"</td>"
                )
            elif row and int(row.get("reviews", 0)) > 0:
                title = f"reviews {row.get('reviews', 0)} | arms {row.get('arms', 0)}"

                parts.append(
                    f'<td class="rev-day" title="{html.escape(title, quote=True)}">'
                    f'<div class="day">{day}</div>'
                    f'<div class="cellpnl">reviews</div>'
                    f"</td>"
                )
            else:
                parts.append(
                    f'<td class="none"><div class="day">{day}</div></td>'
                )

        parts.append("</tr>")

    parts.append("</tbody></table>")

    return "".join(parts)


@st.cache_data(ttl=5, show_spinner=False)
def _db_token():
    try:
        if os.path.exists(rl.RESEARCH_DB):
            stat = os.stat(rl.RESEARCH_DB)
            return rl.RESEARCH_DB, stat.st_mtime, stat.st_size
    except OSError:
        pass

    return rl.RESEARCH_DB, 0, 0


@st.cache_data(ttl=10, show_spinner=False)
def _journal_token():
    journal = get_journal()
    tokens = []

    for path in (getattr(journal, "_live", ""), getattr(journal, "_archive", "")):
        try:
            if path and os.path.exists(path):
                stat = os.stat(path)
                tokens.append((path, stat.st_mtime, stat.st_size))
            else:
                tokens.append((path, 0, 0))
        except OSError:
            tokens.append((path, 0, 0))

    return tuple(tokens)


@st.cache_data(ttl=30, show_spinner=False)
def _tape_summary(token):
    return rl.tape_summary()


@st.cache_data(ttl=20, show_spinner=False)
def _actual_rows(token):
    return rl.read_digit_journal_rows()


@st.cache_data(ttl=300, show_spinner=False)
def _load_accounts(app_id: str, token: str):
    return rl.get_accounts_sync(token, app_id)


st.markdown(
    """
    <div class="rl-panel">
        <div class="rl-h">Digit Research Lab · full bot-lifecycle backtest · calendar · advisor</div>
        <div class="mut">
            Read-only research branch. This page places no trades and does not modify live strategy behavior.
            It can run alongside the real bot. Simulated backtests include cooldowns, recovery, daily cap,
            lower-digit sequence behavior, and the payout assumption.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tab_overview, tab_calendar, tab_evidence, tab_backtest, tab_advisor, tab_backup = st.tabs(
    ["OVERVIEW", "CALENDAR", "EVIDENCE", "FULL BOT BACKTEST", "ADVISOR", "BACKUP"]
)

actual_rows = _actual_rows(_journal_token())
actual_summary = rl.compute_actual_summary(actual_rows)
actual_daily = rl.compute_actual_daily(actual_rows)
tape_rows = _tape_summary(_db_token())

best_condition = st.session_state.get("best_condition")
sim_daily = st.session_state.get("sim_daily") or []

net_accent = "#22c55e" if actual_summary["net_pnl"] >= 0 else "#ef4444"
wr_accent = (
    "#22c55e"
    if actual_summary["win_rate_pct"] >= 55
    else ("#ef4444" if actual_summary["closed"] and actual_summary["win_rate_pct"] < 45 else "#3884ff")
)

tape_ticks = sum(int(row.get("ticks", 0)) for row in tape_rows)
tape_symbols = len(tape_rows)
sim_net = float(best_condition.get("net_pnl", 0.0)) if best_condition else 0.0
sim_accent = "#22c55e" if sim_net >= 0 else "#ef4444"

st.markdown(
    f'<div class="rl-kpis">'
    + _kpi("Tape symbols", f"{tape_symbols}", f"{tape_ticks:,} ticks stored", "#3884ff")
    + _kpi("Actual reviews", f"{actual_summary['reviews']:,}", f"arms {actual_summary['arms']:,}", "#a855f7")
    + _kpi("Actual trades", f"{actual_summary['closed']:,}", f"{actual_summary['wins']}W · {actual_summary['losses']}L", "#f59e0b")
    + _kpi("Actual win rate", f"{actual_summary['win_rate_pct']:.1f}%", "closed journal trades", wr_accent)
    + _kpi("Actual P&L", f"{actual_summary['net_pnl']:+,.2f}", "closed journal trades", net_accent)
    + _kpi("Best sim P&L", f"{sim_net:+,.2f}", "selected backtest condition", sim_accent)
    + "</div>",
    unsafe_allow_html=True,
)


with tab_overview:
    st.markdown('<div class="rl-h">Actual journal daily progress</div>', unsafe_allow_html=True)

    if not actual_daily:
        st.info("No digit journal rows yet.")
    else:
        actual_df = pd.DataFrame(actual_daily)
        _table(actual_df, height=380)

        st.markdown('<div class="rl-h">Actual cumulative closed P&L</div>', unsafe_allow_html=True)
        st.line_chart(actual_df.set_index("date")[["cum_pnl"]])

        st.markdown('<div class="rl-h">Actual daily win rate (%)</div>', unsafe_allow_html=True)
        st.bar_chart(actual_df.set_index("date")[["win_rate_pct"]])

    if sim_daily:
        st.markdown('<div class="rl-h">Simulated best-condition daily progress</div>', unsafe_allow_html=True)

        sim_df = pd.DataFrame(sim_daily)
        _table(sim_df, height=380)

        st.markdown('<div class="rl-h">Simulated cumulative paper P&L</div>', unsafe_allow_html=True)
        st.line_chart(sim_df.set_index("date")[["cum_pnl"]])


with tab_calendar:
    st.markdown('<div class="rl-h">Progress calendar</div>', unsafe_allow_html=True)

    source = st.radio(
        "Calendar source",
        options=["Actual journal", "Simulated best condition"],
        horizontal=True,
        key="calendar_source",
    )

    if source == "Actual journal":
        daily_for_calendar = actual_daily
    else:
        daily_for_calendar = sim_daily

    if not daily_for_calendar:
        if source == "Actual journal":
            st.info("No actual journal daily progress yet.")
        else:
            st.info("No simulated trades yet. Run the full bot backtest first.")
    else:
        days = {row["date"]: row for row in daily_for_calendar}
        years = sorted({int(row["date"][:4]) for row in daily_for_calendar if row.get("date")})

        if not years:
            st.info("No valid dates yet.")
        else:
            now = datetime.now(timezone.utc)

            year = st.selectbox(
                "Year",
                options=years,
                index=len(years) - 1,
                key="calendar_year",
            )

            months = sorted(
                {
                    int(row["date"][5:7])
                    for row in daily_for_calendar
                    if str(row.get("date", "")).startswith(f"{year:04d}")
                }
            ) or list(range(1, 13))

            default_month = now.month if int(year) == now.year else 1
            month_index = months.index(default_month) if default_month in months else len(months) - 1

            month = st.selectbox(
                "Month",
                options=months,
                index=month_index,
                key="calendar_month",
            )

            st.markdown(_render_month_calendar(int(year), int(month), days), unsafe_allow_html=True)

            st.markdown('<div class="rl-h">Monthly progress</div>', unsafe_allow_html=True)
            monthly = rl.aggregate_monthly(daily_for_calendar)
            _table(pd.DataFrame(monthly), height=320)

            st.markdown('<div class="rl-h">Yearly progress</div>', unsafe_allow_html=True)
            yearly = rl.aggregate_yearly(daily_for_calendar)
            _table(pd.DataFrame(yearly), height=260)


with tab_evidence:
    st.markdown(
        """
        <div class="rl-panel">
            <div class="rl-h">Research evidence collector</div>
            <div class="mut">
                This fetches Deriv tick history and stores it locally in logs/research.db.
                It is read-only. It does not trade. It can run while the live bot is running.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not DERIV_APP_ID or not DERIV_API_TOKEN:
        st.warning("Add DERIV_APP_ID and DERIV_API_TOKEN to collect evidence from Deriv.")
    else:
        try:
            accounts = _load_accounts(DERIV_APP_ID, DERIV_API_TOKEN)
        except Exception as exc:
            accounts = []
            st.error(f"Could not load accounts: {exc}")

        if accounts:
            account_map = {
                str(account.get("account_id")): account
                for account in accounts
                if account.get("account_id")
            }
            account_options = list(account_map)

            account_id = st.selectbox(
                "Account for evidence collection",
                options=account_options,
                format_func=lambda value: (
                    f"{str(account_map[value].get('account_type', 'UNKNOWN')).upper()} · {value} · "
                    f"{account_map[value].get('currency', 'USD')} "
                    f"{float(account_map[value].get('balance', 0) or 0):,.2f}"
                ),
                key="evidence_account",
            )

            market_labels = list(rl.available_markets().keys())

            default_label = (
                DEFAULT_MARKET_DISPLAY
                if DEFAULT_MARKET_DISPLAY in market_labels
                else market_labels[0]
            )

            selected_labels = st.multiselect(
                "Markets to collect",
                options=market_labels,
                default=[default_label],
                key="evidence_markets",
            )

            count = st.slider(
                "Ticks to request per symbol",
                min_value=1000,
                max_value=5000,
                value=5000,
                step=100,
                key="evidence_count",
            )

            if st.button("Collect evidence now", type="primary", use_container_width=True):
                if not selected_labels:
                    st.warning("Select at least one market.")
                else:
                    market_map = rl.available_markets()

                    for label in selected_labels:
                        symbol = market_map.get(label)

                        if not symbol:
                            continue

                        try:
                            with st.spinner(f"Collecting {label}…"):
                                result = rl.collect_symbol_history(
                                    DERIV_API_TOKEN,
                                    DERIV_APP_ID,
                                    account_id,
                                    symbol,
                                    count,
                                )

                            st.success(
                                f"{label}: fetched {result['fetched']}, "
                                f"inserted {result['inserted']}, precision {result['precision']}"
                            )
                        except Exception as exc:
                            st.error(f"{label}: {exc}")

                    st.cache_data.clear()
                    st.rerun()

        st.divider()

        st.markdown('<div class="rl-h">Research tape summary</div>', unsafe_allow_html=True)

        if tape_rows:
            _table(pd.DataFrame(tape_rows), height=320)
        else:
            st.info("No research tape stored yet.")


with tab_backtest:
    st.markdown(
        """
        <div class="rl-panel">
            <div class="rl-h">Full bot-lifecycle backtest</div>
            <div class="mut">
                Simulates your bot logic against the collected tape:
                reviews, lower sequence, cooldowns, recovery, daily cap, warmup,
                one attempt per UTC minute bucket, and optional take-profit.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    symbols = [row["symbol"] for row in tape_rows]

    if not symbols:
        st.info("Collect research evidence first.")
    else:
        symbol = st.selectbox(
            "Tape symbol",
            options=symbols,
            key="backtest_symbol",
        )

        thresholds = st.multiselect(
            "Thresholds (%)",
            options=[60, 65, 68, 70, 72, 75, 78, 80, 85],
            default=[72],
            key="backtest_thresholds",
        )

        lower_ns = st.multiselect(
            "Lower confirmation lengths",
            options=[1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20],
            default=[1, 2, 3],
            key="backtest_lower_ns",
        )

        upper_modes = st.multiselect(
            "Upper-digit behavior",
            options=["reset", "kill"],
            default=["kill"],
            key="backtest_upper_modes",
            help=(
                "Live default behavior is kill: a 4–9 digit kills the lower sequence for that review window. "
                "Reset is the more permissive alternative."
            ),
        )

        window_options = {
            "20/50/200": (20, 50, 200),
            "10/30/120": (10, 30, 120),
            "30/80/300": (30, 80, 300),
        }

        selected_window_labels = st.multiselect(
            "Window sets",
            options=list(window_options.keys()),
            default=["20/50/200"],
            key="backtest_window_sets",
        )

        duration_ticks = st.select_slider(
            "Contract duration",
            options=[1, 2],
            value=1,
            format_func=lambda value: f"{value} tick" if value == 1 else f"{value} ticks",
            key="backtest_duration",
        )

        payout_ratio = st.number_input(
            "Payout assumption (net profit on win for stake = 1)",
            min_value=0.10,
            max_value=3.00,
            value=0.95,
            step=0.01,
            key="backtest_payout",
        )

        with st.expander("Bot lifecycle settings", expanded=True):
            use_cooldown = st.checkbox("Use cooldown gates", value=True, key="backtest_use_cooldown")

            c1, c2, c3 = st.columns(3)

            initial_stake = c1.number_input(
                "Initial stake",
                min_value=0.35,
                max_value=10000.0,
                value=1.0,
                step=0.05,
                key="backtest_initial_stake",
            )

            martingale_multiplier = c2.number_input(
                "Recovery multiplier",
                min_value=1.00,
                max_value=4.00,
                value=1.10,
                step=0.01,
                key="backtest_multiplier",
            )

            max_martingale_steps = c3.number_input(
                "Max recovery steps",
                min_value=0,
                max_value=20,
                value=10,
                step=1,
                key="backtest_max_steps",
            )

            d1, d2, d3 = st.columns(3)

            daily_cap = d1.number_input(
                "Daily filled-trade cap",
                min_value=0,
                max_value=100,
                value=10,
                step=1,
                key="backtest_daily_cap",
            )

            take_profit_target = d2.number_input(
                "Take-profit target (0 disables)",
                min_value=0.0,
                max_value=100000.0,
                value=0.0,
                step=1.0,
                key="backtest_take_profit",
            )

            warmup_seconds = d3.number_input(
                "Warmup seconds",
                min_value=0,
                max_value=300,
                value=10,
                step=1,
                key="backtest_warmup",
            )

        min_trades = st.number_input(
            "Minimum trades per condition",
            min_value=0,
            max_value=1000,
            value=1,
            step=1,
            key="backtest_min_trades",
        )

        if st.button("Run full bot backtest", type="primary", use_container_width=True):
            if not thresholds or not lower_ns or not upper_modes or not selected_window_labels:
                st.warning("Select at least one threshold, lower length, upper mode, and window set.")
            else:
                with st.spinner("Running full bot-lifecycle backtest…"):
                    results = rl.run_full_bot_backtest(
                        symbol=symbol,
                        thresholds_pct=thresholds,
                        lower_ns=lower_ns,
                        upper_modes=upper_modes,
                        window_sets=[window_options[label] for label in selected_window_labels],
                        duration_ticks=int(duration_ticks),
                        payout_ratio=float(payout_ratio),
                        initial_stake=float(initial_stake),
                        martingale_multiplier=float(martingale_multiplier),
                        max_martingale_steps=int(max_martingale_steps),
                        daily_cap=int(daily_cap),
                        take_profit_target=float(take_profit_target),
                        use_cooldown=bool(use_cooldown),
                        warmup_seconds=float(warmup_seconds),
                        min_trades=int(min_trades),
                    )

                    settings = {
                        "symbol": symbol,
                        "duration_ticks": int(duration_ticks),
                        "payout_ratio": float(payout_ratio),
                        "initial_stake": float(initial_stake),
                        "martingale_multiplier": float(martingale_multiplier),
                        "max_martingale_steps": int(max_martingale_steps),
                        "daily_cap": int(daily_cap),
                        "take_profit_target": float(take_profit_target),
                        "use_cooldown": bool(use_cooldown),
                        "warmup_seconds": float(warmup_seconds),
                    }

                    st.session_state.research_results = results
                    st.session_state.research_settings = settings

                    if results:
                        best = results[0]
                        st.session_state.best_condition = best

                        trades = rl.simulate_bot_condition_trades(
                            symbol=symbol,
                            threshold_pct=best["threshold_pct"],
                            lower_n=best["lower_N"],
                            upper_mode=best["upper_mode"],
                            window_set=(
                                best["fast_window"],
                                best["medium_window"],
                                best["slow_window"],
                            ),
                            duration_ticks=best["duration_ticks"],
                            payout_ratio=float(payout_ratio),
                            initial_stake=float(initial_stake),
                            martingale_multiplier=float(martingale_multiplier),
                            max_martingale_steps=int(max_martingale_steps),
                            daily_cap=int(daily_cap),
                            take_profit_target=float(take_profit_target),
                            use_cooldown=bool(use_cooldown),
                            warmup_seconds=float(warmup_seconds),
                        )

                        st.session_state.sim_trades = trades
                        st.session_state.sim_daily = rl.simulated_daily_from_trades(trades)
                    else:
                        st.session_state.best_condition = None
                        st.session_state.sim_trades = []
                        st.session_state.sim_daily = []

                st.cache_data.clear()
                st.rerun()

        results = st.session_state.get("research_results")

        if results:
            st.markdown('<div class="rl-h">Full bot-lifecycle condition results</div>', unsafe_allow_html=True)

            results_df = pd.DataFrame(results)
            _table(results_df.head(200), height=420)

            csv_bytes = results_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "Download full backtest CSV",
                data=csv_bytes,
                file_name="digit_full_bot_backtest.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("No backtest results yet.")


with tab_advisor:
    st.markdown(
        """
        <div class="rl-panel">
            <div class="rl-h">Advisor · proposal only</div>
            <div class="mut">
                The advisor ranks collected evidence using the full bot-lifecycle simulation.
                It never changes the live bot. Forward-test any proposal on demo first.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    best_condition = st.session_state.get("best_condition")
    settings = st.session_state.get("research_settings") or {}

    if not best_condition:
        st.info("Run the full bot backtest first.")
    else:
        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Condition",
            f"{best_condition['window_set']} · {best_condition['threshold_pct']}% · N={best_condition['lower_N']} · {best_condition['upper_mode']}",
        )
        c2.metric("Trades", best_condition["trades"])
        c3.metric("Win rate", f"{best_condition['win_rate_pct']}%")
        c4.metric("Net paper P&L", f"{best_condition['net_pnl']:+,.2f}")

        preset_lines = [
            "MomentumMaster Digit — full bot-lifecycle research proposal",
            "STATUS: PROPOSAL ONLY. Do not auto-apply.",
            f"generated_utc: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"symbol: {settings.get('symbol', '')}",
            f"window_set: {best_condition['window_set']}",
            f"fast_window: {best_condition['fast_window']}",
            f"medium_window: {best_condition['medium_window']}",
            f"slow_window: {best_condition['slow_window']}",
            f"threshold_pct: {best_condition['threshold_pct']}",
            f"lower_confirmation: {best_condition['lower_N']}",
            f"upper_digit_rule: {best_condition['upper_mode']}",
            f"duration_ticks: {best_condition['duration_ticks']}",
            f"payout_assumption: {settings.get('payout_ratio', best_condition.get('payout_ratio', ''))}",
            "",
            "bot_lifecycle_settings:",
            f"  initial_stake: {settings.get('initial_stake', '')}",
            f"  recovery_multiplier: {settings.get('martingale_multiplier', '')}",
            f"  max_recovery_steps: {settings.get('max_martingale_steps', '')}",
            f"  daily_cap: {settings.get('daily_cap', '')}",
            f"  take_profit_target: {settings.get('take_profit_target', '')}",
            f"  cooldown_enabled: {settings.get('use_cooldown', '')}",
            f"  warmup_seconds: {settings.get('warmup_seconds', '')}",
            "",
            f"observed_trades: {best_condition['trades']}",
            f"observed_wins: {best_condition['wins']}",
            f"observed_losses: {best_condition['losses']}",
            f"observed_win_rate_pct: {best_condition['win_rate_pct']}",
            f"observed_net_paper_pnl: {best_condition['net_pnl']}",
            f"observed_expectancy: {best_condition['expectancy']}",
            f"observed_max_drawdown: {best_condition.get('max_drawdown', '')}",
            f"stopped_by: {best_condition.get('stopped_by', '')}",
            "",
            "Forward-test on demo before considering any manual opt-in.",
        ]

        preset_text = "\n".join(preset_lines)

        st.code(preset_text, language="text")

        st.download_button(
            "Download advisor preset",
            data=preset_text.encode("utf-8"),
            file_name="digit_full_bot_advisor_preset.txt",
            mime="text/plain",
            use_container_width=True,
        )

        st.warning(
            "This proposal does not modify the live bot. "
            "It is an observation from collected evidence and simulated paper trades."
        )


with tab_backup:
    st.markdown(
        """
        <div class="rl-panel">
            <div class="rl-h">Journal backup & restore</div>
            <div class="mut">
                Export or import the live digit journal archive.
                Import is idempotent.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    journal = get_journal()

    c1, c2 = st.columns(2)

    with c1:
        st.download_button(
            "Download master archive CSV",
            data=export_archive_csv_bytes(journal),
            file_name="momentummaster_digit_archive.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with c2:
        st.download_button(
            "Download merged JSON",
            data=export_merged_json_bytes(journal),
            file_name="momentummaster_digit_merged.json",
            mime="application/json",
            use_container_width=True,
        )

    uploaded = st.file_uploader(
        "Import backup",
        type=["csv", "json"],
        key="research_backup_upload",
    )

    if uploaded is not None and st.button(
        "Restore backup",
        type="primary",
        use_container_width=True,
        key="research_restore",
    ):
        result = import_journal(journal, uploaded.read(), uploaded.name)
        st.success(f"Restore complete: {result}")
        st.cache_data.clear()
