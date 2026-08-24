"""MomentumMaster Digit — Streamlit terminal for 1–2 tick Over 3 contracts."""
from __future__ import annotations

import asyncio
import html
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.runtime.scriptrunner_utils import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(thread, ctx=None):
            return thread

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    AVAILABLE_MARKETS,
    DEFAULT_INITIAL_STAKE,
    DEFAULT_MARKET_DISPLAY,
    DIGIT_DEFAULT_TICK_DURATION,
    DIGIT_DEFAULT_RECOVERY_ENABLED,
    DIGIT_DEFAULT_RECOVERY_MULTIPLIER,
    DIGIT_MAX_RECOVERY_STEPS,
    DIGIT_LOWER_CONFIRM_MAX,
    DIGIT_LOWER_CONFIRMATION_MAX,
    DIGIT_MIN_OVER3_SHARES,
    DIGIT_MAX_UNDER3_SHARES,
    DIGIT_MIN_PER_DIGIT_DOMINANCE,
    DIGIT_MAX_UNDER3_SHARES,
    DIGIT_MIN_PER_DIGIT_DOMINANCE,
    DIGIT_REVIEW_INTERVAL_SECONDS,
    DIGIT_TICK_DURATION_OPTIONS,
    DIGIT_WINDOWS,
    DIGIT_WINDOW_ENABLED,
    DIGIT_UPPER_MODE,
    GLOBAL_TAKE_PROFIT_TARGET,
    DERIV_APP_ID,
    DERIV_API_TOKEN,
)
from src.api_client import DerivAPIClient, DerivAPIError
from src.coordination import GlobalRiskCoordinator
from src.journal import get_journal
from src.persistence import export_archive_csv_bytes, export_merged_json_bytes, import_journal
from src.state_manager import StateManager
from src.trading_engine import TradingEngine, normalize_account_type, resolve_execution_mode

st.set_page_config(page_title="MomentumMaster Digit", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

configured_pat = DERIV_API_TOKEN.strip()
global_risk = GlobalRiskCoordinator()

st.markdown(
    """
    <style>
    html,body,.stApp{background:#060912;color:#c7d2e0;font-family:Inter,system-ui,sans-serif;}
    [data-testid="stSidebar"]{background:#0a0f1c;border-right:1px solid #1b2740;}
    [data-testid="stMainBlockContainer"]{max-width:1480px;padding-top:1.2rem;}
    .mm-head{display:flex;justify-content:space-between;align-items:end;border-bottom:2px solid #10b981;padding:8px 0 14px;margin-bottom:14px;}
    .mm-logo{font-size:1.45rem;font-weight:800;letter-spacing:.12em;color:#eef3fb;text-transform:uppercase;}
    .mm-logo b{color:#10b981;}
    .mm-sub{color:#8294b0;font-size:.78rem;margin-top:5px;}
    .mm-card{background:linear-gradient(150deg,#0c1426,#0e1830);border:1px solid #1d2c49;border-radius:11px;padding:15px 17px;height:100%;}
    .mm-label{font-size:.64rem;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:#6b7c97;}
    .mm-value{font-family:monospace;font-size:1.65rem;font-weight:800;color:#eef3fb;margin-top:7px;}
    .mm-small{font-size:.76rem;color:#8294b0;margin-top:6px;line-height:1.45;}
    .mm-section{font-size:.72rem;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:#6b7c97;margin:20px 0 9px;border-bottom:1px solid #1b2740;padding-bottom:7px;}
    .good{color:#34d399}
    .bad{color:#fb7185}
    .warn{color:#fbbf24}
    .muted{color:#8294b0}
    </style>
    """,
    unsafe_allow_html=True,
)

if "state_manager" not in st.session_state:
    st.session_state.state_manager = StateManager()
if "engine_thread" not in st.session_state:
    st.session_state.engine_thread = None
if "engine_loop" not in st.session_state:
    st.session_state.engine_loop = None
if "engine_instance" not in st.session_state:
    st.session_state.engine_instance = None
if "should_run" not in st.session_state:
    st.session_state.should_run = False
if "engine_config" not in st.session_state:
    st.session_state.engine_config = None
if "auto_restart_count" not in st.session_state:
    st.session_state.auto_restart_count = 0
if "last_auto_restart" not in st.session_state:
    st.session_state.last_auto_restart = 0.0

state: StateManager = st.session_state.state_manager


def _run_engine_in_thread(engine: TradingEngine, loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(engine.run())
    finally:
        try:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass


@st.cache_data(ttl=60, show_spinner=False)
def _load_accounts(app_id: str, token: str):
    return asyncio.run(DerivAPIClient.get_accounts(token, app_id))


def _is_derived_index(item: Dict[str, Any]) -> bool:
    symbol = str(item.get("symbol", "")).strip().upper()
    metadata = " ".join(
        str(item.get(key, ""))
        for key in ("market", "market_type", "submarket", "symbol_type", "display_name", "name")
    ).lower()

    financial_markers = ("forex", "financial", "commodit", "stock", "crypto", "exchange")
    index_markers = (
        "synthetic",
        "derived",
        "volatility",
        "jump",
        "boom",
        "crash",
        "step index",
        "range break",
    )

    if any(marker in metadata for marker in financial_markers):
        return False
    if any(marker in metadata for marker in index_markers):
        return True

    return symbol.startswith(("1HZ", "R", "JD", "BOOM", "CRASH", "STPRNG", "RDBULL", "RDBEAR"))


@st.cache_data(ttl=180, show_spinner=False)
def _load_live_digit_markets(app_id: str, token: str, account_id: str):
    async def fetch():
        client = DerivAPIClient(token, app_id, account_id)

        if not await client.connect():
            raise DerivAPIError(client.last_error or "Could not connect while loading markets.", "MARKET_LOAD_FAILED")

        try:
            symbols = await client.get_active_symbols(full=True)
            result = {}

            for item in symbols:
                symbol = str(item.get("symbol", "")).strip()
                suspended = str(item.get("is_trading_suspended", "")).strip().lower() in {"1", "true", "yes", "y"}

                if not symbol or suspended or not _is_derived_index(item):
                    continue

                label = str(item.get("display_name") or item.get("name") or symbol).strip()
                if label in result and result[label] != symbol:
                    label = f"{label} · {symbol}"

                result[label] = symbol

            return dict(sorted(result.items(), key=lambda pair: pair[0].lower()))
        finally:
            await client.disconnect()

    return asyncio.run(fetch())


def _fallback_markets() -> Dict[str, str]:
    return dict(sorted(AVAILABLE_MARKETS.items(), key=lambda pair: pair[0].lower()))


def _today_filled_trade_count(state_obj: StateManager) -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return sum(
        1
        for trade in state_obj.get_trade_history()
        if getattr(trade, "contract_id", None) is not None and str(trade.timestamp).startswith(today)
    )


def _launch_engine(config: Dict[str, Any], reset_state: bool) -> bool:
    try:
        if reset_state:
            state.reset_for_new_session(config["initial_stake"])
        else:
            state.clear_error()

        history = state.get_trade_history()
        if history and history[0].status == "OPEN":
            state.update_trade_outcome(
                history[0].trade_id,
                "UNKNOWN",
                0.0,
                "The engine restarted while a contract was open — check the Deriv statement.",
            )

        engine = TradingEngine(
            api_token=config["api_token"],
            app_id=config["app_id"],
            account_id=config["account_id"],
            account_currency=config["account_currency"],
            state=state,
            initial_stake=config["initial_stake"],
            max_martingale_steps=config["max_steps"],
            symbol=config["symbol"],
            symbol_display=config["symbol_display"],
            contract_duration=config["duration_ticks"],
            contract_duration_unit="t",
            account_type=config["account_type"],
            real_execution_confirmed=config["real_execution_confirmed"],
            martingale_multiplier=config["martingale_multiplier"],
            quote_precision=config.get("quote_precision", 2),
            min_over3_share=config.get("min_over3_share", 0.72),
            min_over3_shares=config.get("min_over3_shares"),
            max_under3_share=config.get("max_under3_share", 0.28),
            max_under3_shares=config.get("max_under3_shares"),
            min_per_digit_dominance=config.get("min_per_digit_dominance", 0.03),
            lower_tick_max=config["lower_tick_max"],
            review_interval_seconds=config["review_interval_seconds"],
            required_lower_confirmations=config.get("required_lower_confirmations", 1),
            digit_windows=config.get("digit_windows"),
            digit_window_enabled=config.get("digit_window_enabled"),
            upper_mode=config.get("upper_mode", "kill"),
        )

        if not reset_state:
            engine._daily_trade_count = _today_filled_trade_count(state)
            engine._daily_date = datetime.now(timezone.utc).date()

    except Exception as exc:
        state.set_running(False)
        state.set_error(f"The engine could not start: {exc}")
        state.set_status("Could not start.")
        return False

    st.session_state.engine_instance = engine
    state.set_running(True)

    loop = asyncio.new_event_loop()
    st.session_state.engine_loop = loop

    thread = threading.Thread(target=_run_engine_in_thread, args=(engine, loop), daemon=True, name="DigitTradingEngineThread")

    try:
        add_script_run_ctx(thread)
    except Exception:
        pass

    thread.start()
    st.session_state.engine_thread = thread

    return True


RESTART_COOLDOWN_SECONDS = 20.0
MAX_AUTO_RESTARTS = 5
HEALTHY_WINDOW_SECONDS = 120.0


@st.fragment(run_every=2.0)
def watchdog_fragment():
    if not st.session_state.get("should_run", False):
        return

    if state.stop_requested:
        st.session_state.should_run = False
        state.set_running(False)
        return

    thread = st.session_state.get("engine_thread")
    now = time.monotonic()

    if thread is not None and thread.is_alive():
        if (
            st.session_state.get("auto_restart_count", 0) > 0
            and now - st.session_state.get("last_auto_restart", 0.0) > HEALTHY_WINDOW_SECONDS
        ):
            st.session_state.auto_restart_count = 0
        return

    if now - st.session_state.get("last_auto_restart", 0.0) < RESTART_COOLDOWN_SECONDS:
        return

    count = st.session_state.get("auto_restart_count", 0)

    if count >= MAX_AUTO_RESTARTS:
        st.session_state.should_run = False
        state.set_running(False)
        state.set_error(f"The engine stopped {MAX_AUTO_RESTARTS} times in a row — auto-restart paused.")
        state.set_status("Auto-restart paused.")
        st.rerun()
        return

    config = st.session_state.get("engine_config")
    if not config:
        st.session_state.should_run = False
        state.set_running(False)
        return

    st.session_state.last_auto_restart = now
    st.session_state.auto_restart_count = count + 1
    state.set_status(f"Engine stopped unexpectedly — restarting ({count + 1}/{MAX_AUTO_RESTARTS})…")

    if not _launch_engine(config, reset_state=False):
        st.session_state.should_run = False


def start_bot(config: Dict[str, Any]) -> None:
    if st.session_state.engine_thread and st.session_state.engine_thread.is_alive():
        return

    st.session_state.engine_config = config
    st.session_state.should_run = True
    st.session_state.auto_restart_count = 0
    st.session_state.last_auto_restart = 0.0

    if not _launch_engine(config, reset_state=True):
        st.session_state.should_run = False


def stop_bot() -> None:
    st.session_state.should_run = False
    state.request_stop()
    state.set_status("Stopping…")


with st.sidebar:
    st.markdown("### Digit terminal")
    st.caption("Rolling last-digit reviews · Over 3 · 1–2 ticks")

    accounts: List[Dict[str, Any]] = []
    account_error = ""

    if not DERIV_APP_ID or not configured_pat:
        account_error = "Add DERIV_APP_ID and DERIV_API_TOKEN to Streamlit Secrets or .env."
    else:
        try:
            accounts = _load_accounts(DERIV_APP_ID, configured_pat)
        except Exception as exc:
            account_error = str(exc)

    account_id = ""
    account_currency = "USD"
    account_type = "UNKNOWN"
    selected_account = None
    real_execution_confirmed = False

    if account_error:
        st.error(account_error)
    elif not accounts:
        st.warning("No active accounts were found.")
    else:
        account_map = {str(item.get("account_id")): item for item in accounts if item.get("account_id")}
        options = list(account_map)

        picked = st.selectbox(
            "Account",
            options=options,
            disabled=state.is_running,
            format_func=lambda value: (
                f"{normalize_account_type(account_map[value].get('account_type', 'UNKNOWN'))} · {value} · "
                f"{account_map[value].get('currency', 'USD')} {float(account_map[value].get('balance', 0) or 0):,.2f}"
            ),
        )

        account_id = picked
        selected_account = account_map[picked]
        account_type = normalize_account_type(selected_account.get("account_type", "UNKNOWN"))
        account_currency = str(selected_account.get("currency", "USD")).upper()

        st.success(f"{account_type} account · {account_currency} {float(selected_account.get('balance', 0) or 0):,.2f}")

    st.divider()

    market_catalog = _fallback_markets()

    if selected_account and DERIV_APP_ID and configured_pat:
        try:
            live_catalog = _load_live_digit_markets(DERIV_APP_ID, configured_pat, account_id)
            if live_catalog:
                market_catalog = live_catalog
                st.caption(f"{len(market_catalog)} active markets loaded from Deriv; digit offerings are checked when the engine starts")
        except Exception as exc:
            st.caption(f"Live catalogue unavailable; using local list ({str(exc)[:80]})")

    labels = list(market_catalog)
    default_label = DEFAULT_MARKET_DISPLAY if DEFAULT_MARKET_DISPLAY in market_catalog else labels[0]

    market_display = st.selectbox("Market", options=labels, index=labels.index(default_label), disabled=state.is_running)
    selected_symbol = market_catalog[market_display]

    st.caption("Indices only: Volatility, Jump, Boom, Crash, Step Index, and Range Break. Forex, commodities, stocks, and crypto are excluded.")

    st.divider()

    duration_ticks = st.select_slider(
        "Contract duration",
        options=list(DIGIT_TICK_DURATION_OPTIONS),
        value=DIGIT_DEFAULT_TICK_DURATION,
        disabled=state.is_running,
        format_func=lambda value: f"{value} tick" if value == 1 else f"{value} ticks",
    )

    engine_cfg = st.session_state.engine_config if isinstance(st.session_state.engine_config, dict) else {}

    default_lower = int(engine_cfg.get("required_lower_confirmations", 1)) if state.is_running else 1

    lower_confirmations = st.number_input(
        "Lower-tick confirmations before entry",
        min_value=1,
        max_value=int(DIGIT_LOWER_CONFIRMATION_MAX),
        value=default_lower,
        step=1,
        disabled=state.is_running,
        help="1 to 20 consecutive lower digits 0–3. The final lower digit triggers entry.",
    )

    upper_options = ["kill", "reset"]
    default_upper = str(engine_cfg.get("upper_mode", DIGIT_UPPER_MODE)).lower() if state.is_running else str(DIGIT_UPPER_MODE).lower()
    if default_upper not in upper_options:
        default_upper = "kill"

    upper_mode = st.selectbox(
        "Upper-digit behavior",
        options=upper_options,
        index=upper_options.index(default_upper),
        disabled=state.is_running,
        format_func=lambda value: "Kill signal on 4–9" if value == "kill" else "Reset sequence on 4–9",
        help=(
            "Kill: any 4–9 before the lower sequence completes kills the signal for that review window. "
            "Reset: any 4–9 before completion resets the lower sequence."
        ),
    )

    st.caption(
        "This is an entry-timing trigger only: the strict concentration rule still comes from the 4–9 review. "
        "The final required lower digit itself triggers entry."
    )

    st.divider()

    st.markdown("#### Rolling window gates")

    window_cfg = engine_cfg.get("digit_windows", DIGIT_WINDOWS) if state.is_running else DIGIT_WINDOWS
    enabled_cfg = engine_cfg.get("digit_window_enabled", DIGIT_WINDOW_ENABLED) if state.is_running else DIGIT_WINDOW_ENABLED
    share_cfg = engine_cfg.get("min_over3_shares", DIGIT_MIN_OVER3_SHARES) if state.is_running else DIGIT_MIN_OVER3_SHARES
    max_under_cfg = engine_cfg.get("max_under3_shares", DIGIT_MAX_UNDER3_SHARES) if state.is_running else DIGIT_MAX_UNDER3_SHARES
    max_under_cfg = engine_cfg.get("max_under3_shares", DIGIT_MAX_UNDER3_SHARES) if state.is_running else DIGIT_MAX_UNDER3_SHARES

    window_cfg = window_cfg or DIGIT_WINDOWS
    enabled_cfg = enabled_cfg or DIGIT_WINDOW_ENABLED
    share_cfg = share_cfg or DIGIT_MIN_OVER3_SHARES
    max_under_cfg = max_under_cfg or DIGIT_MAX_UNDER3_SHARES
    max_under_cfg = max_under_cfg or DIGIT_MAX_UNDER3_SHARES

    use_fast = st.checkbox(
        "Use fast window",
        value=bool(enabled_cfg.get("fast", True)),
        disabled=state.is_running,
        key="use_fast_window",
    )

    f1, f2, f3 = st.columns(3)

    fast_ticks = f1.number_input(
        "Fast ticks",
        min_value=5,
        max_value=10000,
        value=int(window_cfg.get("fast", DIGIT_WINDOWS.get("fast", 20))),
        step=1,
        disabled=state.is_running or not use_fast,
        key="fast_ticks",
    )

    fast_pct = f2.number_input(
        "Fast 4–9 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(share_cfg.get("fast", DIGIT_MIN_OVER3_SHARES.get("fast", 0.75))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_fast,
        key="fast_pct",
    )

    fast_max_under_pct = f3.number_input(
        "Fast max 0–3 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(max_under_cfg.get("fast", DIGIT_MAX_UNDER3_SHARES.get("fast", 0.25))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_fast,
        key="fast_max_under_pct",
    )

    use_medium = st.checkbox(
        "Use medium window",
        value=bool(enabled_cfg.get("medium", True)),
        disabled=state.is_running,
        key="use_medium_window",
    )

    m1, m2, m3 = st.columns(3)

    medium_ticks = m1.number_input(
        "Medium ticks",
        min_value=5,
        max_value=10000,
        value=int(window_cfg.get("medium", DIGIT_WINDOWS.get("medium", 50))),
        step=1,
        disabled=state.is_running or not use_medium,
        key="medium_ticks",
    )

    medium_pct = m2.number_input(
        "Medium 4–9 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(share_cfg.get("medium", DIGIT_MIN_OVER3_SHARES.get("medium", 0.72))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_medium,
        key="medium_pct",
    )

    medium_max_under_pct = m3.number_input(
        "Medium max 0–3 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(max_under_cfg.get("medium", DIGIT_MAX_UNDER3_SHARES.get("medium", 0.28))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_medium,
        key="medium_max_under_pct",
    )

    use_slow = st.checkbox(
        "Use slow window",
        value=bool(enabled_cfg.get("slow", True)),
        disabled=state.is_running,
        key="use_slow_window",
    )

    s1, s2, s3 = st.columns(3)

    slow_ticks = s1.number_input(
        "Slow ticks",
        min_value=5,
        max_value=10000,
        value=int(window_cfg.get("slow", DIGIT_WINDOWS.get("slow", 200))),
        step=1,
        disabled=state.is_running or not use_slow,
        key="slow_ticks",
    )

    slow_pct = s2.number_input(
        "Slow 4–9 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(share_cfg.get("slow", DIGIT_MIN_OVER3_SHARES.get("slow", 0.68))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_slow,
        key="slow_pct",
    )

    slow_max_under_pct = s3.number_input(
        "Slow max 0–3 %",
        min_value=0.0,
        max_value=100.0,
        value=round(float(max_under_cfg.get("slow", DIGIT_MAX_UNDER3_SHARES.get("slow", 0.32))) * 100.0, 2),
        step=1.0,
        disabled=state.is_running or not use_slow,
        key="slow_max_under_pct",
    )

    min_gap_cfg = float(engine_cfg.get("min_per_digit_dominance", DIGIT_MIN_PER_DIGIT_DOMINANCE)) if state.is_running else DIGIT_MIN_PER_DIGIT_DOMINANCE
    min_per_digit_gap_pct = st.number_input(
        "Minimum per-digit 4–9 advantage (percentage points)",
        min_value=0.0,
        max_value=20.0,
        value=round(min_gap_cfg * 100.0, 2),
        step=0.5,
        disabled=state.is_running,
        help="Every enabled window must show the average frequency per digit for 4–9 at least this many percentage points above the average for 0–3.",
    )

    windows_enabled = bool(use_fast or use_medium or use_slow)

    if not windows_enabled:
        st.error("Enable at least one rolling window. If all windows are disabled, the bot can never arm.")

    st.caption(
        "Disabled windows are ignored completely. "
        "If only medium is enabled, fast and slow cannot block or reject the setup."
    )

    st.divider()

    st.markdown("#### Global app take-profit")

    g_state = global_risk.snapshot()
    current_global_target = float(g_state.get("take_profit_target", GLOBAL_TAKE_PROFIT_TARGET))

    global_target = st.number_input(
        "App-wide take-profit target",
        min_value=0.0,
        max_value=1000000.0,
        value=current_global_target,
        step=1.0,
        key="global_take_profit_target",
        help="0 disables global take-profit. When combined closed P&L across all markets reaches this amount, all markets stop.",
    )

    c_target, c_reset = st.columns(2)

    with c_target:
        apply_global_target = st.button("Apply target", use_container_width=True, key="apply_global_target")

    with c_reset:
        reset_global_session = st.button("Reset global P&L", use_container_width=True, key="reset_global_session")

    if apply_global_target:
        global_risk.set_target(float(global_target))
        st.rerun()

    if reset_global_session:
        global_risk.reset_session()
        st.rerun()

    g_state = global_risk.snapshot()

    st.caption(
        f"Global closed P&L: {float(g_state.get('session_pnl', 0.0)):+.2f} / "
        f"target {float(g_state.get('take_profit_target', 0.0)):.2f}"
    )

    if bool(g_state.get("stop_all", False)):
        st.error(str(g_state.get("stop_reason") or "Global stop active."))

    st.divider()

    st.markdown("#### Account safety")

    if selected_account and account_type == "REAL":
        live_text = st.text_input("Type LIVE to enable real orders", max_chars=4, disabled=state.is_running)
        real_execution_confirmed = live_text == "LIVE"
        st.warning("Real-money orders are ON." if real_execution_confirmed else "Real account: orders remain blocked until LIVE is typed exactly.")
    elif selected_account and account_type == "DEMO":
        st.success("Demo account: virtual funds only.")
    elif selected_account:
        st.error("Unrecognized account type: orders are blocked.")

    st.divider()

    initial_stake = st.number_input(
        "Starting stake",
        min_value=0.35,
        max_value=10000.0,
        value=float(DEFAULT_INITIAL_STAKE),
        step=0.5,
        format="%.2f",
        disabled=state.is_running,
    )

    use_martingale = st.checkbox(
        "Enable recovery multiplier",
        value=bool(DIGIT_DEFAULT_RECOVERY_ENABLED),
        disabled=state.is_running,
    )

    martingale_multiplier = st.number_input(
        "Recovery multiplier",
        min_value=1.01,
        max_value=4.0,
        value=float(DIGIT_DEFAULT_RECOVERY_MULTIPLIER),
        step=0.01,
        format="%.2f",
        disabled=state.is_running,
    )

    max_steps = st.slider(
        "Maximum recovery steps",
        0,
        int(DIGIT_MAX_RECOVERY_STEPS),
        int(DIGIT_MAX_RECOVERY_STEPS if use_martingale else 0),
        disabled=state.is_running,
    )

    if not use_martingale:
        max_steps = 0

    st.caption(
        "Recovery: 1.10 means the next stake is 110% of the previous stake after a loss. "
        "There is no session loss-stop. Per-market take-profit has been replaced by the global app-wide take-profit above. "
        "Daily trade cap is disabled."
    )

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        start_pressed = st.button("Start", type="primary", use_container_width=True, disabled=state.is_running)

    with c2:
        stop_pressed = st.button("Stop", use_container_width=True, disabled=not state.is_running)

    if start_pressed:
        if not selected_account:
            st.error("Choose a Deriv account first.")
        elif not windows_enabled:
            st.error("Enable at least one rolling window before starting.")
        elif bool(global_risk.snapshot().get("stop_all", False)):
            st.error("Global stop is active. Reset global P&L before starting again.")
        else:
            start_bot(
                {
                    "api_token": configured_pat,
                    "app_id": DERIV_APP_ID,
                    "account_id": account_id,
                    "account_currency": account_currency,
                    "account_type": account_type,
                    "real_execution_confirmed": real_execution_confirmed,
                    "initial_stake": float(initial_stake),
                    "max_steps": int(max_steps),
                    "martingale_multiplier": float(martingale_multiplier),
                    "symbol": selected_symbol,
                    "symbol_display": market_display,
                    "duration_ticks": int(duration_ticks),
                    "quote_precision": 2,
                    "min_over3_share": float(medium_pct) / 100.0,
                    "min_over3_shares": {
                        "fast": float(fast_pct) / 100.0,
                        "medium": float(medium_pct) / 100.0,
                        "slow": float(slow_pct) / 100.0,
                    },
                    "max_under3_share": float(medium_max_under_pct) / 100.0,
                    "max_under3_shares": {
                        "fast": float(fast_max_under_pct) / 100.0,
                        "medium": float(medium_max_under_pct) / 100.0,
                        "slow": float(slow_max_under_pct) / 100.0,
                    },
                    "min_per_digit_dominance": float(min_per_digit_gap_pct) / 100.0,
                    "lower_tick_max": DIGIT_LOWER_CONFIRM_MAX,
                    "required_lower_confirmations": int(lower_confirmations),
                    "review_interval_seconds": DIGIT_REVIEW_INTERVAL_SECONDS,
                    "digit_windows": {
                        "fast": int(fast_ticks),
                        "medium": int(medium_ticks),
                        "slow": int(slow_ticks),
                    },
                    "digit_window_enabled": {
                        "fast": bool(use_fast),
                        "medium": bool(use_medium),
                        "slow": bool(use_slow),
                    },
                    "upper_mode": str(upper_mode).lower(),
                }
            )
            st.rerun()

    if stop_pressed:
        stop_bot()
        st.rerun()


ctx = state.get_execution_context()
mode = ctx.get("execution_mode", "UNCONFIGURED")
mode_color = "#34d399" if mode == "DEMO" else "#fbbf24" if mode == "REAL" else "#8294b0"

st.markdown(
    f'<div class="mm-head"><div><div class="mm-logo">Momentum<b>·</b>Master DIGIT</div>'
    f'<div class="mm-sub">Manual-market selection · rolling 4–9 frequency · Over 3 · 1–2 tick settlement</div></div>'
    f'<div style="text-align:right"><b style="color:{mode_color}">{html.escape(str(mode))}</b><br>'
    f'<span class="muted">{html.escape(str(ctx.get("account_id", "")[:12]))}</span></div></div>',
    unsafe_allow_html=True,
)


@st.fragment(run_every=2.0)
def global_risk_fragment():
    g = global_risk.snapshot()

    pnl = float(g.get("session_pnl", 0.0))
    target = float(g.get("take_profit_target", 0.0))
    stop_all = bool(g.get("stop_all", False))

    if stop_all:
        color = "#fb7185"
        label = f"Global closed P&L {pnl:+.2f} / target {target:.2f} — STOP ALL ACTIVE"
    elif target <= 0:
        color = "#8294b0"
        label = f"Global closed P&L {pnl:+.2f} / target disabled"
    else:
        color = "#34d399"
        label = f"Global closed P&L {pnl:+.2f} / target {target:.2f}"

    st.markdown(
        f'<div class="mm-card" style="border-left:4px solid {color};"><b>{html.escape(label)}</b></div>',
        unsafe_allow_html=True,
    )


@st.fragment(run_every=2.0)
def status_fragment():
    message = state.error_message or state.status_message
    color = "#fb7185" if state.error_message else "#34d399" if state.is_running else "#8294b0"

    st.markdown(
        f'<div class="mm-card" style="border-left:4px solid {color};"><b>{html.escape(message)}</b></div>',
        unsafe_allow_html=True,
    )


@st.fragment(run_every=2.0)
def metrics_fragment():
    stats = state.get_performance_stats()
    mart = state.get_martingale_state()
    s = state.get_strategy_state()

    wins = stats["wins"]
    losses = stats["losses"]

    cards = st.columns(5)

    medium = s.get("digit_windows", {}).get("medium", {}) or {}
    medium_count = int(medium.get("count", 0) or 0)
    medium_high = int(medium.get("over3_count", 0) or 0)
    condition_valid = bool(s.get("digit_condition_valid"))
    digit_state = "ARMED" if s.get("digit_armed") else ("QUALIFYING" if condition_valid else "DISARMED")

    values = [
        ("Last digit", str(s.get("last_digit") if s.get("last_digit") is not None else "—"), "latest quote digit"),
        ("4–9 share", f"{medium_high}/{medium_count} = {float(medium.get('p_over3', 0)) * 100:.1f}%", "medium window"),
        ("Digit state", digit_state, "lower tick confirmation"),
        ("Session P&L", f"{stats['total_pnl']:+.2f}", f"{wins}W · {losses}L"),
        ("Next stake", f"{mart['stake']:.2f}", f"recovery step {mart['step']}"),
    ]

    for col, (label, value, sub) in zip(cards, values):
        with col:
            st.markdown(
                f'<div class="mm-card"><div class="mm-label">{label}</div>'
                f'<div class="mm-value">{html.escape(str(value))}</div>'
                f'<div class="mm-small">{html.escape(str(sub))}</div></div>',
                unsafe_allow_html=True,
            )


@st.fragment(run_every=2.0)
def digit_panel_fragment():
    s = state.get_strategy_state()
    windows = s.get("digit_windows") or {}
    counts = s.get("digit_counts") or {}

    left, right = st.columns([1, 1])

    with left:
        st.markdown('<div class="mm-section">Manual-style digit review</div>', unsafe_allow_html=True)

        rows = []

        for name in ("fast", "medium", "slow"):
            w = windows.get(name) or {}

            count = int(w.get("count", 0) or 0)
            high_count = int(w.get("over3_count", 0) or 0)
            comparison_count = int(w.get("comparison_count_0to3", 0) or 0)

            rows.append(
                {
                    "Window": f"{name} · {count} ticks",
                    "4–9 total": f"{high_count}/{count} = {float(w.get('p_over3', 0)) * 100:.1f}%",
                    "4–9 avg/digit": f"{float(w.get('p_over3_avg', 0)) * 100:.1f}% each",
                    "0–3 total": f"{comparison_count}/{count} = {float(w.get('p_0to3', 0)) * 100:.1f}%",
                    "0–3 avg/digit": f"{float(w.get('p_0to3_avg', 0)) * 100:.1f}% each",
                    "Per-digit gap": f"{float(w.get('per_digit_dominance', 0)) * 100:+.1f} pp",
                }
            )

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.caption(
            "Example: 60.0% in a 20-tick window means 12 of 20 ticks ended in 7, 8, or 9. "
            "Each enabled window uses its own percentage threshold."
        )

        armed = bool(s.get("digit_armed"))
        confirmed = bool(s.get("digit_lower_confirmed"))
        required_lower = int(s.get("digit_required_lower_confirmations", 1) or 1)
        lower_count = int(s.get("digit_lower_confirmation_count", 0) or 0)

        if armed and confirmed:
            if s.get("pattern_stage") == "SIGNAL":
                st.success(f"Entry triggered on the final lower digit ({lower_count}/{required_lower}); awaiting execution.")
            else:
                st.success(f"Lower sequence confirmed ({lower_count}/{required_lower}); entry trigger recorded.")
        elif armed:
            st.warning(f"4–9 condition is armed. Waiting for lower digits from 0 to 3: {lower_count}/{required_lower} consecutive.")
        else:
            st.info(str(s.get("digit_last_rejection") or "Collecting digit history…"))

    with right:
        st.markdown('<div class="mm-section">Digit counts · current buffer</div>', unsafe_allow_html=True)

        data = [{"Digit": d, "Count": int(counts.get(str(d), 0))} for d in range(10)]
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=250, hide_index=True)

        st.caption(
            "Numeric counts only; no extra chart is rendered. A trade is sent only after the strategy condition, "
            "lower-tick confirmation, account safeguards, and a valid Deriv proposal all pass."
        )


@st.fragment(run_every=8.0)
def ledger_fragment():
    st.markdown('<div class="mm-section">Trades</div>', unsafe_allow_html=True)

    history = state.get_trade_history()

    if not history:
        st.info("No trades yet. The bot will stand aside until the rolling digit rule qualifies.")
        return

    records = []

    for trade in history[:50]:
        records.append(
            {
                "Time": trade.timestamp,
                "Direction": trade.direction,
                "Barrier": trade.barrier,
                "Stake": trade.stake,
                "Result": trade.status,
                "P&L": trade.pnl,
                "Step": trade.martingale_step,
                "Mode": trade.execution_mode,
            }
        )

    st.dataframe(pd.DataFrame(records), use_container_width=True, height=300, hide_index=True)


@st.fragment(run_every=15.0)
def journal_fragment():
    journal = get_journal()

    st.markdown('<div class="mm-section">Decision log · every minute review and entry decision</div>', unsafe_allow_html=True)

    csv_bytes = journal.to_csv_bytes()

    if csv_bytes:
        st.download_button(
            "Download decision log",
            data=csv_bytes,
            file_name="momentummaster_digit_journal.csv",
            mime="text/csv",
            use_container_width=True,
        )

    rows = journal.read_rows()

    if not rows:
        st.caption("No reviews recorded yet.")
        return

    df = pd.DataFrame(rows).tail(50).iloc[::-1]

    preferred = [
        "timestamp_utc",
        "symbol",
        "direction",
        "taken",
        "rejection_reason",
        "score",
        "threshold",
        "p_over3_fast",
        "p_over3_medium",
        "p_over3_slow",
        "p_over3_avg_fast",
        "p_over3_avg_medium",
        "p_over3_avg_slow",
        "p_0to3_avg_fast",
        "p_0to3_avg_medium",
        "p_0to3_avg_slow",
        "review_timestamp_utc",
        "confirmation_boundary_utc",
        "review_epoch",
        "confirmation_boundary_epoch",
        "entry_tick_epoch",
        "lower_confirmation_digit",
        "lower_confirmation_required",
        "lower_confirmation_count",
        "entry_digit",
        "quote_ask",
        "quote_payout",
        "outcome",
        "pnl",
    ]

    cols = [col for col in preferred if col in df.columns]
    st.dataframe(df[cols], use_container_width=True, height=320, hide_index=True)


@st.fragment(run_every=30.0)
def backup_fragment():
    journal = get_journal()

    with st.expander("Backup & restore", expanded=False):
        st.caption("The append-only archive is the master journal. Restore is idempotent and does not alter the live strategy.")

        a, b = st.columns(2)

        with a:
            st.download_button(
                "Download archive CSV",
                export_archive_csv_bytes(journal),
                "momentummaster_digit_archive.csv",
                "text/csv",
                use_container_width=True,
            )

        with b:
            st.download_button(
                "Download merged JSON",
                export_merged_json_bytes(journal),
                "momentummaster_digit_merged.json",
                "application/json",
                use_container_width=True,
            )

        uploaded = st.file_uploader("Import backup", type=["csv", "json"], key="digit_backup_upload")

        if uploaded is not None and st.button("Restore backup", type="primary", use_container_width=True, key="digit_restore"):
            result = import_journal(journal, uploaded.read(), uploaded.name)
            st.success(f"Restore complete: {result}")
            st.cache_data.clear()


watchdog_fragment()
global_risk_fragment()
status_fragment()
metrics_fragment()
digit_panel_fragment()
ledger_fragment()
journal_fragment()
backup_fragment()
