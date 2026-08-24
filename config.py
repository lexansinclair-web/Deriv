"""config.py — MomentumMaster TF configuration."""
import os

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

load_dotenv()


def _streamlit_secret(name: str) -> str:
    try:
        import streamlit as st
        return str(st.secrets.get(name, ""))
    except Exception:
        return ""


DERIV_APP_ID = os.getenv("DERIV_APP_ID") or _streamlit_secret("DERIV_APP_ID") or ""
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN") or _streamlit_secret("DERIV_API_TOKEN") or ""
DERIV_WS_URL = ""

AVAILABLE_MARKETS = {
    "Volatility 10 (1s)": "1HZ10V",
    "Volatility 25 (1s)": "1HZ25V",
    "Volatility 50 (1s)": "1HZ50V",
    "Volatility 75 (1s)": "1HZ75V",
    "Volatility 100 (1s)": "1HZ100V",
    "Volatility 150 (1s)": "1HZ150V",
    "Volatility 200 (1s)": "1HZ200V",
    "Volatility 300 (1s)": "1HZ300V",
    "Volatility 10": "R_10",
    "Volatility 25": "R_25",
    "Volatility 50": "R_50",
    "Volatility 75": "R_75",
    "Volatility 100": "R_100",
    "Jump 10": "JD10",
    "Jump 25": "JD25",
    "Jump 50": "JD50",
    "Jump 75": "JD75",
    "Jump 100": "JD100",
    "Boom 300": "BOOM300N",
    "Boom 500": "BOOM500",
    "Boom 1000": "BOOM1000",
    "Crash 300": "CRASH300N",
    "Crash 500": "CRASH500",
    "Crash 1000": "CRASH1000",
    "Step Index": "stpRNG",
    "Range Break 100": "RDBEAR",
    "Range Break 200": "RDBULL",
}

DEFAULT_MARKET_DISPLAY = "Volatility 10 (1s)"
SYMBOL = AVAILABLE_MARKETS[DEFAULT_MARKET_DISPLAY]
SYMBOL_DISPLAY = DEFAULT_MARKET_DISPLAY

DIGIT_DEFAULT_BARRIER = 3
DIGIT_TICK_DURATION_OPTIONS = [1, 2]
DIGIT_DEFAULT_TICK_DURATION = 1
DIGIT_REVIEW_INTERVAL_SECONDS = 60.0

# Default rolling window sizes.
DIGIT_WINDOWS = {"fast": 20, "medium": 50, "slow": 200}

# Default window stage switches.
DIGIT_WINDOW_ENABLED = {
    "fast": True,
    "medium": True,
    "slow": True,
}

# Fallback global minimum share for digits 4–9.
DIGIT_MIN_OVER3_SHARE = 0.72

# Separate conservative minimum 4–9 share for each window.
DIGIT_MIN_OVER3_SHARES = {
    "fast": 0.75,
    "medium": 0.72,
    "slow": 0.68,
}

# Lower digit values are 0 through 3 for Over 3.
DIGIT_LOWER_CONFIRM_MAX = 3

# Explicit low-digit rejection gates. These mirror the strict high-share defaults
# but remain independently configurable in the dashboard.
DIGIT_MAX_UNDER3_SHARE = 0.28
DIGIT_MAX_UNDER3_SHARES = {
    "fast": 0.25,
    "medium": 0.28,
    "slow": 0.32,
}

# Required per-digit average advantage of digits 4–9 over digits 0–3.
DIGIT_MIN_PER_DIGIT_DOMINANCE = 0.03

# Maximum selectable lower-tick confirmation length.
DIGIT_LOWER_CONFIRMATION_MAX = 20

# Default lower confirmation count.
DIGIT_DEFAULT_LOWER_CONFIRMATIONS = 1

# Upper-digit behavior:
# "kill"  = any 4–9 before completion kills the signal for that review window.
# "reset" = any 4–9 before completion resets the lower sequence.
DIGIT_UPPER_MODE = "kill"

DIGIT_DEFAULT_RECOVERY_MULTIPLIER = 1.1
DIGIT_DEFAULT_RECOVERY_ENABLED = True
DIGIT_MAX_RECOVERY_STEPS = 10

# Global app-wide take-profit target.
# 0 disables global take-profit.
GLOBAL_TAKE_PROFIT_TARGET = 50.0

# Kept for backward compatibility only.
DIGIT_DEFAULT_PROFIT_TARGET = 1.0

# 0 disables the daily trade cap completely.
MAX_TRADES_PER_DAY = 0

CURRENCY = "USD"
MARTINGALE_MULTIPLIER = DIGIT_DEFAULT_RECOVERY_MULTIPLIER
DEFAULT_INITIAL_STAKE = 1.0
TICK_BUFFER_SIZE = 500

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "deriv_bot.log")
LOG_LEVEL = "INFO"
