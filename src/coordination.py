"""Cross-process coordination for simultaneous dashboard sessions.

Includes:
- SharedMarketCoordinator: per account/symbol trade lifecycle coordination.
- GlobalRiskCoordinator: app-wide collective P&L and global take-profit stop.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import fcntl
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from config import GLOBAL_TAKE_PROFIT_TARGET, LOG_DIR

_THREAD_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: Dict[str, threading.Lock] = {}


class SharedMarketCoordinator:
    """Serialize one account/symbol trade lifecycle across local processes."""

    def __init__(
        self,
        account_id: str,
        symbol: str,
        initial_stake: float,
        storage_dir: Optional[str] = None,
    ) -> None:
        identity = f"{str(account_id).strip()}\0{str(symbol).strip()}".encode("utf-8")
        digest = hashlib.sha256(identity).hexdigest()[:24]

        directory = Path(storage_dir or LOG_DIR)
        directory.mkdir(parents=True, exist_ok=True)

        self._lock_path = directory / f".digit_coord_{digest}.lock"
        self._state_path = directory / f".digit_coord_{digest}.json"

        with _THREAD_LOCKS_GUARD:
            self._thread_lock = _THREAD_LOCKS.setdefault(str(self._lock_path), threading.Lock())

        self.account_id = str(account_id).strip()
        self.symbol = str(symbol).strip()
        self.initial_stake = round(float(initial_stake), 2)
        self._handle = None

    @property
    def state_path(self) -> Path:
        return self._state_path

    async def acquire(self, timeout_seconds: float = 8.0) -> bool:
        if self._handle is not None:
            return True

        return await asyncio.to_thread(self._acquire_sync, float(timeout_seconds))

    def _acquire_sync(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + max(0.1, timeout_seconds)
        remaining = max(0.1, deadline - time.monotonic())

        if not self._thread_lock.acquire(timeout=remaining):
            return False

        try:
            handle = self._lock_path.open("a+")
        except OSError:
            self._thread_lock.release()
            return False

        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._handle = handle
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    handle.close()
                    self._thread_lock.release()
                    return False
                time.sleep(0.10)
            except OSError:
                handle.close()
                self._thread_lock.release()
                return False

    def release(self) -> None:
        handle = self._handle
        self._handle = None

        if handle is None:
            return

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._thread_lock.release()

    def _require_lock(self) -> None:
        if self._handle is None:
            raise RuntimeError("Shared market coordination requires the lock.")

    @staticmethod
    def _today() -> str:
        return dt.datetime.now(dt.timezone.utc).date().isoformat()

    @staticmethod
    def _minute_key(timestamp: float) -> str:
        return dt.datetime.fromtimestamp(float(timestamp), tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M")

    def _default_state(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "initial_stake": self.initial_stake,
            "next_stake": self.initial_stake,
            "recovery_step": 0,
            "consecutive_losses": 0,
            "cooldown_until": 0.0,
            "daily_date": self._today(),
            "daily_filled": 0,
            "in_flight": False,
            "entry_minute": "",
            "last_attempt_minute": "",
            "last_filled_minute": "",
            "blocked": False,
            "blocked_reason": "",
        }

    def _read_state(self) -> Dict[str, Any]:
        self._require_lock()

        if not self._state_path.exists():
            return self._default_state()

        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("state is not an object")
        except (OSError, ValueError, json.JSONDecodeError):
            return {
                **self._default_state(),
                "blocked": True,
                "blocked_reason": "shared coordination state is unreadable; inspect the journal/state file",
            }

        state = self._default_state()
        state.update(raw)
        return state

    def _write_state(self, state: Dict[str, Any]) -> None:
        self._require_lock()

        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, self._state_path)

    def snapshot(self) -> Dict[str, Any]:
        state = self._read_state()

        if state.get("daily_date") != self._today():
            state["daily_date"] = self._today()
            state["daily_filled"] = 0
            self._write_state(state)

        return dict(state)

    def claim_entry(
        self,
        trade_id: str,
        now: Optional[float] = None,
        daily_cap: int = 0,
    ) -> Dict[str, Any]:
        state = self.snapshot()
        current = float(now if now is not None else time.time())

        if state.get("blocked"):
            return {
                "allowed": False,
                "reason": state.get("blocked_reason") or "shared market state is blocked for manual review",
            }

        if state.get("in_flight"):
            age = current - float(state.get("claimed_at", 0.0) or 0.0)

            if not state.get("bought") and age > 180.0:
                state["in_flight"] = False
                state["trade_id"] = ""
                state["claimed_at"] = 0.0
                state["bought_at"] = 0.0
                self._write_state(state)
            elif state.get("bought") and age > 600.0:
                state["blocked"] = True
                state["blocked_reason"] = "possible orphaned bought contract; verify the Deriv statement"
                self._write_state(state)
                return {"allowed": False, "reason": state["blocked_reason"]}
            else:
                return {"allowed": False, "reason": "another session has an unresolved trade for this market"}

        current_minute = self._minute_key(current)

        if state.get("last_attempt_minute") == current_minute:
            return {
                "allowed": False,
                "reason": "same-market same-minute attempt already sent; duplicate blocked",
            }

        cooldown = max(0.0, float(state.get("cooldown_until", 0.0)) - current)
        if cooldown > 0:
            return {"allowed": False, "reason": f"shared cooldown active for {cooldown:.0f}s"}

        # Daily cap is disabled when daily_cap is zero or negative.
        if int(daily_cap) > 0 and int(state.get("daily_filled", 0)) >= int(daily_cap):
            return {"allowed": False, "reason": f"shared daily trade cap ({daily_cap}) reached"}

        state["in_flight"] = True
        state["trade_id"] = str(trade_id)
        state["claimed_at"] = current
        state["entry_minute"] = current_minute
        state["last_attempt_minute"] = current_minute
        state["bought_at"] = 0.0
        state["bought"] = False

        self._write_state(state)

        return {
            "allowed": True,
            "stake": round(float(state.get("next_stake", self.initial_stake)), 2),
            "recovery_step": int(state.get("recovery_step", 0)),
            "consecutive_losses": int(state.get("consecutive_losses", 0)),
            "last_trade_time": float(state.get("last_outcome_at", 0.0) or 0.0),
            "daily_filled": int(state.get("daily_filled", 0)),
        }

    def mark_bought(self, now: Optional[float] = None) -> None:
        state = self._read_state()

        if not state.get("in_flight"):
            raise RuntimeError("Cannot mark a shared entry bought without a reservation.")

        bought_at = float(now if now is not None else time.time())

        state["bought"] = True
        state["bought_at"] = bought_at
        state["last_filled_minute"] = state.get("entry_minute") or self._minute_key(bought_at)
        state["daily_filled"] = int(state.get("daily_filled", 0)) + 1

        self._write_state(state)

    def abort_entry(self) -> None:
        state = self._read_state()

        state["in_flight"] = False
        state["trade_id"] = ""
        state["claimed_at"] = 0.0
        state["entry_minute"] = ""
        state["bought_at"] = 0.0
        state["bought"] = False

        self._write_state(state)

    def complete_entry(
        self,
        outcome: str,
        now: Optional[float] = None,
        multiplier: float = 1.10,
        max_steps: int = 10,
    ) -> Dict[str, Any]:
        state = self._read_state()
        current = float(now if now is not None else time.time())

        normalized = str(outcome or "").upper()

        if normalized == "UNKNOWN":
            state["blocked"] = True
            state["blocked_reason"] = "unknown settlement in another session; verify the Deriv statement"
        elif normalized == "WON":
            state["consecutive_losses"] = 0
            state["recovery_step"] = 0
            state["next_stake"] = round(float(state.get("initial_stake", self.initial_stake)), 2)
        elif normalized == "LOST":
            state["consecutive_losses"] = int(state.get("consecutive_losses", 0)) + 1

            step = int(state.get("recovery_step", 0))

            if step < int(max_steps):
                step += 1
                state["recovery_step"] = step
                state["next_stake"] = round(
                    float(state.get("next_stake", self.initial_stake)) * float(multiplier),
                    2,
                )
            else:
                state["recovery_step"] = 0
                state["next_stake"] = round(float(state.get("initial_stake", self.initial_stake)), 2)

        if normalized in {"WON", "LOST"}:
            consecutive = int(state.get("consecutive_losses", 0))

            required = 180.0 if consecutive >= 2 else 90.0 if consecutive >= 1 else 30.0
            bought_at = float(state.get("bought_at", 0.0) or 0.0)

            state["cooldown_until"] = max(current, bought_at) + required
        elif normalized == "CANCELLED":
            state["cooldown_until"] = float(state.get("cooldown_until", 0.0))

        state["in_flight"] = False
        state["trade_id"] = ""
        state["claimed_at"] = 0.0
        state["entry_minute"] = ""
        state["bought_at"] = 0.0
        state["bought"] = False
        state["last_outcome"] = normalized
        state["last_outcome_at"] = current

        self._write_state(state)

        return dict(state)


class GlobalRiskCoordinator:
    """App-wide collective P&L and global take-profit coordination."""

    def __init__(self, storage_dir: Optional[str] = None) -> None:
        directory = Path(storage_dir or LOG_DIR)
        directory.mkdir(parents=True, exist_ok=True)

        self._lock_path = directory / ".global_risk.lock"
        self._state_path = directory / ".global_risk.json"

        with _THREAD_LOCKS_GUARD:
            self._thread_lock = _THREAD_LOCKS.setdefault(str(self._lock_path), threading.Lock())

        self._handle = None

    def _acquire(self, timeout_seconds: float = 8.0) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        remaining = max(0.1, deadline - time.monotonic())

        if not self._thread_lock.acquire(timeout=remaining):
            return False

        try:
            handle = self._lock_path.open("a+")
        except OSError:
            self._thread_lock.release()
            return False

        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._handle = handle
                return True
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    handle.close()
                    self._thread_lock.release()
                    return False
                time.sleep(0.05)
            except OSError:
                handle.close()
                self._thread_lock.release()
                return False

    def _release(self) -> None:
        handle = self._handle
        self._handle = None

        if handle is None:
            return

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._thread_lock.release()

    def _default_state(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "take_profit_target": float(GLOBAL_TAKE_PROFIT_TARGET),
            "session_pnl": 0.0,
            "stop_all": False,
            "stop_reason": "",
            "counted_trades": {},
        }

    def _read_state(self) -> Dict[str, Any]:
        if not self._state_path.exists():
            return self._default_state()

        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("state is not an object")
        except (OSError, ValueError, json.JSONDecodeError):
            return {
                **self._default_state(),
                "stop_all": True,
                "stop_reason": "global risk state is unreadable; reset global session",
            }

        state = self._default_state()
        state.update(raw)
        return state

    def _write_state(self, state: Dict[str, Any]) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, self._state_path)

    def snapshot(self) -> Dict[str, Any]:
        if not self._acquire():
            return self._default_state()

        try:
            return self._read_state()
        finally:
            self._release()

    def set_target(self, target: float) -> Dict[str, Any]:
        if not self._acquire():
            return self._default_state()

        try:
            state = self._read_state()

            state["take_profit_target"] = max(0.0, float(target))

            target = float(state["take_profit_target"])
            pnl = float(state.get("session_pnl", 0.0))

            if target <= 0:
                state["stop_all"] = False
                state["stop_reason"] = ""
            elif pnl >= target:
                state["stop_all"] = True
                state["stop_reason"] = f"global take-profit reached ({pnl:.2f} / {target:.2f})"
            else:
                state["stop_all"] = False
                state["stop_reason"] = ""

            self._write_state(state)
            return dict(state)
        finally:
            self._release()

    def reset_session(self) -> Dict[str, Any]:
        if not self._acquire():
            return self._default_state()

        try:
            state = self._read_state()

            state["session_pnl"] = 0.0
            state["stop_all"] = False
            state["stop_reason"] = ""
            state["counted_trades"] = {}

            self._write_state(state)
            return dict(state)
        finally:
            self._release()

    def add_trade_pnl(self, trade_id: Any, pnl: float) -> Dict[str, Any]:
        if not self._acquire():
            return self._default_state()

        try:
            state = self._read_state()

            trade_key = str(trade_id or "").strip()
            if not trade_key:
                return dict(state)

            counted = state.get("counted_trades", {})
            if not isinstance(counted, dict):
                counted = {}

            if trade_key in counted:
                return dict(state)

            pnl_value = round(float(pnl), 2)
            counted[trade_key] = pnl_value

            if len(counted) > 2000:
                for old_key in list(counted.keys())[:-2000]:
                    counted.pop(old_key, None)

            state["counted_trades"] = counted
            state["session_pnl"] = round(float(state.get("session_pnl", 0.0)) + pnl_value, 2)

            target = float(state.get("take_profit_target", 0.0))

            if target > 0 and float(state["session_pnl"]) >= target:
                state["stop_all"] = True
                state["stop_reason"] = (
                    f"global take-profit reached ({float(state['session_pnl']):.2f} / {target:.2f})"
                )

            self._write_state(state)
            return dict(state)
        finally:
            self._release()
