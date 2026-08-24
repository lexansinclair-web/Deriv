"""src/journal.py — decision + execution log with a crash-proof archive.

Self-healing: if the live journal file is empty but the archive has rows
(e.g. after a container reboot wiped one file), the live view is rebuilt
from the archive automatically on startup.
"""

import csv
import os
import threading
from typing import Any, Dict, List, Optional

from config import LOG_DIR
from src.logger import get_logger

logger = get_logger("journal")

JOURNAL_FILE = os.path.join(LOG_DIR, "trade_journal.csv")
ARCHIVE_FILE = os.path.join(LOG_DIR, "journal_archive.csv")

COLUMNS = [
    "signal_id", "timestamp_utc", "symbol", "direction", "trend", "taken",
    "executed", "rejection_reason", "note", "score", "threshold",
    "s_trend", "s_trigger", "s_momentum", "s_volatility", "s_alignment",
    "s_adx", "s_macd", "s_rsi_zone", "s_pattern", "s_structure",
    "entry_adx", "entry_rsi", "entry_macd_hist", "atr", "close",
    "outcome", "pnl", "stake", "martingale_step", "contract_id",
    "execution_mode", "regime", "duration_min", "mae", "mfe",
    "tf_5m", "tf_15m", "tf_30m", "tf_1h", "mtf_agreement",
    "strategy_mode", "barrier", "duration_unit", "digit_precision", "last_digit",
    "digit_counts_fast", "digit_counts_medium", "digit_counts_slow",
    "p_over3_fast", "p_over3_medium", "p_over3_slow",
    "p_low_fast", "p_low_medium", "p_low_slow",
    "p_over3_avg_fast", "p_over3_avg_medium", "p_over3_avg_slow",
    "p_0to3_avg_fast", "p_0to3_avg_medium", "p_0to3_avg_slow",
    "max_under3_fast", "max_under3_medium", "max_under3_slow", "min_per_digit_dominance",
    "dominance_fast", "dominance_medium", "dominance_slow",
    "per_digit_dominance_fast", "per_digit_dominance_medium", "per_digit_dominance_slow",
    "review_timestamp_utc", "confirmation_boundary_utc", "review_epoch", "confirmation_boundary_epoch", "entry_tick_epoch", "lower_confirmation_digit", "lower_confirmation_required", "lower_confirmation_count", "entry_digit", "quote_ask", "quote_payout",
    "quote_break_even", "quote_edge", "review_type",
]
ARCHIVE_COLUMNS = ["kind"] + COLUMNS
OUTCOME_MERGE_FIELDS = (
    "outcome", "pnl", "stake", "martingale_step", "contract_id",
    "execution_mode", "executed", "note", "mae", "mfe",
)


class TradeJournal:
    def __init__(self, live_path: str = JOURNAL_FILE, archive_path: str = ARCHIVE_FILE):
        self._live = live_path
        self._archive = archive_path
        self._lock = threading.Lock()
        self._ensure_csv(self._live, COLUMNS)
        self._ensure_csv(self._archive, ARCHIVE_COLUMNS)
        self._rebuild_live_if_needed()

    # ------------------------------------------------------------- self-heal
    def _rebuild_live_if_needed(self) -> None:
        """If the live journal lost its rows but the archive survived, rebuild it."""
        try:
            live_has_rows = False
            if os.path.exists(self._live):
                with open(self._live, "r", newline="", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader, None)  # header
                    live_has_rows = next(reader, None) is not None
            if live_has_rows:
                return
            merged = self.read_archive_merged()
            if not merged:
                return
            with open(self._live, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(COLUMNS)
                for r in merged:
                    w.writerow([r.get(c, "") for c in COLUMNS])
            logger.info("Rebuilt live journal from archive (%d rows).", len(merged))
        except Exception as exc:
            logger.warning("Live journal rebuild failed: %s", exc)

    # -------------------------------------------------------------- plumbing
    def _ensure_csv(self, path: str, header: List[str]) -> None:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if not os.path.exists(path):
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(header)
                return
            with open(path, "r", newline="", encoding="utf-8") as f:
                first = next(csv.reader(f), None)
            if first == header:
                return
            with open(path, "r", newline="", encoding="utf-8") as f:
                old_rows = list(csv.DictReader(f))
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(header)
                for r in old_rows:
                    w.writerow([r.get(c, "") for c in header])
            logger.info("Migrated journal header for %s.", os.path.basename(path))
        except Exception as exc:
            logger.warning("Journal header init/migrate failed for %s: %s", path, exc)

    # -------------------------------------------------------------- recording
    def record_evaluation(self, record: Dict[str, Any]) -> None:
        live_row = [record.get(c, "") for c in COLUMNS]
        try:
            with self._lock:
                self._ensure_csv(self._live, COLUMNS)
                with open(self._live, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(live_row)
        except Exception as exc:
            logger.warning("Journal evaluation write failed: %s", exc)
        try:
            with self._lock:
                self._ensure_csv(self._archive, ARCHIVE_COLUMNS)
                with open(self._archive, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(["EVAL"] + live_row)
        except Exception as exc:
            logger.warning("Archive evaluation write failed: %s", exc)

    def record_outcome(self, signal_id, outcome, pnl, stake, contract_id,
                       execution_mode, martingale_step="", note="", mae="", mfe="") -> None:
        if not signal_id:
            return
        try:
            with self._lock:
                self._ensure_csv(self._live, COLUMNS)
                if os.path.exists(self._live):
                    with open(self._live, "r", newline="", encoding="utf-8") as f:
                        rows = list(csv.reader(f))
                    if rows:
                        idx = {name: i for i, name in enumerate(rows[0])}
                        sid_i = idx.get("signal_id")
                        if sid_i is not None:
                            changed = False
                            for row in rows[1:]:
                                if len(row) > sid_i and row[sid_i] == signal_id:
                                    def put(col, val):
                                        if col in idx and idx[col] < len(row):
                                            row[idx[col]] = val
                                    put("outcome", outcome)
                                    put("pnl", f"{pnl:.2f}")
                                    put("stake", f"{stake:.2f}")
                                    put("martingale_step", str(martingale_step))
                                    put("contract_id", str(contract_id) if contract_id else "")
                                    put("execution_mode", execution_mode)
                                    put("executed", "TRUE" if contract_id else "FALSE")
                                    put("note", note)
                                    put("mae", mae)
                                    put("mfe", mfe)
                                    changed = True
                            if changed:
                                with open(self._live, "w", newline="", encoding="utf-8") as f:
                                    csv.writer(f).writerows(rows)
        except Exception as exc:
            logger.warning("Journal outcome write failed: %s", exc)
        odict = {
            "signal_id": signal_id, "outcome": outcome, "pnl": f"{pnl:.2f}",
            "stake": f"{stake:.2f}", "martingale_step": str(martingale_step),
            "contract_id": str(contract_id) if contract_id else "",
            "execution_mode": execution_mode,
            "executed": "TRUE" if contract_id else "FALSE",
            "note": note, "mae": mae, "mfe": mfe,
        }
        try:
            with self._lock:
                self._ensure_csv(self._archive, ARCHIVE_COLUMNS)
                with open(self._archive, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(["OUTCOME"] + [odict.get(c, "") for c in COLUMNS])
        except Exception as exc:
            logger.warning("Archive outcome write failed: %s", exc)

    # --------------------------------------------------------------- reading
    def read_rows(self) -> List[Dict[str, str]]:
        try:
            with self._lock:
                if not os.path.exists(self._live):
                    return []
                with open(self._live, "r", newline="", encoding="utf-8") as f:
                    return list(csv.DictReader(f))
        except Exception as exc:
            logger.warning("Journal read failed: %s", exc)
            return []

    def to_csv_bytes(self) -> bytes:
        try:
            with self._lock:
                if not os.path.exists(self._live):
                    return b""
                with open(self._live, "rb") as f:
                    return f.read()
        except Exception:
            return b""

    def read_archive_merged(self) -> List[Dict[str, str]]:
        try:
            with self._lock:
                if os.path.exists(self._archive):
                    with open(self._archive, "r", newline="", encoding="utf-8") as f:
                        arows = list(csv.DictReader(f))
                    if arows:
                        evals: Dict[str, Dict[str, str]] = {}
                        order: List[str] = []
                        for r in arows:
                            kind = r.get("kind", "")
                            d = {c: r.get(c, "") for c in COLUMNS}
                            sid = d.get("signal_id", "")
                            if kind == "EVAL":
                                if sid and sid not in evals:
                                    evals[sid] = d
                                    order.append(sid)
                                elif not sid:
                                    key = f"__noid_{len(order)}"
                                    evals[key] = d
                                    order.append(key)
                            elif kind == "OUTCOME":
                                target = evals.get(sid) if sid else None
                                if target is None:
                                    target = d
                                    if sid:
                                        evals[sid] = target
                                        order.append(sid)
                                    else:
                                        key = f"__noid_{len(order)}"
                                        evals[key] = target
                                        order.append(key)
                                for fld in OUTCOME_MERGE_FIELDS:
                                    v = r.get(fld, "")
                                    if v not in (None, ""):
                                        target[fld] = v
                        return [evals[k] for k in order]
                if os.path.exists(self._live):
                    with open(self._live, "r", newline="", encoding="utf-8") as f:
                        return list(csv.DictReader(f))
                return []
        except Exception as exc:
            logger.warning("Archive read/merge failed: %s", exc)
            return []


_journal_singleton: Optional[TradeJournal] = None


def get_journal() -> TradeJournal:
    global _journal_singleton
    if _journal_singleton is None:
        _journal_singleton = TradeJournal()
    return _journal_singleton
