# -*- coding: utf-8 -*-
"""
CryptoBot v18 — Weekly Swing Signals (Bybit V5, USDT Perpetuals)

Phases 0–3C.1 complete: Skeleton · Indicators · Regime · Bar timestamp patch
  · Detectors: Breakout+Retest (3A) · Trend Pullback (3B)
  · Liquidity Sweep (3C / 3C.1 recency + self-confirm patch)
Phase 4 implemented: TP/SL engine · RR gate · can_signal · scan_symbol wiring
Phase 5 implemented: ActiveIdea creation · idea lifecycle (TP1/TP2/SL/expiry)
Phase 6 implemented: Telegram signal formatting · signal/update dispatch
Phase 7 implemented: dry-run mode · /config and /diag commands · target hardening
  Phase 7.1 hotfix: hardened delete_webhook · startup logger · stale comments
Phase 8A implemented: setup freshness gate · current-price gate · price in signal · setup age in signal
Phase 8B implemented: Telegram reply keyboard · command_keyboard() · updated phase text
  Phase 8B.1 hotfix: removed reply_markup from channel sends · Tg.send failure logging
Phase 8C implemented: ScanDiagnostics counters · keepalive/diag visibility · text cleanup
Phase 8D implemented: validate_actionable_setup · SETUP_CONTEXT_MAX_DAYS · RR_FROM_CURRENT_PRICE · refined diag counters
Phase 8E implemented: PendingSetup watchlist · /watchlist command · validate_actionable_setup reordering
Phase 8F implemented: CandidateEval · collect_setup_candidates · choose_best_candidate · evaluate_candidate · multi-candidate scan_symbol
Phase 8G implemented: CandidateDebug · /candidates command · dead-candidate diagnostics buffer
Phase 8H implemented: LIQUIDITY_SWEEP_MAX_AGE_HOURS · is_liquidity_sweep_recent · LS recency gate in evaluate_candidate
Phase 8I implemented: candidate_debug_key dedup · add_candidate_debug returns bool · candidate_debug_dedup counter · report_error runtime_state fix
Phase 8J implemented: TP/SL percentage ranges in signal and command outputs
Phase 8K implemented: Fresh Entry Retest gate for older Liquidity Sweep candidates
Phase 8L implemented: Signal-Eligible Watchlist · pending setups must pass RR + signal gate
Phase 8L.1 hotfix: temporal-safe LS · lifecycle ordering · post-SL lock · quality gates
Phase 8L.2 hotfix: post-confirmation TP/SL timing · dead-matrix diagnostics · Telegram polling visibility
Phase 8L.3 rollback: restore Phase 8L signal-flow thresholds while preserving temporal/lifecycle fixes
Phase 8L.4.1 Diagnostic: persistent SQLite raw-setup/score/outcome statistics + BR/TP internal-stage telemetry (no trading-rule changes)
Phase 8L.4.3 Diagnostic: TP pullback-length distribution + score-bucket outcome analyzer (no trading-rule changes)
Phase 8M Calibration Review: BR/TP cohort readiness + calibration summaries (diagnostic only; no trading-rule changes)
Phase 9A Bybit Read-Only Bridge: RSA auth · account/key health · /apikey + /bybit · expiry reminders (GET-only; no order endpoints; no trading-rule changes)
Phase 9B Minimum-Size Execution Planner: 50/50-safe minimum quantity · 2x margin planning · 10% equity reserve · shadow margin reservations · /plan (planning only; no Bybit write endpoints; no trading-rule changes)
Phase 9C Net PnL & Expiry Safety Shadow: account fee-rate + funding estimate · net TP/SL economics · remaining-leg break-even · persistent EXPIRED_WAIT_EXIT shadow lifecycle (still GET-only; no order endpoints; no trading-rule changes)

Architecture:
  - REST polling only; no WebSocket in MVP (BybitWS class kept for v19 upgrade)
  - Fixed 15-coin universe; no dynamic top-N selection
  - Timeframes polled: 1M / 1W / 1D / 4H / 1H
  - Regime: 1W EMA20 + 1D EMA50 per symbol; BTC global regime as soft filter
  - Ideas: TP1 + TP2 targets; 10-day expiry; per-symbol active-idea lock

Closed-candle rule (hard):
  All indicator calculations and setup detection operate on bars[:-1].
  The last bar in any fetched series is treated as potentially forming and is
  used only for current-price display and idea lifecycle TP/SL monitoring.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import html
import math
import json
import logging
import os
import sqlite3
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import aiohttp
from aiohttp import web
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# =============================================================================
# === 1. CONFIG ===
# =============================================================================

BYBIT_REST             = "https://api.bybit.com"
BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"  # v19 upgrade path

LOG_LEVEL      = os.getenv("LOG_LEVEL", "INFO")
PORT           = int(os.getenv("PORT", "10000"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or ""


def _bool_env(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


# ── Phase 9A: authenticated Bybit read-only bridge ────────────────────────────
# The API key itself may have Order/Position permissions, but this phase only
# implements authenticated GET endpoints.  There is intentionally no method
# for order create/amend/cancel, leverage changes, transfers, or withdrawals.
BYBIT_PRIVATE_READONLY_ENABLED = _bool_env("BYBIT_PRIVATE_READONLY_ENABLED", True)
BYBIT_API_KEY = (os.getenv("BYBIT_API_KEY") or "").strip()
BYBIT_PRIVATE_KEY_PATH = (
    os.getenv("BYBIT_PRIVATE_KEY_PATH") or "/run/secrets/bybit_private_key.pem"
).strip()
BYBIT_RECV_WINDOW = int(os.getenv("BYBIT_RECV_WINDOW", "5000"))
BYBIT_API_REMINDER_CHECK_SEC = int(os.getenv("BYBIT_API_REMINDER_CHECK_SEC", "21600"))
BYBIT_API_REMINDER_STATE_PATH = (
    os.getenv("BYBIT_API_REMINDER_STATE_PATH")
    or "/data/bybit_apikey_reminders.json"
).strip()
BYBIT_API_REMINDER_THRESHOLDS: Tuple[int, ...] = (30, 21, 14, 7, 1)



# ── Phase 9B: minimum-size execution planner (SHADOW ONLY) ────────────────────
# No Bybit write endpoint is implemented in this phase. The planner converts
# an emitted ActiveIdea into the smallest 50/50-splittable Bybit position that
# satisfies current instrument quantity/notional rules, estimates margin at the
# planned leverage, and reserves that margin only in an in-memory shadow ledger.
EXECUTION_PLANNER_ENABLED = _bool_env("EXECUTION_PLANNER_ENABLED", True)
EXECUTION_PLANNER_AUTO_SEND = _bool_env("EXECUTION_PLANNER_AUTO_SEND", True)
EXECUTION_PLANNER_LEVERAGE = max(
    1.0, min(float(os.getenv("EXECUTION_PLANNER_LEVERAGE", "2")), 2.0)
)
EXECUTION_PLANNER_RESERVE_PCT = max(
    0.0, min(float(os.getenv("EXECUTION_PLANNER_RESERVE_PCT", "10")), 50.0)
)
EXECUTION_PLANNER_TP1_FRACTION = 0.50
EXECUTION_PLANNER_TP2_FRACTION = 0.50



# ── Phase 9C: net economics + expiry safety shadow ────────────────────────────
# Still simulation only. Fee-rate is read from the authenticated Bybit account;
# funding is estimated from the current public funding rate and funding interval.
# Shadow execution state is persisted so EXPIRED_WAIT_EXIT survives redeploys.
EXECUTION_ECONOMICS_ENABLED = _bool_env("EXECUTION_ECONOMICS_ENABLED", True)
EXECUTION_SHADOW_STATE_PATH = (
    os.getenv("EXECUTION_SHADOW_STATE_PATH")
    or "/data/bybit_execution_shadow_state.json"
).strip()


ALLOWED_CHAT_IDS   = [int(x) for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip()]
PRIMARY_RECIPIENTS = [i for i in ALLOWED_CHAT_IDS if i < 0] or ALLOWED_CHAT_IDS[:1] or []
ONLY_CHANNEL              = _bool_env("ONLY_CHANNEL", True)
REPORT_ERRORS_TO_TG       = _bool_env("REPORT_ERRORS_TO_TG", False)
ERROR_REPORT_COOLDOWN_SEC = int(os.getenv("ERROR_REPORT_COOLDOWN_SEC", "180"))
# Default True — safe dry-run until explicitly set to 0/false in production.
DRY_RUN_MODE              = _bool_env("DRY_RUN_MODE", True)
# SETUP_MAX_AGE_HOURS (48h) is the legacy strict freshness gate.
# Phase 8D replaces it with a two-tier model:
#   SETUP_CONTEXT_MAX_DAYS — how old the structural context may be (swing timeframe)
#   ENTRY_ZONE_REQUIRED    — current price must be inside entry zone at scan time
#   RR_FROM_CURRENT_PRICE  — recalculate RR from live price instead of entry_mid
SETUP_MAX_AGE_HOURS       = int(os.getenv("SETUP_MAX_AGE_HOURS",    "48"))   # legacy
SETUP_CONTEXT_MAX_DAYS    = int(os.getenv("SETUP_CONTEXT_MAX_DAYS", "30"))
ENTRY_ZONE_REQUIRED       = _bool_env("ENTRY_ZONE_REQUIRED",     True)
RR_FROM_CURRENT_PRICE     = _bool_env("RR_FROM_CURRENT_PRICE",   True)
# Max DEAD candidate debug records kept in memory (diagnostics only, not a trading param).
CANDIDATE_DEBUG_MAX       = int(os.getenv("CANDIDATE_DEBUG_MAX", "100"))
# Phase 8H: Liquidity Sweep confirmation must be at most this old to be actionable.
# This is a STRICTER gate than SETUP_CONTEXT_MAX_DAYS (30d); it applies only to LS.
# Default 168h = 7 days.  BR and TP are not affected.
LIQUIDITY_SWEEP_MAX_AGE_HOURS = int(os.getenv("LIQUIDITY_SWEEP_MAX_AGE_HOURS", "168"))
# Phase 8K: Older LS candidates need a fresh return into the entry zone.
# If LS setup_age > LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS, the latest entry-zone
# return/touch must be no older than ENTRY_RETEST_MAX_AGE_HOURS.
LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS = int(os.getenv("LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS", "96"))
ENTRY_RETEST_MAX_AGE_HOURS = int(os.getenv("ENTRY_RETEST_MAX_AGE_HOURS", "48"))

# ── Universe ──────────────────────────────────────────────────────────────────
UNIVERSE: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",  "XRPUSDT",
    "ADAUSDT", "AVAXUSDT","LINKUSDT","DOGEUSDT",  "TONUSDT",
    "LTCUSDT", "DOTUSDT", "AAVEUSDT","NEARUSDT",  "SUIUSDT",
]
TIER1_SYMBOLS: Set[str] = {"BTCUSDT", "ETHUSDT"}   # lower RR_MIN threshold

# ── Polling schedule (seconds) ─────────────────────────────────────────────────
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
POLL_1H_SEC       = int(os.getenv("POLL_1H_SEC",       "300"))
POLL_4H_SEC       = int(os.getenv("POLL_4H_SEC",       "900"))
POLL_1D_SEC       = int(os.getenv("POLL_1D_SEC",       "3600"))
POLL_1W_SEC       = int(os.getenv("POLL_1W_SEC",       "14400"))
POLL_1M_SEC       = int(os.getenv("POLL_1M_SEC",       "86400"))
POLL_WORKERS      = int(os.getenv("POLL_WORKERS",      "5"))

# ── Preload bar counts ────────────────────────────────────────────────────────
PRELOAD_BARS_1H = int(os.getenv("PRELOAD_BARS_1H", "200"))
PRELOAD_BARS_4H = int(os.getenv("PRELOAD_BARS_4H", "200"))
PRELOAD_BARS_1D = int(os.getenv("PRELOAD_BARS_1D", "300"))
PRELOAD_BARS_1W = int(os.getenv("PRELOAD_BARS_1W", "100"))
PRELOAD_BARS_1M = int(os.getenv("PRELOAD_BARS_1M", "48"))

# Bybit interval string per TF key
TF_MAP: Dict[str, str] = {
    "1h": "60",
    "4h": "240",
    "1d": "D",
    "1w": "W",
    "1m": "M",
}
# Seconds between REST refreshes per TF key
TF_INTERVALS: Dict[str, int] = {
    "1h": POLL_1H_SEC,
    "4h": POLL_4H_SEC,
    "1d": POLL_1D_SEC,
    "1w": POLL_1W_SEC,
    "1m": POLL_1M_SEC,
}
# Bar limit per TF key (used for both preload and subsequent refreshes)
TF_LIMITS: Dict[str, int] = {
    "1h": PRELOAD_BARS_1H,
    "4h": PRELOAD_BARS_4H,
    "1d": PRELOAD_BARS_1D,
    "1w": PRELOAD_BARS_1W,
    "1m": PRELOAD_BARS_1M,
}

# ── EMA periods ────────────────────────────────────────────────────────────────
EMA_FAST      = int(os.getenv("EMA_FAST",      "20"))
EMA_MID       = int(os.getenv("EMA_MID",       "50"))
EMA_SLOW      = int(os.getenv("EMA_SLOW",      "200"))
EMA_REGIME_1W = int(os.getenv("EMA_REGIME_1W", "20"))   # EMA period for 1W regime gate
EMA_REGIME_1D = int(os.getenv("EMA_REGIME_1D", "50"))   # EMA period for 1D regime gate

# ── ATR / Volume ───────────────────────────────────────────────────────────────
ATR_PERIOD     = int(os.getenv("ATR_PERIOD",     "14"))
VOL_SMA_PERIOD = int(os.getenv("VOL_SMA_PERIOD", "20"))

# ── Swing structure ────────────────────────────────────────────────────────────
SWING_PROMINENCE_1D    = int(os.getenv("SWING_PROMINENCE_1D",    "2"))
SWING_PROMINENCE_4H    = int(os.getenv("SWING_PROMINENCE_4H",    "2"))
SWING_LOOKBACK_1D      = int(os.getenv("SWING_LOOKBACK_1D",      "20"))
SWING_LOOKBACK_1D_LONG = int(os.getenv("SWING_LOOKBACK_1D_LONG", "60"))

# ── RR minimums ───────────────────────────────────────────────────────────────
# TP2 remains the main swing target, but TP1 must now also justify the initial risk.
RR_MIN_TIER1     = float(os.getenv("RR_MIN_TIER1",     "1.8"))  # BTC, ETH TP2
RR_MIN_TIER2     = float(os.getenv("RR_MIN_TIER2",     "2.0"))  # other symbols TP2
RR_MIN_TP1_TIER1 = float(os.getenv("RR_MIN_TP1_TIER1", "1.0"))  # BTC, ETH TP1
RR_MIN_TP1_TIER2 = float(os.getenv("RR_MIN_TP1_TIER2", "1.2"))  # other symbols TP1

# ── Idea lifecycle / post-stop protection ─────────────────────────────────────
MAX_IDEA_DURATION_DAYS = int(os.getenv("MAX_IDEA_DURATION_DAYS", "10"))
POST_SL_COOLDOWN_HOURS = int(os.getenv("POST_SL_COOLDOWN_HOURS", "72"))
REPEATED_SL_LOCK_HOURS = int(os.getenv("REPEATED_SL_LOCK_HOURS", "168"))
MAX_CONSECUTIVE_SL_SAME_SIDE = int(os.getenv("MAX_CONSECUTIVE_SL_SAME_SIDE", "2"))

# ── Phase 8L.4 persistent diagnostic database ────────────────────────────────
# Observability only: these settings MUST NOT change signal eligibility.
DIAGNOSTICS_DB_ENABLED = _bool_env("DIAGNOSTICS_DB_ENABLED", False)
DIAGNOSTICS_DB_PATH = os.getenv(
    "DIAGNOSTICS_DB_PATH", "/data/cryptobot_diagnostics.sqlite3"
).strip()
DIAGNOSTICS_OUTCOME_DAYS = int(os.getenv("DIAGNOSTICS_OUTCOME_DAYS", "10"))

# ── Setup scoring / quality gates ──────────────────────────────────────────────
# Phase 8L.3 defaults restore pre-8L.1 signal flow. Temporal/lifecycle safety
# fixes remain active; these quality gates can still be re-enabled via ENV.
MIN_SCORE_NORMAL               = int(os.getenv("MIN_SCORE_NORMAL",               "55"))
MIN_SCORE_CHOP                 = int(os.getenv("MIN_SCORE_CHOP",                 "85"))
LIQUIDITY_SWEEP_PRIORITY_SCORE = int(os.getenv("LIQUIDITY_SWEEP_PRIORITY_SCORE", "85"))
SECONDARY_CANDIDATE_MIN_SCORE  = int(os.getenv("SECONDARY_CANDIDATE_MIN_SCORE",  "0"))
REQUIRE_4H_TREND_ALIGNMENT     = _bool_env("REQUIRE_4H_TREND_ALIGNMENT", False)
REQUIRE_TP_REVERSAL_CONFIRM    = _bool_env("REQUIRE_TP_REVERSAL_CONFIRM", False)
REQUIRE_TP_REVERSAL_VOLUME     = _bool_env("REQUIRE_TP_REVERSAL_VOLUME", False)
REQUIRE_LS_REVERSAL_CONFIRM    = _bool_env("REQUIRE_LS_REVERSAL_CONFIRM", False)
REQUIRE_LS_SWEEP_VOLUME        = _bool_env("REQUIRE_LS_SWEEP_VOLUME", False)

# ── Volume multipliers (per setup type) ───────────────────────────────────────
BREAKOUT_VOL_MIN             = float(os.getenv("BREAKOUT_VOL_MIN",             "1.2"))
BREAKOUT_VOL_STRONG          = float(os.getenv("BREAKOUT_VOL_STRONG",          "1.5"))
PULLBACK_REVERSAL_VOL_MIN    = float(os.getenv("PULLBACK_REVERSAL_VOL_MIN",    "1.0"))
PULLBACK_REVERSAL_VOL_STRONG = float(os.getenv("PULLBACK_REVERSAL_VOL_STRONG", "1.2"))
SWEEP_VOL_MIN                = float(os.getenv("SWEEP_VOL_MIN",                "1.2"))
SWEEP_VOL_STRONG             = float(os.getenv("SWEEP_VOL_STRONG",             "1.5"))

# ── Breakout retest window ─────────────────────────────────────────────────────
BREAKOUT_RETEST_MAX_BARS_1D = int(os.getenv("BREAKOUT_RETEST_MAX_BARS_1D", "5"))

# ── Liquidity sweep lookback ───────────────────────────────────────────────────
# How many recent closed 1D bars to scan when searching for the sweep candle.
SWEEP_LOOKBACK_1D = int(os.getenv("SWEEP_LOOKBACK_1D", "20"))

# ── ATR fallback TP multipliers (used only when no structural target found) ────
TP1_ATR_MULT_BREAKOUT = float(os.getenv("TP1_ATR_MULT_BREAKOUT", "1.0"))
TP2_ATR_MULT_BREAKOUT = float(os.getenv("TP2_ATR_MULT_BREAKOUT", "2.0"))
TP1_ATR_MULT_PULLBACK = float(os.getenv("TP1_ATR_MULT_PULLBACK", "1.5"))
TP2_ATR_MULT_PULLBACK = float(os.getenv("TP2_ATR_MULT_PULLBACK", "2.5"))
TP1_ATR_MULT_SWEEP    = float(os.getenv("TP1_ATR_MULT_SWEEP",    "1.2"))
TP2_ATR_MULT_SWEEP    = float(os.getenv("TP2_ATR_MULT_SWEEP",    "2.2"))

# ── Service ────────────────────────────────────────────────────────────────────
KEEPALIVE_SEC  = int(os.getenv("KEEPALIVE_SEC",  str(13 * 60)))
WATCHDOG_SEC   = int(os.getenv("WATCHDOG_SEC",   "120"))
# Exit if no successful poll cycle for this many seconds (triggers host restart)
STALL_EXIT_SEC = int(os.getenv("STALL_EXIT_SEC", "600"))


# =============================================================================
# === 2. UTILS ===
# =============================================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> int:
    return int(time.time())


def setup_logging(level: str) -> None:
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        force=True,
    )


def get_broadcast_targets() -> List[int]:
    """
    Canonical broadcast target list used by send_signal, send_idea_update,
    startup notification, and /close duplicate prevention.

    Returns PRIMARY_RECIPIENTS when non-empty; otherwise falls back to
    ALLOWED_CHAT_IDS[:1].  Returns [] when both are empty.
    """
    return PRIMARY_RECIPIENTS or (ALLOWED_CHAT_IDS[:1] if ALLOWED_CHAT_IDS else [])


logger = logging.getLogger("cryptobot.swing")


async def report_error(
    app: web.Application,
    where: str,
    exc: Optional[BaseException] = None,
    note: Optional[str] = None,
) -> None:
    """Send a throttled error report to Telegram (requires REPORT_ERRORS_TO_TG=1)."""
    if not REPORT_ERRORS_TO_TG:
        return
    tg = app.get("tg")
    if not tg:
        return
    t    = now_s()
    # Use runtime_state dict (set in on_startup) instead of mutating app directly.
    # Mutating app after startup triggers aiohttp DeprecationWarning.
    runtime_state = app.get("runtime_state")
    if runtime_state is None:
        runtime_state = {"last_error_ts": 0}
    last = runtime_state.get("last_error_ts", 0)
    if t - last < ERROR_REPORT_COOLDOWN_SEC:
        return
    runtime_state["last_error_ts"] = t
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    body = f"\n<b>Note:</b> {html.escape(note)}" if note else ""
    if exc:
        tb   = traceback.format_exc()
        tail = "\n".join(tb.strip().splitlines()[-20:])
        body += "\n<pre>" + html.escape(tail[:3500]) + "</pre>"
    text    = f"⚠️ <b>Runtime error</b> @ {html.escape(where)}\n🕒 {ts} UTC{body}"
    targets = get_broadcast_targets()
    for chat_id in targets:
        with contextlib.suppress(Exception):
            await tg.send(chat_id, text)


# =============================================================================
# === 3. INDICATORS ===
# =============================================================================

# Bar is a plain tuple for performance; use named constants everywhere — no magic indexes.
# Bybit kline format: [startTime, open, high, low, close, volume, turnover]
Bar = Tuple[int, float, float, float, float, float]  # start_ms, open, high, low, close, volume

B_TS     = 0   # start_ms  — Unix milliseconds, bar open time
B_OPEN   = 1
B_HIGH   = 2
B_LOW    = 3
B_CLOSE  = 4
B_VOLUME = 5

# Confirmation timestamps stored by detectors are bar OPEN times.  A setup only
# becomes knowable/actionable after that confirmation candle has CLOSED.
TF_BAR_DURATION_MS: Dict[str, int] = {
    "1h": 3_600_000,
    "4h": 4 * 3_600_000,
    "1d": 86_400_000,
}


def confirmation_available_ts_ms(setup_ts: int, setup_tf: str) -> int:
    """Return the first millisecond at which a confirmation candle is fully known."""
    if setup_ts <= 0:
        return 0
    return setup_ts + TF_BAR_DURATION_MS.get(setup_tf, 0)


def ema_series(values: List[float], period: int) -> List[float]:
    """
    Full EMA series. Output length equals input length.
    The first (period-1) elements are padded with the seed SMA value for
    alignment, so the result can be zipped with the original series.
    The final element is the current EMA.
    """
    if len(values) < period:
        return [sum(values[: i + 1]) / (i + 1) for i in range(len(values))]
    k    = 2.0 / (period + 1.0)
    seed = sum(values[:period]) / period
    res  = [seed]
    for v in values[period:]:
        res.append(v * k + res[-1] * (1.0 - k))
    # Pad the head so output length == input length
    return [res[0]] * (period - 1) + res


def calc_atr(bars: List[Bar], period: int) -> float:
    """
    Simple ATR: arithmetic mean of the last `period` true ranges.
    Requires at least period+1 bars (needs previous close for first TR).
    bars[i] = (start_ms, open, high, low, close, volume)
    """
    if len(bars) < period + 1:
        return 0.0
    total = 0.0
    for i in range(len(bars) - period, len(bars)):
        h, lo, pc = bars[i][B_HIGH], bars[i][B_LOW], bars[i - 1][B_CLOSE]
        total += max(h - lo, abs(h - pc), abs(lo - pc))
    return total / period


def calc_vol_sma(bars: List[Bar], period: int) -> float:
    """
    Volume SMA over the last `period` bars.
    Assumes all passed bars are closed (caller is responsible for exclusion
    of the forming bar before calling this function).
    """
    if len(bars) < period:
        return 0.0
    return sum(b[B_VOLUME] for b in bars[-period:]) / period


def find_swing_highs(bars: List[Bar], lookback: int, prominence: int) -> List[float]:
    """
    Return confirmed swing HIGH prices from the last `lookback` bars.

    A bar at index i within the search window is a confirmed swing high if its
    HIGH is strictly greater than the HIGH of every bar within `prominence`
    positions on each side.

    Args:
        bars:       List of CLOSED bars. The caller must exclude the forming
                    bar before passing (closed-candle rule).
        lookback:   How many trailing bars to search within. Bars outside this
                    window are not examined.
        prominence: Number of bars on each side that must have a lower high.
                    Default 2 avoids single-bar noise without being too slow.

    Returns:
        Swing high prices in chronological order (oldest first).
        Use [-1] to access the most recent confirmed swing high.
    """
    window  = bars[-lookback:] if len(bars) > lookback else bars
    min_len = 2 * prominence + 1
    if len(window) < min_len:
        return []
    result: List[float] = []
    for i in range(prominence, len(window) - prominence):
        h = window[i][B_HIGH]
        if (all(h > window[i - j][B_HIGH] for j in range(1, prominence + 1)) and
                all(h > window[i + j][B_HIGH] for j in range(1, prominence + 1))):
            result.append(h)
    return result


def find_swing_lows(bars: List[Bar], lookback: int, prominence: int) -> List[float]:
    """
    Return confirmed swing LOW prices from the last `lookback` bars.

    A bar at index i is a confirmed swing low if its LOW is strictly less than
    the LOW of every bar within `prominence` positions on each side.

    Args:
        bars:       List of CLOSED bars (caller excludes the forming bar).
        lookback:   How many trailing bars to search within.
        prominence: Number of bars on each side that must have a higher low.

    Returns:
        Swing low prices in chronological order (oldest first).
        Use [-1] to access the most recent confirmed swing low.
    """
    window  = bars[-lookback:] if len(bars) > lookback else bars
    min_len = 2 * prominence + 1
    if len(window) < min_len:
        return []
    result: List[float] = []
    for i in range(prominence, len(window) - prominence):
        lo = window[i][B_LOW]
        if (all(lo < window[i - j][B_LOW] for j in range(1, prominence + 1)) and
                all(lo < window[i + j][B_LOW] for j in range(1, prominence + 1))):
            result.append(lo)
    return result


def update_indicators(state: "SymbolState", tfs_updated: List[str]) -> None:
    """
    Recompute and cache indicator values on `state` for each TF in `tfs_updated`.

    Closed-candle rule: bars[:-1] is used for ALL calculations. The last bar
    in any series is excluded because it may still be forming at poll time.
    Only indicators relevant to updated TFs are recomputed; others are untouched.

    Called after new bar data is written to state.bars_<tf>.
    """

    # ── 1W ─── regime EMA only ────────────────────────────────────────────────
    if "1w" in tfs_updated and len(state.bars_1w) > 1:
        closed = state.bars_1w[:-1]
        closes = [b[B_CLOSE] for b in closed]
        if len(closes) >= EMA_REGIME_1W:
            state.ema20_1w = ema_series(closes, EMA_REGIME_1W)[-1]

    # ── 1D ─── primary trading timeframe ──────────────────────────────────────
    if "1d" in tfs_updated and len(state.bars_1d) > 1:
        closed = state.bars_1d[:-1]
        closes = [b[B_CLOSE] for b in closed]
        n      = len(closes)

        if n >= EMA_FAST:
            state.ema20_1d  = ema_series(closes, EMA_FAST)[-1]
        if n >= EMA_MID:
            state.ema50_1d  = ema_series(closes, EMA_MID)[-1]
        if n >= EMA_SLOW:
            state.ema200_1d = ema_series(closes, EMA_SLOW)[-1]
        if n >= ATR_PERIOD + 1:
            state.atr14_1d  = calc_atr(closed, ATR_PERIOD)
        if n >= VOL_SMA_PERIOD:
            state.vol_sma20_1d = calc_vol_sma(closed, VOL_SMA_PERIOD)

        # Swing highs/lows — short window (nearest TP/SL levels, setup detection)
        if n >= 2 * SWING_PROMINENCE_1D + 1:
            state.swing_highs_1d = find_swing_highs(
                closed, SWING_LOOKBACK_1D, SWING_PROMINENCE_1D
            )
            state.swing_lows_1d = find_swing_lows(
                closed, SWING_LOOKBACK_1D, SWING_PROMINENCE_1D
            )
        # Swing highs/lows — long window (TP2 target search)
        if n >= 2 * SWING_PROMINENCE_1D + 1:
            state.swing_highs_1d_long = find_swing_highs(
                closed, SWING_LOOKBACK_1D_LONG, SWING_PROMINENCE_1D
            )
            state.swing_lows_1d_long = find_swing_lows(
                closed, SWING_LOOKBACK_1D_LONG, SWING_PROMINENCE_1D
            )

        # Trend direction: UP/DOWN requires EMA20 > EMA50 alignment
        if state.ema20_1d > 0 and state.ema50_1d > 0 and closes:
            c = closes[-1]
            if c > state.ema20_1d and state.ema20_1d > state.ema50_1d:
                state.trend_1d = "UP"
            elif c < state.ema20_1d and state.ema20_1d < state.ema50_1d:
                state.trend_1d = "DOWN"
            else:
                state.trend_1d = "NONE"

    # ── 4H ─── setup structure and entry confirmation ─────────────────────────
    if "4h" in tfs_updated and len(state.bars_4h) > 1:
        closed = state.bars_4h[:-1]
        closes = [b[B_CLOSE] for b in closed]
        n      = len(closes)

        if n >= EMA_FAST:
            state.ema20_4h = ema_series(closes, EMA_FAST)[-1]
        if n >= EMA_MID:
            state.ema50_4h = ema_series(closes, EMA_MID)[-1]
        if n >= ATR_PERIOD + 1:
            state.atr14_4h = calc_atr(closed, ATR_PERIOD)
        if n >= 2 * SWING_PROMINENCE_4H + 1:
            state.swing_highs_4h = find_swing_highs(
                closed, SWING_LOOKBACK_1D, SWING_PROMINENCE_4H
            )
            state.swing_lows_4h = find_swing_lows(
                closed, SWING_LOOKBACK_1D, SWING_PROMINENCE_4H
            )

    # ── 1H ─── entry refinement ───────────────────────────────────────────────
    if "1h" in tfs_updated and len(state.bars_1h) > 1:
        closed = state.bars_1h[:-1]
        closes = [b[B_CLOSE] for b in closed]
        n      = len(closes)

        if n >= EMA_FAST:
            state.ema20_1h = ema_series(closes, EMA_FAST)[-1]
        if n >= EMA_MID:
            state.ema50_1h = ema_series(closes, EMA_MID)[-1]

    # ── 1M ─── no indicators; bars cached for background context only ─────────

    # ── Readiness flag ─────────────────────────────────────────────────────────
    # All primary indicators must be non-zero. EMA200 on 1D is excluded from
    # the readiness requirement because newer coins may lack 200 daily bars.
    state.ready = (
        state.ema20_1w  > 0.0
        and state.ema50_1d  > 0.0
        and state.ema20_1d  > 0.0
        and state.atr14_1d  > 0.0
        and state.ema20_4h  > 0.0
        and state.ema20_1h  > 0.0
    )


# ── Candle-structure helpers ───────────────────────────────────────────────────

def candle_closes_upper_pct(bar: Bar, pct: float) -> bool:
    """
    True if the bar's close is in the upper `pct` fraction of its high-low range.
    Example: pct=0.40 means close >= low + 0.60 × range (upper 40%).
    Returns False when range is zero (doji with no movement).
    """
    rng = bar[B_HIGH] - bar[B_LOW]
    if rng <= 0:
        return False
    return bar[B_CLOSE] >= bar[B_LOW] + (1.0 - pct) * rng


def candle_closes_lower_pct(bar: Bar, pct: float) -> bool:
    """
    True if the bar's close is in the lower `pct` fraction of its high-low range.
    Example: pct=0.40 means close <= high - 0.60 × range (lower 40%).
    Returns False when range is zero.
    """
    rng = bar[B_HIGH] - bar[B_LOW]
    if rng <= 0:
        return False
    return bar[B_CLOSE] <= bar[B_HIGH] - (1.0 - pct) * rng


def is_bullish_retest_candle(bar: Bar, prev_bar: Optional[Bar] = None) -> bool:
    """
    True if the bar shows bullish reversal character at a support/retest level.

    Qualifies as either:
      Hammer (pin bar):
        - lower wick >= 2 × body size
        - close in upper 60% of bar range
      Bullish engulfing (requires prev_bar):
        - current bar is green (close > open)
        - opens at or below previous bar's close
        - closes at or above previous bar's open
        - current body >= previous body size
    """
    body = abs(bar[B_CLOSE] - bar[B_OPEN])
    rng  = bar[B_HIGH] - bar[B_LOW]
    if rng <= 0:
        return False
    lower_wick = min(bar[B_CLOSE], bar[B_OPEN]) - bar[B_LOW]

    if lower_wick >= 2.0 * body and bar[B_CLOSE] >= bar[B_LOW] + 0.6 * rng:
        return True   # hammer / pin bar

    if prev_bar is not None:
        prev_body = abs(prev_bar[B_CLOSE] - prev_bar[B_OPEN])
        if (bar[B_CLOSE] > bar[B_OPEN]
                and bar[B_OPEN]  <= prev_bar[B_CLOSE]
                and bar[B_CLOSE] >= prev_bar[B_OPEN]
                and body >= prev_body):
            return True   # bullish engulfing

    return False


def is_bearish_retest_candle(bar: Bar, prev_bar: Optional[Bar] = None) -> bool:
    """
    True if the bar shows bearish reversal character at a resistance/retest level.

    Qualifies as either:
      Shooting star (pin bar):
        - upper wick >= 2 × body size
        - close in lower 60% of bar range
      Bearish engulfing (requires prev_bar):
        - current bar is red (close < open)
        - opens at or above previous bar's close
        - closes at or below previous bar's open
        - current body >= previous body size
    """
    body = abs(bar[B_CLOSE] - bar[B_OPEN])
    rng  = bar[B_HIGH] - bar[B_LOW]
    if rng <= 0:
        return False
    upper_wick = bar[B_HIGH] - max(bar[B_CLOSE], bar[B_OPEN])

    if upper_wick >= 2.0 * body and bar[B_CLOSE] <= bar[B_HIGH] - 0.6 * rng:
        return True   # shooting star

    if prev_bar is not None:
        prev_body = abs(prev_bar[B_CLOSE] - prev_bar[B_OPEN])
        if (bar[B_CLOSE] < bar[B_OPEN]
                and bar[B_OPEN]  >= prev_bar[B_CLOSE]
                and bar[B_CLOSE] <= prev_bar[B_OPEN]
                and body >= prev_body):
            return True   # bearish engulfing

    return False


def calc_rr(side: str, entry_mid: float, stop_loss: float, tp: float) -> float:
    """
    Risk-reward ratio: abs(tp - entry_mid) / abs(entry_mid - stop_loss).
    Returns 0.0 on degenerate input (zero or negative risk).
    Rounded to 2 decimal places.
    """
    risk = abs(entry_mid - stop_loss)
    if risk <= 0:
        return 0.0
    return round(abs(tp - entry_mid) / risk, 2)


# =============================================================================
# === 4. CLIENTS ===
# =============================================================================

class BybitWS:
    """
    WebSocket client — PRESERVED FOR v19 UPGRADE PATH.
    Not instantiated in v18 MVP (REST polling architecture).
    Logic is unchanged from v17 and remains functional.
    To activate: instantiate in on_startup and subscribe to required topics.
    """

    def __init__(self, url: str, http: aiohttp.ClientSession) -> None:
        self.url, self.http = url, http
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.on_message = None
        self._running   = False

    async def connect(self) -> None:
        with contextlib.suppress(Exception):
            if self.ws and not self.ws.closed:
                await self.ws.close()
        self.ws = await self.http.ws_connect(self.url, heartbeat=30)
        logger.info("BybitWS connected")

    async def subscribe(self, topics: List[str]) -> None:
        if not self.ws or self.ws.closed:
            await self.connect()
        for i in range(0, len(topics), 10):
            await self.ws.send_json({"op": "subscribe", "args": topics[i:i + 10]})
            await asyncio.sleep(0.05)

    async def run(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                if not self.ws or self.ws.closed:
                    await self.connect()
                async for msg in self.ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if self.on_message:
                            res = self.on_message(data)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception:
                logger.exception("WS error, reconnecting...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30.0)

    async def stop(self) -> None:
        self._running = False
        if self.ws:
            await self.ws.close()


class Tg:
    """Telegram Bot API client (long-polling mode)."""

    def __init__(self, token: str, http: aiohttp.ClientSession) -> None:
        self.token    = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session  = http
        # Avoid silent command-channel failures while also preventing log spam
        # when Telegram is persistently unavailable or the token is invalid.
        self._last_get_updates_warn_ts = 0
        self._get_updates_warn_cooldown_sec = 60

    def _warn_get_updates(self, message: str) -> None:
        t = now_s()
        if t - self._last_get_updates_warn_ts >= self._get_updates_warn_cooldown_sec:
            self._last_get_updates_warn_ts = t
            logger.warning(message)

    async def delete_webhook(self, drop_pending_updates: bool = False) -> Any:
        url = f"{self.base_url}/deleteWebhook"
        async with self.session.post(
            url, json={"drop_pending_updates": drop_pending_updates}
        ) as r:
            return await r.json()

    async def get_updates(
        self, offset: Optional[int] = None, timeout: int = 25
    ) -> List[Dict]:
        url  = f"{self.base_url}/getUpdates"
        data: Dict[str, Any] = {"timeout": timeout}
        if offset is not None:
            data["offset"] = offset
        try:
            async with self.session.post(
                url,
                json=data,
                timeout=aiohttp.ClientTimeout(total=timeout + 5),
            ) as r:
                try:
                    body = await r.json()
                except Exception:
                    body = await r.text()

                if r.status == 200 and isinstance(body, dict) and body.get("ok", True):
                    return body.get("result", [])

                self._warn_get_updates(
                    f"getUpdates failed status={r.status} response={body}"
                )
        except Exception as exc:
            self._warn_get_updates(
                f"getUpdates exception {type(exc).__name__}: {exc}"
            )
        return []

    async def send(
        self,
        chat_id: Any,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        url     = f"{self.base_url}/sendMessage"
        payload: Dict[str, Any] = {
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "HTML",
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            async with self.session.post(url, json=payload) as r:
                if r.status != 200:
                    try:
                        body = await r.json()
                    except Exception:
                        body = await r.text()
                    logger.warning(
                        f"sendMessage failed chat_id={chat_id} "
                        f"status={r.status} response={body}"
                    )
                    return False
                return True
        except Exception as exc:
            logger.warning(f"sendMessage exception chat_id={chat_id}: {exc}")
            return False


def command_keyboard() -> Dict[str, Any]:
    """
    Telegram ReplyKeyboardMarkup with quick-access buttons for frequently used
    no-argument commands.

    Preserved for possible future private/group use (e.g. personal admin chats).
    It MUST NOT be attached to channel broadcasts — Telegram channels reject
    ReplyKeyboardMarkup and the message will silently fail to deliver.
    (Phase 8B.1 removed reply_markup from all channel sends.)

    Intentionally omits commands that require symbol input (/idea, /close,
    /score) or that can perform irreversible state changes without confirmation.
    """
    return {
        "keyboard": [
            [{"text": "/status"}, {"text": "/regime"}],
            [{"text": "/ideas"},  {"text": "/config"}],
            [{"text": "/apikey"}, {"text": "/bybit"}],
            [{"text": "/diag"},   {"text": "/ping"}],
        ],
        "resize_keyboard":   True,
        "one_time_keyboard": False,
        "is_persistent":     True,
    }


class BybitRest:
    """Bybit V5 REST client (public endpoints only)."""

    def __init__(self, base: str, http: aiohttp.ClientSession) -> None:
        self.base = base.rstrip("/")
        self.http = http

    async def tickers_linear(self) -> List[Dict[str, Any]]:
        """Fetch all USDT perpetual tickers."""
        url = f"{self.base}/v5/market/tickers?category=linear"
        async with self.http.get(
            url, timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return (await r.json()).get("result", {}).get("list", [])

    async def klines(
        self, symbol: str, interval: str, limit: int = 200
    ) -> List[Bar]:
        """
        Fetch OHLCV bars for a symbol, preserving the bar open timestamp.

        Bybit returns bars newest-first; we reverse to get chronological order.
        The LAST bar in the returned list may be the currently forming candle.
        Use bars[:-1] for indicator calculations (closed-candle rule).

        Args:
            symbol:   e.g. "BTCUSDT"
            interval: Bybit interval string — "60", "240", "D", "W", "M"
            limit:    Number of bars requested (Bybit max: 1000 for most TFs)

        Returns:
            Bars in chronological order as
            (start_ms, open, high, low, close, volume).
            Use B_TS / B_OPEN / B_HIGH / B_LOW / B_CLOSE / B_VOLUME constants.
        """
        url = (
            f"{self.base}/v5/market/kline"
            f"?category=linear&symbol={symbol}&interval={interval}&limit={limit}"
        )
        async with self.http.get(
            url, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            raw = (await r.json()).get("result", {}).get("list", [])
            # Bybit format: [startTime, open, high, low, close, volume, turnover]
            return [
                (
                    int(it[0]),    # start_ms — bar open time in Unix milliseconds
                    float(it[1]),  # open
                    float(it[2]),  # high
                    float(it[3]),  # low
                    float(it[4]),  # close
                    float(it[5]),  # volume
                )
                for it in reversed(raw)
            ]


    async def ticker_linear(self, symbol: str) -> Dict[str, Any]:
        """Fetch one linear ticker, including current funding-rate fields."""
        url = f"{self.base}/v5/market/tickers"
        params = {"category": "linear", "symbol": symbol}
        async with self.http.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            body = await r.json()
            if r.status != 200 or int(body.get("retCode", -1)) != 0:
                raise RuntimeError(
                    f"Bybit ticker failed HTTP {r.status}: "
                    f"retCode={body.get('retCode')} retMsg={body.get('retMsg')}"
                )
            rows = body.get("result", {}).get("list", []) or []
            if not rows:
                raise RuntimeError(f"No Bybit linear ticker for {symbol}")
            return rows[0]


    async def instrument_linear(self, symbol: str) -> Dict[str, Any]:
        """Fetch current Bybit contract specification for one linear symbol."""
        url = f"{self.base}/v5/market/instruments-info"
        params = {"category": "linear", "symbol": symbol}
        async with self.http.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            body = await r.json()
            if r.status != 200 or int(body.get("retCode", -1)) != 0:
                raise RuntimeError(
                    f"Bybit instruments-info failed HTTP {r.status}: "
                    f"retCode={body.get('retCode')} retMsg={body.get('retMsg')}"
                )
            rows = body.get("result", {}).get("list", []) or []
            if not rows:
                raise RuntimeError(f"No Bybit linear instrument info for {symbol}")
            return rows[0]


# ── Phase 9A: authenticated Bybit V5 RSA bridge (GET-only) ──────────────────

class BybitPrivateReadOnly:
    """
    Authenticated Bybit V5 RSA client intentionally limited to GET endpoints.

    Security boundary for Phase 9A:
      - supported: API-key info, Unified wallet balance, linear positions
      - not implemented: create/amend/cancel order, leverage/margin changes,
        transfers, withdrawals, or any other state-changing endpoint

    RSA signing follows Bybit V5: timestamp + api_key + recv_window + queryString,
    signed with RSA-SHA256 / PKCS#1 v1.5 and base64 encoded.
    """

    def __init__(
        self,
        base: str,
        http: aiohttp.ClientSession,
        api_key: str,
        private_key_path: str,
        recv_window: int = 5000,
    ) -> None:
        self.base = base.rstrip("/")
        self.http = http
        self.api_key = api_key.strip()
        self.private_key_path = private_key_path.strip()
        self.recv_window = int(recv_window)
        self._private_key: Any = None

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.private_key_path)

    def _load_private_key(self) -> Any:
        if self._private_key is not None:
            return self._private_key
        if not self.private_key_path:
            raise RuntimeError("BYBIT_PRIVATE_KEY_PATH is empty")
        try:
            with open(self.private_key_path, "rb") as fh:
                pem = fh.read()
        except OSError as exc:
            raise RuntimeError(
                f"Bybit RSA private key is not readable at {self.private_key_path}"
            ) from exc
        try:
            self._private_key = serialization.load_pem_private_key(pem, password=None)
        except Exception as exc:
            raise RuntimeError("Bybit RSA private key is not a valid unencrypted PEM key") from exc
        return self._private_key

    def _sign_get(self, timestamp_ms: int, query_string: str) -> str:
        key = self._load_private_key()
        payload = (
            f"{timestamp_ms}{self.api_key}{self.recv_window}{query_string}"
        ).encode("utf-8")
        signature = key.sign(payload, padding.PKCS1v15(), hashes.SHA256())
        return base64.b64encode(signature).decode("ascii")

    async def _get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("BYBIT_API_KEY is empty")

        clean_params = {
            str(k): v for k, v in (params or {}).items() if v is not None
        }
        # Match pybit/Bybit examples: deterministic alphabetical query order.
        query_string = urlencode(sorted(clean_params.items()), doseq=True)
        timestamp_ms = now_ms()
        signature = self._sign_get(timestamp_ms, query_string)
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": str(timestamp_ms),
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "Accept": "application/json",
        }
        url = f"{self.base}{path}"
        if query_string:
            url += f"?{query_string}"

        async with self.http.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as response:
            try:
                body = await response.json()
            except Exception as exc:
                raw = await response.text()
                raise RuntimeError(
                    f"Bybit private GET {path} returned non-JSON HTTP {response.status}: "
                    f"{raw[:200]}"
                ) from exc

            if response.status != 200:
                raise RuntimeError(
                    f"Bybit private GET {path} HTTP {response.status}: "
                    f"retCode={body.get('retCode')} retMsg={body.get('retMsg')}"
                )
            if int(body.get("retCode", -1)) != 0:
                raise RuntimeError(
                    f"Bybit private GET {path} failed: "
                    f"retCode={body.get('retCode')} retMsg={body.get('retMsg')}"
                )
            return body

    async def api_key_info(self) -> Dict[str, Any]:
        body = await self._get("/v5/user/query-api")
        return body.get("result", {}) or {}

    async def wallet_balance(self) -> Dict[str, Any]:
        body = await self._get(
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED", "coin": "USDT"},
        )
        rows = body.get("result", {}).get("list", []) or []
        return rows[0] if rows else {}

    async def positions_linear(self) -> List[Dict[str, Any]]:
        body = await self._get(
            "/v5/position/list",
            {"category": "linear", "settleCoin": "USDT", "limit": 200},
        )
        rows = body.get("result", {}).get("list", []) or []
        # Bybit normally returns only size>0 when settleCoin is supplied; keep a
        # defensive size filter so /bybit never counts empty placeholder rows.
        return [r for r in rows if _safe_float(r.get("size")) > 0.0]

    async def fee_rate_linear(self, symbol: str) -> Dict[str, Any]:
        """Read the account's current linear trading fee rate for one symbol."""
        body = await self._get(
            "/v5/account/fee-rate",
            {"category": "linear", "symbol": symbol},
        )
        rows = body.get("result", {}).get("list", []) or []
        return rows[0] if rows else {}

    async def health_snapshot(self) -> Dict[str, Any]:
        """Query all Phase-9A read endpoints independently; never writes state."""
        out: Dict[str, Any] = {
            "api_key_info": None,
            "wallet": None,
            "positions": None,
            "errors": {},
        }
        for name, fn in (
            ("api_key_info", self.api_key_info),
            ("wallet", self.wallet_balance),
            ("positions", self.positions_linear),
        ):
            try:
                out[name] = await fn()
            except Exception as exc:
                out["errors"][name] = f"{type(exc).__name__}: {exc}"
        return out


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_bybit_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    # 1970 is Bybit's common sentinel for "no finite expiry reported".
    if dt.year <= 1971:
        return None
    return dt


def bybit_api_expiry(info: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Bybit deadlineDay/expiredAt into a safe display/reminder model."""
    ips = info.get("ips") or []
    expired_dt = _parse_bybit_datetime(info.get("expiredAt"))

    days_left: Optional[int] = None
    raw_deadline = info.get("deadlineDay")
    try:
        deadline = int(raw_deadline)
    except (TypeError, ValueError):
        deadline = -999999

    if deadline >= 0:
        days_left = deadline
    elif expired_dt is not None:
        seconds = (expired_dt - datetime.now(timezone.utc)).total_seconds()
        days_left = max(0, int(math.ceil(seconds / 86400.0)))

    if days_left is not None:
        status = "EXPIRED" if days_left <= 0 else "ACTIVE"
    elif ips:
        status = "ACTIVE_IP_BOUND"
    else:
        status = "UNKNOWN"

    return {
        "status": status,
        "days_left": days_left,
        "expired_dt": expired_dt,
        "expired_at": expired_dt.strftime("%Y-%m-%d %H:%M UTC") if expired_dt else None,
        "ips_bound": len(ips),
    }


def _apikey_expiry_identity(info: Dict[str, Any], expiry: Dict[str, Any]) -> str:
    """Stable identifier that changes after key replacement or expiry renewal."""
    expired_at = expiry.get("expired_at") or "no-expiry"
    created_at = str(info.get("createdAt") or "unknown-created")
    # Do not persist the API key itself in the reminder state file.
    return f"{created_at}|{expired_at}"


def _select_apikey_reminder_threshold(days_left: int, already_sent: Set[int]) -> Optional[int]:
    """
    Pick the most relevant newly crossed threshold.

    Example: if the bot was offline at day 21 and returns at day 13, send the
    14-day warning (not stale 30/21-day warnings).  Larger crossed thresholds
    are marked sent together after successful delivery.
    """
    due = [
        t for t in BYBIT_API_REMINDER_THRESHOLDS
        if days_left <= t and t not in already_sent
    ]
    return min(due) if due else None


def _load_apikey_reminder_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _save_apikey_reminder_state(path: str, state: Dict[str, Any]) -> None:
    try:
        parent = os.path.dirname(path) or "."
        os.makedirs(parent, exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, sort_keys=True, separators=(",", ":"))
        os.replace(tmp, path)
    except OSError as exc:
        logger.warning(
            f"Bybit API reminder state could not be persisted at {path}: {exc}"
        )


async def maybe_send_apikey_expiry_reminder(
    app: web.Application,
    info: Optional[Dict[str, Any]] = None,
) -> None:
    """Send at most one current 30/21/14/7/1-day warning per expiry cycle."""
    client = app.get("bybit_private")
    if not isinstance(client, BybitPrivateReadOnly):
        return

    if info is None:
        info = await client.api_key_info()
    expiry = bybit_api_expiry(info)
    days_left = expiry.get("days_left")
    if days_left is None or days_left > max(BYBIT_API_REMINDER_THRESHOLDS):
        return

    runtime_state = app.get("runtime_state") or {}
    state = runtime_state.get("apikey_reminder_state")
    if not isinstance(state, dict):
        state = _load_apikey_reminder_state(BYBIT_API_REMINDER_STATE_PATH)
        runtime_state["apikey_reminder_state"] = state

    expiry_id = _apikey_expiry_identity(info, expiry)
    if state.get("expiry_id") != expiry_id:
        state.clear()
        state.update({"expiry_id": expiry_id, "sent_thresholds": []})

    already_sent = {
        int(x) for x in state.get("sent_thresholds", [])
        if isinstance(x, (int, float, str)) and str(x).lstrip("-").isdigit()
    }
    threshold = _select_apikey_reminder_threshold(int(days_left), already_sent)
    if threshold is None:
        return

    expiry_text = expiry.get("expired_at") or "not reported"
    text = (
        "⚠️ <b>Bybit API key expiry</b>\n\n"
        f"<b>Remaining:</b> {int(days_left)} day{'s' if int(days_left) != 1 else ''}\n"
        f"<b>Expires:</b> {html.escape(expiry_text)}\n\n"
        "Renew the key validity in Bybit before expiry. Check anytime with /apikey."
    )
    delivered = False
    for chat_id in get_broadcast_targets():
        with contextlib.suppress(Exception):
            delivered = (await app["tg"].send(chat_id, text)) or delivered

    if delivered:
        # Mark the selected threshold and any older/larger threshold as handled,
        # preventing catch-up spam after downtime.
        handled = {
            t for t in BYBIT_API_REMINDER_THRESHOLDS if t >= threshold
        }
        already_sent.update(handled)
        state["sent_thresholds"] = sorted(already_sent, reverse=True)
        state["last_days_left"] = int(days_left)
        state["last_sent_ts"] = now_s()
        _save_apikey_reminder_state(BYBIT_API_REMINDER_STATE_PATH, state)


async def apikey_reminder_loop(app: web.Application) -> None:
    """Periodic key-expiry watcher.  GET-only and isolated from trading logic."""
    while True:
        try:
            await asyncio.sleep(max(3600, BYBIT_API_REMINDER_CHECK_SEC))
            await maybe_send_apikey_expiry_reminder(app)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                f"Bybit API expiry check failed: {type(exc).__name__}: {exc}"
            )


# =============================================================================
# === 5. STATE / DATA CLASSES ===
# =============================================================================

@dataclass
class ActiveIdea:
    """
    Represents a live swing trade idea that has been emitted to Telegram.
    Remains active until TP2 hit, SL hit, manual close, or expiry.
    """
    symbol:       str
    side:         str        # "LONG" | "SHORT"
    setup_type:   str        # "BREAKOUT_RETEST" | "TREND_PULLBACK" | "LIQUIDITY_SWEEP"
    setup_score:  int        # 0–100

    entry_low:    float      # lower bound of the suggested entry zone
    entry_high:   float      # upper bound of the suggested entry zone
    stop_loss:    float

    tp1:          float
    tp2:          float
    rr_tp1:       float      # RR using entry_mid as the reference point
    rr_tp2:       float

    status:       str        # "ACTIVE" | "TP1_HIT" | "TP2_HIT" | "SL_HIT"
                             #           | "EXPIRED" | "INVALIDATED" | "AMBIGUOUS"
    emitted_at:   int        # unix seconds
    expires_at:   int        # = emitted_at + MAX_IDEA_DURATION_DAYS * 86400
    invalidation: str        # plain-text thesis invalidation note sent in signal

    tp1_hit_at:   Optional[int] = None   # unix seconds when TP1 was first tagged
    # ── Phase 8A additions ───────────────────────────────────────────────────
    # Price sampled from the latest available bar at the moment the signal was
    # emitted.  Used to display "Current price" and "IN ENTRY ZONE" in the
    # Telegram message.  0.0 = not recorded (old ideas or test fixtures).
    current_price_at_signal: float = 0.0
    # Millisecond timestamp of the setup confirmation bar (same value as
    # SetupResult.setup_ts).  Used to display "Setup age" in the signal.
    # 0 = not recorded.
    setup_ts: int = 0
    # Timeframe of the confirmation candle whose OPEN timestamp is setup_ts.
    # Used to derive the true confirmation-available time (bar close).
    setup_tf: str = ""

    @property
    def entry_mid(self) -> float:
        """Midpoint of the entry zone; used as reference for RR calculations."""
        return (self.entry_low + self.entry_high) / 2.0


@dataclass
class SymbolState:
    """Per-symbol state: raw bar stores, cached indicator values, regime."""

    # ── Raw bar stores (fully replaced on each REST refresh) ──────────────────
    bars_1m: List[Bar] = field(default_factory=list)   # monthly
    bars_1w: List[Bar] = field(default_factory=list)   # weekly
    bars_1d: List[Bar] = field(default_factory=list)   # daily
    bars_4h: List[Bar] = field(default_factory=list)   # 4-hour
    bars_1h: List[Bar] = field(default_factory=list)   # 1-hour

    # ── 1W indicators ─────────────────────────────────────────────────────────
    ema20_1w: float = 0.0    # EMA{EMA_REGIME_1W} of weekly closes

    # ── 1D indicators (primary trading timeframe) ──────────────────────────────
    ema20_1d:     float = 0.0
    ema50_1d:     float = 0.0
    ema200_1d:    float = 0.0    # 0.0 if fewer than 200 closed 1D bars
    atr14_1d:     float = 0.0
    vol_sma20_1d: float = 0.0
    trend_1d:     str   = "NONE"  # "UP" | "DOWN" | "NONE"

    # 1D swing structure — short lookback (nearest TP/SL levels, setup detection)
    swing_highs_1d: List[float] = field(default_factory=list)
    swing_lows_1d:  List[float] = field(default_factory=list)
    # 1D swing structure — long lookback (TP2 target search)
    swing_highs_1d_long: List[float] = field(default_factory=list)
    swing_lows_1d_long:  List[float] = field(default_factory=list)

    # ── 4H indicators (setup structure, entry confirmation) ────────────────────
    ema20_4h:        float = 0.0
    ema50_4h:        float = 0.0
    atr14_4h:        float = 0.0
    swing_highs_4h:  List[float] = field(default_factory=list)
    swing_lows_4h:   List[float] = field(default_factory=list)

    # ── 1H indicators (entry refinement) ──────────────────────────────────────
    ema20_1h: float = 0.0
    ema50_1h: float = 0.0

    # ── Regime & direction ────────────────────────────────────────────────────
    regime:        str = "NEUTRAL"  # "BULLISH" | "BEARISH" | "NEUTRAL"
    regime_reason: str = ""         # human-readable; included in /regime and signals

    # ── Active idea ───────────────────────────────────────────────────────────
    active_idea: Optional[ActiveIdea] = None

    # ── Housekeeping ──────────────────────────────────────────────────────────
    last_fetch:      Dict[str, int] = field(default_factory=dict)  # tf_key → unix seconds
    last_signal_ts:  int  = 0
    last_scanned_ts: int  = 0
    ready:           bool = False

    # Phase 8L.1: post-stop memory.  Exact stopped setup keys are never reused;
    # same-direction retries also require a new confirmation after the stop and
    # must wait through the configured cooldown.
    last_exit_ts:             int = 0
    last_exit_event:          str = ""
    last_stopped_side:        str = ""
    consecutive_sl_same_side: int = 0
    post_sl_lock_until:       int = 0
    stopped_setup_keys: Set[Tuple[str, int]] = field(default_factory=set)


@dataclass
class Market:
    """Top-level shared state for the running bot."""
    symbols:    List[str]
    state:      Dict[str, SymbolState]

    btc_regime:        str = "NEUTRAL"
    btc_regime_reason: str = ""

    signal_stats: Dict[str, int] = field(default_factory=lambda: {
        "total": 0, "long": 0, "short": 0,
        "tp1_hit": 0, "tp2_hit": 0, "sl_hit": 0, "expired": 0,
        "ambiguous": 0,
    })
    last_poll_ts: int = 0
    poll_count:   int = 0

    # ── Phase 8C scan diagnostics ──────────────────────────────────────────────
    # diag_last: counters for the current keepalive interval (reset each cycle)
    # diag_total: cumulative counters since startup
    diag_last:  "ScanDiagnostics" = field(default_factory=lambda: ScanDiagnostics())
    diag_total: "ScanDiagnostics" = field(default_factory=lambda: ScanDiagnostics())

    # ── Phase 8E watchlist ────────────────────────────────────────────────────
    # One PendingSetup per symbol.  Key = symbol string.
    # A pending setup is a valid structural setup whose current price is outside
    # the entry zone.  It does not generate a trade signal; it is informational.
    pending_setups: Dict[str, "PendingSetup"] = field(default_factory=dict)

    # ── Phase 8G dead-candidate diagnostics ──────────────────────────────────
    # Rolling buffer of recent DEAD CandidateDebug records (newest last).
    # Trimmed to CANDIDATE_DEBUG_MAX entries.  Read-only from the bot perspective;
    # inspected via /candidates command.  Does not affect signal generation.
    candidate_debug: List["CandidateDebug"] = field(default_factory=list)


@dataclass
class ScanDiagnostics:
    """
    Lightweight per-scan-cycle counters that explain why candidates do or do
    not become active ideas.  Two instances live on Market:
      diag_last  — reset every keepalive cycle; shows what just happened.
      diag_total — cumulative since startup; useful for long-term trends.

    Phase 8F note: reason counters (hit_tp, hit_sl, rr_current_fail, …) are
    now CANDIDATE-level, not symbol-level.  A single scan_symbol() call may
    evaluate up to 3 candidates (BR / TP / LS) and increment each counter once
    per candidate, so totals may exceed symbols_checked.

    Counter semantics:
      symbols_checked       — entered scan_symbol
      symbols_not_ready     — symbol state not ready yet
      active_idea_lock      — symbol already has an active idea
      detector_none         — collect_setup_candidates() returned empty list
      candidates_total      — candidates evaluated (across all symbols)
      candidates_actionable — candidates that passed all gates
      candidates_pending    — candidates alive but price outside entry zone
      candidates_dead       — candidates rejected by TP/SL/context/RR/gate
      context_too_old       — setup_ts=0 or age > SETUP_CONTEXT_MAX_DAYS
      price_missing         — get_current_price() returned 0.0
      outside_entry_zone    — price outside zone (pending candidate)
      already_hit_tp        — TP1 or TP2 already touched since setup_ts
      already_hit_sl        — SL already touched since setup_ts
      invalidated_since_setup — setup explicitly invalidated since setup_ts
      tpsl_fail             — calc_swing_tpsl produced degenerate geometry (rr≤0)
      rr_current_fail       — RR below threshold (current-price or entry_mid)
      signal_gate_fail      — can_signal() returned False (score/regime/BTC filter)
      entry_retest_too_old  — old LS returned/touched entry zone too long ago
      actionable_ok         — validate_actionable_setup() returned ok
      new_idea              — ActiveIdea successfully created
      errors                — unexpected exception caught inside scan_symbol
    """
    symbols_checked:        int = 0
    symbols_not_ready:      int = 0
    active_idea_lock:       int = 0
    detector_none:          int = 0
    candidates_total:       int = 0
    candidates_actionable:  int = 0
    candidates_pending:     int = 0
    candidates_dead:        int = 0
    context_too_old:        int = 0
    price_missing:          int = 0
    outside_entry_zone:     int = 0
    already_hit_tp:         int = 0
    already_hit_sl:         int = 0
    invalidated_since_setup: int = 0
    tpsl_fail:              int = 0
    rr_current_fail:        int = 0
    signal_gate_fail:       int = 0
    entry_retest_too_old:   int = 0
    fast_4h_conflict:       int = 0
    post_sl_cooldown:       int = 0
    reused_stopped_setup:   int = 0
    secondary_score_fail:   int = 0
    actionable_ok:          int = 0
    new_idea:               int = 0
    errors:                 int = 0
    # Phase 8H: LS candidates too old for the LS-specific freshness gate
    liquidity_sweep_too_old: int = 0
    # Phase 8I: existing candidate_debug record updated in place (dedup hit)
    candidate_debug_dedup:  int = 0


@dataclass
class PendingSetup:
    """
    A signal-eligible setup whose current price is outside the entry zone.
    Does NOT generate a trade signal — informational/watchlist only.

    Phase 8L: pending setups must already pass RR + score/regime/BTC signal
    gates when evaluated from the worst acceptable entry-zone price.  In other
    words, /watchlist should show candidates that are waiting only for price to
    return into the entry zone, not candidates that would be blocked anyway.

    Stored in Market.pending_setups[sym] (one per symbol, newest replaces old).
    Cleared when the detector returns None, TP/SL was already touched, setup
    context is too old, the symbol has an active idea, or price enters the zone
    and a real signal fires.
    """
    symbol:         str
    side:           str    # "LONG" | "SHORT"
    setup_type:     str
    score:          int

    entry_low:      float
    entry_high:     float
    stop_loss:      float
    tp1:            float
    tp2:            float
    rr_tp1:         float
    rr_tp2:         float

    current_price:  float
    distance_pct:   float  # abs % distance from px to nearest entry boundary
    distance_side:  str    # "BELOW_ENTRY_ZONE" | "ABOVE_ENTRY_ZONE" | "IN_ENTRY_ZONE"
    reason:         str    # normally "outside_entry_zone"

    setup_ts:       int    # ms timestamp of confirmation bar
    setup_age_h:    int    # hours since confirmation bar
    updated_at:     int    # unix seconds when this record was last written

    regime:         str    # symbol-level regime
    btc_regime:     str    # global BTC regime at time of update
    invalidation:   str    # from SetupResult.invalidation


@dataclass
class RegimeResult:
    """Output of compute_regime()."""
    regime: str   # "BULLISH" | "BEARISH" | "NEUTRAL"
    reason: str   # human-readable detail for /regime command and signal messages


@dataclass
class SetupResult:
    """
    Output of a setup detector function.
    Defined here in Phase 0 for complete typing; populated by Phase 3 detectors.
    """
    setup_type:   str
    side:         str        # "LONG" | "SHORT"
    score:        int        # 0–100
    entry_low:    float
    entry_high:   float
    stop_loss:    float
    tp1:          float
    tp2:          float
    rr_tp1:       float
    rr_tp2:       float
    invalidation: str
    notes:        str = ""   # optional debug/log info
    # Millisecond timestamp of the confirmation bar (retest / 4H confirm / sweep confirm).
    # 0 = unknown (detectors built before Phase 8A, or test fixtures without a bar).
    # is_setup_fresh() returns False when setup_ts == 0.
    setup_ts:     int = 0
    # Confirmation timeframe ("1h" | "4h" | "1d").  setup_ts is the candle
    # OPEN time; setup_tf lets Phase 8L.2 calculate when that candle closed.
    setup_tf:     str = ""
    # Phase 8L.4: exact points awarded by the detector before any score floor.
    # Diagnostics only; does not participate in trading decisions.
    score_components: Dict[str, int] = field(default_factory=dict)


@dataclass
class CandidateEval:
    """
    Result of evaluating a single SetupResult through the Phase 8F pipeline.

    Phase 8F collects up to 3 candidates (one per detector) and evaluates each
    independently, then chooses the best actionable one.

    status:
      "ACTIONABLE" — all gates pass; ready to fire a signal
      "PENDING"    — setup is alive but price outside entry zone; goes to watchlist
      "DEAD"       — setup is stale, invalidated, or fails an early gate
      "NONE"       — no detector result (result is None)

    reason corresponds to the first failing gate or "ok" for actionable.
    """
    result:          Optional[SetupResult]
    status:          str    # "ACTIONABLE" | "PENDING" | "DEAD" | "NONE"
    reason:          str    # "ok" | gate name that failed
    rr_ok:           bool = False
    signal_ok:       bool = False
    actionable_ok:   bool = False
    pending_ok:      bool = False
    candidate_source: str = ""   # "BREAKOUT_RETEST" | "TREND_PULLBACK" | "LIQUIDITY_SWEEP"


@dataclass
class CandidateDebug:
    """
    Diagnostic snapshot of one evaluated candidate.  Stored in
    Market.candidate_debug (rolling buffer, newest-last, max CANDIDATE_DEBUG_MAX).
    Used by /candidates (/dead) command.  Diagnostics only — does not affect
    signal generation or filters.
    """
    symbol:        str
    side:          str    # "LONG" | "SHORT"
    setup_type:    str
    score:         int

    status:        str    # "ACTIONABLE" | "PENDING" | "DEAD"
    reason:        str    # first failing gate name, or "ok"

    current_price: float
    entry_low:     float
    entry_high:    float
    stop_loss:     float
    tp1:           float
    tp2:           float
    rr_tp1:        float
    rr_tp2:        float

    setup_ts:      int    # ms
    setup_age_h:   int    # hours since confirmation bar
    updated_at:    int    # unix seconds

    regime:        str
    btc_regime:    str
    notes:         str = ""




# =============================================================================
# === 5A. PHASE 9B/9C EXECUTION PLANNER + NET ECONOMICS (SHADOW ONLY) ===
# =============================================================================

@dataclass
class ExecutionPlan:
    symbol: str
    side: str
    status: str
    reason: str
    entry_price: float
    qty: float
    tp1_qty: float
    tp2_qty: float
    notional_usdt: float
    leverage: float
    margin_required_usdt: float
    equity_usdt: float
    available_usdt: float
    reserve_usdt: float
    shadow_reserved_before_usdt: float
    planner_available_before_usdt: float
    planner_available_after_usdt: float
    same_size_capacity: int
    stop_loss: float
    tp1: float
    tp2: float
    sl_distance_pct: float
    loss_at_sl_usdt: float
    account_risk_pct: float
    tp1_profit_usdt: float
    tp2_profit_usdt: float
    min_order_qty: float
    qty_step: float
    min_notional_usdt: float
    tick_size: float
    instrument_max_leverage: float
    open_positions: int
    already_open_symbol: bool

    # Phase 9C economics. All fee/funding values are estimates until real fills
    # exist. Entry and exits are conservatively modelled at taker fee rate.
    economics_ready: bool = False
    taker_fee_rate: float = 0.0
    maker_fee_rate: float = 0.0
    fee_source: str = "UNAVAILABLE"
    funding_rate: float = 0.0
    funding_interval_min: int = 0
    funding_periods_est: float = 0.0
    funding_cost_est_usdt: float = 0.0
    entry_fee_est_usdt: float = 0.0
    tp1_exit_fee_est_usdt: float = 0.0
    tp2_exit_fee_est_usdt: float = 0.0
    sl_exit_fee_est_usdt: float = 0.0
    tp1_gross_profit_usdt: float = 0.0
    tp2_gross_profit_usdt: float = 0.0
    total_gross_profit_usdt: float = 0.0
    tp1_net_profit_usdt: float = 0.0
    tp2_net_profit_usdt: float = 0.0
    total_net_profit_usdt: float = 0.0
    tp1_net_equity_pct: float = 0.0
    tp2_net_equity_pct: float = 0.0
    total_net_equity_pct: float = 0.0
    total_net_notional_pct: float = 0.0
    sl_net_pnl_usdt: float = 0.0
    sl_net_equity_pct: float = 0.0
    remaining_break_even_price: float = 0.0
    remaining_break_even_move_pct: float = 0.0


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return units * step


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    units = (value / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return units * tick


def _step_decimals(step: Any) -> int:
    d = _dec(step)
    if d <= 0:
        return 8
    return max(0, -d.normalize().as_tuple().exponent)


def _fmt_step(value: float, step: Any) -> str:
    decimals = min(12, _step_decimals(step))
    return f"{value:.{decimals}f}"



def _ceil_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    units = (value / tick).to_integral_value(rounding=ROUND_CEILING)
    return units * tick


def _floor_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    units = (value / tick).to_integral_value(rounding=ROUND_FLOOR)
    return units * tick


def _load_execution_shadow_state(path: str) -> Dict[str, Dict[str, Any]]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return raw if isinstance(raw, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning(
            f"Phase 9C shadow-state load failed {path}: "
            f"{type(exc).__name__}: {exc}"
        )
        return {}


def _save_execution_shadow_state(app: web.Application) -> None:
    if not EXECUTION_SHADOW_STATE_PATH:
        return
    runtime = app.get("runtime_state")
    if not isinstance(runtime, dict):
        return
    state = runtime.get("execution_shadow_reservations")
    if not isinstance(state, dict):
        return
    try:
        parent = os.path.dirname(EXECUTION_SHADOW_STATE_PATH) or "."
        os.makedirs(parent, exist_ok=True)
        tmp = EXECUTION_SHADOW_STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, sort_keys=True, indent=2)
        os.replace(tmp, EXECUTION_SHADOW_STATE_PATH)
    except Exception as exc:
        logger.warning(
            f"Phase 9C shadow-state save failed {EXECUTION_SHADOW_STATE_PATH}: "
            f"{type(exc).__name__}: {exc}"
        )


def _funding_interval_minutes(
    instrument: Dict[str, Any],
    ticker: Dict[str, Any],
) -> int:
    # Current REST ticker may expose fundingIntervalHour; instruments-info is
    # the authoritative fallback and documents fundingInterval in minutes.
    hours = _safe_float(ticker.get("fundingIntervalHour"))
    if hours > 0:
        return max(1, int(round(hours * 60.0)))
    try:
        minutes = int(instrument.get("fundingInterval") or 0)
    except (TypeError, ValueError):
        minutes = 0
    return minutes if minutes > 0 else 480


def _signed_funding_cost(
    side: str,
    notional_usdt: Decimal,
    funding_rate: Decimal,
    periods: Decimal,
) -> Decimal:
    """
    Positive result = cost paid by this position; negative = funding credit.
    Bybit convention: with a positive funding rate LONG pays SHORT.
    """
    direction = Decimal("1") if side == "LONG" else Decimal("-1")
    return notional_usdt * funding_rate * periods * direction


def _remaining_break_even_price(
    side: str,
    entry: Decimal,
    taker_fee_rate: Decimal,
    funding_cost_usdt: Decimal,
    remaining_qty: Decimal,
    tick: Decimal,
) -> Decimal:
    """
    Net break-even for the still-open leg only.
    Realised TP1 profit is deliberately NOT included.
    """
    if entry <= 0 or remaining_qty <= 0:
        return Decimal("0")

    funding_per_unit = funding_cost_usdt / remaining_qty

    if side == "LONG":
        denom = Decimal("1") - taker_fee_rate
        if denom <= 0:
            return Decimal("0")
        raw = (
            entry * (Decimal("1") + taker_fee_rate)
            + funding_per_unit
        ) / denom
        return _ceil_to_tick(raw, tick)

    denom = Decimal("1") + taker_fee_rate
    raw = (
        entry * (Decimal("1") - taker_fee_rate)
        - funding_per_unit
    ) / denom
    return _floor_to_tick(raw, tick)


def _apply_execution_economics(
    plan: ExecutionPlan,
    idea: ActiveIdea,
    instrument: Dict[str, Any],
    ticker: Dict[str, Any],
    fee_row: Dict[str, Any],
) -> None:
    """Populate Phase 9C fee/funding-aware economics on an existing plan."""
    if not EXECUTION_ECONOMICS_ENABLED:
        return

    taker = _dec(fee_row.get("takerFeeRate"))
    maker = _dec(fee_row.get("makerFeeRate"))
    if taker < 0 or maker < 0 or not fee_row:
        return

    entry = _dec(plan.entry_price)
    qty = _dec(plan.qty)
    q1 = _dec(plan.tp1_qty)
    q2 = _dec(plan.tp2_qty)
    tp1 = _dec(plan.tp1)
    tp2 = _dec(plan.tp2)
    sl = _dec(plan.stop_loss)
    equity = _dec(plan.equity_usdt)
    notional = _dec(plan.notional_usdt)
    tick = _dec(plan.tick_size)

    funding_rate = _dec(ticker.get("fundingRate"))
    interval_min = _funding_interval_minutes(instrument, ticker)
    horizon_min = Decimal(str(MAX_IDEA_DURATION_DAYS * 24 * 60))
    funding_periods = horizon_min / Decimal(str(interval_min))
    funding_cost = _signed_funding_cost(
        plan.side, notional, funding_rate, funding_periods
    )

    entry_fee = notional * taker
    tp1_exit_fee = q1 * tp1 * taker
    tp2_exit_fee = q2 * tp2 * taker
    sl_exit_fee = qty * sl * taker

    gross1 = q1 * abs(tp1 - entry)
    gross2 = q2 * abs(tp2 - entry)
    gross_total = gross1 + gross2

    # Funding is allocated by quantity/notional share. With a 50/50 split this
    # is exactly half to each leg. It is a conservative max-hold estimate.
    f1 = funding_cost * (q1 / qty) if qty > 0 else Decimal("0")
    f2 = funding_cost * (q2 / qty) if qty > 0 else Decimal("0")
    entry_fee1 = q1 * entry * taker
    entry_fee2 = q2 * entry * taker

    net1 = gross1 - entry_fee1 - tp1_exit_fee - f1
    net2 = gross2 - entry_fee2 - tp2_exit_fee - f2
    net_total = net1 + net2

    gross_sl_loss = qty * abs(entry - sl)
    sl_net_pnl = -gross_sl_loss - entry_fee - sl_exit_fee - funding_cost

    remaining_notional = q2 * entry
    remaining_funding = _signed_funding_cost(
        plan.side, remaining_notional, funding_rate, funding_periods
    )
    be = _remaining_break_even_price(
        plan.side, entry, taker, remaining_funding, q2, tick
    )
    be_move_pct = Decimal("0")
    if entry > 0 and be > 0:
        if plan.side == "LONG":
            be_move_pct = (be - entry) / entry * Decimal("100")
        else:
            be_move_pct = (entry - be) / entry * Decimal("100")

    plan.economics_ready = True
    plan.taker_fee_rate = float(taker)
    plan.maker_fee_rate = float(maker)
    plan.fee_source = "BYBIT_ACCOUNT"
    plan.funding_rate = float(funding_rate)
    plan.funding_interval_min = int(interval_min)
    plan.funding_periods_est = float(funding_periods)
    plan.funding_cost_est_usdt = float(funding_cost)
    plan.entry_fee_est_usdt = float(entry_fee)
    plan.tp1_exit_fee_est_usdt = float(tp1_exit_fee)
    plan.tp2_exit_fee_est_usdt = float(tp2_exit_fee)
    plan.sl_exit_fee_est_usdt = float(sl_exit_fee)
    plan.tp1_gross_profit_usdt = float(gross1)
    plan.tp2_gross_profit_usdt = float(gross2)
    plan.total_gross_profit_usdt = float(gross_total)
    plan.tp1_net_profit_usdt = float(net1)
    plan.tp2_net_profit_usdt = float(net2)
    plan.total_net_profit_usdt = float(net_total)
    plan.tp1_net_equity_pct = float(net1 / equity * Decimal("100")) if equity > 0 else 0.0
    plan.tp2_net_equity_pct = float(net2 / equity * Decimal("100")) if equity > 0 else 0.0
    plan.total_net_equity_pct = float(net_total / equity * Decimal("100")) if equity > 0 else 0.0
    plan.total_net_notional_pct = float(net_total / notional * Decimal("100")) if notional > 0 else 0.0
    plan.sl_net_pnl_usdt = float(sl_net_pnl)
    plan.sl_net_equity_pct = float(sl_net_pnl / equity * Decimal("100")) if equity > 0 else 0.0
    plan.remaining_break_even_price = float(be)
    plan.remaining_break_even_move_pct = float(be_move_pct)


def _shadow_remaining_net_pnl(
    row: Dict[str, Any],
    price: float,
    at_ts: Optional[int] = None,
) -> Tuple[float, float, float]:
    """
    Approximate net PnL of the remaining shadow leg at `price`.

    Returns (net_pnl_usdt, break_even_price, funding_cost_usdt).
    TP1 realised profit is intentionally excluded.
    """
    at_ts = int(at_ts or now_s())
    entry = _dec(row.get("entry_price"))
    qty = _dec(row.get("open_qty"))
    px = _dec(price)
    taker = _dec(row.get("taker_fee_rate"))
    funding_rate = _dec(row.get("funding_rate"))
    interval_min = max(1, int(row.get("funding_interval_min") or 480))
    emitted_at = int(row.get("emitted_at") or at_ts)
    tick = _dec(row.get("tick_size"))

    if entry <= 0 or qty <= 0 or px <= 0:
        return 0.0, 0.0, 0.0

    elapsed_min = Decimal(str(max(0, at_ts - emitted_at))) / Decimal("60")
    periods = elapsed_min / Decimal(str(interval_min))
    remaining_notional = qty * entry
    funding_cost = _signed_funding_cost(
        str(row.get("side") or ""), remaining_notional, funding_rate, periods
    )

    entry_fee = qty * entry * taker
    exit_fee = qty * px * taker

    if row.get("side") == "LONG":
        gross = qty * (px - entry)
    else:
        gross = qty * (entry - px)

    net = gross - entry_fee - exit_fee - funding_cost
    be = _remaining_break_even_price(
        str(row.get("side") or ""),
        entry,
        taker,
        funding_cost,
        qty,
        tick,
    )
    return float(net), float(be), float(funding_cost)


def _execution_shadow_reservations(app: web.Application) -> Dict[str, Dict[str, Any]]:
    runtime = app.get("runtime_state")
    if not isinstance(runtime, dict):
        return {}
    reservations = runtime.get("execution_shadow_reservations")
    if not isinstance(reservations, dict):
        reservations = {}
        runtime["execution_shadow_reservations"] = reservations
    return reservations


def _execution_shadow_reserved_total(
    app: web.Application,
    exclude_symbol: Optional[str] = None,
) -> float:
    reservations = _execution_shadow_reservations(app)
    total = 0.0
    for sym, row in reservations.items():
        if exclude_symbol and sym == exclude_symbol:
            continue
        total += max(0.0, _safe_float(row.get("margin_usdt")))
    return total


def _update_execution_shadow_reservation(
    app: web.Application,
    idea: ActiveIdea,
    event: str,
) -> None:
    """
    Mirror the future 50/50 margin release in memory only.

    TP1_HIT releases half of the shadow margin. Final lifecycle events release
    the rest. This ledger exists only to make Phase 9B capacity checks realistic
    while no actual Bybit positions are opened.
    """
    if not EXECUTION_PLANNER_ENABLED:
        return
    reservations = _execution_shadow_reservations(app)
    row = reservations.get(idea.symbol)
    if not isinstance(row, dict):
        return

    if event == "TP1_HIT":
        if not row.get("tp1_released"):
            row["margin_usdt"] = max(0.0, _safe_float(row.get("margin_usdt")) * 0.5)
            row["open_qty"] = max(0.0, _safe_float(row.get("tp2_qty")))
            row["status"] = "TP1_HIT"
            row["tp1_released"] = True
            row["updated_at"] = now_s()
            _save_execution_shadow_state(app)
    elif event in ("TP2_HIT", "SL_HIT", "INVALIDATED", "AMBIGUOUS"):
        reservations.pop(idea.symbol, None)
        _save_execution_shadow_state(app)
    elif event == "EXPIRED":
        # Phase 9C decides separately whether expiry closes the remaining
        # shadow position or moves it into EXPIRED_WAIT_EXIT.
        row["strategy_expired_at"] = now_s()
        row["updated_at"] = now_s()
        _save_execution_shadow_state(app)


def calculate_execution_plan(
    idea: ActiveIdea,
    instrument: Dict[str, Any],
    wallet: Dict[str, Any],
    positions: List[Dict[str, Any]],
    shadow_reserved_before_usdt: float,
) -> ExecutionPlan:
    """
    Pure Phase 9B sizing/risk calculation.

    Policy:
      - market-entry reference = price captured when the signal was emitted
      - position size = smallest Bybit quantity that can be split 50/50
      - each 50% leg is conservatively sized to satisfy minOrderQty and
        minNotionalValue at the lowest relevant entry/TP price
      - leverage plan = up to 2x (Phase 9B never sends a leverage change)
      - 10% of total equity is kept outside planner allocation
      - SL/TP are the strategy's own levels, rounded only to Bybit tick size
      - there is NO artificial 1% stop/risk cap in Phase 9B
    """
    symbol = idea.symbol
    side = idea.side

    lot = instrument.get("lotSizeFilter") or {}
    price_filter = instrument.get("priceFilter") or {}
    leverage_filter = instrument.get("leverageFilter") or {}

    min_qty = _dec(lot.get("minOrderQty"))
    qty_step = _dec(lot.get("qtyStep"))
    min_notional = _dec(lot.get("minNotionalValue"))
    max_mkt_qty = _dec(lot.get("maxMktOrderQty"))
    tick = _dec(price_filter.get("tickSize"))
    instrument_max_lev = _dec(leverage_filter.get("maxLeverage"))

    entry = _dec(idea.current_price_at_signal)
    if entry <= 0:
        entry = _dec((idea.entry_low + idea.entry_high) / 2.0)

    sl = _round_to_tick(_dec(idea.stop_loss), tick)
    tp1 = _round_to_tick(_dec(idea.tp1), tick)
    tp2 = _round_to_tick(_dec(idea.tp2), tick)

    equity = _dec(wallet.get("totalEquity"))
    available = _dec(wallet.get("totalAvailableBalance"))
    reserve = equity * _dec(EXECUTION_PLANNER_RESERVE_PCT) / Decimal("100")
    alloc_cap = max(Decimal("0"), equity - reserve)
    planner_available = max(
        Decimal("0"),
        min(available, alloc_cap) - _dec(shadow_reserved_before_usdt),
    )

    planned_lev = _dec(EXECUTION_PLANNER_LEVERAGE)
    if instrument_max_lev > 0:
        planned_lev = min(planned_lev, instrument_max_lev)
    planned_lev = max(Decimal("1"), planned_lev)

    open_positions = [p for p in positions if _safe_float(p.get("size")) > 0.0]
    already_open = any(str(p.get("symbol") or "") == symbol for p in open_positions)

    reason = "ok"
    status = "EXECUTABLE"

    if instrument.get("status") not in (None, "", "Trading"):
        status, reason = "SKIPPED_INSTRUMENT", "instrument_not_trading"
    elif entry <= 0 or qty_step <= 0 or min_qty <= 0 or min_notional <= 0:
        status, reason = "SKIPPED_INSTRUMENT", "invalid_instrument_limits"
    elif already_open:
        status, reason = "SKIPPED_ALREADY_OPEN", "symbol_already_open"

    # Geometry remains the Telegram bot's own strategy geometry.
    if status == "EXECUTABLE":
        if side == "LONG":
            geometry_ok = sl < entry < tp1 and tp2 > tp1
        else:
            geometry_ok = sl > entry > tp1 and tp2 < tp1
        if not geometry_ok:
            status, reason = "SKIPPED_GEOMETRY", "signal_geometry_invalid_after_tick_rounding"

    # Make each 50% close leg independently valid under current Bybit minimums.
    relevant_prices = [x for x in (entry, tp1, tp2) if x > 0]
    worst_price = min(relevant_prices) if relevant_prices else Decimal("0")
    half_by_notional = min_notional / worst_price if worst_price > 0 else Decimal("0")
    half_qty = _ceil_to_step(max(min_qty, half_by_notional), qty_step)
    total_qty = half_qty * Decimal("2")
    notional = total_qty * entry
    margin_required = notional / planned_lev if planned_lev > 0 else notional

    if status == "EXECUTABLE" and max_mkt_qty > 0 and total_qty > max_mkt_qty:
        status, reason = "SKIPPED_SIZE_LIMIT", "minimum_50_50_qty_above_max_market_qty"
    if status == "EXECUTABLE" and margin_required > planner_available:
        status, reason = "SKIPPED_NO_MARGIN", "insufficient_margin_after_10pct_reserve"

    sl_distance = abs(entry - sl)
    sl_pct = (sl_distance / entry * Decimal("100")) if entry > 0 else Decimal("0")
    loss_at_sl = total_qty * sl_distance
    account_risk = loss_at_sl / equity * Decimal("100") if equity > 0 else Decimal("0")
    tp1_profit = half_qty * abs(tp1 - entry)
    tp2_profit = half_qty * abs(tp2 - entry)

    if margin_required > 0:
        same_size_capacity = int(planner_available // margin_required)
    else:
        same_size_capacity = 0

    after = planner_available
    if status == "EXECUTABLE":
        after = max(Decimal("0"), planner_available - margin_required)

    return ExecutionPlan(
        symbol=symbol,
        side=side,
        status=status,
        reason=reason,
        entry_price=float(entry),
        qty=float(total_qty),
        tp1_qty=float(half_qty),
        tp2_qty=float(half_qty),
        notional_usdt=float(notional),
        leverage=float(planned_lev),
        margin_required_usdt=float(margin_required),
        equity_usdt=float(equity),
        available_usdt=float(available),
        reserve_usdt=float(reserve),
        shadow_reserved_before_usdt=float(_dec(shadow_reserved_before_usdt)),
        planner_available_before_usdt=float(planner_available),
        planner_available_after_usdt=float(after),
        same_size_capacity=max(0, same_size_capacity),
        stop_loss=float(sl),
        tp1=float(tp1),
        tp2=float(tp2),
        sl_distance_pct=float(sl_pct),
        loss_at_sl_usdt=float(loss_at_sl),
        account_risk_pct=float(account_risk),
        tp1_profit_usdt=float(tp1_profit),
        tp2_profit_usdt=float(tp2_profit),
        min_order_qty=float(min_qty),
        qty_step=float(qty_step),
        min_notional_usdt=float(min_notional),
        tick_size=float(tick),
        instrument_max_leverage=float(instrument_max_lev),
        open_positions=len(open_positions),
        already_open_symbol=already_open,
    )


async def build_execution_plan(
    app: web.Application,
    idea: ActiveIdea,
    state: SymbolState,
    reserve_shadow_margin: bool = False,
) -> Tuple[ExecutionPlan, Dict[str, Any]]:
    if not EXECUTION_PLANNER_ENABLED:
        raise RuntimeError("Phase 9B execution planner is disabled")

    rest = app.get("rest")
    private = app.get("bybit_private")
    if not isinstance(rest, BybitRest):
        raise RuntimeError("Bybit public REST client unavailable")
    if not isinstance(private, BybitPrivateReadOnly):
        raise RuntimeError("Bybit private read-only bridge unavailable")

    instrument, wallet, positions, ticker = await asyncio.gather(
        rest.instrument_linear(idea.symbol),
        private.wallet_balance(),
        private.positions_linear(),
        rest.ticker_linear(idea.symbol),
    )

    fee_row: Dict[str, Any] = {}
    fee_error = ""
    if EXECUTION_ECONOMICS_ENABLED:
        try:
            fee_row = await private.fee_rate_linear(idea.symbol)
        except Exception as exc:
            fee_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                f"Phase 9C fee-rate read failed {idea.symbol}: {fee_error}"
            )

    reservations = _execution_shadow_reservations(app)
    existing = reservations.get(idea.symbol)
    reserved_other = _execution_shadow_reserved_total(app, exclude_symbol=idea.symbol)
    plan = calculate_execution_plan(idea, instrument, wallet, positions, reserved_other)

    if isinstance(existing, dict) and int(existing.get("emitted_at") or 0) != idea.emitted_at:
        plan.status = "SKIPPED_SHADOW_POSITION_OPEN"
        plan.reason = str(existing.get("status") or "shadow_position_open").lower()

    if fee_row:
        _apply_execution_economics(plan, idea, instrument, ticker, fee_row)
    elif fee_error:
        plan.fee_source = "UNAVAILABLE"

    if reserve_shadow_margin and plan.status == "EXECUTABLE":
        if idea.symbol not in reservations:
            reservations[idea.symbol] = {
                "status": "ACTIVE",
                "symbol": idea.symbol,
                "side": idea.side,
                "setup_type": idea.setup_type,
                "entry_price": plan.entry_price,
                "stop_loss": plan.stop_loss,
                "tp1": plan.tp1,
                "tp2": plan.tp2,
                "qty": plan.qty,
                "open_qty": plan.qty,
                "tp1_qty": plan.tp1_qty,
                "tp2_qty": plan.tp2_qty,
                "margin_usdt": plan.margin_required_usdt,
                "original_margin_usdt": plan.margin_required_usdt,
                "notional_usdt": plan.notional_usdt,
                "emitted_at": idea.emitted_at,
                "expires_at": idea.expires_at,
                "tp1_released": False,
                "taker_fee_rate": plan.taker_fee_rate,
                "maker_fee_rate": plan.maker_fee_rate,
                "funding_rate": plan.funding_rate,
                "funding_interval_min": plan.funding_interval_min,
                "tick_size": plan.tick_size,
                "planned_break_even_price": plan.remaining_break_even_price,
                "updated_at": now_s(),
            }
            _save_execution_shadow_state(app)
    return plan, instrument


def format_execution_plan(plan: ExecutionPlan, instrument: Dict[str, Any]) -> str:
    qty_step = (instrument.get("lotSizeFilter") or {}).get("qtyStep") or plan.qty_step
    tick = (instrument.get("priceFilter") or {}).get("tickSize") or plan.tick_size

    if plan.status == "EXECUTABLE":
        status_line = "✅ <b>EXECUTABLE — SHADOW ONLY</b>"
    elif plan.status == "SKIPPED_NO_MARGIN":
        status_line = "⛔ <b>SKIPPED — NO MARGIN</b>"
    elif plan.status == "SKIPPED_ALREADY_OPEN":
        status_line = "⛔ <b>SKIPPED — SYMBOL ALREADY OPEN</b>"
    elif plan.status == "SKIPPED_SHADOW_POSITION_OPEN":
        status_line = "⛔ <b>SKIPPED — SHADOW POSITION STILL OPEN</b>"
    else:
        status_line = f"⛔ <b>{html.escape(plan.status)}</b>"

    qty = _fmt_step(plan.qty, qty_step)
    q1 = _fmt_step(plan.tp1_qty, qty_step)
    q2 = _fmt_step(plan.tp2_qty, qty_step)
    sl = _fmt_step(plan.stop_loss, tick)
    tp1 = _fmt_step(plan.tp1, tick)
    tp2 = _fmt_step(plan.tp2, tick)

    parts: List[str] = [
        "🧮 <b>Execution Plan — Phase 9C</b>\n\n",
        f"<b>{html.escape(plan.symbol)} {html.escape(plan.side)}</b>\n",
        f"<b>Status:</b> {status_line}\n",
        f"<b>Reason:</b> <code>{html.escape(plan.reason)}</code>\n\n",
        f"<b>Entry:</b> Market @ signal reference {plan.entry_price:.6g}\n",
        f"<b>Qty:</b> <code>{html.escape(qty)}</code> · ",
        f"<b>Notional:</b> ${plan.notional_usdt:.2f}\n",
        f"<b>Planned leverage:</b> {plan.leverage:.2f}x ",
        "<i>(not changed by Phase 9C)</i>\n",
        f"<b>Estimated margin:</b> ${plan.margin_required_usdt:.2f}\n\n",
        f"<b>SL:</b> <code>{html.escape(sl)}</code> · {plan.sl_distance_pct:.2f}% from entry\n",
        f"<b>Gross loss at SL:</b> ~${plan.loss_at_sl_usdt:.3f}\n",
    ]

    if plan.economics_ready:
        parts.extend([
            f"<b>Net PnL at SL:</b> ${plan.sl_net_pnl_usdt:+.3f} · "
            f"{plan.sl_net_equity_pct:+.2f}% equity\n",
            f"<b>TP1:</b> <code>{html.escape(tp1)}</code> · qty {html.escape(q1)} (50%)\n",
            f"  Gross: +${plan.tp1_gross_profit_usdt:.3f} · "
            f"Net est.: ${plan.tp1_net_profit_usdt:+.3f} "
            f"({plan.tp1_net_equity_pct:+.2f}% equity)\n",
            f"<b>TP2:</b> <code>{html.escape(tp2)}</code> · qty {html.escape(q2)} (50%)\n",
            f"  Gross: +${plan.tp2_gross_profit_usdt:.3f} · "
            f"Net est.: ${plan.tp2_net_profit_usdt:+.3f} "
            f"({plan.tp2_net_equity_pct:+.2f}% equity)\n",
            f"<b>Expected total:</b> gross +${plan.total_gross_profit_usdt:.3f} · "
            f"net est. ${plan.total_net_profit_usdt:+.3f} · "
            f"{plan.total_net_equity_pct:+.2f}% equity · "
            f"{plan.total_net_notional_pct:+.2f}% notional\n\n",
            f"<b>Fees est.:</b> entry ${plan.entry_fee_est_usdt:.3f} · "
            f"TP exits ${plan.tp1_exit_fee_est_usdt + plan.tp2_exit_fee_est_usdt:.3f} · "
            f"taker {plan.taker_fee_rate * 100:.4f}%\n",
            f"<b>Funding est.:</b> ${plan.funding_cost_est_usdt:+.3f} over "
            f"{MAX_IDEA_DURATION_DAYS}d at current {plan.funding_rate * 100:+.4f}% / "
            f"{plan.funding_interval_min}m\n",
            f"<b>Remaining-leg net BE:</b> {plan.remaining_break_even_price:.6g} "
            f"({plan.remaining_break_even_move_pct:+.3f}% from entry)\n",
            "<i>TP1 realised profit is NOT counted in this break-even.</i>\n\n",
        ])
    else:
        parts.extend([
            "<b>Net economics:</b> unavailable — Bybit account fee-rate could not be read.\n",
            f"<b>TP1 gross:</b> +${plan.tp1_profit_usdt:.3f}\n",
            f"<b>TP2 gross:</b> +${plan.tp2_profit_usdt:.3f}\n\n",
        ])

    parts.extend([
        f"<b>Equity:</b> ${plan.equity_usdt:.2f} · ",
        f"<b>Bybit available:</b> ${plan.available_usdt:.2f}\n",
        f"<b>10% reserve:</b> ${plan.reserve_usdt:.2f}\n",
        f"<b>Shadow reserved before:</b> ${plan.shadow_reserved_before_usdt:.2f}\n",
        f"<b>Planner margin before:</b> ${plan.planner_available_before_usdt:.2f}\n",
        f"<b>Planner margin after:</b> ${plan.planner_available_after_usdt:.2f}\n",
        f"<b>Same-size capacity before this plan:</b> {plan.same_size_capacity}\n\n",
        f"<b>Bybit minimums:</b> qty {plan.min_order_qty:g} · "
        f"step {plan.qty_step:g} · notional ${plan.min_notional_usdt:g}\n",
        f"<b>Expiry policy:</b> after {MAX_IDEA_DURATION_DAYS}d, positive/net-BE remainder exits; "
        "negative remainder waits for original SL or its own net break-even.\n",
        "<b>SL safety:</b> future live SL remains active until Bybit confirms position size = 0.\n",
        "<b>ORDER NOT SENT.</b> Phase 9C still contains no Bybit write endpoints. ✅",
    ])
    return "".join(parts)


async def send_execution_plan(
    app: web.Application,
    idea: ActiveIdea,
    state: SymbolState,
    reserve_shadow_margin: bool = True,
) -> None:
    if not EXECUTION_PLANNER_ENABLED or not EXECUTION_PLANNER_AUTO_SEND:
        return
    tg = app.get("tg")
    if not isinstance(tg, Tg):
        return

    try:
        plan, instrument = await build_execution_plan(
            app, idea, state, reserve_shadow_margin=reserve_shadow_margin
        )
        text = format_execution_plan(plan, instrument)
    except Exception as exc:
        logger.warning(
            f"Phase 9C execution plan failed {idea.symbol}: {type(exc).__name__}: {exc}"
        )
        text = (
            "⚠️ <b>Execution Plan — Phase 9C failed</b>\n\n"
            f"<b>{html.escape(idea.symbol)} {html.escape(idea.side)}</b>\n"
            f"<code>{html.escape(type(exc).__name__ + ': ' + str(exc))}</code>\n\n"
            "<b>ORDER NOT SENT.</b>"
        )

    for cid in get_broadcast_targets():
        with contextlib.suppress(Exception):
            await tg.send(cid, text)



async def _handle_execution_shadow_expiry(
    app: web.Application,
    idea: ActiveIdea,
) -> Optional[str]:
    """
    Phase 9C shadow implementation of the approved future live expiry rule.

    If the remaining leg is net non-negative at expiry, the shadow position
    closes. If it is net negative, it remains open as EXPIRED_WAIT_EXIT until
    either the original strategy SL or the remaining leg's own net break-even.
    TP1 realised profit is never used to subsidise that break-even.
    """
    reservations = _execution_shadow_reservations(app)
    row = reservations.get(idea.symbol)
    if not isinstance(row, dict):
        return None

    mkt = app.get("mkt")
    state = mkt.state.get(idea.symbol) if isinstance(mkt, Market) else None
    if not isinstance(state, SymbolState):
        return None

    price = get_current_price(state)
    if price <= 0:
        row["status"] = "EXPIRED_WAIT_EXIT"
        row["expiry_wait_started_at"] = now_s()
        row["updated_at"] = now_s()
        _save_execution_shadow_state(app)
        return (
            "⏳ <b>Execution Shadow — EXPIRED_WAIT_EXIT</b>\n"
            f"<b>{html.escape(idea.symbol)}</b>: current price unavailable, "
            "so the shadow remainder is NOT force-closed."
        )

    net_pnl, be_price, funding_cost = _shadow_remaining_net_pnl(row, price)
    row["last_net_pnl_usdt"] = net_pnl
    row["last_break_even_price"] = be_price
    row["last_funding_cost_usdt"] = funding_cost

    if net_pnl >= 0:
        reservations.pop(idea.symbol, None)
        _save_execution_shadow_state(app)
        return (
            "✅ <b>Execution Shadow — EXPIRY EXIT</b>\n"
            f"<b>{html.escape(idea.symbol)}</b> remainder is net non-negative at expiry.\n"
            f"Price: {price:.6g} · Net PnL est.: ${net_pnl:+.3f}\n"
            "<i>Future live action: reduce-only close; original SL remains until "
            "Bybit confirms position size = 0.</i>"
        )

    row["status"] = "EXPIRED_WAIT_EXIT"
    row["expiry_wait_started_at"] = now_s()
    row["updated_at"] = now_s()
    _save_execution_shadow_state(app)

    return (
        "⏳ <b>Execution Shadow — EXPIRED_WAIT_EXIT</b>\n"
        f"<b>{html.escape(idea.symbol)}</b> remainder is negative at expiry, "
        "so it is NOT force-closed.\n"
        f"Current net PnL est.: ${net_pnl:+.3f}\n"
        f"Original SL: {float(row.get('stop_loss') or 0):.6g}\n"
        f"Current net break-even: {be_price:.6g}\n"
        f"Funding accrued est.: ${funding_cost:+.3f}\n"
        "<i>Wait for original SL or the remaining leg's own net break-even. "
        "TP1 realised profit is excluded.</i>"
    )


async def check_execution_shadow_wait_exit(
    sym: str,
    state: SymbolState,
    app: web.Application,
) -> None:
    """
    Continue Phase 9C EXPIRED_WAIT_EXIT after the strategy ActiveIdea is gone.

    This is a shadow policy test only; no order is sent. The real live phase
    will keep an exchange-side SL active and require a confirmed flat position.
    """
    reservations = _execution_shadow_reservations(app)
    row = reservations.get(sym)
    if not isinstance(row, dict) or row.get("status") != "EXPIRED_WAIT_EXIT":
        return

    price = get_current_price(state)
    if price <= 0:
        return

    side = str(row.get("side") or "")
    stop = _safe_float(row.get("stop_loss"))
    net_pnl, be_price, funding_cost = _shadow_remaining_net_pnl(row, price)

    sl_hit = (price <= stop) if side == "LONG" else (price >= stop)
    be_hit = (price >= be_price) if side == "LONG" else (price <= be_price)

    outcome = ""
    if sl_hit:
        outcome = "SL"
    elif be_price > 0 and be_hit and net_pnl >= -0.000001:
        outcome = "NET_BREAK_EVEN"

    row["last_net_pnl_usdt"] = net_pnl
    row["last_break_even_price"] = be_price
    row["last_funding_cost_usdt"] = funding_cost
    row["updated_at"] = now_s()

    if not outcome:
        _save_execution_shadow_state(app)
        return

    reservations.pop(sym, None)
    _save_execution_shadow_state(app)

    tg = app.get("tg")
    if not isinstance(tg, Tg):
        return

    if outcome == "SL":
        msg = (
            "🛑 <b>Execution Shadow — WAIT EXIT finished at SL</b>\n"
            f"<b>{html.escape(sym)}</b> · price {price:.6g} · "
            f"original SL {stop:.6g}\n"
            f"Net PnL est.: ${net_pnl:+.3f}"
        )
    else:
        msg = (
            "🟰 <b>Execution Shadow — WAIT EXIT reached net break-even</b>\n"
            f"<b>{html.escape(sym)}</b> · price {price:.6g} · "
            f"net BE {be_price:.6g}\n"
            f"Net PnL est.: ${net_pnl:+.3f}\n"
            "<i>Future live action: reduce-only close, then wait for confirmed flat "
            "before removing the original SL.</i>"
        )

    for cid in get_broadcast_targets():
        with contextlib.suppress(Exception):
            await tg.send(cid, msg)




# =============================================================================
# === 5B. PHASE 8L.4 PERSISTENT DIAGNOSTICS (SQLite) ===
# =============================================================================

_DIAG_SCHEMA_VERSION = "3"
_DIAG_DETECTOR_ABBR = {
    "BREAKOUT_RETEST": "BR",
    "TREND_PULLBACK": "TP",
    "LIQUIDITY_SWEEP": "LS",
}


def diagnostic_setup_key(symbol: str, result: SetupResult) -> str:
    """Stable dedup key: one row per detector/side/confirmation timestamp."""
    return f"{symbol}|{result.setup_type}|{result.side}|{int(result.setup_ts)}"


@dataclass
class BRShadowCandidate:
    """
    Diagnostics-only Breakout+Retest candidate captured immediately before the
    production geometry guard rejects it.  It NEVER enters the trading pipeline.

    The shadow model keeps the production entry zone and targets but places a
    hypothetical stop just outside the entry zone by 0.05 ATR.  This lets us
    prospectively measure whether shallow retests rejected by the geometry guard
    would have produced useful outcomes without changing live/dry-run signals.
    """
    result: SetupResult
    breakout_ts_ms: int
    retest_ts_ms: int
    retest_tf: str
    key_level: float
    original_stop_loss: float
    shadow_stop_loss: float
    geometry_gap_abs: float
    geometry_gap_atr: float
    stop_model: str = "ZONE_EDGE_0.05ATR"


@dataclass
class DetectorStageTrace:
    """
    Diagnostics-only trace of the *actual detector execution path*.

    counters keys are ``SIDE|stage`` (e.g. ``LONG|broken_swing``).  The trace
    is populated during the same detector call that produces the trading
    SetupResult, so no detector is executed twice and signal behaviour is not
    changed.  ``terminal`` stores exactly one terminal reason per side/call.
    """
    detector: str
    counters: Dict[str, int] = field(default_factory=dict)
    terminal: Dict[str, str] = field(default_factory=dict)
    progress: Dict[str, int] = field(default_factory=dict)
    br_shadow_candidates: List[BRShadowCandidate] = field(default_factory=list)

    def bump(self, side: str, stage: str, amount: int = 1) -> None:
        key = f"{side}|{stage}"
        self.counters[key] = self.counters.get(key, 0) + int(amount)

    def reach(self, side: str, stage: str, rank: int, amount: int = 1) -> None:
        self.bump(side, stage, amount)
        self.progress[side] = max(self.progress.get(side, 0), int(rank))

    def finish(self, side: str, reason: str) -> None:
        # One terminal bucket per side detector call.  Repeated calls to finish
        # within the same trace are ignored so aggregate terminal counts remain
        # interpretable as detector-side invocations.
        if side not in self.terminal:
            self.terminal[side] = reason
            self.bump(side, f"terminal_{reason}")


class DiagnosticStore:
    """
    Persistent Phase 8L.4 observability store.

    The database is deliberately isolated from signal eligibility: DB failures
    are handled by callers as diagnostics-only warnings and must never block a
    scan or a Telegram signal.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            parent = os.path.dirname(path) or "."
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(path, timeout=5.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS diag_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS diag_counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS raw_setups (
                setup_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                setup_type TEXT NOT NULL,
                side TEXT NOT NULL,
                setup_ts_ms INTEGER NOT NULL,
                setup_tf TEXT NOT NULL DEFAULT '',
                setup_available_ts_ms INTEGER NOT NULL DEFAULT 0,

                first_seen_ts INTEGER NOT NULL,
                last_seen_ts INTEGER NOT NULL,
                raw_score_first INTEGER NOT NULL,
                raw_score_latest INTEGER NOT NULL,
                score_components_first TEXT NOT NULL DEFAULT '{}',
                score_components_latest TEXT NOT NULL DEFAULT '{}',
                regime_first TEXT NOT NULL DEFAULT '',
                regime_latest TEXT NOT NULL DEFAULT '',
                btc_regime_first TEXT NOT NULL DEFAULT '',
                btc_regime_latest TEXT NOT NULL DEFAULT '',
                score_floor_first INTEGER NOT NULL DEFAULT 0,
                score_floor_latest INTEGER NOT NULL DEFAULT 0,
                score_passed_first INTEGER NOT NULL DEFAULT 0,
                score_passed_latest INTEGER NOT NULL DEFAULT 0,

                gate_status_first TEXT,
                gate_reason_first TEXT,
                gate_status_latest TEXT,
                gate_reason_latest TEXT,
                signal_emitted INTEGER NOT NULL DEFAULT 0,
                signal_emitted_ts INTEGER,

                entry_low REAL NOT NULL,
                entry_high REAL NOT NULL,
                entry_mid REAL NOT NULL,
                stop_loss REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                rr_tp1 REAL NOT NULL,
                rr_tp2 REAL NOT NULL,
                current_price_first REAL NOT NULL DEFAULT 0,
                current_price_latest REAL NOT NULL DEFAULT 0,
                in_entry_zone_first INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',

                observation_status TEXT NOT NULL DEFAULT 'WAITING_ENTRY',
                entry_activated_ts INTEGER,
                tp1_hit_ts INTEGER,
                tp2_hit_ts INTEGER,
                sl_hit_ts INTEGER,
                final_outcome TEXT,
                final_outcome_ts INTEGER,
                expires_at_ts INTEGER NOT NULL,
                mfe_pct REAL NOT NULL DEFAULT 0,
                mae_pct REAL NOT NULL DEFAULT 0,
                mfe_r REAL NOT NULL DEFAULT 0,
                mae_r REAL NOT NULL DEFAULT 0,
                last_eval_bar_ts_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ts INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS br_shadow_setups (
                shadow_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                breakout_ts_ms INTEGER NOT NULL,
                retest_ts_ms INTEGER NOT NULL,
                retest_tf TEXT NOT NULL DEFAULT '',
                key_level REAL NOT NULL,

                first_seen_ts INTEGER NOT NULL,
                last_seen_ts INTEGER NOT NULL,
                raw_score_first INTEGER NOT NULL,
                raw_score_latest INTEGER NOT NULL,
                score_components_first TEXT NOT NULL DEFAULT '{}',
                score_components_latest TEXT NOT NULL DEFAULT '{}',
                regime_first TEXT NOT NULL DEFAULT '',
                regime_latest TEXT NOT NULL DEFAULT '',
                btc_regime_first TEXT NOT NULL DEFAULT '',
                btc_regime_latest TEXT NOT NULL DEFAULT '',

                entry_low REAL NOT NULL,
                entry_high REAL NOT NULL,
                entry_mid REAL NOT NULL,
                original_stop_loss REAL NOT NULL,
                stop_loss REAL NOT NULL,
                stop_model TEXT NOT NULL DEFAULT 'ZONE_EDGE_0.05ATR',
                geometry_gap_abs REAL NOT NULL DEFAULT 0,
                geometry_gap_atr REAL NOT NULL DEFAULT 0,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                rr_tp1 REAL NOT NULL,
                rr_tp2 REAL NOT NULL,
                current_price_first REAL NOT NULL DEFAULT 0,
                current_price_latest REAL NOT NULL DEFAULT 0,
                in_entry_zone_first INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',

                observation_status TEXT NOT NULL DEFAULT 'WAITING_ENTRY',
                entry_activated_ts INTEGER,
                tp1_hit_ts INTEGER,
                tp2_hit_ts INTEGER,
                sl_hit_ts INTEGER,
                final_outcome TEXT,
                final_outcome_ts INTEGER,
                expires_at_ts INTEGER NOT NULL,
                mfe_pct REAL NOT NULL DEFAULT 0,
                mae_pct REAL NOT NULL DEFAULT 0,
                mfe_r REAL NOT NULL DEFAULT 0,
                mae_r REAL NOT NULL DEFAULT 0,
                last_eval_bar_ts_ms INTEGER NOT NULL DEFAULT 0,
                updated_at_ts INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_br_shadow_symbol_status
                ON br_shadow_setups(symbol, observation_status);
            CREATE INDEX IF NOT EXISTS idx_br_shadow_outcome
                ON br_shadow_setups(final_outcome);
            CREATE INDEX IF NOT EXISTS idx_br_shadow_side_score
                ON br_shadow_setups(side, raw_score_first);

            CREATE TABLE IF NOT EXISTS setup_score_components (
                setup_key TEXT NOT NULL,
                component TEXT NOT NULL,
                points INTEGER NOT NULL,
                PRIMARY KEY (setup_key, component),
                FOREIGN KEY (setup_key) REFERENCES raw_setups(setup_key)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS detector_stage_counters (
                symbol TEXT NOT NULL,
                detector TEXT NOT NULL,
                side TEXT NOT NULL,
                stage TEXT NOT NULL,
                value INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (symbol, detector, side, stage)
            );

            CREATE INDEX IF NOT EXISTS idx_detector_stage_detector_side
                ON detector_stage_counters(detector, side, stage);

            CREATE INDEX IF NOT EXISTS idx_raw_setups_detector_score
                ON raw_setups(setup_type, raw_score_first);
            CREATE INDEX IF NOT EXISTS idx_raw_setups_outcome
                ON raw_setups(final_outcome);
            CREATE INDEX IF NOT EXISTS idx_raw_setups_gate_reason
                ON raw_setups(gate_reason_first);
            CREATE INDEX IF NOT EXISTS idx_raw_setups_symbol_status
                ON raw_setups(symbol, observation_status);
            """
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO diag_meta(key,value) VALUES('schema_version',?)",
            (_DIAG_SCHEMA_VERSION,),
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO diag_meta(key,value) VALUES('phase','8L.4.3')"
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def _bump(self, name: str, amount: int = 1) -> None:
        self.conn.execute(
            """
            INSERT INTO diag_counters(name,value) VALUES(?,?)
            ON CONFLICT(name) DO UPDATE SET value=value+excluded.value
            """,
            (name, amount),
        )

    def record_detector_scan(
        self,
        raw_results: List[SetupResult],
        floor: int,
    ) -> None:
        by_type = {r.setup_type: r for r in raw_results}
        for setup_type, abbr in _DIAG_DETECTOR_ABBR.items():
            self._bump(f"detector_runs_{abbr}")
            r = by_type.get(setup_type)
            if r is None:
                self._bump(f"detector_none_{abbr}")
            else:
                self._bump(f"detector_raw_{abbr}")
                if r.score >= floor:
                    self._bump(f"score_pass_{abbr}")
                else:
                    self._bump(f"score_fail_{abbr}")
        self.conn.commit()

    def record_stage_traces(
        self, symbol: str, traces: List[DetectorStageTrace]
    ) -> None:
        """Persist BR/TP detector-stage counters without affecting trading."""
        for trace in traces:
            for key, amount in trace.counters.items():
                try:
                    side, stage = key.split("|", 1)
                except ValueError:
                    continue
                self.conn.execute(
                    """
                    INSERT INTO detector_stage_counters(symbol,detector,side,stage,value)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(symbol,detector,side,stage)
                    DO UPDATE SET value=value+excluded.value
                    """,
                    (symbol, trace.detector, side, stage, int(amount)),
                )
        self.conn.commit()

    def detector_stage_summary(self, detector: str) -> Dict[str, Dict[str, int]]:
        rows = self.conn.execute(
            """
            SELECT side,stage,SUM(value) AS n
            FROM detector_stage_counters
            WHERE detector=?
            GROUP BY side,stage
            """,
            (detector,),
        ).fetchall()
        out: Dict[str, Dict[str, int]] = {}
        for row in rows:
            out.setdefault(row["side"], {})[row["stage"]] = int(row["n"] or 0)
        return out

    def detector_raw_detail(self, setup_type: str) -> Dict[str, Any]:
        """Persistent unique-setup quality detail for /brtp."""
        rows = self.conn.execute(
            """SELECT raw_score_first,regime_first,score_passed_first,final_outcome,
                      signal_emitted,observation_status
               FROM raw_setups WHERE setup_type=?""",
            (setup_type,),
        ).fetchall()
        total = len(rows)
        scores = [int(r["raw_score_first"]) for r in rows]
        bucket_order = ["<45", "45-54", "55-64", "65-74", "75-84", "85+"]
        buckets = {k: 0 for k in bucket_order}
        bucket_stats: Dict[str, Dict[str, Any]] = {
            k: {"total": 0, "statuses": {}, "outcomes": {}} for k in bucket_order
        }
        regimes: Dict[str, int] = {}
        outcomes: Dict[str, int] = {}
        pass_outcomes: Dict[str, Dict[str, int]] = {"pass": {}, "fail": {}}
        score_passed = 0
        signals = 0
        for r in rows:
            score = int(r["raw_score_first"])
            if score < 45:
                bucket = "<45"
            elif score < 55:
                bucket = "45-54"
            elif score < 65:
                bucket = "55-64"
            elif score < 75:
                bucket = "65-74"
            elif score < 85:
                bucket = "75-84"
            else:
                bucket = "85+"
            buckets[bucket] += 1
            bucket_stats[bucket]["total"] += 1
            status = r["observation_status"] or "UNKNOWN"
            bs = bucket_stats[bucket]["statuses"]
            bs[status] = bs.get(status, 0) + 1
            regime = r["regime_first"] or "UNKNOWN"
            regimes[regime] = regimes.get(regime, 0) + 1
            passed = bool(r["score_passed_first"])
            score_passed += int(passed)
            signals += int(bool(r["signal_emitted"]))
            outcome = r["final_outcome"]
            if outcome:
                outcomes[outcome] = outcomes.get(outcome, 0) + 1
                bo = bucket_stats[bucket]["outcomes"]
                bo[outcome] = bo.get(outcome, 0) + 1
                grp = "pass" if passed else "fail"
                pass_outcomes[grp][outcome] = pass_outcomes[grp].get(outcome, 0) + 1

        component_rows = self.conn.execute(
            """
            SELECT c.component,COUNT(*) AS n,SUM(c.points) AS pts
            FROM setup_score_components c
            JOIN raw_setups r ON r.setup_key=c.setup_key
            WHERE r.setup_type=?
            GROUP BY c.component
            ORDER BY n DESC,c.component ASC
            """,
            (setup_type,),
        ).fetchall()
        components = [
            {
                "name": r["component"],
                "n": int(r["n"] or 0),
                "rate": (int(r["n"] or 0) / total * 100.0) if total else 0.0,
                "points": int(r["pts"] or 0),
            }
            for r in component_rows
        ]
        return {
            "total": total,
            "avg_score": (sum(scores) / total) if total else 0.0,
            "buckets": buckets,
            "bucket_stats": bucket_stats,
            "regimes": regimes,
            "score_passed": score_passed,
            "signals": signals,
            "outcomes": outcomes,
            "pass_outcomes": pass_outcomes,
            "components": components,
        }

    def upsert_raw_setup(
        self,
        symbol: str,
        state: SymbolState,
        btc_regime: str,
        raw_result: SetupResult,
        final_result: SetupResult,
        floor: int,
    ) -> str:
        """Insert a unique raw setup or refresh its latest diagnostic snapshot."""
        key = diagnostic_setup_key(symbol, raw_result)
        now = now_s()
        px = get_current_price(state)
        entry_mid = (final_result.entry_low + final_result.entry_high) / 2.0
        in_zone = int(
            px > 0.0 and final_result.entry_low <= px <= final_result.entry_high
        )
        components_json = json.dumps(
            raw_result.score_components or {}, sort_keys=True, separators=(",", ":")
        )
        score_passed = int(raw_result.score >= floor)
        available_ts = confirmation_available_ts_ms(
            raw_result.setup_ts, raw_result.setup_tf
        )
        closed_1h = state.bars_1h[:-1] if len(state.bars_1h) > 1 else []
        baseline_bar_ts = closed_1h[-1][B_TS] if closed_1h else 0
        initial_status = "ACTIVE" if in_zone else "WAITING_ENTRY"
        initial_entry_ts = now if in_zone else None
        expiry_base = initial_entry_ts if initial_entry_ts is not None else now
        expires_at = expiry_base + DIAGNOSTICS_OUTCOME_DAYS * 86400

        self.conn.execute(
            """
            INSERT INTO raw_setups (
                setup_key,symbol,setup_type,side,setup_ts_ms,setup_tf,
                setup_available_ts_ms,first_seen_ts,last_seen_ts,
                raw_score_first,raw_score_latest,
                score_components_first,score_components_latest,
                regime_first,regime_latest,btc_regime_first,btc_regime_latest,
                score_floor_first,score_floor_latest,
                score_passed_first,score_passed_latest,
                entry_low,entry_high,entry_mid,stop_loss,tp1,tp2,rr_tp1,rr_tp2,
                current_price_first,current_price_latest,in_entry_zone_first,notes,
                observation_status,entry_activated_ts,expires_at_ts,
                last_eval_bar_ts_ms,updated_at_ts
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(setup_key) DO UPDATE SET
                last_seen_ts=excluded.last_seen_ts,
                raw_score_latest=excluded.raw_score_latest,
                score_components_latest=excluded.score_components_latest,
                regime_latest=excluded.regime_latest,
                btc_regime_latest=excluded.btc_regime_latest,
                score_floor_latest=excluded.score_floor_latest,
                score_passed_latest=excluded.score_passed_latest,
                current_price_latest=excluded.current_price_latest,
                updated_at_ts=excluded.updated_at_ts
            """,
            (
                key, symbol, raw_result.setup_type, raw_result.side,
                int(raw_result.setup_ts), raw_result.setup_tf or "",
                int(available_ts), now, now,
                int(raw_result.score), int(raw_result.score),
                components_json, components_json,
                state.regime, state.regime, btc_regime, btc_regime,
                int(floor), int(floor), score_passed, score_passed,
                float(final_result.entry_low), float(final_result.entry_high),
                float(entry_mid), float(final_result.stop_loss),
                float(final_result.tp1), float(final_result.tp2),
                float(final_result.rr_tp1), float(final_result.rr_tp2),
                float(px), float(px), in_zone, raw_result.notes or "",
                initial_status, initial_entry_ts, int(expires_at),
                int(baseline_bar_ts), now,
            ),
        )

        for component, points in (raw_result.score_components or {}).items():
            self.conn.execute(
                """
                INSERT OR IGNORE INTO setup_score_components(setup_key,component,points)
                VALUES(?,?,?)
                """,
                (key, component, int(points)),
            )
        self.conn.commit()
        return key

    @staticmethod
    def _br_shadow_key(symbol: str, shadow: BRShadowCandidate) -> str:
        # Include breakout + retest + key level so repeated polling dedups the
        # same structural candidate without merging distinct breakouts.
        return (
            f"{symbol}|BR_SHADOW|{shadow.result.side}|"
            f"{int(shadow.breakout_ts_ms)}|{int(shadow.retest_ts_ms)}|"
            f"{shadow.key_level:.10g}"
        )

    def upsert_br_shadow(
        self,
        symbol: str,
        state: SymbolState,
        btc_regime: str,
        shadow: BRShadowCandidate,
    ) -> str:
        """Persist one prospective BR geometry-fail shadow setup."""
        result = shadow.result
        final_result = calc_swing_tpsl(result, state)
        key = self._br_shadow_key(symbol, shadow)
        now = now_s()
        px = get_current_price(state)
        entry_mid = (final_result.entry_low + final_result.entry_high) / 2.0
        in_zone = int(px > 0.0 and final_result.entry_low <= px <= final_result.entry_high)
        components_json = json.dumps(
            result.score_components or {}, sort_keys=True, separators=(",", ":")
        )
        closed_1h = state.bars_1h[:-1] if len(state.bars_1h) > 1 else []
        baseline_bar_ts = closed_1h[-1][B_TS] if closed_1h else 0
        initial_status = "ACTIVE" if in_zone else "WAITING_ENTRY"
        initial_entry_ts = now if in_zone else None
        expiry_base = initial_entry_ts if initial_entry_ts is not None else now
        expires_at = expiry_base + DIAGNOSTICS_OUTCOME_DAYS * 86400

        self.conn.execute(
            """
            INSERT INTO br_shadow_setups (
                shadow_key,symbol,side,breakout_ts_ms,retest_ts_ms,retest_tf,key_level,
                first_seen_ts,last_seen_ts,raw_score_first,raw_score_latest,
                score_components_first,score_components_latest,
                regime_first,regime_latest,btc_regime_first,btc_regime_latest,
                entry_low,entry_high,entry_mid,original_stop_loss,stop_loss,stop_model,
                geometry_gap_abs,geometry_gap_atr,tp1,tp2,rr_tp1,rr_tp2,
                current_price_first,current_price_latest,in_entry_zone_first,notes,
                observation_status,entry_activated_ts,expires_at_ts,
                last_eval_bar_ts_ms,updated_at_ts
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(shadow_key) DO UPDATE SET
                last_seen_ts=excluded.last_seen_ts,
                raw_score_latest=excluded.raw_score_latest,
                score_components_latest=excluded.score_components_latest,
                regime_latest=excluded.regime_latest,
                btc_regime_latest=excluded.btc_regime_latest,
                current_price_latest=excluded.current_price_latest,
                updated_at_ts=excluded.updated_at_ts
            """,
            (
                key, symbol, result.side, int(shadow.breakout_ts_ms),
                int(shadow.retest_ts_ms), shadow.retest_tf or "", float(shadow.key_level),
                now, now, int(result.score), int(result.score),
                components_json, components_json,
                state.regime, state.regime, btc_regime, btc_regime,
                float(final_result.entry_low), float(final_result.entry_high), float(entry_mid),
                float(shadow.original_stop_loss), float(final_result.stop_loss), shadow.stop_model,
                float(shadow.geometry_gap_abs), float(shadow.geometry_gap_atr),
                float(final_result.tp1), float(final_result.tp2),
                float(final_result.rr_tp1), float(final_result.rr_tp2),
                float(px), float(px), in_zone, result.notes or "",
                initial_status, initial_entry_ts, int(expires_at),
                int(baseline_bar_ts), now,
            ),
        )
        self.conn.commit()
        return key

    def update_br_shadow_outcomes_for_symbol(self, symbol: str, state: SymbolState) -> None:
        """
        Advance prospective BR-shadow outcomes using newly closed 1H bars.

        This is diagnostics-only and mirrors raw-setup tracking.  No shadow row
        can create, block, or modify a trading signal.
        """
        closed = state.bars_1h[:-1] if len(state.bars_1h) > 1 else []
        now = now_s()
        rows = self.conn.execute(
            """
            SELECT * FROM br_shadow_setups
            WHERE symbol=? AND final_outcome IS NULL
              AND observation_status IN ('WAITING_ENTRY','ACTIVE')
            ORDER BY first_seen_ts ASC
            """,
            (symbol,),
        ).fetchall()

        for row in rows:
            status = row["observation_status"]
            last_eval = int(row["last_eval_bar_ts_ms"] or 0)
            bars = [b for b in closed if b[B_TS] > last_eval]
            tp1_ts = row["tp1_hit_ts"]
            mfe_pct = float(row["mfe_pct"] or 0.0)
            mae_pct = float(row["mae_pct"] or 0.0)
            mfe_r = float(row["mfe_r"] or 0.0)
            mae_r = float(row["mae_r"] or 0.0)
            final_outcome = None
            final_ts = None
            sl_ts = row["sl_hit_ts"]
            tp2_ts = row["tp2_hit_ts"]
            entry_ts = row["entry_activated_ts"]
            expires_at = int(row["expires_at_ts"])

            for bar in bars:
                bar_ts_s = int(bar[B_TS] // 1000)

                if status == "WAITING_ENTRY":
                    if bar_ts_s > expires_at:
                        break
                    if self._bar_touches_entry(row, bar):
                        sl_hit, tp1_hit, tp2_hit = self._bar_hits(row, bar)
                        entry_ts = bar_ts_s
                        last_eval = bar[B_TS]
                        if sl_hit or tp1_hit or tp2_hit:
                            final_outcome = "ENTRY_BAR_AMBIGUOUS"
                            final_ts = bar_ts_s
                            sl_ts = bar_ts_s if sl_hit else sl_ts
                            tp1_ts = bar_ts_s if tp1_hit else tp1_ts
                            tp2_ts = bar_ts_s if tp2_hit else tp2_ts
                            status = "DONE"
                            break
                        status = "ACTIVE"
                        expires_at = entry_ts + DIAGNOSTICS_OUTCOME_DAYS * 86400
                        continue
                    last_eval = bar[B_TS]
                    continue

                if status == "ACTIVE":
                    if bar_ts_s > expires_at:
                        break
                    b_mfe_pct, b_mae_pct, b_mfe_r, b_mae_r = self._excursions(row, bar)
                    mfe_pct = max(mfe_pct, b_mfe_pct)
                    mae_pct = max(mae_pct, b_mae_pct)
                    mfe_r = max(mfe_r, b_mfe_r)
                    mae_r = max(mae_r, b_mae_r)
                    sl_hit, tp1_hit, tp2_hit = self._bar_hits(row, bar)

                    ambiguous = sl_hit and (tp2_hit or (tp1_ts is None and tp1_hit))
                    if ambiguous:
                        final_outcome = "AMBIGUOUS"
                        final_ts = bar_ts_s
                        sl_ts = bar_ts_s
                        if tp1_hit and tp1_ts is None:
                            tp1_ts = bar_ts_s
                        if tp2_hit:
                            tp2_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if sl_hit:
                        sl_ts = bar_ts_s
                        final_outcome = "TP1_THEN_SL" if tp1_ts is not None else "SL"
                        final_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if tp2_hit:
                        if tp1_ts is None:
                            tp1_ts = bar_ts_s
                        tp2_ts = bar_ts_s
                        final_outcome = "TP2"
                        final_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if tp1_hit and tp1_ts is None:
                        tp1_ts = bar_ts_s
                    last_eval = bar[B_TS]

            if final_outcome is None:
                if status == "WAITING_ENTRY" and now >= expires_at:
                    final_outcome = "NO_ENTRY"
                    final_ts = expires_at
                    status = "DONE"
                elif status == "ACTIVE" and now >= expires_at:
                    final_outcome = "TP1_ONLY_EXPIRED" if tp1_ts is not None else "EXPIRED"
                    final_ts = expires_at
                    status = "DONE"

            self.conn.execute(
                """
                UPDATE br_shadow_setups SET
                    observation_status=?, entry_activated_ts=?, expires_at_ts=?,
                    tp1_hit_ts=?, tp2_hit_ts=?, sl_hit_ts=?,
                    final_outcome=COALESCE(final_outcome, ?),
                    final_outcome_ts=COALESCE(final_outcome_ts, ?),
                    mfe_pct=?, mae_pct=?, mfe_r=?, mae_r=?,
                    last_eval_bar_ts_ms=?, updated_at_ts=?
                WHERE shadow_key=?
                """,
                (
                    status, entry_ts, expires_at, tp1_ts, tp2_ts, sl_ts,
                    final_outcome, final_ts,
                    mfe_pct, mae_pct, mfe_r, mae_r,
                    last_eval, now, row["shadow_key"],
                ),
            )
        self.conn.commit()

    def br_shadow_summary(self) -> Dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT side,raw_score_first,geometry_gap_atr,observation_status,
                   final_outcome,mfe_r,mae_r
            FROM br_shadow_setups
            """
        ).fetchall()
        total = len(rows)
        sides: Dict[str, int] = {}
        statuses: Dict[str, int] = {}
        outcomes: Dict[str, int] = {}
        scores: List[int] = []
        gaps: List[float] = []
        mfe_r: List[float] = []
        mae_r: List[float] = []
        for r in rows:
            side = r["side"] or "UNKNOWN"
            sides[side] = sides.get(side, 0) + 1
            status = r["observation_status"] or "UNKNOWN"
            statuses[status] = statuses.get(status, 0) + 1
            outcome = r["final_outcome"]
            if outcome:
                outcomes[outcome] = outcomes.get(outcome, 0) + 1
            scores.append(int(r["raw_score_first"] or 0))
            gaps.append(float(r["geometry_gap_atr"] or 0.0))
            mfe_r.append(float(r["mfe_r"] or 0.0))
            mae_r.append(float(r["mae_r"] or 0.0))
        return {
            "total": total,
            "sides": sides,
            "statuses": statuses,
            "outcomes": outcomes,
            "avg_score": (sum(scores) / total) if total else 0.0,
            "avg_gap_atr": (sum(gaps) / total) if total else 0.0,
            "avg_mfe_r": (sum(mfe_r) / total) if total else 0.0,
            "avg_mae_r": (sum(mae_r) / total) if total else 0.0,
        }

    def update_gate(
        self,
        setup_key: str,
        status: str,
        reason: str,
    ) -> None:
        now = now_s()
        self.conn.execute(
            """
            UPDATE raw_setups SET
                gate_status_first=COALESCE(gate_status_first, ?),
                gate_reason_first=COALESCE(gate_reason_first, ?),
                gate_status_latest=?,
                gate_reason_latest=?,
                updated_at_ts=?
            WHERE setup_key=?
            """,
            (status, reason, status, reason, now, setup_key),
        )
        self.conn.commit()

    def mark_signal_emitted(self, setup_key: str) -> None:
        now = now_s()
        self.conn.execute(
            """
            UPDATE raw_setups SET signal_emitted=1, signal_emitted_ts=?, updated_at_ts=?
            WHERE setup_key=?
            """,
            (now, now, setup_key),
        )
        self.conn.commit()

    @staticmethod
    def _bar_touches_entry(row: sqlite3.Row, bar: Bar) -> bool:
        return bar[B_HIGH] >= row["entry_low"] and bar[B_LOW] <= row["entry_high"]

    @staticmethod
    def _bar_hits(row: sqlite3.Row, bar: Bar) -> Tuple[bool, bool, bool]:
        if row["side"] == "LONG":
            return (
                bar[B_LOW] <= row["stop_loss"],
                bar[B_HIGH] >= row["tp1"],
                bar[B_HIGH] >= row["tp2"],
            )
        return (
            bar[B_HIGH] >= row["stop_loss"],
            bar[B_LOW] <= row["tp1"],
            bar[B_LOW] <= row["tp2"],
        )

    @staticmethod
    def _excursions(row: sqlite3.Row, bar: Bar) -> Tuple[float, float, float, float]:
        entry = float(row["entry_mid"])
        risk = abs(entry - float(row["stop_loss"]))
        if entry <= 0.0:
            return 0.0, 0.0, 0.0, 0.0
        if row["side"] == "LONG":
            fav_abs = max(0.0, bar[B_HIGH] - entry)
            adv_abs = max(0.0, entry - bar[B_LOW])
        else:
            fav_abs = max(0.0, entry - bar[B_LOW])
            adv_abs = max(0.0, bar[B_HIGH] - entry)
        mfe_pct = fav_abs / entry * 100.0
        mae_pct = adv_abs / entry * 100.0
        mfe_r = fav_abs / risk if risk > 0.0 else 0.0
        mae_r = adv_abs / risk if risk > 0.0 else 0.0
        return mfe_pct, mae_pct, mfe_r, mae_r

    def update_outcomes_for_symbol(self, symbol: str, state: SymbolState) -> None:
        """
        Advance hypothetical outcomes for raw setups using newly closed 1H bars.

        Entry is activated only after a closed 1H bar touches the recorded entry
        zone. If that same bar also spans TP/SL, the sample is recorded as
        ENTRY_BAR_AMBIGUOUS because OHLC cannot establish event ordering.
        """
        closed = state.bars_1h[:-1] if len(state.bars_1h) > 1 else []
        now = now_s()
        rows = self.conn.execute(
            """
            SELECT * FROM raw_setups
            WHERE symbol=? AND final_outcome IS NULL
              AND observation_status IN ('WAITING_ENTRY','ACTIVE')
            ORDER BY first_seen_ts ASC
            """,
            (symbol,),
        ).fetchall()

        for row in rows:
            status = row["observation_status"]
            last_eval = int(row["last_eval_bar_ts_ms"] or 0)
            bars = [b for b in closed if b[B_TS] > last_eval]
            tp1_ts = row["tp1_hit_ts"]
            mfe_pct = float(row["mfe_pct"] or 0.0)
            mae_pct = float(row["mae_pct"] or 0.0)
            mfe_r = float(row["mfe_r"] or 0.0)
            mae_r = float(row["mae_r"] or 0.0)
            final_outcome = None
            final_ts = None
            sl_ts = row["sl_hit_ts"]
            tp2_ts = row["tp2_hit_ts"]
            entry_ts = row["entry_activated_ts"]
            expires_at = int(row["expires_at_ts"])

            for bar in bars:
                bar_ts_s = int(bar[B_TS] // 1000)

                if status == "WAITING_ENTRY":
                    # first_seen + outcome window is the maximum wait for entry
                    if bar_ts_s > expires_at:
                        break
                    if self._bar_touches_entry(row, bar):
                        # If the same OHLC bar also spans a target or stop, the
                        # order relative to the entry touch is unknowable. Keep
                        # it as an explicit ambiguous sample instead of forcing
                        # a win/loss or silently skipping the bar.
                        sl_hit, tp1_hit, tp2_hit = self._bar_hits(row, bar)
                        entry_ts = bar_ts_s
                        last_eval = bar[B_TS]
                        if sl_hit or tp1_hit or tp2_hit:
                            final_outcome = "ENTRY_BAR_AMBIGUOUS"
                            final_ts = bar_ts_s
                            sl_ts = bar_ts_s if sl_hit else sl_ts
                            tp1_ts = bar_ts_s if tp1_hit else tp1_ts
                            tp2_ts = bar_ts_s if tp2_hit else tp2_ts
                            status = "DONE"
                            break
                        status = "ACTIVE"
                        expires_at = entry_ts + DIAGNOSTICS_OUTCOME_DAYS * 86400
                        continue
                    last_eval = bar[B_TS]
                    continue

                if status == "ACTIVE":
                    if bar_ts_s > expires_at:
                        break
                    b_mfe_pct, b_mae_pct, b_mfe_r, b_mae_r = self._excursions(row, bar)
                    mfe_pct = max(mfe_pct, b_mfe_pct)
                    mae_pct = max(mae_pct, b_mae_pct)
                    mfe_r = max(mfe_r, b_mfe_r)
                    mae_r = max(mae_r, b_mae_r)
                    sl_hit, tp1_hit, tp2_hit = self._bar_hits(row, bar)

                    ambiguous = sl_hit and (tp2_hit or (tp1_ts is None and tp1_hit))
                    if ambiguous:
                        final_outcome = "AMBIGUOUS"
                        final_ts = bar_ts_s
                        sl_ts = bar_ts_s
                        if tp1_hit and tp1_ts is None:
                            tp1_ts = bar_ts_s
                        if tp2_hit:
                            tp2_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if sl_hit:
                        sl_ts = bar_ts_s
                        final_outcome = "TP1_THEN_SL" if tp1_ts is not None else "SL"
                        final_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if tp2_hit:
                        if tp1_ts is None:
                            tp1_ts = bar_ts_s
                        tp2_ts = bar_ts_s
                        final_outcome = "TP2"
                        final_ts = bar_ts_s
                        status = "DONE"
                        last_eval = bar[B_TS]
                        break

                    if tp1_hit and tp1_ts is None:
                        tp1_ts = bar_ts_s

                    last_eval = bar[B_TS]

            if final_outcome is None:
                if status == "WAITING_ENTRY" and now >= expires_at:
                    final_outcome = "NO_ENTRY"
                    final_ts = expires_at
                    status = "DONE"
                elif status == "ACTIVE" and now >= expires_at:
                    final_outcome = "TP1_ONLY_EXPIRED" if tp1_ts is not None else "EXPIRED"
                    final_ts = expires_at
                    status = "DONE"

            self.conn.execute(
                """
                UPDATE raw_setups SET
                    observation_status=?, entry_activated_ts=?, expires_at_ts=?,
                    tp1_hit_ts=?, tp2_hit_ts=?, sl_hit_ts=?,
                    final_outcome=COALESCE(final_outcome, ?),
                    final_outcome_ts=COALESCE(final_outcome_ts, ?),
                    mfe_pct=?, mae_pct=?, mfe_r=?, mae_r=?,
                    last_eval_bar_ts_ms=?, updated_at_ts=?
                WHERE setup_key=?
                """,
                (
                    status, entry_ts, expires_at, tp1_ts, tp2_ts, sl_ts,
                    final_outcome, final_ts,
                    mfe_pct, mae_pct, mfe_r, mae_r,
                    last_eval, now, row["setup_key"],
                ),
            )
        self.conn.commit()

    def summary(self) -> Dict[str, Any]:
        total = int(self.conn.execute("SELECT COUNT(*) FROM raw_setups").fetchone()[0])
        detector_rows = self.conn.execute(
            """
            SELECT setup_type, COUNT(*) n, ROUND(AVG(raw_score_first),1) avg_score
            FROM raw_setups GROUP BY setup_type
            """
        ).fetchall()
        detectors = {
            _DIAG_DETECTOR_ABBR.get(r["setup_type"], r["setup_type"]): {
                "n": int(r["n"]), "avg_score": float(r["avg_score"] or 0.0)
            }
            for r in detector_rows
        }
        score_fail = int(self.conn.execute(
            "SELECT COUNT(*) FROM raw_setups WHERE score_passed_first=0"
        ).fetchone()[0])
        signal_emitted = int(self.conn.execute(
            "SELECT COUNT(*) FROM raw_setups WHERE signal_emitted=1"
        ).fetchone()[0])
        status_rows = self.conn.execute(
            "SELECT observation_status,COUNT(*) n FROM raw_setups GROUP BY observation_status"
        ).fetchall()
        statuses = {r["observation_status"]: int(r["n"]) for r in status_rows}
        outcome_rows = self.conn.execute(
            """
            SELECT final_outcome,COUNT(*) n FROM raw_setups
            WHERE final_outcome IS NOT NULL GROUP BY final_outcome
            """
        ).fetchall()
        outcomes = {r["final_outcome"]: int(r["n"]) for r in outcome_rows}
        counters = {
            r["name"]: int(r["value"])
            for r in self.conn.execute("SELECT name,value FROM diag_counters").fetchall()
        }
        size_bytes = 0
        if self.path != ":memory:":
            for suffix in ("", "-wal", "-shm"):
                fp = self.path + suffix
                if os.path.exists(fp):
                    size_bytes += os.path.getsize(fp)
        return {
            "total": total,
            "detectors": detectors,
            "score_fail": score_fail,
            "signal_emitted": signal_emitted,
            "statuses": statuses,
            "outcomes": outcomes,
            "counters": counters,
            "size_bytes": size_bytes,
        }


_DIAG_LAST_WARN_TS = 0


def _diag_warn(message: str) -> None:
    global _DIAG_LAST_WARN_TS
    t = now_s()
    if t - _DIAG_LAST_WARN_TS >= 60:
        _DIAG_LAST_WARN_TS = t
        logger.warning(f"Phase 8L.4 diagnostics: {message}")


def _diag_store(app: web.Application) -> Optional[DiagnosticStore]:
    store = app.get("diag_store")
    return store if isinstance(store, DiagnosticStore) else None

# =============================================================================
# === 6. REGIME ===
# =============================================================================

def compute_regime(state: SymbolState) -> RegimeResult:
    """
    Determine market regime for a symbol from closed 1W and 1D bar EMAs.

    Rules (per approved specification):
      BULLISH: last closed 1W close > EMA{EMA_REGIME_1W}_1W
               AND last closed 1D close > EMA{EMA_REGIME_1D}_1D
      BEARISH: last closed 1W close < EMA{EMA_REGIME_1W}_1W
               AND last closed 1D close < EMA{EMA_REGIME_1D}_1D
      NEUTRAL: 1W and 1D signals conflict, or insufficient indicator data.

    Closed-candle rule: uses bars[-2][B_CLOSE] as the last confirmed close
    (bars[-1] may be the currently forming candle at poll time).
    Indicator values (ema20_1w, ema50_1d) are themselves computed on bars[:-1],
    so all comparisons are consistently on closed data.
    """
    if state.ema20_1w <= 0.0 or state.ema50_1d <= 0.0:
        return RegimeResult(
            regime="NEUTRAL",
            reason="Insufficient indicator data (EMA not yet computed)",
        )

    if len(state.bars_1w) < 2 or len(state.bars_1d) < 2:
        return RegimeResult(
            regime="NEUTRAL",
            reason="Not enough bars for closed-close comparison",
        )

    close_1w = state.bars_1w[-2][B_CLOSE]   # last confirmed weekly close
    close_1d = state.bars_1d[-2][B_CLOSE]   # last confirmed daily close

    w_bull = close_1w > state.ema20_1w
    d_bull = close_1d > state.ema50_1d

    w_sym  = ">" if w_bull else "<"
    d_sym  = ">" if d_bull else "<"
    reason = (
        f"1W close {close_1w:.4f} {w_sym} EMA{EMA_REGIME_1W}W {state.ema20_1w:.4f}"
        f" | 1D close {close_1d:.4f} {d_sym} EMA{EMA_REGIME_1D}D {state.ema50_1d:.4f}"
    )

    if w_bull and d_bull:
        return RegimeResult(regime="BULLISH", reason=reason)
    if (not w_bull) and (not d_bull):
        return RegimeResult(regime="BEARISH", reason=reason)
    # 1W and 1D disagree → CHOP / NEUTRAL
    return RegimeResult(regime="NEUTRAL", reason=f"CHOP — {reason}")


# =============================================================================
# === 7. SETUP DETECTORS  (3A: Breakout+Retest | 3B: Trend Pullback | 3C: Liquidity Sweep) ===
# =============================================================================

# ── Private helpers for detect_breakout_retest ────────────────────────────────

def _prev_bar(bars: List[Bar], ts: int) -> Optional[Bar]:
    """Return the bar immediately before the bar whose B_TS equals `ts`."""
    for i, b in enumerate(bars):
        if b[B_TS] == ts:
            return bars[i - 1] if i > 0 else None
    return None


def _find_retest(
    side: str,
    bo_bar_ts: int,
    key_level: float,
    atr: float,
    closed_4h: List[Bar],
    closed_1h: List[Bar],
    trace: Optional[DetectorStageTrace] = None,
) -> Optional[Tuple[Bar, str]]:
    """
    Search for the first valid retest candle on 4H (preferred) then 1H.

    Timing window:
      start  : bo_bar_ts + 86_400_000  (after the breakout day's close)
      end    : bo_bar_ts + (BREAKOUT_RETEST_MAX_BARS_1D + 1) * 86_400_000

    LONG valid retest:
      bar[B_LOW]  <= key_level + 0.5 × atr   (price touched near the level)
      bar[B_CLOSE] >  key_level               (closed back above)

    SHORT valid retest:
      bar[B_HIGH] >= key_level - 0.5 × atr   (price touched near the level)
      bar[B_CLOSE] <  key_level               (closed back below)

    Returns (retest_bar, tf_key) on success, None if nothing found.
    Bars must be in chronological order (oldest first).
    """
    bo_close_ts = bo_bar_ts + 86_400_000
    deadline_ts = bo_bar_ts + (BREAKOUT_RETEST_MAX_BARS_1D + 1) * 86_400_000
    tolerance   = 0.5 * atr

    for tf_key, bars in (("4h", closed_4h), ("1h", closed_1h)):
        for bar in bars:
            ts = bar[B_TS]
            if ts < bo_close_ts:
                continue
            if ts >= deadline_ts:
                break        # bars are chronological; no point scanning further
            if trace is not None:
                trace.bump(side, f"retest_window_bar_{tf_key}")
            if side == "LONG":
                touch = bar[B_LOW] <= key_level + tolerance
                close_ok = bar[B_CLOSE] > key_level
            else:            # SHORT
                touch = bar[B_HIGH] >= key_level - tolerance
                close_ok = bar[B_CLOSE] < key_level
            if trace is not None:
                if touch:
                    trace.bump(side, f"retest_touch_{tf_key}")
                if close_ok:
                    trace.bump(side, f"retest_close_ok_{tf_key}")
            if touch and close_ok:
                if trace is not None:
                    trace.bump(side, f"retest_valid_{tf_key}")
                return (bar, tf_key)
    return None


def _score_breakout(
    side: str,
    state: "SymbolState",
    bo_bar: Bar,
    retest_bar: Bar,
    retest_tf: str,
    key_level: float,
    atr: float,
    vol_sma: float,
    bo_bar_ts: int,
    vol_sma_4h: float,
    vol_sma_1h: float,
    prev_retest: Optional[Bar],
) -> Tuple[int, Dict[str, int]]:
    """Return Breakout+Retest score and exact awarded-point decomposition."""
    components: Dict[str, int] = {}

    def award(name: str, points: int, condition: bool) -> None:
        if condition:
            components[name] = points

    award(
        "breakout_strong_volume", 20,
        bo_bar[B_VOLUME] > vol_sma * BREAKOUT_VOL_STRONG,
    )
    if side == "LONG":
        reversal = is_bullish_retest_candle(retest_bar, prev_retest)
    else:
        reversal = is_bearish_retest_candle(retest_bar, prev_retest)
    award("retest_reversal_candle", 15, reversal)

    ema1d_ok = False
    if state.ema20_1d > 0 and state.ema50_1d > 0:
        ema1d_ok = (
            state.ema20_1d > state.ema50_1d
            if side == "LONG"
            else state.ema20_1d < state.ema50_1d
        )
    award("ema_1d_alignment", 15, ema1d_ok)

    ema4h_ok = False
    if state.ema20_4h > 0 and state.ema50_4h > 0:
        ema4h_ok = (
            state.ema20_4h > state.ema50_4h
            if side == "LONG"
            else state.ema20_4h < state.ema50_4h
        )
    award("ema_4h_alignment", 10, ema4h_ok)

    award("fast_retest", 10, retest_bar[B_TS] < bo_bar_ts + 4 * 86_400_000)

    not_overextended = False
    if key_level > 0:
        dist_pct = (
            (bo_bar[B_CLOSE] - key_level) / key_level
            if side == "LONG"
            else (key_level - bo_bar[B_CLOSE]) / key_level
        )
        not_overextended = dist_pct <= 0.01
    award("breakout_not_overextended", 10, not_overextended)

    tf_vsma = vol_sma_4h if retest_tf == "4h" else vol_sma_1h
    award(
        "low_volume_retest", 10,
        tf_vsma > 0 and retest_bar[B_VOLUME] < tf_vsma * 0.9,
    )

    return sum(components.values()), components

def _scan_breakout_side(
    side: str,
    state: "SymbolState",
    closed_1d: List[Bar],
    closed_4h: List[Bar],
    closed_1h: List[Bar],
    atr: float,
    vol_sma: float,
    vol_sma_4h: float,
    vol_sma_1h: float,
    trace: Optional[DetectorStageTrace] = None,
) -> Optional[SetupResult]:
    """
    Scan closed 1D bars for the most recent valid Breakout + Retest on one side.

    Breakout bar conditions:
      LONG:  close > max broken swing high  AND  volume >= vol_sma × BREAKOUT_VOL_MIN
             AND  close in upper 40% of range
      SHORT: close < min broken swing low   AND  same volume/close conditions (mirrored)

    Key level:
      LONG:  max(swing_highs broken by the bar's close) — highest cleared resistance
      SHORT: min(swing_lows  broken by the bar's close) — nearest broken support above close

    Retest search is delegated to _find_retest (4H first, then 1H fallback).
    """
    if trace is not None:
        trace.bump(side, "calls")

    max_idx   = len(closed_1d) - 1
    min_idx   = SWING_LOOKBACK_1D + SWING_PROMINENCE_1D * 2
    # Only scan breakout bars that are recent enough for a retest to still be in window
    scan_floor = max(min_idx, max_idx - BREAKOUT_RETEST_MAX_BARS_1D)

    for bo_idx in range(max_idx, scan_floor - 1, -1):
        if trace is not None:
            trace.reach(side, "scan_bar", 1)
        bo_bar   = closed_1d[bo_idx]
        lookback = closed_1d[bo_idx - SWING_LOOKBACK_1D : bo_idx]

        # ── Find key level ──────────────────────────────────────────────────────
        if side == "LONG":
            sw     = find_swing_highs(lookback, SWING_LOOKBACK_1D, SWING_PROMINENCE_1D)
            broken = [h for h in sw if bo_bar[B_CLOSE] > h]
            if not broken:
                continue
            key_level = max(broken)   # highest resistance cleared
        else:
            sw     = find_swing_lows(lookback, SWING_LOOKBACK_1D, SWING_PROMINENCE_1D)
            broken = [lo for lo in sw if bo_bar[B_CLOSE] < lo]
            if not broken:
                continue
            key_level = min(broken)   # nearest broken support above the close

        if trace is not None:
            trace.reach(side, "broken_swing", 2)

        # ── Breakout candle quality ─────────────────────────────────────────────
        if bo_bar[B_VOLUME] < vol_sma * BREAKOUT_VOL_MIN:
            continue
        if trace is not None:
            trace.reach(side, "volume_pass", 3)
        if side == "LONG":
            if not candle_closes_upper_pct(bo_bar, 0.40):
                continue
        else:
            if not candle_closes_lower_pct(bo_bar, 0.40):
                continue

        if trace is not None:
            trace.reach(side, "candle_pass", 4)

        # ── Retest search ───────────────────────────────────────────────────────
        found = _find_retest(
            side, bo_bar[B_TS], key_level, atr, closed_4h, closed_1h, trace=trace
        )
        if found is None:
            continue
        if trace is not None:
            trace.reach(side, "retest_found", 5)
        retest_bar, retest_tf = found
        prev_retest = _prev_bar(
            closed_4h if retest_tf == "4h" else closed_1h,
            retest_bar[B_TS],
        )

        # ── Score ───────────────────────────────────────────────────────────────
        score, score_components = _score_breakout(
            side, state, bo_bar, retest_bar, retest_tf,
            key_level, atr, vol_sma,
            bo_bar[B_TS], vol_sma_4h, vol_sma_1h, prev_retest,
        )

        # ── Entry / SL / TP levels (TP is ATR-fallback; Phase 4 adds structure) ─
        if side == "LONG":
            entry_low    = key_level - 0.2 * atr
            entry_high   = key_level + 0.3 * atr
            entry_mid    = (entry_low + entry_high) / 2.0
            stop_loss    = retest_bar[B_LOW]  - 0.1 * atr
            # Geometry guard: SL must be below the entry zone.
            # A shallow retest (low above entry_low) places the SL inside the
            # zone, making the trade structurally invalid.  Skip this candidate
            # and continue scanning for an older breakout bar.
            if stop_loss >= entry_low:
                if trace is not None:
                    trace.bump(side, "geometry_fail")
                    shadow_stop = entry_low - 0.05 * atr
                    shadow_tp1 = entry_mid + TP1_ATR_MULT_BREAKOUT * atr
                    shadow_tp2 = entry_mid + TP2_ATR_MULT_BREAKOUT * atr
                    gap_abs = max(0.0, stop_loss - entry_low)
                    shadow_result = SetupResult(
                        setup_type="BREAKOUT_RETEST", side=side, score=score,
                        entry_low=round(entry_low, 6), entry_high=round(entry_high, 6),
                        stop_loss=round(shadow_stop, 6),
                        tp1=round(shadow_tp1, 6), tp2=round(shadow_tp2, 6),
                        rr_tp1=calc_rr(side, entry_mid, shadow_stop, shadow_tp1),
                        rr_tp2=calc_rr(side, entry_mid, shadow_stop, shadow_tp2),
                        invalidation=(
                            f"Daily close below {key_level:.4f} "
                            f"(broken resistance reverts to resistance)"
                        ),
                        notes=(
                            f"BR_SHADOW geometry_fail bo_ts={bo_bar[B_TS]} "
                            f"key={key_level:.4f} retest_tf={retest_tf} "
                            f"retest_ts={retest_bar[B_TS]} original_sl={stop_loss:.6f}"
                        ),
                        setup_ts=retest_bar[B_TS], setup_tf=retest_tf,
                        score_components=score_components,
                    )
                    trace.br_shadow_candidates.append(BRShadowCandidate(
                        result=shadow_result, breakout_ts_ms=bo_bar[B_TS],
                        retest_ts_ms=retest_bar[B_TS], retest_tf=retest_tf,
                        key_level=key_level, original_stop_loss=stop_loss,
                        shadow_stop_loss=shadow_stop, geometry_gap_abs=gap_abs,
                        geometry_gap_atr=(gap_abs / atr if atr > 0 else 0.0),
                    ))
                    trace.bump(side, "shadow_captured")
                continue
            tp1          = entry_mid + TP1_ATR_MULT_BREAKOUT * atr
            tp2          = entry_mid + TP2_ATR_MULT_BREAKOUT * atr
            invalidation = (
                f"Daily close below {key_level:.4f} "
                f"(broken resistance reverts to resistance)"
            )
        else:
            entry_high   = key_level + 0.2 * atr
            entry_low    = key_level - 0.3 * atr
            entry_mid    = (entry_low + entry_high) / 2.0
            stop_loss    = retest_bar[B_HIGH] + 0.1 * atr
            # Geometry guard: SL must be above the entry zone.
            if stop_loss <= entry_high:
                if trace is not None:
                    trace.bump(side, "geometry_fail")
                    shadow_stop = entry_high + 0.05 * atr
                    shadow_tp1 = entry_mid - TP1_ATR_MULT_BREAKOUT * atr
                    shadow_tp2 = entry_mid - TP2_ATR_MULT_BREAKOUT * atr
                    gap_abs = max(0.0, entry_high - stop_loss)
                    shadow_result = SetupResult(
                        setup_type="BREAKOUT_RETEST", side=side, score=score,
                        entry_low=round(entry_low, 6), entry_high=round(entry_high, 6),
                        stop_loss=round(shadow_stop, 6),
                        tp1=round(shadow_tp1, 6), tp2=round(shadow_tp2, 6),
                        rr_tp1=calc_rr(side, entry_mid, shadow_stop, shadow_tp1),
                        rr_tp2=calc_rr(side, entry_mid, shadow_stop, shadow_tp2),
                        invalidation=(
                            f"Daily close above {key_level:.4f} "
                            f"(broken support reverts to support)"
                        ),
                        notes=(
                            f"BR_SHADOW geometry_fail bo_ts={bo_bar[B_TS]} "
                            f"key={key_level:.4f} retest_tf={retest_tf} "
                            f"retest_ts={retest_bar[B_TS]} original_sl={stop_loss:.6f}"
                        ),
                        setup_ts=retest_bar[B_TS], setup_tf=retest_tf,
                        score_components=score_components,
                    )
                    trace.br_shadow_candidates.append(BRShadowCandidate(
                        result=shadow_result, breakout_ts_ms=bo_bar[B_TS],
                        retest_ts_ms=retest_bar[B_TS], retest_tf=retest_tf,
                        key_level=key_level, original_stop_loss=stop_loss,
                        shadow_stop_loss=shadow_stop, geometry_gap_abs=gap_abs,
                        geometry_gap_atr=(gap_abs / atr if atr > 0 else 0.0),
                    ))
                    trace.bump(side, "shadow_captured")
                continue
            tp1          = entry_mid - TP1_ATR_MULT_BREAKOUT * atr
            tp2          = entry_mid - TP2_ATR_MULT_BREAKOUT * atr
            invalidation = (
                f"Daily close above {key_level:.4f} "
                f"(broken support reverts to support)"
            )

        if trace is not None:
            trace.reach(side, "geometry_pass", 6)
            trace.reach(side, "raw_result", 7)
            trace.finish(side, "raw_result")

        return SetupResult(
            setup_type   = "BREAKOUT_RETEST",
            side         = side,
            score        = score,
            entry_low    = round(entry_low,  6),
            entry_high   = round(entry_high, 6),
            stop_loss    = round(stop_loss,  6),
            tp1          = round(tp1, 6),
            tp2          = round(tp2, 6),
            rr_tp1       = calc_rr(side, entry_mid, stop_loss, tp1),
            rr_tp2       = calc_rr(side, entry_mid, stop_loss, tp2),
            invalidation = invalidation,
            notes        = (
                f"bo_ts={bo_bar[B_TS]} key={key_level:.4f} "
                f"retest_tf={retest_tf} retest_ts={retest_bar[B_TS]}"
            ),
            setup_ts     = retest_bar[B_TS],
            setup_tf     = retest_tf,
            score_components = score_components,
        )

    if trace is not None:
        furthest = trace.progress.get(side, 0)
        reason = {
            0: "no_scan",
            1: "no_broken_swing",
            2: "volume_fail",
            3: "candle_fail",
            4: "retest_fail",
            5: "geometry_fail",
            6: "no_raw_result",
        }.get(furthest, "no_raw_result")
        trace.finish(side, reason)
    return None


def detect_breakout_retest(
    state: SymbolState, trace: Optional[DetectorStageTrace] = None
) -> Optional[SetupResult]:
    """
    Detect a Breakout + Retest setup on a single symbol.

    Scans both LONG and SHORT candidates, then selects according to regime
    and score:
      - Only one side found         → return it.
      - Both found, BULLISH regime  → return LONG.
      - Both found, BEARISH regime  → return SHORT.
      - Both found, NEUTRAL regime  → return the higher-scoring side;
                                      if scores are equal → return None
                                      (simultaneous equal LONG/SHORT in CHOP
                                      is not a clean setup).

    Closed-candle rule (hard): uses bars_1d[:-1], bars_4h[:-1], bars_1h[:-1].
    The currently forming candle does not confirm any condition.

    Min data required:
      closed 1D bars >= SWING_LOOKBACK_1D + 2×SWING_PROMINENCE_1D + 2
                        (= 26 with default params)
      atr14_1d > 0, vol_sma20_1d > 0  (both precomputed on SymbolState)
      Note: BREAKOUT_RETEST_MAX_BARS_1D is a timing window for 4H/1H bar
      matching — it does not add to the minimum 1D bar count.

    Scoring: 0–90 (BTC regime +10 is applied later in can_signal, Phase 4).
    """
    closed_1d = state.bars_1d[:-1]
    closed_4h = state.bars_4h[:-1]
    closed_1h = state.bars_1h[:-1]

    atr     = state.atr14_1d
    vol_sma = state.vol_sma20_1d

    min_bars = SWING_LOOKBACK_1D + SWING_PROMINENCE_1D * 2 + 2  # +2 small buffer
    if len(closed_1d) < min_bars or atr <= 0.0 or vol_sma <= 0.0:
        if trace is not None:
            trace.bump("ALL", "data_not_ready")
        return None

    # Pre-compute TF-specific vol SMAs for retest-volume scoring
    vol_sma_4h = (calc_vol_sma(closed_4h, VOL_SMA_PERIOD)
                  if len(closed_4h) >= VOL_SMA_PERIOD else 0.0)
    vol_sma_1h = (calc_vol_sma(closed_1h, VOL_SMA_PERIOD)
                  if len(closed_1h) >= VOL_SMA_PERIOD else 0.0)

    _args = (state, closed_1d, closed_4h, closed_1h,
             atr, vol_sma, vol_sma_4h, vol_sma_1h)

    long_result  = _scan_breakout_side("LONG",  *_args, trace=trace)
    short_result = _scan_breakout_side("SHORT", *_args, trace=trace)

    # ── Selection: only one side present ─────────────────────────────────────
    if long_result is None:
        selected = short_result   # may also be None
    elif short_result is None:
        selected = long_result
    else:
        # ── Both sides present: choose by regime then score ───────────────────
        regime = state.regime
        if regime == "BULLISH":
            selected = long_result
        elif regime == "BEARISH":
            selected = short_result
        elif long_result.score > short_result.score:
            selected = long_result
        elif short_result.score > long_result.score:
            selected = short_result
        else:
            selected = None
            if trace is not None:
                trace.bump("ALL", "selection_equal_score_tie")
    if trace is not None:
        trace.bump("ALL", "selection_returned" if selected is not None else "selection_none")
    return selected


# ─── Private helpers for detect_trend_pullback ───────────────────────────────

def _find_pullback_window(
    closed_1d: List[Bar],
    ema20: float,
    atr: float,
    side: str,
) -> List[Bar]:
    """
    Return the most recent consecutive pullback bars in chronological order.

    A bar is "in pullback" when price is near or inside the EMA20 zone:
      LONG:  close <= ema20 + 0.3×atr   (pulled back into/toward EMA20)
      SHORT: close >= ema20 - 0.3×atr   (rallied back into/toward EMA20)

    Iterates backwards from the most recent closed bar; stops at the first
    bar that falls outside the in-pullback condition.  Returns an empty list
    when the most recent bar is not in pullback.
    """
    zone = 0.3 * atr
    result: List[Bar] = []
    for bar in reversed(closed_1d):
        in_pb = (bar[B_CLOSE] <= ema20 + zone if side == "LONG"
                 else bar[B_CLOSE] >= ema20 - zone)
        if in_pb:
            result.append(bar)
        else:
            break
    result.reverse()
    return result


def _is_ema_touch(bars: List[Bar], ema: float, atr: float, side: str) -> bool:
    """
    True if any bar in the list shows a valid EMA touch — price entered the
    EMA zone without breaking through it entirely:

      LONG:  low  <= ema + 0.3×atr  AND  close >= ema - 0.3×atr
      SHORT: high >= ema - 0.3×atr  AND  close <= ema + 0.3×atr
    """
    zone = 0.3 * atr
    for bar in bars:
        if side == "LONG":
            if bar[B_LOW] <= ema + zone and bar[B_CLOSE] >= ema - zone:
                return True
        else:
            if bar[B_HIGH] >= ema - zone and bar[B_CLOSE] <= ema + zone:
                return True
    return False


def _score_pullback(
    side: str,
    state: "SymbolState",
    pullback_bars: List[Bar],
    last_4h: Bar,
    pullback_type: str,
    ema20: float,
    ema50: float,
    ema200: float,
    atr: float,
    vol_sma: float,
    closed_4h: List[Bar],
) -> Tuple[int, Dict[str, int]]:
    """Return Trend Pullback score and exact awarded-point decomposition."""
    components: Dict[str, int] = {}

    def award(name: str, points: int, condition: bool) -> None:
        if condition:
            components[name] = points

    stack_ok = False
    if ema200 > 0:
        stack_ok = (
            ema20 > ema50 > ema200
            if side == "LONG"
            else ema20 < ema50 < ema200
        )
    award("ema_full_stack", 20, stack_ok)

    precise = 0.2 * atr
    precise_touch = any(
        (bar[B_LOW] <= ema20 + precise)
        if side == "LONG"
        else (bar[B_HIGH] >= ema20 - precise)
        for bar in pullback_bars
    )
    award("ema20_touch", 15, precise_touch)

    prev_4h = _prev_bar(closed_4h, last_4h[B_TS])
    reversal = (
        is_bullish_retest_candle(last_4h, prev_4h)
        if side == "LONG"
        else is_bearish_retest_candle(last_4h, prev_4h)
    )
    award("reversal_4h_candle", 15, reversal)

    weak_pb_volume = False
    if vol_sma > 0 and pullback_bars:
        pb_vol_avg = sum(b[B_VOLUME] for b in pullback_bars) / len(pullback_bars)
        weak_pb_volume = pb_vol_avg < vol_sma * 0.9
    award("weak_pullback_volume", 15, weak_pb_volume)

    vol_sma_4h = (
        calc_vol_sma(closed_4h, VOL_SMA_PERIOD)
        if len(closed_4h) >= VOL_SMA_PERIOD else 0.0
    )
    award(
        "reversal_4h_volume", 10,
        vol_sma_4h > 0 and last_4h[B_VOLUME] > vol_sma_4h * PULLBACK_REVERSAL_VOL_MIN,
    )

    ema1h_ok = False
    if state.ema20_1h > 0 and state.ema50_1h > 0:
        ema1h_ok = (
            state.ema20_1h > state.ema50_1h
            if side == "LONG"
            else state.ema20_1h < state.ema50_1h
        )
    award("ema_1h_alignment", 10, ema1h_ok)
    award("optimal_pullback_duration", 5, 3 <= len(pullback_bars) <= 5)

    return sum(components.values()), components

def _scan_pullback_side(
    side: str,
    state: "SymbolState",
    closed_1d: List[Bar],
    closed_4h: List[Bar],
    trace: Optional[DetectorStageTrace] = None,
) -> Optional[SetupResult]:
    """
    Scan for a valid Trend Pullback on one side.  Returns SetupResult or None.

    Preconditions checked:
      LONG:  state.trend_1d == "UP"   (last closed 1D: close > EMA20 > EMA50)
      SHORT: state.trend_1d == "DOWN" (last closed 1D: close < EMA20 < EMA50)

    Pipeline:
      1. Trend context  2. Pullback window  3. Duration 2–7
      4. EMA touch (EMA20 preferred; EMA50 deeper pullback as fallback)
      5. 4H confirmation  6. SL  7. Geometry guard  8. TP  9. Score
    """
    if trace is not None:
        trace.bump(side, "calls")

    if side == "LONG":
        if state.trend_1d != "UP":
            if trace is not None:
                trace.finish(side, "trend_context_fail")
            return None
    else:
        if state.trend_1d != "DOWN":
            if trace is not None:
                trace.finish(side, "trend_context_fail")
            return None
    if trace is not None:
        trace.bump(side, "trend_context_pass")

    ema20   = state.ema20_1d
    ema50   = state.ema50_1d
    ema200  = state.ema200_1d
    atr     = state.atr14_1d
    vol_sma = state.vol_sma20_1d

    # ── Pullback window ──────────────────────────────────────────────────────
    pullback_bars = _find_pullback_window(closed_1d, ema20, atr, side)
    if trace is not None:
        trace.bump(side, "pullback_window_nonempty", int(bool(pullback_bars)))
        if pullback_bars:
            trace.bump(side, f"pullback_len_{min(len(pullback_bars), 8)}")
    if not (2 <= len(pullback_bars) <= 7):
        if trace is not None:
            trace.finish(side, "pullback_duration_fail")
        return None
    if trace is not None:
        trace.bump(side, "pullback_duration_pass")

    # ── EMA touch: EMA20 preferred, EMA50 as deeper fallback ────────────────
    if _is_ema_touch(pullback_bars, ema20, atr, side):
        pullback_type = "EMA20"
        if trace is not None:
            trace.bump(side, "ema20_touch_pass")
        if side == "LONG":
            entry_low  = ema20 - 0.2 * atr
            entry_high = ema20 + 0.3 * atr
        else:
            entry_high = ema20 + 0.2 * atr
            entry_low  = ema20 - 0.3 * atr
    elif _is_ema_touch(pullback_bars, ema50, atr, side):
        pullback_type = "EMA50"
        if trace is not None:
            trace.bump(side, "ema50_touch_pass")
        entry_low  = ema50 - 0.25 * atr
        entry_high = ema50 + 0.25 * atr
    else:
        if trace is not None:
            trace.finish(side, "ema_touch_fail")
        return None
    if trace is not None:
        trace.bump(side, "ema_touch_pass")

    entry_mid = (entry_low + entry_high) / 2.0

    # ── 4H confirmation ──────────────────────────────────────────────────────
    if not closed_4h:
        if trace is not None:
            trace.finish(side, "4h_data_fail")
        return None
    last_4h = closed_4h[-1]
    if side == "LONG"  and last_4h[B_CLOSE] <= state.ema20_4h:
        if trace is not None:
            trace.finish(side, "4h_close_confirm_fail")
        return None    # 4H hasn't closed back above EMA20_4H yet
    if side == "SHORT" and last_4h[B_CLOSE] >= state.ema20_4h:
        if trace is not None:
            trace.finish(side, "4h_close_confirm_fail")
        return None    # 4H hasn't closed back below EMA20_4H yet
    if trace is not None:
        trace.bump(side, "4h_close_confirm_pass")

    # Phase 8L.1: "4H confirmation" must be an actual reversal candle, not
    # merely a close on the correct side of EMA20.
    prev_4h = _prev_bar(closed_4h, last_4h[B_TS])
    if REQUIRE_TP_REVERSAL_CONFIRM:
        reversal_ok = (
            is_bullish_retest_candle(last_4h, prev_4h)
            if side == "LONG"
            else is_bearish_retest_candle(last_4h, prev_4h)
        )
        if not reversal_ok:
            if trace is not None:
                trace.finish(side, "hard_reversal_confirm_fail")
            return None
        if trace is not None:
            trace.bump(side, "hard_reversal_confirm_pass")

    # The reversal must also carry at least normal 4H volume when enabled.
    if REQUIRE_TP_REVERSAL_VOLUME:
        vol_sma_4h = (
            calc_vol_sma(closed_4h, VOL_SMA_PERIOD)
            if len(closed_4h) >= VOL_SMA_PERIOD else 0.0
        )
        if vol_sma_4h <= 0.0 or last_4h[B_VOLUME] < vol_sma_4h * PULLBACK_REVERSAL_VOL_MIN:
            if trace is not None:
                trace.finish(side, "hard_reversal_volume_fail")
            return None
        if trace is not None:
            trace.bump(side, "hard_reversal_volume_pass")

    # ── SL: beyond the pullback extreme ─────────────────────────────────────
    if side == "LONG":
        sl_extreme = min(b[B_LOW]  for b in pullback_bars)
        stop_loss  = sl_extreme - 0.15 * atr
    else:
        sl_extreme = max(b[B_HIGH] for b in pullback_bars)
        stop_loss  = sl_extreme + 0.15 * atr

    # ── Geometry guard: SL must be outside the entry zone ───────────────────
    if side == "LONG"  and stop_loss >= entry_low:
        if trace is not None:
            trace.finish(side, "geometry_fail")
        return None
    if side == "SHORT" and stop_loss <= entry_high:
        if trace is not None:
            trace.finish(side, "geometry_fail")
        return None
    if trace is not None:
        trace.bump(side, "geometry_pass")

    # ── TP (ATR fallback; Phase 4 adds swing-structure targets) ─────────────
    if side == "LONG":
        tp1 = entry_mid + TP1_ATR_MULT_PULLBACK * atr
        tp2 = entry_mid + TP2_ATR_MULT_PULLBACK * atr
        invalidation = (
            f"Daily close below EMA50 ({ema50:.4f}) — uptrend structure broken"
        )
    else:
        tp1 = entry_mid - TP1_ATR_MULT_PULLBACK * atr
        tp2 = entry_mid - TP2_ATR_MULT_PULLBACK * atr
        invalidation = (
            f"Daily close above EMA50 ({ema50:.4f}) — downtrend structure broken"
        )

    # ── Score ────────────────────────────────────────────────────────────────
    score, score_components = _score_pullback(
        side, state, pullback_bars, last_4h,
        pullback_type, ema20, ema50, ema200, atr, vol_sma,
        closed_4h,
    )

    if trace is not None:
        trace.bump(side, "raw_result")
        trace.finish(side, "raw_result")

    return SetupResult(
        setup_type   = "TREND_PULLBACK",
        side         = side,
        score        = score,
        entry_low    = round(entry_low,  6),
        entry_high   = round(entry_high, 6),
        stop_loss    = round(stop_loss,  6),
        tp1          = round(tp1, 6),
        tp2          = round(tp2, 6),
        rr_tp1       = calc_rr(side, entry_mid, stop_loss, tp1),
        rr_tp2       = calc_rr(side, entry_mid, stop_loss, tp2),
        invalidation = invalidation,
        notes        = (
            f"pullback={pullback_type} duration={len(pullback_bars)} "
            f"sl_extreme={sl_extreme:.4f}"
        ),
        setup_ts     = last_4h[B_TS],
        setup_tf     = "4h",
        score_components = score_components,
    )


def detect_trend_pullback(
    state: SymbolState, trace: Optional[DetectorStageTrace] = None
) -> Optional[SetupResult]:
    """
    Detect a Trend Pullback setup on a single symbol.

    Scans both LONG and SHORT candidates, then selects according to regime
    and score (same selection logic as detect_breakout_retest):
      - Only one side found          → return it.
      - Both found, BULLISH regime   → return LONG.
      - Both found, BEARISH regime   → return SHORT.
      - Both found, NEUTRAL regime   → higher score wins; tie → None.

    LONG  precondition: trend_1d == "UP"   (close > EMA20 > EMA50)
    SHORT precondition: trend_1d == "DOWN" (close < EMA20 < EMA50)

    Closed-candle rule (hard): uses bars_1d[:-1] and bars_4h[:-1].
    The currently forming candle does not confirm any condition.

    Min data required:
      atr14_1d > 0, ema20_1d > 0, ema50_1d > 0, vol_sma20_1d > 0
      len(bars_1d[:-1]) >= 2,  len(bars_4h[:-1]) >= 1

    Scoring: 0–90 (BTC regime +10 is applied later in can_signal, Phase 4).
    """
    closed_1d = state.bars_1d[:-1]
    closed_4h = state.bars_4h[:-1]

    if (len(closed_1d) < 2 or not closed_4h
            or state.atr14_1d   <= 0.0
            or state.ema20_1d   <= 0.0
            or state.ema50_1d   <= 0.0
            or state.vol_sma20_1d <= 0.0):
        if trace is not None:
            trace.bump("ALL", "data_not_ready")
        return None

    _args = (state, closed_1d, closed_4h)
    long_result  = _scan_pullback_side("LONG",  *_args, trace=trace)
    short_result = _scan_pullback_side("SHORT", *_args, trace=trace)

    if long_result is None:
        selected = short_result
    elif short_result is None:
        selected = long_result
    else:
        regime = state.regime
        if regime == "BULLISH":
            selected = long_result
        elif regime == "BEARISH":
            selected = short_result
        elif long_result.score > short_result.score:
            selected = long_result
        elif short_result.score > long_result.score:
            selected = short_result
        else:
            selected = None
            if trace is not None:
                trace.bump("ALL", "selection_equal_score_tie")
    if trace is not None:
        trace.bump("ALL", "selection_returned" if selected is not None else "selection_none")
    return selected


# ── Private helpers for detect_liquidity_sweep ───────────────────────────────

def _is_clear_sweep(bar: Bar, level: float, side: str, atr: float) -> bool:
    """
    Check hard conditions for a single sweep candle against `level`.

    LONG (sweep of support):
      bar low  <  level                  — price pierced below
      bar close > level                  — closed back above
      lower wick >= 40% of range
      close in upper 50% of range
      sweep distance (level − low) <= 1.2 × atr

    SHORT (sweep of resistance):
      bar high  > level                  — price pierced above
      bar close < level                  — closed back below
      upper wick >= 40% of range
      close in lower 50% of range
      sweep distance (high − level) <= 1.2 × atr
    """
    rng = bar[B_HIGH] - bar[B_LOW]
    if rng <= 0:
        return False

    if side == "LONG":
        if bar[B_LOW] >= level or bar[B_CLOSE] <= level:
            return False
        lower_wick = min(bar[B_CLOSE], bar[B_OPEN]) - bar[B_LOW]
        if lower_wick < 0.4 * rng:
            return False
        if not candle_closes_upper_pct(bar, 0.50):
            return False
        if (level - bar[B_LOW]) > 1.2 * atr:
            return False
    else:  # SHORT
        if bar[B_HIGH] <= level or bar[B_CLOSE] >= level:
            return False
        upper_wick = bar[B_HIGH] - max(bar[B_CLOSE], bar[B_OPEN])
        if upper_wick < 0.4 * rng:
            return False
        if not candle_closes_lower_pct(bar, 0.50):
            return False
        if (bar[B_HIGH] - level) > 1.2 * atr:
            return False

    return True


def _find_sweep_confirmation(
    side: str,
    sweep_bar_ts: int,
    sweep_tf: str,
    swept_level: float,
    closed_4h: List[Bar],
    closed_1h: List[Bar],
) -> Optional[Tuple[Bar, str]]:
    """
    Find the first temporally valid 4H/1H reversal confirmation after a sweep.

    Critical ordering rule (Phase 8L.1): lower-timeframe bars that belong to
    the sweep candle itself cannot confirm it because OHLC data does not reveal
    whether those bars occurred before or after the sweep extreme.

      1D sweep → confirmation starts after the daily candle has closed.
      4H sweep → confirmation starts after the four-hour candle has closed.

    The search window is three days after that close.  When enabled, the
    confirming bar must also be a pin/engulfing reversal candle.
    """
    duration_ms = 86_400_000 if sweep_tf == "1d" else 4 * 3_600_000
    start_ts = sweep_bar_ts + duration_ms
    end_ts   = start_ts + 3 * 86_400_000

    for tf_key, bars in (("4h", closed_4h), ("1h", closed_1h)):
        for bar in bars:
            ts = bar[B_TS]
            if ts < start_ts:
                continue
            if ts >= end_ts:
                break

            level_ok = (
                bar[B_CLOSE] > swept_level
                if side == "LONG"
                else bar[B_CLOSE] < swept_level
            )
            if not level_ok:
                continue

            if REQUIRE_LS_REVERSAL_CONFIRM:
                prev = _prev_bar(bars, ts)
                reversal_ok = (
                    is_bullish_retest_candle(bar, prev)
                    if side == "LONG"
                    else is_bearish_retest_candle(bar, prev)
                )
                if not reversal_ok:
                    continue

            return (bar, tf_key)
    return None


def _score_sweep(
    side: str,
    sweep_bar: Bar,
    swept_level: float,
    atr: float,
    vol_sma: float,
    state: "SymbolState",
    confirm_bar: Optional[Bar],
    confirm_tf: str,
    closed_4h: List[Bar],
    closed_1h: List[Bar],
    level_in_short_lookback: bool,
) -> Tuple[int, Dict[str, int]]:
    """Return Liquidity Sweep score and exact awarded-point decomposition."""
    components: Dict[str, int] = {"clear_sweep": 20}

    def award(name: str, points: int, condition: bool) -> None:
        if condition:
            components[name] = points

    rng = sweep_bar[B_HIGH] - sweep_bar[B_LOW]
    wick_quality = False
    if rng > 0:
        if side == "LONG":
            wick = min(sweep_bar[B_CLOSE], sweep_bar[B_OPEN]) - sweep_bar[B_LOW]
        else:
            wick = sweep_bar[B_HIGH] - max(sweep_bar[B_CLOSE], sweep_bar[B_OPEN])
        wick_quality = wick >= 0.5 * rng
    award("wick_quality", 15, wick_quality)

    reversal = False
    if confirm_bar is not None:
        confirm_bars = closed_4h if confirm_tf == "4h" else closed_1h
        prev = _prev_bar(confirm_bars, confirm_bar[B_TS])
        reversal = (
            is_bullish_retest_candle(confirm_bar, prev)
            if side == "LONG"
            else is_bearish_retest_candle(confirm_bar, prev)
        )
    award("reversal_confirmation", 15, reversal)
    award("recent_swing_level", 10, level_in_short_lookback)

    if side == "LONG":
        sweep_dist = abs(swept_level - sweep_bar[B_LOW])
    else:
        sweep_dist = abs(sweep_bar[B_HIGH] - swept_level)
    award("controlled_sweep_distance", 10, sweep_dist <= 0.7 * atr)

    if vol_sma > 0:
        award("sweep_volume_normal", 10, sweep_bar[B_VOLUME] > vol_sma * SWEEP_VOL_MIN)
        award("sweep_volume_strong", 10, sweep_bar[B_VOLUME] > vol_sma * SWEEP_VOL_STRONG)

    ema1h_ok = False
    if state.ema20_1h > 0 and state.ema50_1h > 0:
        ema1h_ok = (
            state.ema20_1h > state.ema50_1h
            if side == "LONG"
            else state.ema20_1h < state.ema50_1h
        )
    award("ema_1h_alignment", 10, ema1h_ok)

    return sum(components.values()), components

def _scan_sweep_side(
    side: str,
    state: "SymbolState",
    closed_1d: List[Bar],
    closed_4h: List[Bar],
    closed_1h: List[Bar],
    atr: float,
    vol_sma: float,
) -> Optional[SetupResult]:
    """
    Scan for a valid Liquidity Sweep on one side.  Returns SetupResult or None.

    Pipeline:
      1. Collect candidate swept levels from precomputed 1D swing structure.
      2. Find most recent qualifying sweep candle in the last SWEEP_LOOKBACK_1D
         closed 1D bars, then (if not found) in closed 4H bars.
      3. Find first 4H or 1H confirmation bar within 3 days of the sweep.
      4. Compute entry zone, SL (ATR-based), geometry guard.
      5. Compute TP1/TP2 (ATR fallback; Phase 4 replaces with swing structure).
      6. Score.
    """
    # ── Candidate levels ──────────────────────────────────────────────────────
    if side == "LONG":
        short_levels = set(state.swing_lows_1d)
        long_levels  = set(state.swing_lows_1d_long)
    else:
        short_levels = set(state.swing_highs_1d)
        long_levels  = set(state.swing_highs_1d_long)

    all_levels = list(short_levels | long_levels)
    if not all_levels:
        return None

    # ── Find sweep candle: 1D first, then 4H ─────────────────────────────────
    sweep_bar:   Optional[Bar] = None
    swept_level: float         = 0.0
    sweep_tf:    str           = "1d"

    recent_1d = closed_1d[-SWEEP_LOOKBACK_1D:]
    for bar in reversed(recent_1d):
        for level in all_levels:
            if _is_clear_sweep(bar, level, side, atr):
                sweep_bar   = bar
                swept_level = level
                sweep_tf    = "1d"
                break
        if sweep_bar is not None:
            break

    if sweep_bar is None:
        # 4H fallback: limit to the same recency window as 1D search.
        # cutoff_ts = oldest 1D bar in recent_1d window minus one 1D bar width,
        # expressed as the timestamp of the earliest bar we would accept.
        # Equivalent: only 4H bars whose open time >= that of the oldest
        # closed 1D bar included in recent_1d.
        cutoff_ts = closed_1d[-1][B_TS] - SWEEP_LOOKBACK_1D * 86_400_000
        recent_4h = [b for b in closed_4h if b[B_TS] >= cutoff_ts]
        for bar in reversed(recent_4h):
            for level in all_levels:
                if _is_clear_sweep(bar, level, side, atr):
                    sweep_bar   = bar
                    swept_level = level
                    sweep_tf    = "4h"
                    break
            if sweep_bar is not None:
                break

    if sweep_bar is None:
        return None

    level_in_short_lookback = swept_level in short_levels

    # Phase 8L.1: a sweep without at least normal relative volume is too easy
    # to generate in noise and is rejected before confirmation/scoring.
    if REQUIRE_LS_SWEEP_VOLUME:
        if vol_sma <= 0.0 or sweep_bar[B_VOLUME] < vol_sma * SWEEP_VOL_MIN:
            return None

    # ── 4H / 1H confirmation ──────────────────────────────────────────────────
    # Confirmation starts only after the sweep candle has fully closed.
    confirm = _find_sweep_confirmation(
        side, sweep_bar[B_TS], sweep_tf, swept_level, closed_4h, closed_1h
    )
    if confirm is None:
        return None
    confirm_bar, confirm_tf = confirm

    # ── Entry zone ────────────────────────────────────────────────────────────
    if side == "LONG":
        entry_low  = swept_level - 0.2 * atr
        entry_high = swept_level + 0.3 * atr
    else:
        entry_low  = swept_level - 0.3 * atr
        entry_high = swept_level + 0.2 * atr

    entry_mid = (entry_low + entry_high) / 2.0

    # ── SL ────────────────────────────────────────────────────────────────────
    if side == "LONG":
        stop_loss = sweep_bar[B_LOW]  - 0.15 * atr
    else:
        stop_loss = sweep_bar[B_HIGH] + 0.15 * atr

    # ── Geometry guard ────────────────────────────────────────────────────────
    if side == "LONG"  and stop_loss >= entry_low:
        return None
    if side == "SHORT" and stop_loss <= entry_high:
        return None

    # ── TP (ATR fallback; Phase 4 adds structural targets) ───────────────────
    if side == "LONG":
        tp1 = entry_mid + TP1_ATR_MULT_SWEEP * atr
        tp2 = entry_mid + TP2_ATR_MULT_SWEEP * atr
        invalidation = (
            f"Daily close below sweep low ({sweep_bar[B_LOW]:.4f})"
            f" — sweep reversal failed"
        )
    else:
        tp1 = entry_mid - TP1_ATR_MULT_SWEEP * atr
        tp2 = entry_mid - TP2_ATR_MULT_SWEEP * atr
        invalidation = (
            f"Daily close above sweep high ({sweep_bar[B_HIGH]:.4f})"
            f" — sweep reversal failed"
        )

    # ── Score ─────────────────────────────────────────────────────────────────
    score, score_components = _score_sweep(
        side, sweep_bar, swept_level, atr, vol_sma,
        state, confirm_bar, confirm_tf, closed_4h, closed_1h,
        level_in_short_lookback,
    )

    return SetupResult(
        setup_type   = "LIQUIDITY_SWEEP",
        side         = side,
        score        = score,
        entry_low    = round(entry_low,  6),
        entry_high   = round(entry_high, 6),
        stop_loss    = round(stop_loss,  6),
        tp1          = round(tp1, 6),
        tp2          = round(tp2, 6),
        rr_tp1       = calc_rr(side, entry_mid, stop_loss, tp1),
        rr_tp2       = calc_rr(side, entry_mid, stop_loss, tp2),
        invalidation = invalidation,
        notes        = (
            f"sweep_tf={sweep_tf} swept={swept_level:.4f} "
            f"sweep_extreme={sweep_bar[B_LOW] if side=='LONG' else sweep_bar[B_HIGH]:.4f} "
            f"confirm_tf={confirm_tf}"
        ),
        setup_ts     = confirm_bar[B_TS],
        setup_tf     = confirm_tf,
        score_components = score_components,
    )


def detect_liquidity_sweep(state: SymbolState) -> Optional[SetupResult]:
    """
    Detect a Liquidity Sweep Reversal setup on a single symbol.

    Scans both LONG and SHORT candidates, then selects according to regime
    and score (same selection logic as other detectors):
      - Only one side found          → return it.
      - Both found, BULLISH regime   → return LONG.
      - Both found, BEARISH regime   → return SHORT.
      - Both found, NEUTRAL regime   → higher score wins; tie → None.

    LONG  (sweep of support):  finds a recent 1D/4H bar that wicks below a
      prior 1D swing low and closes back above it, then confirmed on 4H/1H.
    SHORT (sweep of resistance): mirror logic using 1D swing highs.

    Closed-candle rule (hard): uses bars_1d[:-1], bars_4h[:-1], bars_1h[:-1].
    The currently forming candle does not confirm any condition.

    Min data required: len(bars_1d[:-1]) >= 2, atr14_1d > 0, vol_sma20_1d > 0,
      and at least one swing level present (swing_lows_1d or _long for LONG;
      swing_highs_1d or _long for SHORT).

    Scoring: 0–100 without BTC regime.
    BTC regime +10 is applied later in can_signal (Phase 4).

    Priority in run_setup_pipeline: LIQUIDITY_SWEEP does NOT automatically beat
    BREAKOUT_RETEST or TREND_PULLBACK.  It may override only when
    score >= LIQUIDITY_SWEEP_PRIORITY_SCORE and score > current winner score.
    """
    closed_1d = state.bars_1d[:-1]
    closed_4h = state.bars_4h[:-1]
    closed_1h = state.bars_1h[:-1]

    atr     = state.atr14_1d
    vol_sma = state.vol_sma20_1d

    if len(closed_1d) < 2 or atr <= 0.0 or vol_sma <= 0.0:
        return None

    _args = (state, closed_1d, closed_4h, closed_1h, atr, vol_sma)

    long_result  = _scan_sweep_side("LONG",  *_args)
    short_result = _scan_sweep_side("SHORT", *_args)

    if long_result is None:
        return short_result
    if short_result is None:
        return long_result

    regime = state.regime
    if regime == "BULLISH":
        return long_result
    if regime == "BEARISH":
        return short_result
    if long_result.score > short_result.score:
        return long_result
    if short_result.score > long_result.score:
        return short_result
    return None  # equal scores in NEUTRAL — not a clean setup


def run_setup_pipeline(state: SymbolState) -> Optional[SetupResult]:
    """
    Run all detectors in priority order and return the winning setup.

    Priority (approved specification):
      1. BREAKOUT_RETEST  — implemented (Phase 3A)
      2. TREND_PULLBACK   — implemented (Phase 3B)
      3. LIQUIDITY_SWEEP  — implemented (Phase 3C); can override ranks 1–2
                            only if score >= LIQUIDITY_SWEEP_PRIORITY_SCORE

    Score floor (enforced here before returning):
      - NEUTRAL regime:  winner must have score >= MIN_SCORE_CHOP  (85)
      - Normal regime:   winner must have score >= MIN_SCORE_NORMAL (55)
    A candidate that fails its floor is discarded (returns None).
    """
    br = detect_breakout_retest(state)
    tp = detect_trend_pullback(state)
    ls = detect_liquidity_sweep(state)

    winner = br or tp   # first non-None by priority order

    # Liquidity sweep can override if its score qualifies
    if ls is not None:
        can_override = ls.score >= LIQUIDITY_SWEEP_PRIORITY_SCORE
        if can_override and (winner is None or ls.score > winner.score):
            winner = ls
        elif winner is None and ls.score >= MIN_SCORE_NORMAL:
            winner = ls

    if winner is None:
        return None

    # Apply score floor based on current regime
    floor = MIN_SCORE_CHOP if state.regime == "NEUTRAL" else MIN_SCORE_NORMAL
    if winner.score < floor:
        return None

    return winner


# =============================================================================
# === 8. TP/SL ENGINE  (Phase 4) ===
# =============================================================================

from dataclasses import replace as _dc_replace


def _find_structural_tp(
    side: str,
    entry_mid: float,
    risk: float,
    candidates: List[float],
    min_rr: float,
    skip_up_to: Optional[float] = None,
) -> Optional[float]:
    """
    Find the nearest structural target meeting `min_rr` beyond `skip_up_to`.

    candidates must be sorted:
      ascending  for LONG  (nearest first)
      descending for SHORT (nearest first)

    skip_up_to: if set, only consider candidates strictly beyond this level.
    """
    for c in candidates:
        if skip_up_to is not None:
            if side == "LONG"  and c <= skip_up_to:
                continue
            if side == "SHORT" and c >= skip_up_to:
                continue
        rr = abs(c - entry_mid) / risk
        if rr >= min_rr:
            return c
    return None


def calc_swing_tpsl(result: SetupResult, state: SymbolState) -> SetupResult:
    """
    Finalise TP1 and TP2 using 1D/4H swing structure where available.
    ATR-based fallback (already embedded in the detector's result.tp1/tp2) is
    kept when no structural target qualifies.

    Preserved: setup_type, side, score, entry_low, entry_high, stop_loss,
               invalidation, notes.
    Recalculated: tp1, tp2, rr_tp1, rr_tp2.

    Structural TP selection:
      LONG:
        TP1 — nearest swing high above entry_mid with RR >= 1.0
        TP2 — next swing high above TP1 with RR >= RR_MIN_TIER2
      SHORT:
        TP1 — nearest swing low below entry_mid with RR >= 1.0
        TP2 — next swing low below TP1 with RR >= RR_MIN_TIER2

    Fallback chain (per target):
      1. Structural target (as above)
      2. Detector ATR placeholder (result.tp1 / result.tp2) — kept when
         it is farther than the selected TP1 and ordering can be maintained
      3. Setup-specific ATR multiple from entry_mid — last resort when
         detector placeholder would move TP2 closer than TP1

    "Do not move closer" rule: structural targets nearer to entry_mid than
    the ATR fallback are still preferred because they represent real
    structure; the overall ordering invariant (tp2 farther than tp1) is
    enforced by the fallback chain.

    If risk <= 0 (degenerate geometry): return result with rr_tp1 = rr_tp2 = 0.
    """
    entry_mid = (result.entry_low + result.entry_high) / 2.0
    side      = result.side

    risk = (entry_mid - result.stop_loss
            if side == "LONG"
            else result.stop_loss - entry_mid)

    if risk <= 0.0:
        return _dc_replace(result, rr_tp1=0.0, rr_tp2=0.0)

    # ── Collect and sort structural candidates ────────────────────────────────
    atr = state.atr14_1d

    if side == "LONG":
        raw = set(state.swing_highs_1d) | set(state.swing_highs_1d_long) | set(state.swing_highs_4h)
        candidates = sorted(c for c in raw if c > entry_mid)        # ascending
    else:
        raw = set(state.swing_lows_1d) | set(state.swing_lows_1d_long) | set(state.swing_lows_4h)
        candidates = sorted((c for c in raw if c < entry_mid), reverse=True)  # descending

    # ── TP1: nearest structural with RR >= 1.0 ────────────────────────────────
    tp1_struct = _find_structural_tp(side, entry_mid, risk, candidates, 1.0)
    tp1 = tp1_struct if tp1_struct is not None else result.tp1

    # ── TP2: next structural past TP1 with RR >= RR_MIN_TIER2 ────────────────
    tp2_struct = _find_structural_tp(
        side, entry_mid, risk, candidates, RR_MIN_TIER2, skip_up_to=tp1
    )

    if tp2_struct is not None:
        tp2 = tp2_struct
    else:
        # Attempt detector's ATR fallback, but only if it maintains ordering.
        atr_fallback_tp2 = result.tp2
        if side == "LONG"  and atr_fallback_tp2 > tp1:
            tp2 = atr_fallback_tp2
        elif side == "SHORT" and atr_fallback_tp2 < tp1:
            tp2 = atr_fallback_tp2
        else:
            # ATR fallback is not past TP1 (structural pulled TP1 far out).
            # Compute a fresh ATR-based TP2 from entry_mid using setup multiplier.
            mult2 = {
                "BREAKOUT_RETEST": TP2_ATR_MULT_BREAKOUT,
                "TREND_PULLBACK":  TP2_ATR_MULT_PULLBACK,
                "LIQUIDITY_SWEEP": TP2_ATR_MULT_SWEEP,
            }.get(result.setup_type, TP2_ATR_MULT_BREAKOUT)
            if side == "LONG":
                tp2 = entry_mid + mult2 * atr
                if tp2 <= tp1:
                    tp2 = tp1 + atr   # extend one ATR beyond structural TP1
            else:
                tp2 = entry_mid - mult2 * atr
                if tp2 >= tp1:
                    tp2 = tp1 - atr

    return _dc_replace(
        result,
        tp1    = round(tp1, 6),
        tp2    = round(tp2, 6),
        rr_tp1 = calc_rr(side, entry_mid, result.stop_loss, tp1),
        rr_tp2 = calc_rr(side, entry_mid, result.stop_loss, tp2),
    )


# =============================================================================
# === 9. SIGNAL GATE  (Phase 4) ===
# =============================================================================

def passes_rr_gate(result: SetupResult, symbol: str) -> bool:
    """
    Phase 8L.3 controlled rollback: use the original Phase 8L RR gate.

    Only TP2 must meet the tier-specific swing RR minimum:
      Tier 1 (BTC/ETH): TP2 >= RR_MIN_TIER1
      Tier 2 (others):  TP2 >= RR_MIN_TIER2

    TP1 RR is still calculated and displayed, but it is no longer a hard
    signal blocker.  This restores the pre-8L.1 behavior without reverting
    the temporal/lifecycle correctness fixes added later.
    """
    if result is None or result.rr_tp2 <= 0.0:
        return False
    threshold = RR_MIN_TIER1 if symbol in TIER1_SYMBOLS else RR_MIN_TIER2
    return result.rr_tp2 >= threshold


def validate_fast_4h_alignment(state: SymbolState, result: SetupResult) -> Tuple[bool, str]:
    """Block entries against the fast closed-4H trend state."""
    if not REQUIRE_4H_TREND_ALIGNMENT:
        return True, "ok"
    if len(state.bars_4h) < 2 or state.ema20_4h <= 0.0 or state.ema50_4h <= 0.0:
        return False, "fast_4h_conflict"
    close_4h = state.bars_4h[-2][B_CLOSE]
    if result.side == "LONG":
        ok = close_4h > state.ema20_4h > state.ema50_4h
    else:
        ok = close_4h < state.ema20_4h < state.ema50_4h
    return (True, "ok") if ok else (False, "fast_4h_conflict")


def validate_post_sl_reentry(state: SymbolState, result: SetupResult) -> Tuple[bool, str]:
    """Prevent recycling a stopped thesis and immediate same-direction revenge entries."""
    key = (result.setup_type, result.setup_ts)
    if key in state.stopped_setup_keys:
        return False, "reused_stopped_setup"

    if state.last_exit_event != "SL_HIT" or result.side != state.last_stopped_side:
        return True, "ok"

    # A candidate whose confirmation had not fully closed after the stopped
    # trade is not a new thesis.
    if setup_available_ts_ms(result) <= state.last_exit_ts * 1000:
        return False, "post_sl_cooldown"
    if now_s() < state.post_sl_lock_until:
        return False, "post_sl_cooldown"
    return True, "ok"


def can_signal(
    sym: str,
    state: SymbolState,
    mkt: Market,
    result: Optional[SetupResult],
) -> bool:
    """
    Full emission gate.  Returns True only when every condition is satisfied.

    Checks (in order):
      1. result is not None.
      2. state.active_idea is None   — no duplicate signal for this symbol.
      3. state.ready is True         — all indicators computed.
      4. Score floor:
           NEUTRAL regime → score >= MIN_SCORE_CHOP  (85)
           otherwise      → score >= MIN_SCORE_NORMAL (55)
      5. Direction / symbol-regime compatibility:
           LONG  allowed when regime == BULLISH
           SHORT allowed when regime == BEARISH
           Either allowed when regime == NEUTRAL only if score >= MIN_SCORE_CHOP
      6. BTC global regime (soft filter — Tier 1 symbols are exempt):
           Non-Tier-1 LONG  blocked when mkt.btc_regime == BEARISH
             unless score >= MIN_SCORE_CHOP
           Non-Tier-1 SHORT blocked when mkt.btc_regime == BULLISH
             unless score >= MIN_SCORE_CHOP
           Non-Tier-1 NEUTRAL btc_regime → blocked unless score >= MIN_SCORE_CHOP
      7. passes_rr_gate(result, sym) is True.
    """
    if result is None:
        return False
    if state.active_idea is not None:
        return False
    if not state.ready:
        return False

    score  = result.score
    side   = result.side
    regime = state.regime

    # Score floor
    floor = MIN_SCORE_CHOP if regime == "NEUTRAL" else MIN_SCORE_NORMAL
    if score < floor:
        return False

    # Direction / regime compatibility
    if regime == "BULLISH" and side == "SHORT":
        return False
    if regime == "BEARISH" and side == "LONG":
        return False
    if regime == "NEUTRAL" and score < MIN_SCORE_CHOP:
        return False

    # BTC global regime soft filter (Tier 1 exempt)
    if sym not in TIER1_SYMBOLS:
        btc = mkt.btc_regime
        if btc == "BEARISH" and side == "LONG"  and score < MIN_SCORE_CHOP:
            return False
        if btc == "BULLISH" and side == "SHORT" and score < MIN_SCORE_CHOP:
            return False
        if btc == "NEUTRAL" and score < MIN_SCORE_CHOP:
            return False

    # Fast 4H alignment and post-stop protection (defence in depth).
    if not validate_fast_4h_alignment(state, result)[0]:
        return False
    if not validate_post_sl_reentry(state, result)[0]:
        return False

    # RR gate
    return passes_rr_gate(result, sym)


# =============================================================================
# === 10. SWING ENGINE  (Phase 5) ===
# =============================================================================

# ── Phase 8A helpers ──────────────────────────────────────────────────────────

def get_current_price(state: SymbolState) -> float:
    """
    Return the most recent close price from the latest available bar.

    Uses the forming (last) candle intentionally — we want the live market
    price, not the most recent closed candle.
    Preference order: 1H → 4H → 1D.
    Returns 0.0 when no bars are available (prevents false zone rejection).
    """
    for bars in (state.bars_1h, state.bars_4h, state.bars_1d):
        if bars:
            return bars[-1][B_CLOSE]
    return 0.0


def is_price_in_entry_zone(price: float, entry_low: float, entry_high: float) -> bool:
    """True when price is within [entry_low, entry_high] (inclusive)."""
    return entry_low <= price <= entry_high


def is_setup_fresh(result: SetupResult) -> bool:
    """
    True when the setup confirmation bar is within SETUP_MAX_AGE_HOURS of now.

    Returns False when setup_ts == 0 (unknown timestamp, or pre-Phase-8A
    detector / test fixture that did not populate setup_ts).
    This is intentionally strict: a zero setup_ts is treated as stale so
    that old / test-only results do not accidentally trigger real emissions.
    """
    available_ts = confirmation_available_ts_ms(result.setup_ts, result.setup_tf)
    if available_ts <= 0:
        return False
    return (now_ms() - available_ts) <= SETUP_MAX_AGE_HOURS * 3_600_000


# ── Phase 8D helpers ──────────────────────────────────────────────────────────

def calc_rr_from_current(
    side: str,
    current_price: float,
    stop_loss: float,
    tp1: float,
    tp2: float,
) -> Tuple[float, float]:
    """
    Recalculate TP1/TP2 risk-reward ratios using the current market price as
    the risk reference point rather than entry_mid.

    LONG:  risk = current_price − stop_loss
           rr   = (tp − current_price) / risk
    SHORT: risk = stop_loss − current_price
           rr   = (current_price − tp) / risk

    Returns (rr_tp1, rr_tp2).  Returns (0.0, 0.0) when risk ≤ 0 (current price
    has moved past stop-loss or equals it — bad geometry).
    """
    if side == "LONG":
        risk = current_price - stop_loss
    else:
        risk = stop_loss - current_price

    if risk <= 0.0:
        return 0.0, 0.0

    if side == "LONG":
        rr1 = (tp1 - current_price) / risk
        rr2 = (tp2 - current_price) / risk
    else:
        rr1 = (current_price - tp1) / risk
        rr2 = (current_price - tp2) / risk

    return round(rr1, 2), round(rr2, 2)


def setup_available_ts_ms(result: SetupResult) -> int:
    """Return the real confirmation-available time for a SetupResult."""
    return confirmation_available_ts_ms(result.setup_ts, result.setup_tf)


def _post_setup_monitor_bars(result: SetupResult, state: SymbolState) -> List[Bar]:
    """
    Return the highest-resolution bar series that fully covers the period after
    the confirmation candle CLOSED.

    1H is preferred when its retained history reaches back to confirmation;
    otherwise 4H is used (200×4H ≈ 33 days, matching the 30-day context cap).
    1D is a defensive fallback.  Bars that belong to the confirmation candle
    itself are excluded, so pre-confirmation highs/lows cannot kill a setup.
    """
    start_ts = setup_available_ts_ms(result)
    if start_ts <= 0:
        return []

    for bars in (state.bars_1h, state.bars_4h, state.bars_1d):
        if not bars:
            continue
        # Do not use a high-resolution series if its retained history starts
        # after confirmation; that would leave an unverified history gap.
        if bars[0][B_TS] > start_ts:
            continue
        return [bar for bar in bars if bar[B_TS] >= start_ts]

    return []


def validate_actionable_setup(
    result: SetupResult,
    state: SymbolState,
    current_price: float,
) -> Tuple[bool, str]:
    """
    Determine whether a detected setup is actionable at the current market price.

    Phase 8E reordering (safety improvement):
    TP/SL bar scans now run BEFORE the outside_entry_zone check.
    This prevents polluting the watchlist with setups whose TP or SL was
    already hit even when the price happens to be outside the entry zone.

    Checks (in order):
      1. context_too_old       — setup_ts = 0 or age > SETUP_CONTEXT_MAX_DAYS
      2. price_missing         — current_price ≤ 0.0
      3. already_hit_tp / sl  — any bar after the confirmation candle CLOSED
                                  touched target/stop
      4. bad geometry          — current_price on the wrong side of stop_loss
      5. outside_entry_zone    — price outside [entry_low, entry_high]
                                 (only when ENTRY_ZONE_REQUIRED is True)
      6. ok

    Returns (True, "ok") or (False, reason_string).
    Reason strings correspond to ScanDiagnostics field names.
    """
    side = result.side

    # 1. Context age — measured from the close of the confirmation candle.
    available_ts = setup_available_ts_ms(result)
    if available_ts <= 0:
        return False, "context_too_old"
    age_ms = now_ms() - available_ts
    if age_ms > SETUP_CONTEXT_MAX_DAYS * 86_400_000:
        return False, "context_too_old"

    # 2. Price availability
    if current_price <= 0.0:
        return False, "price_missing"

    # 3. Phase 8L.2 temporal boundary: scan only bars that START after the
    #    confirmation candle has CLOSED.  setup_ts itself is the confirmation
    #    candle OPEN time and must never be scanned for later TP/SL touches.
    #    Priority: TP2 hit > SL hit > TP1 hit (worst case surfaces first).
    for bar in _post_setup_monitor_bars(result, state):
        if side == "LONG":
            if bar[B_HIGH] >= result.tp2:
                return False, "already_hit_tp"
            if bar[B_LOW] <= result.stop_loss:
                return False, "already_hit_sl"
            if bar[B_HIGH] >= result.tp1:
                return False, "already_hit_tp"
        else:  # SHORT
            if bar[B_LOW] <= result.tp2:
                return False, "already_hit_tp"
            if bar[B_HIGH] >= result.stop_loss:
                return False, "already_hit_sl"
            if bar[B_LOW] <= result.tp1:
                return False, "already_hit_tp"

    # 4. Geometry: current price must still be on the correct side of SL
    if side == "LONG" and current_price <= result.stop_loss:
        return False, "already_hit_sl"
    if side == "SHORT" and current_price >= result.stop_loss:
        return False, "already_hit_sl"

    # 5. Entry zone (when required) — after TP/SL checks so stale setups
    #    don't appear as "outside_zone" pending items
    if ENTRY_ZONE_REQUIRED:
        if not is_price_in_entry_zone(current_price, result.entry_low, result.entry_high):
            return False, "outside_entry_zone"

    return True, "ok"


def _diag_actionable_fail(
    d: ScanDiagnostics,
    t: ScanDiagnostics,
    reason: str,
) -> None:
    """Increment the ScanDiagnostics counter that matches a validate_actionable_setup reason."""
    if reason == "context_too_old":
        d.context_too_old += 1; t.context_too_old += 1
    elif reason == "price_missing":
        d.price_missing += 1; t.price_missing += 1
    elif reason == "outside_entry_zone":
        d.outside_entry_zone += 1; t.outside_entry_zone += 1
    elif reason == "already_hit_tp":
        d.already_hit_tp += 1; t.already_hit_tp += 1
    elif reason == "already_hit_sl":
        d.already_hit_sl += 1; t.already_hit_sl += 1
    elif reason == "invalidated_since_setup":
        d.invalidated_since_setup += 1; t.invalidated_since_setup += 1
    elif reason == "tpsl_fail":
        d.tpsl_fail += 1; t.tpsl_fail += 1
    elif reason == "rr_current_fail":
        d.rr_current_fail += 1; t.rr_current_fail += 1
    elif reason == "signal_gate_fail":
        d.signal_gate_fail += 1; t.signal_gate_fail += 1
    elif reason == "entry_retest_too_old":
        d.entry_retest_too_old += 1; t.entry_retest_too_old += 1
    elif reason == "liquidity_sweep_too_old":
        d.liquidity_sweep_too_old += 1; t.liquidity_sweep_too_old += 1
    elif reason == "fast_4h_conflict":
        d.fast_4h_conflict += 1; t.fast_4h_conflict += 1
    elif reason == "post_sl_cooldown":
        d.post_sl_cooldown += 1; t.post_sl_cooldown += 1
    elif reason == "reused_stopped_setup":
        d.reused_stopped_setup += 1; t.reused_stopped_setup += 1
    elif reason == "secondary_score_fail":
        d.secondary_score_fail += 1; t.secondary_score_fail += 1
    else:
        logger.debug(f"_diag_actionable_fail: unhandled reason '{reason}'")


# ── Phase 8E watchlist helpers ────────────────────────────────────────────────

def calc_distance_to_entry_zone(
    price: float,
    entry_low: float,
    entry_high: float,
) -> Tuple[float, str]:
    """
    Return (distance_pct, distance_side) describing how far the current price
    is from the entry zone.

      price < entry_low  → (pct_below, "BELOW_ENTRY_ZONE")
      price > entry_high → (pct_above, "ABOVE_ENTRY_ZONE")
      price in zone      → (0.0,       "IN_ENTRY_ZONE")
      price ≤ 0          → (0.0,       "UNKNOWN")
    """
    if price <= 0.0:
        return 0.0, "UNKNOWN"
    if price < entry_low:
        return round((entry_low - price) / price * 100.0, 2), "BELOW_ENTRY_ZONE"
    if price > entry_high:
        return round((price - entry_high) / price * 100.0, 2), "ABOVE_ENTRY_ZONE"
    return 0.0, "IN_ENTRY_ZONE"


def make_pending_setup(
    sym: str,
    state: SymbolState,
    mkt: Market,
    result: SetupResult,
    current_price: float,
    reason: str,
) -> PendingSetup:
    """Build a PendingSetup from the current scan context."""
    dist_pct, dist_side = calc_distance_to_entry_zone(
        current_price, result.entry_low, result.entry_high
    )
    available_ts = setup_available_ts_ms(result)
    age_h = int((now_ms() - available_ts) / 3_600_000) if available_ts > 0 else -1
    return PendingSetup(
        symbol        = sym,
        side          = result.side,
        setup_type    = result.setup_type,
        score         = result.score,
        entry_low     = result.entry_low,
        entry_high    = result.entry_high,
        stop_loss     = result.stop_loss,
        tp1           = result.tp1,
        tp2           = result.tp2,
        rr_tp1        = result.rr_tp1,
        rr_tp2        = result.rr_tp2,
        current_price = current_price,
        distance_pct  = dist_pct,
        distance_side = dist_side,
        reason        = reason,
        setup_ts      = result.setup_ts,
        setup_age_h   = age_h,
        updated_at    = now_s(),
        regime        = state.regime,
        btc_regime    = mkt.btc_regime,
        invalidation  = result.invalidation,
    )


def clear_pending_setup(mkt: Market, sym: str, reason: str = "") -> None:
    """Remove a pending setup for a symbol, if one exists."""
    if sym in mkt.pending_setups:
        del mkt.pending_setups[sym]
        logger.debug(
            f"clear_pending_setup {sym}"
            + (f" ({reason})" if reason else "")
        )


# ── Phase 8H: Liquidity Sweep recency helpers ─────────────────────────────────

def is_liquidity_sweep_recent(result: SetupResult) -> bool:
    """
    Return True when the candidate is NOT a Liquidity Sweep, or when it IS a
    Liquidity Sweep whose confirmation bar is within LIQUIDITY_SWEEP_MAX_AGE_HOURS.

    This is a STRICTER gate than SETUP_CONTEXT_MAX_DAYS (30 days):
      BR and TP are not affected (always return True here).
      LS with setup_ts = 0 is treated as stale (False).
    """
    if result.setup_type != "LIQUIDITY_SWEEP":
        return True
    available_ts = setup_available_ts_ms(result)
    if available_ts <= 0:
        return False
    return (now_ms() - available_ts) <= LIQUIDITY_SWEEP_MAX_AGE_HOURS * 3_600_000


def liquidity_sweep_age_h(result: SetupResult) -> int:
    """Return hours since LS confirmation became knowable, or -1 if unknown."""
    available_ts = setup_available_ts_ms(result)
    if available_ts <= 0:
        return -1
    return int((now_ms() - available_ts) / 3_600_000)


# ── Phase 8K: Fresh Entry Retest gate ─────────────────────────────────────────

def bar_touches_entry_zone(bar: Bar, entry_low: float, entry_high: float) -> bool:
    """
    True when a bar's high-low range overlaps the entry zone.

    Used by the Phase 8K fresh-entry-retouch gate.  We intentionally use the
    full bar range, not just close, because a return into the zone may happen
    intrabar while the final close is outside.
    """
    return bar[B_LOW] <= entry_high and bar[B_HIGH] >= entry_low


def _bars_for_entry_retest_scan(state: SymbolState) -> List[Bar]:
    """
    Return the best available bar series for entry-zone retest timing.

    1H is preferred for precision.  4H and 1D are fallbacks for defensive
    completeness, although normal readiness means 1H should be available.
    """
    if state.bars_1h:
        return state.bars_1h
    if state.bars_4h:
        return state.bars_4h
    return state.bars_1d


def latest_entry_zone_return_ts(result: SetupResult, state: SymbolState) -> Optional[int]:
    """
    Return the timestamp of the latest transition back into the entry zone
    after setup_ts, or None if the zone was never touched.

    A "return" is counted when a bar touches the entry zone and the previous
    scanned bar did not.  If the first scanned bar is already inside the zone,
    its timestamp is used.  This is conservative: a setup that has been sitting
    in the entry zone for days will have an old return timestamp and can be
    rejected by the freshness gate.
    """
    start_ts = setup_available_ts_ms(result)
    if start_ts <= 0:
        return None

    bars = [b for b in _bars_for_entry_retest_scan(state) if b[B_TS] >= start_ts]
    if not bars:
        return None

    last_return_ts: Optional[int] = None
    prev_in_zone = False

    for bar in bars:
        in_zone = bar_touches_entry_zone(bar, result.entry_low, result.entry_high)
        if in_zone and not prev_in_zone:
            last_return_ts = bar[B_TS]
        prev_in_zone = in_zone

    return last_return_ts


def validate_fresh_entry_retest(
    result: SetupResult,
    state: SymbolState,
    current_price: float,
) -> Tuple[bool, str]:
    """
    Phase 8K safety gate for older Liquidity Sweep candidates.

    Purpose:
      Allow LS candidates up to LIQUIDITY_SWEEP_MAX_AGE_HOURS, but if the LS
      is older than LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS, require a fresh return
      into the entry zone within ENTRY_RETEST_MAX_AGE_HOURS.

    This prevents signals from firing on old LS structures where price has been
    drifting around the entry zone for too long.  TP/SL touch checks still run
    earlier in validate_actionable_setup().
    """
    if result.setup_type != "LIQUIDITY_SWEEP":
        return True, "ok"
    if result.setup_ts <= 0:
        return False, "entry_retest_too_old"
    if not ENTRY_ZONE_REQUIRED:
        return True, "ok"

    setup_age_h = liquidity_sweep_age_h(result)
    if setup_age_h <= LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS:
        return True, "ok"

    if not is_price_in_entry_zone(current_price, result.entry_low, result.entry_high):
        return True, "ok"  # outside_entry_zone is handled by validate_actionable_setup()

    retest_ts = latest_entry_zone_return_ts(result, state)
    if retest_ts is None:
        return False, "entry_retest_too_old"

    retest_age_h = int((now_ms() - retest_ts) / 3_600_000)
    if retest_age_h > ENTRY_RETEST_MAX_AGE_HOURS:
        return False, "entry_retest_too_old"

    return True, "ok"


def candidate_age_bucket(age_h: int) -> str:
    """Compact age buckets for dead-candidate diagnostics."""
    if age_h < 0:
        return "unknown"
    if age_h < 24:
        return "<24h"
    if age_h < 48:
        return "24-48h"
    if age_h < 96:
        return "48-96h"
    if age_h < 168:
        return "96-168h"
    if age_h < 336:
        return "168-336h"
    return ">=336h"


def dead_candidate_matrix(records: List[CandidateDebug], limit: int = 8) -> List[Tuple[str, str, str, int]]:
    """Aggregate unique debug records by detector × reason × setup-age bucket."""
    from collections import Counter

    ctr = Counter(
        (
            _SETUP_ABBREV.get(r.setup_type, r.setup_type),
            _REASON_ABBREV.get(r.reason, r.reason),
            candidate_age_bucket(r.setup_age_h),
        )
        for r in records
        if r.status == "DEAD"
    )
    return [(*key, count) for key, count in ctr.most_common(limit)]


def candidate_debug_key(item: CandidateDebug) -> Tuple[str, str, str, int, str]:
    """
    Dedup key for a CandidateDebug record.
    Same symbol + detector + side + setup confirmation timestamp + rejection reason
    identifies the same diagnostic candidate across repeated scan cycles.
    """
    return (item.symbol, item.setup_type, item.side, item.setup_ts, item.reason)


def add_candidate_debug(mkt: Market, item: CandidateDebug) -> bool:
    """
    Append a CandidateDebug record with deduplication.

    If a record with the same key already exists:
      - update it in place with the newer snapshot
      - move it to the end so newest-first display works
      - return False (dedup hit; existing updated)

    If no matching record exists:
      - append new record
      - trim to CANDIDATE_DEBUG_MAX (oldest first removed)
      - return True (new record appended)
    """
    key = candidate_debug_key(item)
    for i, old in enumerate(mkt.candidate_debug):
        if candidate_debug_key(old) == key:
            mkt.candidate_debug[i] = item
            # Bubble to end so newest-first display sees it first
            mkt.candidate_debug.append(mkt.candidate_debug.pop(i))
            return False  # dedup: updated existing

    mkt.candidate_debug.append(item)
    excess = len(mkt.candidate_debug) - CANDIDATE_DEBUG_MAX
    if excess > 0:
        del mkt.candidate_debug[:excess]
    return True  # new record appended


def make_candidate_debug(
    sym: str,
    state: SymbolState,
    mkt: Market,
    ev: CandidateEval,
) -> Optional[CandidateDebug]:
    """
    Build a CandidateDebug snapshot from a CandidateEval.
    Returns None when ev.result is None (NONE-status candidates).
    """
    if ev.result is None:
        return None
    result = ev.result
    px = get_current_price(state)
    available_ts = setup_available_ts_ms(result)
    age_h = (
        int((now_ms() - available_ts) / 3_600_000)
        if available_ts > 0 else -1
    )
    return CandidateDebug(
        symbol        = sym,
        side          = result.side,
        setup_type    = result.setup_type,
        score         = result.score,
        status        = ev.status,
        reason        = ev.reason,
        current_price = px,
        entry_low     = result.entry_low,
        entry_high    = result.entry_high,
        stop_loss     = result.stop_loss,
        tp1           = result.tp1,
        tp2           = result.tp2,
        rr_tp1        = result.rr_tp1,
        rr_tp2        = result.rr_tp2,
        setup_ts      = result.setup_ts,
        setup_age_h   = age_h,
        updated_at    = now_s(),
        regime        = state.regime,
        btc_regime    = mkt.btc_regime,
        notes         = getattr(result, 'notes', '') or "",
    )

# ── Phase 8L: Signal-Eligible Watchlist gate ─────────────────────────────────

def worst_entry_zone_price_for_rr(side: str, entry_low: float, entry_high: float) -> float:
    """
    Return the worst acceptable entry-zone reference price for RR validation.

    LONG worst case:  entry_high (highest buy price → smaller reward, larger risk)
    SHORT worst case: entry_low  (lowest sell price → smaller reward, larger risk)

    Used only for pending setup eligibility.  It avoids listing a /watchlist
    candidate whose RR would fail as soon as price actually returns into the
    entry zone.
    """
    return entry_high if side == "LONG" else entry_low


def validate_signal_eligible_pending(
    sym: str,
    state: SymbolState,
    mkt: Market,
    result: SetupResult,
) -> Tuple[bool, SetupResult, str]:
    """
    Phase 8L gate for /watchlist quality.

    A PENDING setup is allowed into the watchlist only if it would pass RR and
    signal gates after price returns into the entry zone.  Because the current
    price is outside the zone at this point, RR is recalculated from the worst
    valid entry-zone price instead of the current outside-zone price.

    Returns:
      (True,  possibly_rr_adjusted_result, "outside_entry_zone") when eligible.
      (False, possibly_rr_adjusted_result, failure_reason) when it should be
      treated as DEAD (rr_current_fail or signal_gate_fail).
    """
    eligible_result = result

    if RR_FROM_CURRENT_PRICE:
        ref_price = worst_entry_zone_price_for_rr(
            result.side, result.entry_low, result.entry_high
        )
        rr1_zone, rr2_zone = calc_rr_from_current(
            result.side, ref_price, result.stop_loss, result.tp1, result.tp2
        )
        eligible_result = _dc_replace(result, rr_tp1=rr1_zone, rr_tp2=rr2_zone)

    if not passes_rr_gate(eligible_result, sym):
        return False, eligible_result, "rr_current_fail"

    fast_ok, fast_reason = validate_fast_4h_alignment(state, eligible_result)
    if not fast_ok:
        return False, eligible_result, fast_reason
    retry_ok, retry_reason = validate_post_sl_reentry(state, eligible_result)
    if not retry_ok:
        return False, eligible_result, retry_reason

    if not can_signal(sym, state, mkt, eligible_result):
        return False, eligible_result, "signal_gate_fail"

    return True, eligible_result, "outside_entry_zone"


# ── Phase 8F helpers ─────────────────────────────────────────────────────────

def collect_raw_setup_candidates(
    state: SymbolState,
    trace_out: Optional[List[DetectorStageTrace]] = None,
) -> List[SetupResult]:
    """
    Phase 8L.4.1: run all three detectors and return raw SetupResult outputs
    BEFORE score-floor filtering.  When ``trace_out`` is supplied, BR and TP
    populate diagnostics-only internal stage traces during these *same* calls.

    No detector is called twice and the returned SetupResult objects are the
    exact objects used by the trading pipeline.
    """
    raw: List[SetupResult] = []

    br_trace = DetectorStageTrace("BR") if trace_out is not None else None
    br = detect_breakout_retest(state, trace=br_trace)
    if br_trace is not None:
        trace_out.append(br_trace)
    if br is not None:
        raw.append(br)

    tp_trace = DetectorStageTrace("TP") if trace_out is not None else None
    tp = detect_trend_pullback(state, trace=tp_trace)
    if tp_trace is not None:
        trace_out.append(tp_trace)
    if tp is not None:
        raw.append(tp)

    ls = detect_liquidity_sweep(state)
    if ls is not None:
        raw.append(ls)
    return raw


def collect_setup_candidates(state: SymbolState) -> List[SetupResult]:
    """
    Preserve Phase 8L.3 trading behavior: raw detector outputs must meet the
    regime-specific score floor before entering the actionable pipeline.
    """
    floor = MIN_SCORE_CHOP if state.regime == "NEUTRAL" else MIN_SCORE_NORMAL
    return [r for r in collect_raw_setup_candidates(state) if r.score >= floor]

def choose_best_candidate(candidates: List[SetupResult]) -> Optional[SetupResult]:
    """
    Apply the original run_setup_pipeline priority logic to a pre-filtered list.

    Priority rules (preserved from run_setup_pipeline spec):
      1. BREAKOUT_RETEST beats TREND_PULLBACK by default.
      2. LIQUIDITY_SWEEP overrides the leader only when
         ls.score >= LIQUIDITY_SWEEP_PRIORITY_SCORE AND ls.score > leader.score.
      3. LIQUIDITY_SWEEP is selected as sole candidate when no BR/TP exists
         and ls.score >= MIN_SCORE_NORMAL.
    """
    if not candidates:
        return None

    br = next((c for c in candidates if c.setup_type == "BREAKOUT_RETEST"), None)
    tp = next((c for c in candidates if c.setup_type == "TREND_PULLBACK"),   None)
    ls = next((c for c in candidates if c.setup_type == "LIQUIDITY_SWEEP"),  None)

    winner = br or tp   # BR takes priority; fallback to TP

    if ls is not None:
        can_override = ls.score >= LIQUIDITY_SWEEP_PRIORITY_SCORE
        if can_override and (winner is None or ls.score > winner.score):
            winner = ls
        elif winner is None and ls.score >= MIN_SCORE_NORMAL:
            winner = ls

    return winner


def evaluate_candidate(
    sym: str,
    state: SymbolState,
    mkt: Market,
    raw_result: SetupResult,
) -> CandidateEval:
    """
    Evaluate a single detector result through the full gate sequence.

    Returns a CandidateEval describing whether this candidate is ACTIONABLE,
    PENDING (alive but price outside zone), or DEAD (failed an early gate).

    The score floor is NOT re-applied here; collect_setup_candidates() already
    filtered by floor.  This function applies structural and market gates only.
    """
    source = raw_result.setup_type

    # 1. TP/SL geometry via calc_swing_tpsl
    result = calc_swing_tpsl(raw_result, state)
    if result.rr_tp2 <= 0.0:
        return CandidateEval(result, "DEAD", "tpsl_fail", candidate_source=source)

    # 1b. Phase 8H: Liquidity Sweep recency gate (stricter than SETUP_CONTEXT_MAX_DAYS)
    #     Applied before TP/SL bar-scan so stale LS shows as ls_old, not hit_tp/hit_sl.
    if not is_liquidity_sweep_recent(result):
        return CandidateEval(
            result, "DEAD", "liquidity_sweep_too_old", candidate_source=source
        )

    # 2. Current price
    px = get_current_price(state)

    # 3. Actionable validation
    valid, reason = validate_actionable_setup(result, state, px)
    if not valid:
        if reason == "outside_entry_zone":
            # Phase 8L: do not put candidates into /watchlist if they would
            # be blocked by RR, score, symbol regime, or BTC regime after price
            # returns into the entry zone.
            pending_ok, pending_result, pending_reason = validate_signal_eligible_pending(
                sym, state, mkt, result
            )
            if not pending_ok:
                return CandidateEval(
                    pending_result, "DEAD", pending_reason,
                    rr_ok=(pending_reason != "rr_current_fail"),
                    signal_ok=False, candidate_source=source
                )
            return CandidateEval(
                pending_result, "PENDING", "outside_entry_zone",
                rr_ok=True, signal_ok=True, pending_ok=True, candidate_source=source
            )
        return CandidateEval(result, "DEAD", reason, candidate_source=source)

    # 3b. Phase 8K: older Liquidity Sweep candidates need a fresh entry-zone retest.
    retest_ok, retest_reason = validate_fresh_entry_retest(result, state, px)
    if not retest_ok:
        return CandidateEval(result, "DEAD", retest_reason, candidate_source=source)

    # 3c. Fast 4H direction and post-stop re-entry protection.
    fast_ok, fast_reason = validate_fast_4h_alignment(state, result)
    if not fast_ok:
        return CandidateEval(result, "DEAD", fast_reason, candidate_source=source)
    retry_ok, retry_reason = validate_post_sl_reentry(state, result)
    if not retry_ok:
        return CandidateEval(result, "DEAD", retry_reason, candidate_source=source)

    # 4. Update RR from current price
    if RR_FROM_CURRENT_PRICE and px > 0.0:
        rr1_px, rr2_px = calc_rr_from_current(
            result.side, px, result.stop_loss, result.tp1, result.tp2
        )
        result = _dc_replace(result, rr_tp1=rr1_px, rr_tp2=rr2_px)

    # 5. RR gate
    if not passes_rr_gate(result, sym):
        return CandidateEval(result, "DEAD", "rr_current_fail", rr_ok=False, candidate_source=source)

    # 6. Signal gate (score/regime/BTC)
    if not can_signal(sym, state, mkt, result):
        return CandidateEval(result, "DEAD", "signal_gate_fail", rr_ok=True, candidate_source=source)

    return CandidateEval(
        result, "ACTIONABLE", "ok",
        rr_ok=True, signal_ok=True, actionable_ok=True, candidate_source=source
    )


async def scan_symbol(
    sym: str,
    state: SymbolState,
    mkt: Market,
    app: web.Application,
) -> None:
    """
    Phase 5 / Phase 8C / Phase 8D / Phase 8E / Phase 8F.

    Phase 8F change (candidate-aware selection):
    Instead of picking one winner and then validating it, all three detectors
    are run in parallel; every candidate is evaluated independently through the
    full gate sequence.  The best ACTIONABLE candidate fires a signal; the best
    PENDING candidate (alive but price outside zone) updates the watchlist;
    dead/stale candidates are individually counted in diagnostics.

    This prevents a dead high-priority setup (e.g. BR already hit TP) from
    blocking a live lower-priority setup (e.g. LS still in zone).

    Phase 8L: PENDING candidates are kept in /watchlist only when they already
    pass RR + score/regime/BTC signal gates from the worst acceptable entry-zone
    price.  The watchlist should therefore represent setups waiting only for
    price to return to entry.

    Diagnostics note: reason counters (hit_tp, hit_sl, …) are candidate-level,
    so they may exceed symbols_checked when multiple candidates are evaluated.
    """
    d = mkt.diag_last   # last-cycle counters (reset each keepalive)
    t = mkt.diag_total  # cumulative since startup

    try:
        d.symbols_checked += 1
        t.symbols_checked += 1

        # ── Pre-flight: skip immediately if symbol is not ready ───────────────
        if not state.ready:
            d.symbols_not_ready += 1
            t.symbols_not_ready += 1
            return

        # ── Phase 8L.4: advance existing raw-setup outcomes first ─────────────
        store = _diag_store(app)
        if store is not None:
            try:
                store.update_outcomes_for_symbol(sym, state)
                store.update_br_shadow_outcomes_for_symbol(sym, state)
            except Exception as exc:
                _diag_warn(f"outcome update failed {sym}: {type(exc).__name__}: {exc}")

        # ── Phase 8L.4: collect RAW detector outputs before score filtering ───
        floor = MIN_SCORE_CHOP if state.regime == "NEUTRAL" else MIN_SCORE_NORMAL
        stage_traces: List[DetectorStageTrace] = []
        raw_candidates = collect_raw_setup_candidates(state, trace_out=stage_traces)
        diag_keys: Dict[str, str] = {}
        if store is not None:
            try:
                store.record_detector_scan(raw_candidates, floor)
                store.record_stage_traces(sym, stage_traces)
                for trace in stage_traces:
                    if trace.detector == "BR":
                        for shadow in trace.br_shadow_candidates:
                            store.upsert_br_shadow(sym, state, mkt.btc_regime, shadow)
                for raw in raw_candidates:
                    final_for_stats = calc_swing_tpsl(raw, state)
                    key = store.upsert_raw_setup(
                        sym, state, mkt.btc_regime, raw, final_for_stats, floor
                    )
                    diag_keys[diagnostic_setup_key(sym, raw)] = key
                    if raw.score < floor:
                        store.update_gate(key, "DEAD", "score_floor_fail")
            except Exception as exc:
                _diag_warn(f"raw setup write failed {sym}: {type(exc).__name__}: {exc}")

        # ── Pre-flight: symbol already has an active idea ─────────────────────
        if state.active_idea is not None:
            d.active_idea_lock += 1
            t.active_idea_lock += 1
            if store is not None:
                for raw in raw_candidates:
                    key = diag_keys.get(diagnostic_setup_key(sym, raw))
                    if key and raw.score >= floor:
                        try:
                            store.update_gate(key, "BLOCKED", "active_idea_lock")
                        except Exception as exc:
                            _diag_warn(f"gate write failed {sym}: {type(exc).__name__}: {exc}")
            clear_pending_setup(mkt, sym, "active_idea")
            return

        # Preserve Phase 8L.3 signal flow: only score-passing raw setups proceed.
        candidates = [r for r in raw_candidates if r.score >= floor]
        if not candidates:
            # Historical counter name kept for compatibility.  In 8L.4 the DB
            # separates true detector-none from raw setups that failed score.
            d.detector_none += 1
            t.detector_none += 1
            clear_pending_setup(mkt, sym, "detector_none")
            return

        # Phase 8L.3 rollback: every candidate that survives the normal
        # detector score floor may compete.  Do not impose the extra 8L.1
        # secondary-candidate score floor.
        # ── Evaluate each candidate independently ─────────────────────────────
        evals: List[CandidateEval] = [
            evaluate_candidate(sym, state, mkt, c) for c in candidates
        ]

        # Update diagnostics — candidate-level (may exceed symbols_checked)
        for raw, ev in zip(candidates, evals):
            if store is not None:
                key = diag_keys.get(diagnostic_setup_key(sym, raw))
                if key:
                    try:
                        store.update_gate(key, ev.status, ev.reason)
                    except Exception as exc:
                        _diag_warn(f"gate write failed {sym}: {type(exc).__name__}: {exc}")
            d.candidates_total += 1; t.candidates_total += 1
            if ev.status == "ACTIONABLE":
                d.candidates_actionable += 1; t.candidates_actionable += 1
                d.actionable_ok += 1;         t.actionable_ok += 1
            elif ev.status == "PENDING":
                d.candidates_pending += 1; t.candidates_pending += 1
                d.outside_entry_zone += 1;  t.outside_entry_zone += 1
            else:  # DEAD
                d.candidates_dead += 1; t.candidates_dead += 1
                _diag_actionable_fail(d, t, ev.reason)
                # Phase 8G/8I: record dead candidate; count dedup hits
                dbg = make_candidate_debug(sym, state, mkt, ev)
                if dbg is not None:
                    appended = add_candidate_debug(mkt, dbg)
                    if not appended:
                        d.candidate_debug_dedup += 1
                        t.candidate_debug_dedup += 1

        # ── Prefer ACTIONABLE candidate ───────────────────────────────────────
        actionable = [ev.result for ev in evals if ev.status == "ACTIONABLE" and ev.result]
        if actionable:
            # Phase 8L behavior: choose the best surviving actionable candidate.
            # No extra secondary score floor.
            result = choose_best_candidate(actionable)
            if result is None:
                clear_pending_setup(mkt, sym, "no_actionable_winner")
                return

            clear_pending_setup(mkt, sym, "signal_firing")
            px   = get_current_price(state)
            now  = now_s()
            side = result.side
            idea = ActiveIdea(
                symbol                  = sym,
                side                    = side,
                setup_type              = result.setup_type,
                setup_score             = result.score,
                entry_low               = result.entry_low,
                entry_high              = result.entry_high,
                stop_loss               = result.stop_loss,
                tp1                     = result.tp1,
                tp2                     = result.tp2,
                rr_tp1                  = result.rr_tp1,
                rr_tp2                  = result.rr_tp2,
                status                  = "ACTIVE",
                emitted_at              = now,
                expires_at              = now + MAX_IDEA_DURATION_DAYS * 86400,
                invalidation            = result.invalidation,
                current_price_at_signal = px,
                setup_ts                = result.setup_ts,
                setup_tf                = result.setup_tf,
            )
            state.active_idea    = idea
            state.last_signal_ts = now
            mkt.signal_stats["total"] += 1
            if side == "LONG":
                mkt.signal_stats["long"]  += 1
            else:
                mkt.signal_stats["short"] += 1
            d.new_idea += 1
            t.new_idea += 1
            if store is not None:
                key = diag_keys.get(diagnostic_setup_key(sym, result))
                if key:
                    try:
                        store.mark_signal_emitted(key)
                    except Exception as exc:
                        _diag_warn(f"signal marker failed {sym}: {type(exc).__name__}: {exc}")
            logger.info(
                f"NEW IDEA {sym} {side} {result.setup_type} score={result.score} "
                f"entry={result.entry_low:.4f}–{result.entry_high:.4f} "
                f"sl={result.stop_loss:.4f} "
                f"tp1={result.tp1:.4f}(RR{result.rr_tp1:.2f}) "
                f"tp2={result.tp2:.4f}(RR{result.rr_tp2:.2f})"
            )
            try:
                await send_signal(app, idea, state)
            except Exception as e:
                logger.warning(f"send_signal failed {sym}: {e}")
                await report_error(app, f"send_signal/{sym}", e)

            # Phase 9B is downstream of the existing signal engine. It cannot
            # create or block an ActiveIdea and never calls a Bybit write endpoint.
            if EXECUTION_PLANNER_ENABLED and EXECUTION_PLANNER_AUTO_SEND:
                try:
                    await send_execution_plan(
                        app, idea, state, reserve_shadow_margin=True
                    )
                except Exception as e:
                    logger.warning(f"Phase 9B auto-plan failed {sym}: {e}")
                    await report_error(app, f"execution_plan/{sym}", e)
            return

        # ── No actionable — check for signal-eligible pending (Phase 8L) ──────
        pending = [ev.result for ev in evals if ev.status == "PENDING" and ev.result]
        if pending:
            best_pending = choose_best_candidate(pending)
            if best_pending is not None:
                px = get_current_price(state)
                mkt.pending_setups[sym] = make_pending_setup(
                    sym, state, mkt, best_pending, px, "outside_entry_zone"
                )
                logger.debug(
                    f"pending_setup updated {sym} "
                    f"{mkt.pending_setups[sym].distance_pct:.2f}% "
                    f"{mkt.pending_setups[sym].distance_side}"
                )
            return

        # ── All candidates dead — clear any stale pending ─────────────────────
        clear_pending_setup(mkt, sym, "no_alive_candidates")

    except Exception as exc:
        d.errors += 1
        t.errors += 1
        raise


# =============================================================================
# === 11. PRELOAD ===
# =============================================================================

async def preload_symbol(
    rest: BybitRest,
    mkt: Market,
    sym: str,
    sem: asyncio.Semaphore,
) -> bool:
    """
    Fetch all 5 timeframes for a symbol and compute initial indicators + regime.
    Returns True if at least one TF was successfully fetched, False otherwise.
    """
    async with sem:
        state   = mkt.state[sym]
        fetched: List[str] = []

        for tf_key, bybit_interval in TF_MAP.items():
            limit = TF_LIMITS[tf_key]
            try:
                bars = await rest.klines(sym, bybit_interval, limit=limit)
                if bars:
                    setattr(state, f"bars_{tf_key}", bars)
                    state.last_fetch[tf_key] = now_s()
                    fetched.append(tf_key)
            except Exception as e:
                logger.warning(f"Preload {sym}/{tf_key}: {e}")

        if fetched:
            update_indicators(state, fetched)
            result              = compute_regime(state)
            state.regime        = result.regime
            state.regime_reason = result.reason

        return bool(fetched)


async def preload_all(rest: BybitRest, mkt: Market) -> int:
    """
    Preload all 5 TFs for every universe symbol, concurrently.
    Returns the number of symbols that reached ready=True after preload.
    """
    sem   = asyncio.Semaphore(POLL_WORKERS)
    tasks = [preload_symbol(rest, mkt, sym, sem) for sym in mkt.symbols]
    logger.info(f"Preloading {len(tasks)} symbols across 5 TFs (workers={POLL_WORKERS})…")
    await asyncio.gather(*tasks)
    ready_count = sum(1 for s in mkt.symbols if mkt.state[s].ready)
    logger.info(f"Preload complete: {ready_count}/{len(mkt.symbols)} symbols ready")
    return ready_count


# =============================================================================
# === 12. UNIVERSE VALIDATOR ===
# =============================================================================

async def validate_universe(rest: BybitRest) -> List[str]:
    """
    Confirm that each UNIVERSE symbol is listed and active on Bybit linear.
    Logs a warning for any missing symbol but does not block startup.
    Returns the filtered list of valid symbols (maintains UNIVERSE order).
    """
    try:
        tickers = await rest.tickers_linear()
        active  = {t["symbol"] for t in tickers}
        valid:   List[str] = []
        for sym in UNIVERSE:
            if sym in active:
                valid.append(sym)
            else:
                logger.warning(f"Universe: {sym} not found on Bybit linear — excluded")
        logger.info(f"Universe validated: {len(valid)}/{len(UNIVERSE)} symbols active")
        return valid
    except Exception as e:
        logger.error(f"validate_universe failed ({e}); using full UNIVERSE list as fallback")
        return list(UNIVERSE)


# =============================================================================
# === 13. POLLING LOOP ===
# =============================================================================

async def poll_symbol(
    sym: str,
    rest: BybitRest,
    mkt: Market,
    app: web.Application,
    sem: asyncio.Semaphore,
) -> None:
    """
    Refresh stale TFs for one symbol, update indicators and regime,
    then run Phase 5 lifecycle check and Phase 5 scan pipeline.

    A TF is considered stale when now - last_fetch[tf] >= TF_INTERVALS[tf].
    Bar stores are fully replaced on each refresh (no incremental append).
    If no TF is stale this cycle, the function returns immediately.

    Phase 5 lifecycle (active):
      check_idea_lifecycle() — closes/updates any active idea based on latest bar.
      Called before scan so a newly closed idea allows a fresh setup in the same cycle.

    Phase 5 scan pipeline (active):
      scan_symbol() — detector → TP/SL engine → gate → ActiveIdea creation → logger.info.
      Phase 6 Telegram signal dispatch is active via scan_symbol().
    """
    async with sem:
        state   = mkt.state[sym]
        t       = now_s()
        updated: List[str] = []

        for tf_key, interval_sec in TF_INTERVALS.items():
            last = state.last_fetch.get(tf_key, 0)
            if t - last < interval_sec:
                continue   # not yet due
            try:
                bars = await rest.klines(sym, TF_MAP[tf_key], limit=TF_LIMITS[tf_key])
                if bars:
                    setattr(state, f"bars_{tf_key}", bars)
                    state.last_fetch[tf_key] = t
                    updated.append(tf_key)
            except Exception as e:
                logger.warning(f"Poll {sym}/{tf_key}: {e}")
                await report_error(app, f"poll_symbol/{sym}/{tf_key}", e)

        if not updated:
            return   # nothing new this cycle

        update_indicators(state, updated)

        result              = compute_regime(state)
        state.regime        = result.regime
        state.regime_reason = result.reason
        state.last_scanned_ts = t

        # ── Phase 5: lifecycle check (before scan — closed idea allows new setup) ──
        try:
            await check_idea_lifecycle(sym, state, app)
        except Exception as e:
            logger.warning(f"check_idea_lifecycle failed {sym}: {e}")
            await report_error(app, f"check_idea_lifecycle/{sym}", e)

        # ── Phase 9C: execution-shadow post-expiry safety lifecycle ────────────
        try:
            await check_execution_shadow_wait_exit(sym, state, app)
        except Exception as e:
            logger.warning(f"check_execution_shadow_wait_exit failed {sym}: {e}")
            await report_error(app, f"execution_shadow_wait/{sym}", e)

        # ── Phase 5: scan pipeline — ActiveIdea creation + Phase 6 Telegram dispatch ──
        try:
            await scan_symbol(sym, state, mkt, app)
        except Exception as e:
            logger.warning(f"scan_symbol failed {sym}: {e}")
            await report_error(app, f"scan_symbol/{sym}", e)


async def poll_loop(app: web.Application) -> None:
    """
    Master polling loop. Wakes every POLL_INTERVAL_SEC seconds.
    Polls all symbols concurrently (semaphore-capped to POLL_WORKERS).
    Updates Market.last_poll_ts and Market.poll_count after each cycle.
    Market.last_poll_ts is watched by the watchdog; must stay fresh.
    """
    rest: BybitRest = app["rest"]
    mkt:  Market    = app["mkt"]
    sem   = asyncio.Semaphore(POLL_WORKERS)

    while True:
        try:
            await asyncio.gather(*[
                poll_symbol(sym, rest, mkt, app, sem)
                for sym in mkt.symbols
            ])
            # Update BTC global regime from BTCUSDT symbol state
            if "BTCUSDT" in mkt.state:
                mkt.btc_regime        = mkt.state["BTCUSDT"].regime
                mkt.btc_regime_reason = mkt.state["BTCUSDT"].regime_reason
            mkt.last_poll_ts = now_s()
            mkt.poll_count  += 1
        except Exception as e:
            logger.exception("poll_loop top-level error")
            await report_error(app, "poll_loop", e)

        await asyncio.sleep(POLL_INTERVAL_SEC)


# =============================================================================
# === 14. IDEA LIFECYCLE  (Phase 5) ===
# =============================================================================

def _lifecycle_bars_after_emission(state: SymbolState, idea: ActiveIdea) -> List[Bar]:
    """
    Return chronological monitoring bars that started strictly after emission.

    Ignoring the candle containing the signal prevents a pre-signal high/low
    from being counted as a later TP or SL.  1H is preferred to reduce OHLC
    ordering ambiguity; 4H/1D are defensive fallbacks.
    """
    bars = state.bars_1h or state.bars_4h or state.bars_1d
    emitted_ms = idea.emitted_at * 1000
    return [bar for bar in bars if bar[B_TS] > emitted_ms]


def _idea_bar_hits(idea: ActiveIdea, bar: Bar) -> Tuple[bool, bool, bool]:
    """Return (sl_hit, tp1_hit, tp2_hit) for one bar."""
    if idea.side == "LONG":
        return (
            bar[B_LOW] <= idea.stop_loss,
            bar[B_HIGH] >= idea.tp1,
            bar[B_HIGH] >= idea.tp2,
        )
    return (
        bar[B_HIGH] >= idea.stop_loss,
        bar[B_LOW] <= idea.tp1,
        bar[B_LOW] <= idea.tp2,
    )


def _record_sl_memory(state: SymbolState, idea: ActiveIdea, exit_ts: int) -> None:
    """Update cooldown/streak state after an actual stop-loss event."""
    if state.last_stopped_side == idea.side:
        state.consecutive_sl_same_side += 1
    else:
        state.consecutive_sl_same_side = 1

    state.last_exit_ts = exit_ts
    state.last_exit_event = "SL_HIT"
    state.last_stopped_side = idea.side
    state.stopped_setup_keys.add((idea.setup_type, idea.setup_ts))
    if len(state.stopped_setup_keys) > 200:
        state.stopped_setup_keys = {(idea.setup_type, idea.setup_ts)}

    lock_h = (
        REPEATED_SL_LOCK_HOURS
        if state.consecutive_sl_same_side >= MAX_CONSECUTIVE_SL_SAME_SIDE
        else POST_SL_COOLDOWN_HOURS
    )
    state.post_sl_lock_until = exit_ts + lock_h * 3600


def _reset_sl_streak_after_success(state: SymbolState) -> None:
    state.consecutive_sl_same_side = 0
    state.last_stopped_side = ""
    state.post_sl_lock_until = 0


async def check_idea_lifecycle(
    sym: str,
    state: SymbolState,
    app: web.Application,
) -> None:
    """
    Temporal-safe TP/SL lifecycle.

    Only bars that begin after the idea was emitted are eligible.  Bars are
    processed chronologically on 1H where possible.  If an unobserved bar hits
    both profit and stop levels, OHLC cannot reveal the order, so the idea is
    closed as AMBIGUOUS and is excluded from TP/SL performance counts.
    """
    idea = state.active_idea
    if idea is None:
        return

    now = now_s()
    mkt: Market = app["mkt"]
    bars = _lifecycle_bars_after_emission(state, idea)

    for bar in bars:
        sl_hit, tp1_hit, tp2_hit = _idea_bar_hits(idea, bar)

        # If both sides of the trade were first observed inside one OHLC bar,
        # order is unknowable.  Do not force the result into the SL bucket.
        ambiguous = sl_hit and (tp2_hit or (idea.status == "ACTIVE" and tp1_hit))
        if ambiguous:
            idea.status = "AMBIGUOUS"
            state.active_idea = None
            state.last_exit_ts = now
            state.last_exit_event = "AMBIGUOUS"
            mkt.signal_stats["ambiguous"] += 1
            logger.info(
                f"IDEA AMBIGUOUS {sym} {idea.side} {idea.setup_type} "
                f"bar_ts={bar[B_TS]} (TP/SL order unknown)"
            )
            try:
                await send_idea_update(app, idea, "AMBIGUOUS")
            except Exception as e:
                logger.warning(f"send_idea_update AMBIGUOUS failed {sym}: {e}")
                await report_error(app, f"send_idea_update/{sym}/AMBIGUOUS", e)
            return

        if sl_hit and idea.status in ("ACTIVE", "TP1_HIT"):
            idea.status = "SL_HIT"
            state.active_idea = None
            mkt.signal_stats["sl_hit"] += 1
            _record_sl_memory(state, idea, now)
            logger.info(
                f"IDEA SL_HIT {sym} {idea.side} {idea.setup_type} "
                f"sl={idea.stop_loss:.4f}"
            )
            try:
                await send_idea_update(app, idea, "SL_HIT")
            except Exception as e:
                logger.warning(f"send_idea_update SL_HIT failed {sym}: {e}")
                await report_error(app, f"send_idea_update/{sym}/SL_HIT", e)
            return

        if tp2_hit and idea.status in ("ACTIVE", "TP1_HIT"):
            idea.status = "TP2_HIT"
            state.active_idea = None
            state.last_exit_ts = now
            state.last_exit_event = "TP2_HIT"
            _reset_sl_streak_after_success(state)
            mkt.signal_stats["tp2_hit"] += 1
            logger.info(
                f"IDEA TP2_HIT {sym} {idea.side} {idea.setup_type} "
                f"tp2={idea.tp2:.4f}"
            )
            try:
                await send_idea_update(app, idea, "TP2_HIT")
            except Exception as e:
                logger.warning(f"send_idea_update TP2_HIT failed {sym}: {e}")
                await report_error(app, f"send_idea_update/{sym}/TP2_HIT", e)
            return

        if tp1_hit and idea.status == "ACTIVE":
            idea.status = "TP1_HIT"
            idea.tp1_hit_at = now
            mkt.signal_stats["tp1_hit"] += 1
            logger.info(
                f"IDEA TP1_HIT {sym} {idea.side} {idea.setup_type} "
                f"tp1={idea.tp1:.4f}"
            )
            try:
                await send_idea_update(app, idea, "TP1_HIT")
            except Exception as e:
                logger.warning(f"send_idea_update TP1_HIT failed {sym}: {e}")
                await report_error(app, f"send_idea_update/{sym}/TP1_HIT", e)
            # Continue: later chronological bars in this same poll may hit TP2/SL.

    if now >= idea.expires_at and idea.status in ("ACTIVE", "TP1_HIT"):
        idea.status = "EXPIRED"
        state.active_idea = None
        state.last_exit_ts = now
        state.last_exit_event = "EXPIRED"
        mkt.signal_stats["expired"] += 1
        logger.info(f"IDEA EXPIRED {sym} {idea.side} {idea.setup_type}")
        try:
            await send_idea_update(app, idea, "EXPIRED")
        except Exception as e:
            logger.warning(f"send_idea_update EXPIRED failed {sym}: {e}")
            await report_error(app, f"send_idea_update/{sym}/EXPIRED", e)


# =============================================================================
# === 15. SIGNAL FORMATTING  (Phase 6) ===
# =============================================================================

_SETUP_LABELS: Dict[str, str] = {
    "BREAKOUT_RETEST": "Breakout + Retest",
    "TREND_PULLBACK":  "Trend Pullback",
    "LIQUIDITY_SWEEP": "Liquidity Sweep",
}


def calc_pct_from_entry(side: str, entry_price: float, target_price: float) -> float:
    """
    Percentage move from one entry price to a target price.

    Positive value = profit direction for the idea.
    Negative value = adverse direction / risk.

    LONG:  (target - entry) / entry × 100
    SHORT: (entry - target) / entry × 100
    """
    if entry_price <= 0.0:
        return 0.0
    if side == "SHORT":
        return (entry_price - target_price) / entry_price * 100.0
    return (target_price - entry_price) / entry_price * 100.0


def format_pct_from_entry_zone(
    side: str,
    entry_low: float,
    entry_high: float,
    target_price: float,
) -> str:
    """
    Format the percentage move from the whole entry zone to a target price.

    For entry ranges the output is also a range, sorted by absolute distance:
      +2.10%–+2.75%
      -1.20%–-1.60%

    If the rounded values are equal, returns a single value: +2.10%.
    """
    entries = [p for p in (entry_low, entry_high) if p > 0.0]
    if not entries:
        return "+0.00%"

    values = [calc_pct_from_entry(side, p, target_price) for p in entries]
    values = sorted(values, key=lambda x: (abs(x), x))
    rendered = [f"{v:+.2f}%" for v in values]

    if len(rendered) == 1 or rendered[0] == rendered[-1]:
        return rendered[0]
    return f"{rendered[0]}–{rendered[-1]}"


def format_level_pct(
    side: str,
    entry_low: float,
    entry_high: float,
    target_price: float,
) -> str:
    """Return parenthesised percentage text for TP/SL display."""
    return f"({format_pct_from_entry_zone(side, entry_low, entry_high, target_price)})"


def format_tp_with_rr(
    side: str,
    entry_low: float,
    entry_high: float,
    tp: float,
    rr: float,
) -> str:
    """Return combined percentage + RR text for take-profit display."""
    pct = format_pct_from_entry_zone(side, entry_low, entry_high, tp)
    return f"({pct} | RR {rr:.2f})"


def _selftest_pct_format_helpers() -> None:
    """Tiny deterministic self-test for TP/SL percentage formatting."""
    # LONG: entry 100–102, TP 110, SL 95
    assert format_pct_from_entry_zone("LONG", 100.0, 102.0, 110.0) == "+7.84%–+10.00%"
    assert format_pct_from_entry_zone("LONG", 100.0, 102.0, 95.0) == "-5.00%–-6.86%"

    # SHORT: entry 100–102, TP 90, SL 105
    assert format_pct_from_entry_zone("SHORT", 100.0, 102.0, 90.0) == "+10.00%–+11.76%"
    assert format_pct_from_entry_zone("SHORT", 100.0, 102.0, 105.0) == "-2.94%–-5.00%"



def _selftest_entry_retest_helpers() -> None:
    """Tiny deterministic self-test for Phase 8K entry-zone return timing."""
    now = now_ms()
    hour = 3_600_000
    result = SetupResult(
        setup_type="LIQUIDITY_SWEEP",
        side="LONG",
        score=80,
        entry_low=100.0,
        entry_high=102.0,
        stop_loss=95.0,
        tp1=106.0,
        tp2=110.0,
        rr_tp1=1.2,
        rr_tp2=2.0,
        invalidation="test",
        setup_ts=now - 120 * hour,
    )
    state = SymbolState(
        bars_1h=[
            (now - 60 * hour, 0, 99.0, 98.0, 98.5, 1.0),
            (now - 40 * hour, 0, 103.0, 101.0, 101.5, 1.0),
            (now - 1 * hour, 0, 102.5, 100.5, 101.0, 1.0),
        ]
    )
    assert latest_entry_zone_return_ts(result, state) == now - 40 * hour



def _selftest_pending_signal_eligible_watchlist() -> None:
    """Tiny deterministic self-test for Phase 8L pending watchlist gating."""
    state = SymbolState(
        bars_4h=[
            (now_ms() - 8 * 3_600_000, 100.0, 101.0, 97.0, 98.0, 1.0),
            (now_ms() - 4 * 3_600_000, 98.0, 99.0, 95.0, 96.0, 1.0),
            (now_ms(), 96.0, 97.0, 94.0, 95.0, 1.0),
        ],
        ema20_4h=97.0,
        ema50_4h=99.0,
    )
    state.ready = True
    state.regime = "BEARISH"
    mkt = Market(symbols=["SOLUSDT"], state={"SOLUSDT": state})
    mkt.btc_regime = "BEARISH"

    long_bad = SetupResult(
        setup_type="BREAKOUT_RETEST", side="LONG", score=65,
        entry_low=100.0, entry_high=102.0, stop_loss=95.0,
        tp1=112.0, tp2=120.0, rr_tp1=0.0, rr_tp2=0.0,
        invalidation="test", setup_ts=now_ms(),
    )
    ok, checked, reason = validate_signal_eligible_pending(
        "SOLUSDT", state, mkt, long_bad
    )
    assert not ok and reason in ("fast_4h_conflict", "signal_gate_fail")
    assert checked.rr_tp2 >= RR_MIN_TIER2

    short_ok = SetupResult(
        setup_type="LIQUIDITY_SWEEP", side="SHORT", score=65,
        entry_low=100.0, entry_high=102.0, stop_loss=105.0,
        tp1=94.0, tp2=90.0, rr_tp1=0.0, rr_tp2=0.0,
        invalidation="test", setup_ts=now_ms(),
    )
    ok, checked, reason = validate_signal_eligible_pending(
        "SOLUSDT", state, mkt, short_ok
    )
    assert ok and reason == "outside_entry_zone"
    assert checked.rr_tp2 >= RR_MIN_TIER2


def format_signal(idea: ActiveIdea, state: SymbolState) -> str:
    """
    Render a new-idea Telegram message in HTML.

    Includes: side, symbol, setup label, score, regime, current price (Phase 8A),
    entry zone + IN ENTRY ZONE status, SL, TP1, TP2, RR, setup age, expiry,
    invalidation condition (renamed from "❌ Invalidation" to "🛑 Idea invalid if"),
    and risk disclaimer.
    When DRY_RUN_MODE=True: prefixed with 🧪 DRY RUN banner, adjusted disclaimer.
    No fake probability, no leverage, no position size.
    """
    side_emoji  = "🟢" if idea.side == "LONG" else "🔴"
    side_label  = "LONG IDEA"  if idea.side == "LONG" else "SHORT IDEA"
    sym_pretty  = idea.symbol.replace("USDT", "/USDT")
    setup_label = _SETUP_LABELS.get(idea.setup_type, idea.setup_type.replace("_", " "))
    regime_e    = _regime_emoji(state.regime)
    dry_banner  = "🧪 <b>DRY RUN</b>\n" if DRY_RUN_MODE else ""
    disclaimer  = ("Dry-run signal. Not financial advice. Manage risk."
                   if DRY_RUN_MODE else
                   "Not financial advice. Manage risk.")

    # Current price and entry zone status (Phase 8A)
    if idea.current_price_at_signal > 0.0:
        price_line  = (f"💵 <b>Current price:</b>  "
                       f"<code>{idea.current_price_at_signal:.5f}</code>\n")
        status_line = "✅ <b>Entry status:</b> IN ENTRY ZONE\n"
    else:
        price_line  = ""
        status_line = ""

    # Setup age
    if idea.setup_ts > 0:
        available_ts = confirmation_available_ts_ms(idea.setup_ts, idea.setup_tf)
        age_h       = max(0, (now_ms() - available_ts) // 3_600_000)
        age_line    = f"⏱ Setup age: {age_h}h  |  Expires in: {MAX_IDEA_DURATION_DAYS} days\n"
    else:
        age_line    = f"⏱ Expires in: {MAX_IDEA_DURATION_DAYS} days\n"

    return (
        f"{dry_banner}"
        f"{side_emoji} <b>{side_label} — {sym_pretty}</b>\n"
        f"<b>{html.escape(setup_label)}</b>  |  Score: {idea.setup_score}/100\n\n"
        f"📊 Regime: {regime_e} {state.regime}\n\n"
        f"{price_line}"
        f"📍 <b>Entry zone:</b>  "
        f"<code>{idea.entry_low:.5f} – {idea.entry_high:.5f}</code>\n"
        f"{status_line}"
        f"🛡 <b>Stop Loss:</b>   <code>{idea.stop_loss:.5f}</code>  "
        f"<i>{format_level_pct(idea.side, idea.entry_low, idea.entry_high, idea.stop_loss)}</i>\n\n"
        f"🎯 <b>TP1:</b>  <code>{idea.tp1:.5f}</code>  "
        f"<i>{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp1, idea.rr_tp1)}</i>\n"
        f"🎯 <b>TP2:</b>  <code>{idea.tp2:.5f}</code>  "
        f"<i>{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp2, idea.rr_tp2)}</i>\n\n"
        f"{age_line}"
        f"🛑 <b>Idea invalid if:</b> {html.escape(idea.invalidation)}\n\n"
        f"<i>{disclaimer}</i>"
    )


def format_idea_update(idea: ActiveIdea, event: str) -> str:
    """
    Render a lifecycle-event Telegram message in HTML.

    Events: TP1_HIT | TP2_HIT | SL_HIT | EXPIRED | INVALIDATED
    When DRY_RUN_MODE=True, each message is prefixed with 🧪 DRY RUN.
    """
    sym_pretty = idea.symbol.replace("USDT", "/USDT")
    side_e     = "🟢" if idea.side == "LONG" else "🔴"
    header     = f"{side_e} <b>{sym_pretty} {idea.side}</b>"
    dry_prefix = "🧪 <b>DRY RUN</b>\n" if DRY_RUN_MODE else ""

    if event == "TP1_HIT":
        return (
            f"{dry_prefix}🟡 <b>TP1 HIT</b> — {header}\n\n"
            f"<b>TP1:</b> <code>{idea.tp1:.5f}</code>  "
            f"<i>{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp1, idea.rr_tp1)}</i>\n\n"
            f"Idea remains active toward TP2."
        )
    if event == "TP2_HIT":
        return (
            f"{dry_prefix}✅ <b>TP2 HIT — Idea completed!</b>\n{header}\n\n"
            f"<b>TP2:</b> <code>{idea.tp2:.5f}</code>  "
            f"<i>{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp2, idea.rr_tp2)}</i>"
        )
    if event == "SL_HIT":
        return (
            f"{dry_prefix}❌ <b>STOP LOSS HIT</b>\n{header}\n\n"
            f"<b>SL:</b> <code>{idea.stop_loss:.5f}</code>  "
            f"<i>{format_level_pct(idea.side, idea.entry_low, idea.entry_high, idea.stop_loss)}</i>"
        )
    if event == "EXPIRED":
        return (
            f"{dry_prefix}⏳ <b>IDEA EXPIRED</b>\n{header}\n\n"
            f"Max duration of {MAX_IDEA_DURATION_DAYS} days reached."
        )
    if event == "INVALIDATED":
        return f"{dry_prefix}🚫 <b>IDEA CLOSED / INVALIDATED</b>\n{header}"
    if event == "AMBIGUOUS":
        return (
            f"{dry_prefix}⚖️ <b>AMBIGUOUS TP/SL ORDER</b>\n{header}\n\n"
            f"Both sides were touched inside one OHLC bar. "
            f"The result is excluded from TP/SL statistics."
        )
    # Fallback for unexpected events
    return f"{dry_prefix}ℹ️ Idea update: {html.escape(event)}\n{header}"


async def send_signal(
    app: web.Application, idea: ActiveIdea, state: SymbolState
) -> None:
    """
    Send the formatted new-idea message via get_broadcast_targets().
    Returns silently when no targets are configured or tg is unavailable.
    """
    tg: Optional[Tg] = app.get("tg")
    if tg is None:
        return
    targets = get_broadcast_targets()
    if not targets:
        return
    text = format_signal(idea, state)
    for cid in targets:
        ok = await tg.send(cid, text)
        if ok:
            logger.info(
                f"send_signal OK → {cid}  "
                f"({idea.symbol} {idea.side} {idea.setup_type})"
            )
        else:
            logger.warning(
                f"send_signal FAIL → {cid}  "
                f"({idea.symbol} {idea.side})"
            )


async def send_idea_update(
    app: web.Application,
    idea: ActiveIdea,
    event: str,   # TP1_HIT | TP2_HIT | SL_HIT | EXPIRED | INVALIDATED | AMBIGUOUS
) -> None:
    """
    Send a lifecycle-event message via get_broadcast_targets().
    Returns silently when no targets are configured or tg is unavailable.

    Phase 9B also mirrors future 50/50 margin release in an in-memory
    shadow ledger. This does not touch Bybit or alter strategy lifecycle.
    """
    _update_execution_shadow_reservation(app, idea, event)
    expiry_shadow_message: Optional[str] = None
    if event == "EXPIRED":
        with contextlib.suppress(Exception):
            expiry_shadow_message = await _handle_execution_shadow_expiry(app, idea)

    tg: Optional[Tg] = app.get("tg")
    if tg is None:
        return
    targets = get_broadcast_targets()
    if not targets:
        return
    text = format_idea_update(idea, event)
    for cid in targets:
        ok = await tg.send(cid, text)
        if ok:
            logger.info(
                f"send_idea_update {event} OK → {cid}  ({idea.symbol})"
            )
        else:
            logger.warning(
                f"send_idea_update {event} FAIL → {cid}  ({idea.symbol})"
            )
        if expiry_shadow_message:
            with contextlib.suppress(Exception):
                await tg.send(cid, expiry_shadow_message)


# =============================================================================
# === 16. TELEGRAM LOOP ===
# =============================================================================

def _regime_emoji(regime: str) -> str:
    return {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}.get(regime, "⚪")


async def tg_loop(app: web.Application) -> None:
    """Telegram long-polling command dispatcher."""
    tg: Tg = app["tg"]
    offset: Optional[int] = None

    while True:
        try:
            updates = await tg.get_updates(offset=offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                msg    = upd.get("message") or upd.get("channel_post")
                if not msg or "text" not in msg:
                    continue
                text = msg["text"].strip()
                cid  = msg["chat"]["id"]

                if text == "/ping":
                    await tg.send(cid, "🏓 pong")
                elif text == "/status":
                    await _cmd_status(app, cid)
                elif text == "/regime":
                    await _cmd_regime(app, cid)
                elif text == "/ideas":
                    await _cmd_ideas(app, cid)
                elif text.startswith("/idea "):
                    sym = text.split(maxsplit=1)[1].upper().strip()
                    await _cmd_idea_detail(app, cid, sym)
                elif text.startswith("/close "):
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        sym = text.split(maxsplit=1)[1].upper().strip()
                        await _cmd_close(app, cid, sym)
                elif text.startswith("/score "):
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        sym = text.split(maxsplit=1)[1].upper().strip()
                        await _cmd_score(app, cid, sym)
                elif text == "/config":
                    await _cmd_config(app, cid)
                elif text == "/diag":
                    await _cmd_diag(app, cid)
                elif text == "/apikey":
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        await _cmd_apikey(app, cid)
                elif text in ("/bybit", "/account"):
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        await _cmd_bybit(app, cid)
                elif text.startswith("/plan"):
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        parts = text.split(maxsplit=1)
                        sym = parts[1].upper().strip() if len(parts) > 1 else ""
                        await _cmd_plan(app, cid, sym)
                elif text in ("/execshadow", "/shadowexec"):
                    if cid not in ALLOWED_CHAT_IDS:
                        await tg.send(cid, "⛔ Unauthorized.")
                    else:
                        await _cmd_execshadow(app, cid)
                elif text in ("/statsdb", "/rawstats"):
                    await _cmd_statsdb(app, cid)
                elif text in ("/brtp", "/detstats"):
                    await _cmd_brtp(app, cid)
                elif text in ("/brshadow", "/shadowbr"):
                    await _cmd_brshadow(app, cid)
                elif text in ("/tpdiag", "/tpdb"):
                    await _cmd_tpdiag(app, cid)
                elif text in ("/calibration", "/review8m", "/8m"):
                    await _cmd_calibration(app, cid)
                elif text in ("/watchlist", "/pending"):
                    await _cmd_watchlist(app, cid)
                elif text in ("/candidates", "/dead"):
                    await _cmd_candidates(app, cid)
        except Exception as exc:
            logger.warning(f"tg_loop exception {type(exc).__name__}: {exc}")
            await report_error(app, "tg_loop", exc)
            await asyncio.sleep(5)


# ── Command handlers ──────────────────────────────────────────────────────────

async def _cmd_apikey(app: web.Application, cid: int) -> None:
    """Show Bybit API-key validity without exposing the key itself."""
    tg: Tg = app["tg"]
    client = app.get("bybit_private")
    if not isinstance(client, BybitPrivateReadOnly):
        await tg.send(
            cid,
            "⚠️ <b>Bybit RSA bridge is not configured.</b>\n"
            "Check BYBIT_API_KEY and the private-key secret file.",
        )
        return
    try:
        info = await client.api_key_info()
        expiry = bybit_api_expiry(info)
    except Exception as exc:
        await tg.send(
            cid,
            "❌ <b>Bybit API check failed.</b>\n"
            f"<code>{html.escape(str(exc)[:500])}</code>",
        )
        return

    status = expiry["status"]
    if status == "ACTIVE":
        status_text = "✅ ACTIVE"
    elif status == "ACTIVE_IP_BOUND":
        status_text = "✅ ACTIVE · IP-bound"
    elif status == "EXPIRED":
        status_text = "❌ EXPIRED"
    else:
        status_text = "⚠️ UNKNOWN"

    days = expiry.get("days_left")
    days_text = f"{days} days" if days is not None else "not reported"
    expiry_text = expiry.get("expired_at") or (
        "no finite expiry reported" if expiry.get("ips_bound") else "not reported"
    )
    permissions = info.get("permissions") or {}
    contract = permissions.get("ContractTrade") or []
    perm_text = ", ".join(str(x) for x in contract) or "none"
    key_mode = "Read-only" if int(info.get("readOnly", 0) or 0) == 1 else "Read-write"

    await tg.send(cid, (
        "🔐 <b>Bybit API Key</b>\n\n"
        f"<b>Status:</b> {status_text}\n"
        f"<b>Remaining:</b> {html.escape(days_text)}\n"
        f"<b>Expiration:</b> {html.escape(expiry_text)}\n"
        f"<b>API-key mode:</b> {html.escape(key_mode)}\n"
        f"<b>Contract permissions:</b> {html.escape(perm_text)}\n"
        f"<b>IP bindings:</b> {expiry.get('ips_bound', 0)}\n"
        "<b>Bot bridge:</b> GET-only (Phase 9A) ✅"
    ))


async def _cmd_bybit(app: web.Application, cid: int) -> None:
    """Read-only authenticated Bybit account snapshot: key, wallet, positions."""
    tg: Tg = app["tg"]
    client = app.get("bybit_private")
    if not isinstance(client, BybitPrivateReadOnly):
        await tg.send(cid, "⚠️ <b>Bybit RSA bridge is not configured.</b>")
        return

    snap = await client.health_snapshot()
    errors = snap.get("errors") or {}
    info = snap.get("api_key_info") or {}
    wallet = snap.get("wallet") or {}
    positions = snap.get("positions") or []

    if info:
        expiry = bybit_api_expiry(info)
        days = expiry.get("days_left")
        key_line = (
            f"✅ API authenticated · {days}d left"
            if days is not None else "✅ API authenticated"
        )
    else:
        key_line = "❌ API-key info unavailable"

    if wallet:
        equity = _safe_float(wallet.get("totalEquity"))
        available = _safe_float(wallet.get("totalAvailableBalance"))
        wallet_line = f"${equity:.2f} equity · ${available:.2f} available"
    else:
        wallet_line = "unavailable"

    pos_lines: List[str] = []
    for pos in positions[:5]:
        sym = html.escape(str(pos.get("symbol") or "?"))
        side = html.escape(str(pos.get("side") or "?"))
        size = html.escape(str(pos.get("size") or "0"))
        upl = _safe_float(pos.get("unrealisedPnl"))
        pos_lines.append(f"  {sym} {side} · size {size} · uPnL {upl:+.2f}")
    positions_text = "\n".join(pos_lines) if pos_lines else "  none"

    error_text = ""
    if errors:
        compact = "; ".join(
            f"{k}: {str(v)[:180]}" for k, v in sorted(errors.items())
        )
        error_text = f"\n\n⚠️ <b>Partial errors:</b> <code>{html.escape(compact)}</code>"

    await tg.send(cid, (
        "🏦 <b>Bybit Read-Only Bridge — Phase 9A</b>\n\n"
        f"<b>API:</b> {key_line}\n"
        f"<b>Unified account:</b> {html.escape(wallet_line)}\n"
        f"<b>Open USDT-perp positions:</b> {len(positions)}\n"
        f"{positions_text}\n\n"
        "<b>Trading actions:</b> disabled by code — GET endpoints only ✅"
        + error_text
    ))


async def _cmd_plan(app: web.Application, cid: int, sym: str) -> None:
    """Build a fresh Phase 9B plan for an existing active idea."""
    tg: Tg = app["tg"]
    mkt: Market = app["mkt"]

    if not EXECUTION_PLANNER_ENABLED:
        await tg.send(cid, "⚠️ <b>Phase 9B execution planner is disabled.</b>")
        return
    if not sym:
        await tg.send(cid, "Usage: <code>/plan BTCUSDT</code>")
        return
    if not sym.endswith("USDT"):
        sym += "USDT"

    state = mkt.state.get(sym)
    if state is None or state.active_idea is None:
        await tg.send(cid, f"No active idea for <b>{html.escape(sym)}</b>.")
        return

    idea = state.active_idea
    try:
        plan, instrument = await build_execution_plan(
            app, idea, state, reserve_shadow_margin=False
        )
        await tg.send(cid, format_execution_plan(plan, instrument))
    except Exception as exc:
        await tg.send(
            cid,
            "❌ <b>Phase 9B plan failed</b>\n"
            f"<code>{html.escape(type(exc).__name__ + ': ' + str(exc))}</code>",
        )


async def _cmd_execshadow(app: web.Application, cid: int) -> None:
    """Show currently reserved/open Phase 9C shadow execution positions."""
    tg: Tg = app["tg"]
    rows = _execution_shadow_reservations(app)
    if not rows:
        await tg.send(cid, "🫥 <b>Execution Shadow</b>\nNo open shadow positions.")
        return

    lines = ["🧪 <b>Execution Shadow — Phase 9C</b>", ""]
    total_margin = 0.0
    for sym, row in sorted(rows.items()):
        status = str(row.get("status") or "ACTIVE")
        margin = _safe_float(row.get("margin_usdt"))
        qty = _safe_float(row.get("open_qty") or row.get("qty"))
        total_margin += margin
        state = app["mkt"].state.get(sym)
        price = get_current_price(state) if isinstance(state, SymbolState) else 0.0
        net = be = funding = 0.0
        if price > 0:
            net, be, funding = _shadow_remaining_net_pnl(row, price)
        lines.append(
            f"<b>{html.escape(sym)}</b> {html.escape(str(row.get('side') or ''))} · "
            f"{html.escape(status)}"
        )
        lines.append(
            f"qty {qty:g} · shadow margin ${margin:.2f} · "
            f"net est. ${net:+.3f}"
        )
        if status == "EXPIRED_WAIT_EXIT":
            lines.append(
                f"SL {_safe_float(row.get('stop_loss')):.6g} · "
                f"net BE {be:.6g} · funding ${funding:+.3f}"
            )
        lines.append("")

    lines.append(f"<b>Total shadow margin:</b> ${total_margin:.2f}")
    lines.append("<i>No Bybit orders are created by this state.</i>")
    await tg.send(cid, "\n".join(lines))




async def _cmd_status(app: web.Application, cid: int) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    ready    = sum(1 for s in mkt.symbols if mkt.state[s].ready)
    active   = sum(1 for s in mkt.symbols if mkt.state[s].active_idea is not None)
    poll_ago = f"{now_s() - mkt.last_poll_ts}s ago" if mkt.last_poll_ts else "not yet"
    btc_e    = _regime_emoji(mkt.btc_regime)
    stats    = mkt.signal_stats

    await tg.send(cid, (
        f"📊 <b>CryptoBot v18 — Weekly Swing</b>\n\n"
        f"<b>BTC Global Regime:</b> {btc_e} {mkt.btc_regime}\n"
        f"<i>{html.escape(mkt.btc_regime_reason)}</i>\n\n"
        f"<b>Universe:</b> {len(mkt.symbols)} symbols\n"
        f"<b>Ready:</b> {ready}/{len(mkt.symbols)}\n"
        f"<b>Active ideas:</b> {active}\n"
        f"<b>Pending setups:</b> {len(mkt.pending_setups)}\n"
        f"<b>Recent dead candidates:</b> {len(mkt.candidate_debug)} unique\n\n"
        f"<b>Ideas:</b> {stats['total']} total  "
        f"(L:{stats['long']} / S:{stats['short']})\n"
        f"TP1:{stats['tp1_hit']}  TP2:{stats['tp2_hit']}  "
        f"SL:{stats['sl_hit']}  Exp:{stats['expired']}  "
        f"Amb:{stats['ambiguous']}\n\n"
        f"<b>Last poll:</b> {poll_ago}  (#{mkt.poll_count})\n"
        f"<b>Mode:</b> {'🧪 DRY RUN' if DRY_RUN_MODE else '✅ LIVE SIGNALS'}\n"
        f"<b>Phase:</b> 3 det · 4 RR · 5 lifecycle · 6 Tg · 7 dry-run · "
        f"8A entry gate · 8B.1 safe-send · 8C diag · 8D actionable · 8E watchlist · 8F candidates · 8G dead-diag · 8H LS recency · 8I dedup · 8J TP/SL % · 8K entry retest · 8L eligible watchlist · 8L.2 temporal fixes · 8L.3 signal-flow rollback · 8L.4.3 persistent raw + BR/TP deep + BR shadow + TP stats analyzer · 9A Bybit read-only · 9B execution planner · 9C net economics/expiry shadow"
    ))


async def _cmd_regime(app: web.Application, cid: int) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    groups: Dict[str, List[str]] = {"BULLISH": [], "BEARISH": [], "NEUTRAL": []}
    for sym in mkt.symbols:
        groups.get(mkt.state[sym].regime, groups["NEUTRAL"]).append(
            sym.replace("USDT", "")
        )

    def grp(label: str, emoji: str, syms: List[str]) -> str:
        if not syms:
            return ""
        return f"{emoji} <b>{label}</b> ({len(syms)}): {' '.join(syms)}\n"

    btc_e = _regime_emoji(mkt.btc_regime)
    await tg.send(cid, (
        f"🌐 <b>Regimes — v18</b>\n\n"
        + grp("BULLISH",        "🟢", groups["BULLISH"])
        + grp("BEARISH",        "🔴", groups["BEARISH"])
        + grp("NEUTRAL / CHOP", "🟡", groups["NEUTRAL"])
        + f"\n<b>BTC Global:</b> {btc_e} {mkt.btc_regime}\n"
        f"<i>{html.escape(mkt.btc_regime_reason)}</i>"
    ))


async def _cmd_ideas(app: web.Application, cid: int) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    ideas = [
        (sym, mkt.state[sym].active_idea)
        for sym in mkt.symbols
        if mkt.state[sym].active_idea is not None
    ]

    if not ideas:
        await tg.send(cid, "📭 No active ideas.")
        return

    lines = ["📋 <b>Active Ideas</b>\n"]
    for sym, idea in ideas:
        e     = "🟢" if idea.side == "LONG" else "🔴"
        age_h = (now_s() - idea.emitted_at) // 3600
        lines.append(
            f"{e} <b>{sym.replace('USDT','')}</b> {idea.side} | "
            f"{idea.setup_type.replace('_',' ')} | score {idea.setup_score}\n"
            f"   Entry {idea.entry_low:.4f}–{idea.entry_high:.4f} | "
            f"SL {idea.stop_loss:.4f} "
            f"{format_level_pct(idea.side, idea.entry_low, idea.entry_high, idea.stop_loss)}\n"
            f"   TP1 {idea.tp1:.4f} "
            f"{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp1, idea.rr_tp1)} | "
            f"TP2 {idea.tp2:.4f} "
            f"{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp2, idea.rr_tp2)}\n"
            f"   Status: {idea.status} | Age: {age_h}h\n"
        )
    await tg.send(cid, "\n".join(lines))


async def _cmd_idea_detail(app: web.Application, cid: int, sym: str) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    if not sym.endswith("USDT"):
        sym += "USDT"
    state = mkt.state.get(sym)
    if state is None or state.active_idea is None:
        await tg.send(cid, f"No active idea for <b>{sym}</b>.")
        return

    idea  = state.active_idea
    e     = "🟢" if idea.side == "LONG" else "🔴"
    age_h = (now_s() - idea.emitted_at) // 3600
    exp_h = max(0, (idea.expires_at - now_s()) // 3600)

    await tg.send(cid, (
        f"{e} <b>{sym} — {idea.side}</b>\n\n"
        f"<b>Setup:</b>  {idea.setup_type.replace('_',' ')} (score {idea.setup_score})\n"
        f"<b>Status:</b> {idea.status}\n\n"
        f"<b>Entry zone:</b> {idea.entry_low:.5f} – {idea.entry_high:.5f}\n"
        f"<b>Stop Loss:</b>  {idea.stop_loss:.5f}  "
        f"{format_level_pct(idea.side, idea.entry_low, idea.entry_high, idea.stop_loss)}\n"
        f"<b>TP1:</b>        {idea.tp1:.5f}  "
        f"{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp1, idea.rr_tp1)}\n"
        f"<b>TP2:</b>        {idea.tp2:.5f}  "
        f"{format_tp_with_rr(idea.side, idea.entry_low, idea.entry_high, idea.tp2, idea.rr_tp2)}\n\n"
        f"<b>Invalidation:</b> {html.escape(idea.invalidation)}\n\n"
        f"<b>Age:</b> {age_h}h  |  <b>Expires in:</b> {exp_h}h"
    ))


async def _cmd_close(app: web.Application, cid: int, sym: str) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    if not sym.endswith("USDT"):
        sym += "USDT"
    state = mkt.state.get(sym)
    if state is None or state.active_idea is None:
        await tg.send(cid, f"No active idea for <b>{sym}</b> to close.")
        return

    idea        = state.active_idea
    idea.status = "INVALIDATED"
    state.active_idea = None

    # Broadcast INVALIDATED lifecycle update to PRIMARY_RECIPIENTS (or fallback)
    try:
        await send_idea_update(app, idea, "INVALIDATED")
    except Exception as e:
        logger.warning(f"send_idea_update INVALIDATED failed {sym}: {e}")
        await report_error(app, f"send_idea_update/{sym}/INVALIDATED", e)

    # Send a short direct confirmation only when cid is not already receiving
    # the broadcast above (covers both PRIMARY_RECIPIENTS and the fallback).
    targets = get_broadcast_targets()
    if cid not in targets:
        ts = datetime.now(timezone.utc).strftime("%H:%M UTC")
        await tg.send(cid, (
            f"🚫 <b>Idea manually closed</b>\n"
            f"{sym} {idea.side} | {idea.setup_type.replace('_', ' ')}\n"
            f"Closed at {ts}"
        ))


_SETUP_ABBREV = {
    "BREAKOUT_RETEST": "BR",
    "TREND_PULLBACK":  "TP",
    "LIQUIDITY_SWEEP": "LS",
}
_REASON_ABBREV = {
    "already_hit_tp":           "hit_tp",
    "already_hit_sl":           "hit_sl",
    "outside_entry_zone":       "outside_zone",
    "rr_current_fail":          "rr_curr",
    "signal_gate_fail":         "gate_fail",
    "tpsl_fail":                "tpsl_fail",
    "context_too_old":          "ctx_old",
    "price_missing":            "price_miss",
    "invalidated_since_setup":  "invalidated",
    "liquidity_sweep_too_old":  "ls_old",
    "entry_retest_too_old":     "entry_old",
    "fast_4h_conflict":           "4h_conflict",
    "post_sl_cooldown":           "sl_cooldown",
    "reused_stopped_setup":       "reused_stop",
    "secondary_score_fail":       "secondary_low",
}


async def _cmd_candidates(app: web.Application, cid: int) -> None:
    """
    /candidates (/dead) — show recent DEAD candidates from the diagnostic buffer.
    Read-only; diagnostics only; does not affect signal generation.
    """
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    records = mkt.candidate_debug   # newest-last
    if not records:
        await tg.send(cid, "📭 <b>No recent dead candidates.</b>")
        return

    total    = len(records)
    shown_r  = list(reversed(records))[:10]   # newest first, max 10

    # Summary counts
    from collections import Counter
    reason_c  = Counter(_REASON_ABBREV.get(r.reason, r.reason) for r in records)
    det_c     = Counter(_SETUP_ABBREV.get(r.setup_type, r.setup_type) for r in records)
    now_ts    = now_s()

    reason_str = "  ".join(f"{k}={v}" for k, v in reason_c.most_common())
    det_str    = "  ".join(f"{k}={v}" for k, v in sorted(det_c.items()))

    lines = [
        f"🧪 <b>Candidate Diagnostics — recent DEAD candidates</b>\n",
        f"<b>Summary:</b>\n"
        f"  unique stored: {total}\n"
        f"  reasons: {reason_str}\n"
        f"  detectors: {det_str}",
    ]

    for i, rec in enumerate(shown_r, 1):
        side_e     = "🟢" if rec.side == "LONG" else "🔴"
        sym_pretty = rec.symbol.replace("USDT", "/USDT")
        det_abbr   = _SETUP_ABBREV.get(rec.setup_type, rec.setup_type)
        reason_lbl = _REASON_ABBREV.get(rec.reason, rec.reason)
        regime_e   = _regime_emoji(rec.regime)
        age_ago    = int(now_ts - rec.updated_at)

        lines.append(
            f"\n{i}) {side_e} <b>{sym_pretty}</b> — {det_abbr}\n"
            f"Reason: <b>{reason_lbl}</b> | Score: {rec.score}/100 | "
            f"Regime: {regime_e} {rec.regime}\n"
            f"Current: <code>{rec.current_price:.5f}</code>\n"
            f"Entry: <code>{rec.entry_low:.5f} – {rec.entry_high:.5f}</code>\n"
            f"SL: <code>{rec.stop_loss:.5f}</code> "
            f"{format_level_pct(rec.side, rec.entry_low, rec.entry_high, rec.stop_loss)}\n"
            f"TP1: <code>{rec.tp1:.5f}</code> "
            f"{format_tp_with_rr(rec.side, rec.entry_low, rec.entry_high, rec.tp1, rec.rr_tp1)}  "
            f"TP2: <code>{rec.tp2:.5f}</code> "
            f"{format_tp_with_rr(rec.side, rec.entry_low, rec.entry_high, rec.tp2, rec.rr_tp2)}\n"
            f"Setup age: {rec.setup_age_h}h | Updated: {age_ago}s ago"
        )

    if total > 10:
        lines.append(f"\n<i>Showing 10 of {total} unique stored records. "
                     f"Buffer max: {CANDIDATE_DEBUG_MAX}.</i>")

    text = "\n".join(lines)
    if len(text) > 3900:
        text = (text[:3800] +
                "\n\n<i>Output truncated to stay within Telegram message size limit.</i>")
    await tg.send(cid, text)


async def _cmd_watchlist(app: web.Application, cid: int) -> None:
    """
    /watchlist — show pending setups sorted by distance to entry zone (ascending).
    Pending setups are valid structural setups where current price is outside
    the entry zone.  They are informational only; no trade signal is sent.
    """
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    if not mkt.pending_setups:
        await tg.send(cid, "📭 <b>No pending setups.</b>")
        return

    # Sort by distance ascending (closest to entry zone first)
    setups = sorted(mkt.pending_setups.values(), key=lambda p: p.distance_pct)
    total  = len(setups)
    shown  = setups[:10]

    lines = [f"📌 <b>Pending Setups / Watchlist</b>  ({total} total)\n"
             f"<i>Signal-eligible; waiting only for entry-zone return.</i>\n"]
    for i, p in enumerate(shown, 1):
        side_e      = "🟢" if p.side == "LONG" else "🔴"
        sym_pretty  = p.symbol.replace("USDT", "/USDT")
        setup_label = _SETUP_LABELS.get(p.setup_type, p.setup_type.replace("_", " "))
        regime_e    = _regime_emoji(p.regime)

        if p.distance_side == "BELOW_ENTRY_ZONE":
            dist_str = (f"<b>{p.distance_pct:.2f}% below</b> entry zone "
                        f"— needs +{p.distance_pct:.2f}% move to entry")
        elif p.distance_side == "ABOVE_ENTRY_ZONE":
            dist_str = (f"<b>{p.distance_pct:.2f}% above</b> entry zone "
                        f"— needs -{p.distance_pct:.2f}% move to entry")
        else:
            dist_str = "in entry zone"

        lines.append(
            f"{i}) {side_e} <b>{sym_pretty} {p.side}</b> — {html.escape(setup_label)}\n"
            f"   Score: {p.score}/100 | Regime: {regime_e} {p.regime}\n"
            f"   Current: <code>{p.current_price:.5f}</code>  "
            f"Distance: {dist_str}\n"
            f"   Entry: <code>{p.entry_low:.5f} – {p.entry_high:.5f}</code>\n"
            f"   SL: <code>{p.stop_loss:.5f}</code> "
            f"{format_level_pct(p.side, p.entry_low, p.entry_high, p.stop_loss)}  "
            f"TP1: <code>{p.tp1:.5f}</code> "
            f"{format_tp_with_rr(p.side, p.entry_low, p.entry_high, p.tp1, p.rr_tp1)}  "
            f"TP2: <code>{p.tp2:.5f}</code> "
            f"{format_tp_with_rr(p.side, p.entry_low, p.entry_high, p.tp2, p.rr_tp2)}\n"
            f"   Setup age: {p.setup_age_h}h  "
            f"Updated: {int(now_s()-p.updated_at)}s ago\n"
            f"   <i>Waiting for price to return to entry zone</i>"
        )

    if total > 10:
        lines.append(f"\n<i>Showing 10 of {total} pending setups.</i>")

    await tg.send(cid, "\n\n".join(lines))


async def _cmd_statsdb(app: web.Application, cid: int) -> None:
    """Phase 8L.4.3 persistent raw-setup + TP-statistics + BR-shadow summary."""
    tg: Tg = app["tg"]
    store = _diag_store(app)
    if not DIAGNOSTICS_DB_ENABLED:
        await tg.send(cid, "📊 <b>Diagnostic DB is disabled in ENV.</b>")
        return
    if store is None:
        await tg.send(
            cid,
            "⚠️ <b>Diagnostic DB is configured but unavailable.</b>\n"
            "Check Northflank logs for the Phase 8L.4 DB open error.",
        )
        return

    try:
        st = store.summary()
        shadow_summary = store.br_shadow_summary()
    except Exception as exc:
        _diag_warn(f"/statsdb failed: {type(exc).__name__}: {exc}")
        await tg.send(cid, "⚠️ <b>Could not read Diagnostic DB statistics.</b>")
        return

    det_parts = []
    for abbr in ("BR", "TP", "LS"):
        item = st["detectors"].get(abbr, {"n": 0, "avg_score": 0.0})
        det_parts.append(f"{abbr}: {item['n']} (avg {item['avg_score']:.1f})")

    c = st["counters"]
    scan_lines = []
    for abbr in ("BR", "TP", "LS"):
        runs = c.get(f"detector_runs_{abbr}", 0)
        raw = c.get(f"detector_raw_{abbr}", 0)
        score_fail = c.get(f"score_fail_{abbr}", 0)
        scan_lines.append(
            f"  {abbr}: raw {raw}/{runs} · score_fail {score_fail}"
        )

    status_order = ("WAITING_ENTRY", "ACTIVE", "DONE")
    status_str = " · ".join(
        f"{k}={st['statuses'].get(k, 0)}" for k in status_order
    )
    outcome_order = (
        "TP2", "SL", "TP1_THEN_SL", "TP1_ONLY_EXPIRED",
        "EXPIRED", "NO_ENTRY", "AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS",
    )
    outcome_items = [
        f"{k}={st['outcomes'].get(k, 0)}"
        for k in outcome_order if st["outcomes"].get(k, 0)
    ]
    outcome_str = " · ".join(outcome_items) if outcome_items else "—"
    size_mb = st["size_bytes"] / (1024 * 1024)

    await tg.send(cid, (
        f"📊 <b>Phase 8L.4.3 — Persistent Diagnostic DB</b>\n\n"
        f"<b>Status:</b> ✅ active\n"
        f"<b>DB:</b> <code>{html.escape(DIAGNOSTICS_DB_PATH)}</code>\n"
        f"<b>Size:</b> {size_mb:.2f} MB\n"
        f"<b>Unique raw setups:</b> {st['total']}\n"
        f"<b>By detector:</b> {' · '.join(det_parts)}\n"
        f"<b>First-seen score failures:</b> {st['score_fail']}\n"
        f"<b>Actual signals emitted:</b> {st['signal_emitted']}\n"
        f"<b>BR geometry-fail shadows:</b> {shadow_summary['total']} "
        f"(DONE {shadow_summary['statuses'].get('DONE',0)})\n\n"
        f"<b>Detector observations (persistent):</b>\n"
        + "\n".join(scan_lines) + "\n\n"
        f"<b>Outcome tracker:</b> {status_str}\n"
        f"<b>Final outcomes:</b> {outcome_str}\n\n"
        f"<i>Raw setups are recorded before the score floor. Outcomes are "
        f"hypothetical and do not change signal generation.</i>\n"
        f"Deep BR/TP funnel: /brtp · Calibration review: /calibration"
    ))


def _fmt_stage_line(label: str, c: Dict[str, int], stages: List[Tuple[str, str]]) -> str:
    vals = [f"{name}={c.get(key, 0)}" for key, name in stages]
    return f"{label}: " + " · ".join(vals)


def _fmt_terminal_line(c: Dict[str, int], order: List[Tuple[str, str]]) -> str:
    vals = [
        f"{name}={c.get('terminal_' + key, 0)}"
        for key, name in order if c.get('terminal_' + key, 0)
    ]
    return " · ".join(vals) if vals else "—"


def _tp_pullback_length_line(side: str, c: Dict[str, int]) -> str:
    """Distribution of TP pullback-window lengths among trend-context passes."""
    trend_pass = int(c.get("trend_context_pass", 0))
    lens = {i: int(c.get(f"pullback_len_{i}", 0)) for i in range(1, 8)}
    eight_plus = int(c.get("pullback_len_8", 0))
    zero = max(0, trend_pass - sum(lens.values()) - eight_plus)
    if trend_pass <= 0:
        return f"{side}: no trend-context passes"
    parts = [f"0D={zero}"] + [f"{i}D={lens[i]}" for i in range(1, 8)] + [f"8+D={eight_plus}"]
    return f"{side}: " + " · ".join(parts)


def _tp_score_bucket_outcome_lines(tp_raw: Dict[str, Any]) -> List[str]:
    """Compact outcome/status matrix for persistent unique TP setups by raw-score bucket."""
    lines: List[str] = []
    for label, x in tp_raw.get("bucket_stats", {}).items():
        total = int(x.get("total", 0))
        if total <= 0:
            continue
        st = x.get("statuses", {})
        out = x.get("outcomes", {})
        waiting = int(st.get("WAITING_ENTRY", 0))
        active = int(st.get("ACTIVE", 0))
        done = int(st.get("DONE", 0))
        outcome_text = ", ".join(f"{k}={v}" for k, v in sorted(out.items())) or "—"
        lines.append(
            f"{html.escape(str(label))}: n={total} · W/A/D={waiting}/{active}/{done} · {html.escape(outcome_text)}"
        )
    return lines or ["—"]



def _cal_score_bucket(score: int) -> str:
    if score < 45:
        return "<45"
    if score < 55:
        return "45-54"
    if score < 65:
        return "55-64"
    if score < 75:
        return "65-74"
    if score < 85:
        return "75-84"
    return "85+"


def _cal_gap_bucket(gap_atr: float) -> str:
    gap = float(gap_atr or 0.0)
    if gap <= 0.10:
        return "≤0.10"
    if gap <= 0.20:
        return "0.10-0.20"
    if gap <= 0.30:
        return "0.20-0.30"
    return ">0.30"


def _cal_is_clean_done(row: Dict[str, Any]) -> bool:
    if (row.get("observation_status") or "") != "DONE":
        return False
    return (row.get("final_outcome") or "") not in (
        "AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS", ""
    )


def _cal_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    statuses: Dict[str, int] = {}
    outcomes: Dict[str, int] = {}
    regimes: Dict[str, int] = {}
    btc_regimes: Dict[str, int] = {}
    symbols: Dict[str, Dict[str, int]] = {}
    mfe: List[float] = []
    mae: List[float] = []
    rr2: List[float] = []
    entry_delays_h: List[float] = []
    final_after_entry_h: List[float] = []
    first_seen_values: List[int] = []
    clean_done = 0
    ambiguous_done = 0

    for row in rows:
        status = str(row.get("observation_status") or "UNKNOWN")
        statuses[status] = statuses.get(status, 0) + 1

        outcome = str(row.get("final_outcome") or "")
        if outcome:
            outcomes[outcome] = outcomes.get(outcome, 0) + 1

        if _cal_is_clean_done(row):
            clean_done += 1
        elif status == "DONE" and outcome in ("AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS"):
            ambiguous_done += 1

        regime = str(row.get("regime_first") or "UNKNOWN")
        regimes[regime] = regimes.get(regime, 0) + 1
        btc_regime = str(row.get("btc_regime_first") or "UNKNOWN")
        btc_regimes[btc_regime] = btc_regimes.get(btc_regime, 0) + 1

        sym = str(row.get("symbol") or "UNKNOWN")
        s = symbols.setdefault(sym, {"n": 0, "clean_done": 0})
        s["n"] += 1
        s["clean_done"] += int(_cal_is_clean_done(row))

        mfe.append(float(row.get("mfe_r") or 0.0))
        mae.append(float(row.get("mae_r") or 0.0))
        rr2.append(float(row.get("rr_tp2") or 0.0))

        first_seen = int(row.get("first_seen_ts") or 0)
        if first_seen > 0:
            first_seen_values.append(first_seen)

        entry_ts = int(row.get("entry_activated_ts") or 0)
        final_ts = int(row.get("final_outcome_ts") or 0)
        if first_seen > 0 and entry_ts > 0 and entry_ts >= first_seen:
            entry_delays_h.append((entry_ts - first_seen) / 3600.0)
        if entry_ts > 0 and final_ts > 0 and final_ts >= entry_ts:
            final_after_entry_h.append((final_ts - entry_ts) / 3600.0)

    symbol_rows = sorted(
        (
            {"symbol": sym, "n": vals["n"], "clean_done": vals["clean_done"]}
            for sym, vals in symbols.items()
        ),
        key=lambda x: (-x["n"], x["symbol"]),
    )

    age_days = (
        max(0.0, (now_s() - min(first_seen_values)) / 86400.0)
        if first_seen_values else 0.0
    )
    return {
        "n": len(rows),
        "statuses": statuses,
        "outcomes": outcomes,
        "regimes": regimes,
        "btc_regimes": btc_regimes,
        "symbols": symbol_rows,
        "clean_done": clean_done,
        "ambiguous_done": ambiguous_done,
        "avg_mfe_r": (sum(mfe) / len(mfe)) if mfe else 0.0,
        "avg_mae_r": (sum(mae) / len(mae)) if mae else 0.0,
        "avg_rr2": (sum(rr2) / len(rr2)) if rr2 else 0.0,
        "avg_entry_delay_h": (
            sum(entry_delays_h) / len(entry_delays_h) if entry_delays_h else 0.0
        ),
        "avg_final_after_entry_h": (
            sum(final_after_entry_h) / len(final_after_entry_h)
            if final_after_entry_h else 0.0
        ),
        "age_days": age_days,
    }


def _cal_group(rows: List[Dict[str, Any]], key_fn) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(key_fn(row)), []).append(row)
    return {k: _cal_summary(v) for k, v in grouped.items()}


def _calibration_review_data(store: DiagnosticStore) -> Dict[str, Any]:
    br_rows = [
        dict(r) for r in store.conn.execute(
            """
            SELECT symbol,side,raw_score_first,regime_first,btc_regime_first,
                   geometry_gap_atr,observation_status,final_outcome,
                   mfe_r,mae_r,rr_tp2,first_seen_ts,entry_activated_ts,
                   final_outcome_ts
            FROM br_shadow_setups
            """
        ).fetchall()
    ]

    tp_rows = [
        dict(r) for r in store.conn.execute(
            """
            SELECT setup_key,symbol,side,raw_score_first,regime_first,
                   btc_regime_first,observation_status,final_outcome,
                   mfe_r,mae_r,rr_tp2,first_seen_ts,entry_activated_ts,
                   final_outcome_ts
            FROM raw_setups
            WHERE setup_type='TREND_PULLBACK'
            """
        ).fetchall()
    ]

    full_stack_keys = {
        str(r["setup_key"])
        for r in store.conn.execute(
            """
            SELECT setup_key FROM setup_score_components
            WHERE component='ema_full_stack'
            """
        ).fetchall()
    }
    for row in tp_rows:
        row["full_stack"] = str(row.get("setup_key") or "") in full_stack_keys

    return {
        "br": _cal_summary(br_rows),
        "br_gap": _cal_group(
            br_rows, lambda r: _cal_gap_bucket(r.get("geometry_gap_atr", 0.0))
        ),
        "br_score": _cal_group(
            br_rows, lambda r: _cal_score_bucket(int(r.get("raw_score_first") or 0))
        ),
        "tp": _cal_summary(tp_rows),
        "tp_score": _cal_group(
            tp_rows, lambda r: _cal_score_bucket(int(r.get("raw_score_first") or 0))
        ),
        "tp_full_stack": _cal_group(
            tp_rows, lambda r: "YES" if r.get("full_stack") else "NO"
        ),
    }


def _cal_outcome_text(x: Dict[str, Any]) -> str:
    order = (
        "TP2", "SL", "TP1_THEN_SL", "TP1_ONLY_EXPIRED",
        "EXPIRED", "NO_ENTRY", "AMBIGUOUS", "ENTRY_BAR_AMBIGUOUS",
    )
    parts = [
        f"{k}={x['outcomes'].get(k, 0)}"
        for k in order if x["outcomes"].get(k, 0)
    ]
    return " · ".join(parts) if parts else "—"


def _cal_cohort_line(label: str, x: Dict[str, Any]) -> str:
    w = x["statuses"].get("WAITING_ENTRY", 0)
    a = x["statuses"].get("ACTIVE", 0)
    return (
        f"{html.escape(label)}: n={x['n']} · cleanD={x['clean_done']} · "
        f"W/A={w}/{a} · {_cal_outcome_text(x)} · "
        f"MFE/MAE={x['avg_mfe_r']:.2f}/{x['avg_mae_r']:.2f}R"
    )


def _cal_dict_text(d: Dict[str, int]) -> str:
    return " · ".join(
        f"{html.escape(str(k))}:{v}" for k, v in sorted(d.items())
    ) or "—"


def _cal_symbols_text(x: Dict[str, Any], limit: int = 5) -> str:
    rows = x.get("symbols", [])[:limit]
    if not rows:
        return "—"
    return " · ".join(
        f"{html.escape(r['symbol'])} n={r['n']}/D={r['clean_done']}" for r in rows
    )


async def _cmd_calibration(app: web.Application, cid: int) -> None:
    """Phase 8M calibration review. Read-only diagnostics; never changes trading."""
    tg: Tg = app["tg"]
    store = _diag_store(app)
    if store is None:
        await tg.send(cid, "⚠️ <b>Diagnostic DB unavailable.</b>")
        return
    try:
        data = _calibration_review_data(store)
    except Exception as exc:
        _diag_warn(f"/calibration failed: {type(exc).__name__}: {exc}")
        await tg.send(cid, "⚠️ <b>Could not build Phase 8M calibration review.</b>")
        return

    br = data["br"]
    tp = data["tp"]
    empty = _cal_summary([])
    tp55 = data["tp_score"].get("55-64", empty)
    tp65 = data["tp_score"].get("65-74", empty)

    br_first_target = 15
    br_robust_target = 30
    tp_first_target = 20
    tp_robust_target = 50
    tp_bucket_target = 15

    first_ready = (
        br["clean_done"] >= br_first_target
        and tp["clean_done"] >= tp_first_target
    )
    robust_ready = (
        br["clean_done"] >= br_robust_target
        and tp["clean_done"] >= tp_robust_target
        and tp55["clean_done"] >= tp_bucket_target
        and tp65["clean_done"] >= tp_bucket_target
    )
    readiness = (
        "🟢 ROBUST REVIEW READY" if robust_ready
        else "🟡 FIRST REVIEW READY" if first_ready
        else "🔵 COLLECTING"
    )

    await tg.send(cid, (
        "🧪 <b>Phase 8M — Calibration Review</b>\n\n"
        "<b>Mode:</b> diagnostic only — trading logic unchanged\n"
        f"<b>Status:</b> {readiness}\n\n"
        "<b>Collection age:</b>\n"
        f"BR shadow ≈ {br['age_days']:.1f}d · TP raw ≈ {tp['age_days']:.1f}d\n\n"
        "<b>Decision readiness:</b>\n"
        f"BR clean DONE: {br['clean_done']}/{br_first_target} first review "
        f"· {br['clean_done']}/{br_robust_target} robust\n"
        f"TP clean DONE: {tp['clean_done']}/{tp_first_target} first review "
        f"· {tp['clean_done']}/{tp_robust_target} robust\n"
        f"TP 55-64 clean DONE: {tp55['clean_done']}/{tp_bucket_target}\n"
        f"TP 65-74 clean DONE: {tp65['clean_done']}/{tp_bucket_target}\n\n"
        "<i>Readiness is a sample-size gate only. It does not automatically "
        "recommend any trading-rule change.</i>"
    ))

    gap_order = ("≤0.10", "0.10-0.20", "0.20-0.30", ">0.30")
    br_gap_lines = [
        _cal_cohort_line(label, data["br_gap"][label])
        for label in gap_order if label in data["br_gap"]
    ] or ["—"]
    score_order = ("<45", "45-54", "55-64", "65-74", "75-84", "85+")
    br_score_lines = [
        _cal_cohort_line(label, data["br_score"][label])
        for label in score_order if label in data["br_score"]
    ] or ["—"]

    await asyncio.sleep(1.1)
    await tg.send(cid, (
        "🫥 <b>8M BR Calibration Cohorts</b>\n\n"
        f"<b>Total shadows:</b> {br['n']} · clean DONE {br['clean_done']} "
        f"· ambiguous DONE {br['ambiguous_done']}\n"
        f"<b>Overall:</b> {_cal_outcome_text(br)}\n"
        f"<b>Avg MFE/MAE:</b> {br['avg_mfe_r']:.2f}/{br['avg_mae_r']:.2f}R "
        f"· avg RR2 {br['avg_rr2']:.2f}\n"
        f"<b>Avg entry delay:</b> {br['avg_entry_delay_h']:.1f}h "
        f"· avg entry→final {br['avg_final_after_entry_h']:.1f}h\n"
        f"<b>First regime:</b> {_cal_dict_text(br['regimes'])}\n"
        f"<b>BTC regime:</b> {_cal_dict_text(br['btc_regimes'])}\n"
        f"<b>Top symbols:</b> {_cal_symbols_text(br)}\n\n"
        "<b>By geometry gap (ATR inside entry zone):</b>\n"
        + "\n".join(br_gap_lines) + "\n\n"
        "<b>By raw score:</b>\n"
        + "\n".join(br_score_lines)
    ))

    tp_score_lines = [
        _cal_cohort_line(label, data["tp_score"][label])
        for label in score_order if label in data["tp_score"]
    ] or ["—"]
    stack_lines = [
        _cal_cohort_line(label, data["tp_full_stack"][label])
        for label in ("YES", "NO") if label in data["tp_full_stack"]
    ] or ["—"]

    await asyncio.sleep(1.1)
    await tg.send(cid, (
        "📐 <b>8M TP Calibration Cohorts</b>\n\n"
        f"<b>Total raw TP:</b> {tp['n']} · clean DONE {tp['clean_done']} "
        f"· ambiguous DONE {tp['ambiguous_done']}\n"
        f"<b>Overall:</b> {_cal_outcome_text(tp)}\n"
        f"<b>Avg MFE/MAE:</b> {tp['avg_mfe_r']:.2f}/{tp['avg_mae_r']:.2f}R "
        f"· avg RR2 {tp['avg_rr2']:.2f}\n"
        f"<b>Avg entry delay:</b> {tp['avg_entry_delay_h']:.1f}h "
        f"· avg entry→final {tp['avg_final_after_entry_h']:.1f}h\n"
        f"<b>First regime:</b> {_cal_dict_text(tp['regimes'])}\n"
        f"<b>BTC regime:</b> {_cal_dict_text(tp['btc_regimes'])}\n"
        f"<b>Top symbols:</b> {_cal_symbols_text(tp)}\n\n"
        "<b>By first-seen raw score:</b>\n"
        + "\n".join(tp_score_lines) + "\n\n"
        "<b>EMA full-stack cohort:</b>\n"
        + "\n".join(stack_lines) + "\n\n"
        "<i>Use these cohorts for the month-end calibration decision; "
        "do not infer a threshold from unfinished cohorts alone.</i>"
    ))


async def _cmd_brtp(app: web.Application, cid: int) -> None:
    """Deep persistent BR/TP funnel + TP statistics diagnostics (Phase 8L.4.3)."""
    tg: Tg = app["tg"]
    store = _diag_store(app)
    if store is None:
        await tg.send(cid, "⚠️ <b>Diagnostic DB unavailable.</b>")
        return
    try:
        br = store.detector_stage_summary("BR")
        tp = store.detector_stage_summary("TP")
        br_raw = store.detector_raw_detail("BREAKOUT_RETEST")
        tp_raw = store.detector_raw_detail("TREND_PULLBACK")
    except Exception as exc:
        _diag_warn(f"/brtp failed: {type(exc).__name__}: {exc}")
        await tg.send(cid, "⚠️ <b>Could not read BR/TP deep diagnostics.</b>")
        return

    br_stages = [
        ("calls", "calls"), ("broken_swing", "swing"),
        ("volume_pass", "vol"), ("candle_pass", "candle"),
        ("retest_found", "retest"), ("geometry_pass", "geom"),
        ("raw_result", "raw"),
    ]
    br_term = [
        ("no_broken_swing", "no_swing"), ("volume_fail", "vol_fail"),
        ("candle_fail", "candle_fail"), ("retest_fail", "retest_fail"),
        ("geometry_fail", "geom_fail"), ("raw_result", "raw"),
    ]
    br_lines = []
    for side in ("LONG", "SHORT"):
        c = br.get(side, {})
        br_lines.append(_fmt_stage_line(side, c, br_stages))
        br_lines.append("  terminal: " + _fmt_terminal_line(c, br_term))
        ret = (
            f"  retest detail: 4H window={c.get('retest_window_bar_4h',0)} "
            f"touch={c.get('retest_touch_4h',0)} valid={c.get('retest_valid_4h',0)} | "
            f"1H window={c.get('retest_window_bar_1h',0)} "
            f"touch={c.get('retest_touch_1h',0)} valid={c.get('retest_valid_1h',0)}"
        )
        br_lines.append(ret)

    await tg.send(cid, (
        "🔎 <b>BR Deep Diagnostic — Phase 8L.4.3</b>\n\n"
        "<i>Stage counters continue from 8L.4.1; BR shadow outcomes start prospectively from 8L.4.2.</i>\n\n"
        + "\n".join(br_lines) + "\n\n"
        f"<b>Persistent unique BR setups (all DB history):</b> {br_raw['total']} "
        f"· avg score {br_raw['avg_score']:.1f} · signals {br_raw['signals']}\n"
        "<i>Geometry-fail outcome tracker: /brshadow</i>"
    ))

    tp_stages = [
        ("calls", "calls"), ("trend_context_pass", "trend"),
        ("pullback_duration_pass", "dur2-7"), ("ema_touch_pass", "touch"),
        ("4h_close_confirm_pass", "4Hclose"), ("geometry_pass", "geom"),
        ("raw_result", "raw"),
    ]
    tp_term = [
        ("trend_context_fail", "trend_fail"),
        ("pullback_duration_fail", "duration_fail"),
        ("ema_touch_fail", "touch_fail"),
        ("4h_close_confirm_fail", "4Hclose_fail"),
        ("hard_reversal_confirm_fail", "rev_fail"),
        ("hard_reversal_volume_fail", "vol_fail"),
        ("geometry_fail", "geom_fail"), ("raw_result", "raw"),
    ]
    tp_lines = []
    for side in ("LONG", "SHORT"):
        c = tp.get(side, {})
        tp_lines.append(_fmt_stage_line(side, c, tp_stages))
        tp_lines.append("  terminal: " + _fmt_terminal_line(c, tp_term))
        tp_lines.append(
            f"  touches: EMA20={c.get('ema20_touch_pass',0)} "
            f"EMA50={c.get('ema50_touch_pass',0)}"
        )

    buckets = " · ".join(f"{html.escape(str(k))}:{v}" for k, v in tp_raw["buckets"].items())
    regimes = " · ".join(f"{k}:{v}" for k, v in sorted(tp_raw["regimes"].items())) or "—"
    components = " · ".join(
        f"{x['name']} {x['n']}/{tp_raw['total']} ({x['rate']:.0f}%)"
        for x in tp_raw["components"][:7]
    ) or "—"
    outcomes = " · ".join(f"{k}:{v}" for k, v in tp_raw["outcomes"].items()) or "—"
    passed = tp_raw["score_passed"]
    duration_lines = [
        _tp_pullback_length_line("LONG", tp.get("LONG", {})),
        _tp_pullback_length_line("SHORT", tp.get("SHORT", {})),
    ]
    bucket_outcome_lines = _tp_score_bucket_outcome_lines(tp_raw)

    tp_text = (
        "📐 <b>TP Deep Diagnostic — Phase 8L.4.3</b>\n\n"
        + "\n".join(tp_lines) + "\n\n"
        + "<b>Pullback length among trend-context passes:</b>\n"
        + "\n".join(duration_lines) + "\n\n"
        f"<b>Unique TP setups:</b> {tp_raw['total']} · avg score {tp_raw['avg_score']:.1f}\n"
        f"<b>Passed first-seen floor:</b> {passed}/{tp_raw['total']} · "
        f"signals {tp_raw['signals']}\n"
        f"<b>Score buckets:</b> {buckets}\n"
        f"<b>First regime:</b> {regimes}\n"
        f"<b>Awarded components:</b> {components}\n"
        f"<b>Final outcomes so far:</b> {outcomes}\n"
        + "<b>Outcomes by first-seen score bucket:</b>\n"
        + "\n".join(bucket_outcome_lines) + "\n\n"
        "<i>Raw TP statistics include setups rejected by the score floor. "
        "They remain hypothetical and do not change signal generation.</i>"
    )
    # Two immediate channel sends can occasionally trip Telegram flood limits.
    # Delay the second diagnostic message and also expose /tpdiag as a direct command.
    await asyncio.sleep(1.1)
    await tg.send(cid, tp_text)


async def _cmd_tpdiag(app: web.Application, cid: int) -> None:
    """Direct TP diagnostic command; avoids dependence on the second /brtp send."""
    tg: Tg = app["tg"]
    store = _diag_store(app)
    if store is None:
        await tg.send(cid, "⚠️ <b>Diagnostic DB unavailable.</b>")
        return
    try:
        tp = store.detector_stage_summary("TP")
        tp_raw = store.detector_raw_detail("TREND_PULLBACK")
    except Exception as exc:
        _diag_warn(f"/tpdiag failed: {type(exc).__name__}: {exc}")
        await tg.send(cid, "⚠️ <b>Could not read TP diagnostics.</b>")
        return

    tp_stages = [
        ("calls", "calls"), ("trend_context_pass", "trend"),
        ("pullback_duration_pass", "dur2-7"), ("ema_touch_pass", "touch"),
        ("4h_close_confirm_pass", "4Hclose"), ("geometry_pass", "geom"),
        ("raw_result", "raw"),
    ]
    tp_term = [
        ("trend_context_fail", "trend_fail"),
        ("pullback_duration_fail", "duration_fail"),
        ("ema_touch_fail", "touch_fail"),
        ("4h_close_confirm_fail", "4Hclose_fail"),
        ("hard_reversal_confirm_fail", "rev_fail"),
        ("hard_reversal_volume_fail", "vol_fail"),
        ("geometry_fail", "geom_fail"), ("raw_result", "raw"),
    ]
    tp_lines = []
    for side in ("LONG", "SHORT"):
        c = tp.get(side, {})
        tp_lines.append(_fmt_stage_line(side, c, tp_stages))
        tp_lines.append("  terminal: " + _fmt_terminal_line(c, tp_term))
        tp_lines.append(
            f"  touches: EMA20={c.get('ema20_touch_pass',0)} "
            f"EMA50={c.get('ema50_touch_pass',0)}"
        )
    buckets = " · ".join(f"{html.escape(str(k))}:{v}" for k, v in tp_raw["buckets"].items())
    regimes = " · ".join(f"{k}:{v}" for k, v in sorted(tp_raw["regimes"].items())) or "—"
    components = " · ".join(
        f"{x['name']} {x['n']}/{tp_raw['total']} ({x['rate']:.0f}%)"
        for x in tp_raw["components"][:7]
    ) or "—"
    outcomes = " · ".join(f"{k}:{v}" for k, v in tp_raw["outcomes"].items()) or "—"
    passed = tp_raw["score_passed"]
    duration_lines = [
        _tp_pullback_length_line("LONG", tp.get("LONG", {})),
        _tp_pullback_length_line("SHORT", tp.get("SHORT", {})),
    ]
    bucket_outcome_lines = _tp_score_bucket_outcome_lines(tp_raw)
    await tg.send(cid, (
        "📐 <b>TP Deep Diagnostic — Phase 8L.4.3</b>\n\n"
        + "\n".join(tp_lines) + "\n\n"
        + "<b>Pullback length among trend-context passes:</b>\n"
        + "\n".join(duration_lines) + "\n\n"
        f"<b>Unique TP setups:</b> {tp_raw['total']} · avg score {tp_raw['avg_score']:.1f}\n"
        f"<b>Passed first-seen floor:</b> {passed}/{tp_raw['total']} · signals {tp_raw['signals']}\n"
        f"<b>Score buckets:</b> {buckets}\n"
        f"<b>First regime:</b> {regimes}\n"
        f"<b>Awarded components:</b> {components}\n"
        f"<b>Final outcomes so far:</b> {outcomes}\n"
        + "<b>Outcomes by first-seen score bucket:</b>\n"
        + "\n".join(bucket_outcome_lines) + "\n\n"
        "<i>Raw TP statistics include setups rejected by the score floor. "
        "They remain hypothetical and do not change signal generation.</i>"
    ))


async def _cmd_brshadow(app: web.Application, cid: int) -> None:
    """Prospective outcomes for BR candidates rejected only by geometry."""
    tg: Tg = app["tg"]
    store = _diag_store(app)
    if store is None:
        await tg.send(cid, "⚠️ <b>Diagnostic DB unavailable.</b>")
        return
    try:
        x = store.br_shadow_summary()
    except Exception as exc:
        _diag_warn(f"/brshadow failed: {type(exc).__name__}: {exc}")
        await tg.send(cid, "⚠️ <b>Could not read BR shadow statistics.</b>")
        return

    sides = " · ".join(f"{k}:{v}" for k, v in sorted(x["sides"].items())) or "—"
    statuses = " · ".join(f"{k}:{v}" for k, v in sorted(x["statuses"].items())) or "—"
    outcomes = " · ".join(f"{k}:{v}" for k, v in sorted(x["outcomes"].items())) or "—"
    await tg.send(cid, (
        "🫥 <b>BR Shadow Outcome Tracker — Phase 8L.4.3</b>\n\n"
        f"<b>Unique geometry-fail shadows:</b> {x['total']}\n"
        f"<b>By side:</b> {sides}\n"
        f"<b>Avg raw score:</b> {x['avg_score']:.1f}\n"
        f"<b>Avg geometry gap:</b> {x['avg_gap_atr']:.3f} ATR inside entry zone\n"
        f"<b>Tracker:</b> {statuses}\n"
        f"<b>Final outcomes:</b> {outcomes}\n"
        f"<b>Avg MFE / MAE:</b> {x['avg_mfe_r']:.2f}R / {x['avg_mae_r']:.2f}R\n\n"
        "<b>Shadow SL model:</b> 0.05 ATR outside the entry-zone edge.\n"
        f"<b>Outcome window:</b> {DIAGNOSTICS_OUTCOME_DAYS} days after entry.\n\n"
        "<i>Shadow setups never enter the trading pipeline and can never emit a signal. "
        "Tracking starts prospectively from first observation after this deployment.</i>"
    ))


async def _cmd_config(app: web.Application, cid: int) -> None:
    """Show sanitised bot configuration — no token or raw chat IDs exposed."""
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    mode = "🧪 DRY RUN" if DRY_RUN_MODE else "✅ LIVE SIGNALS"
    await tg.send(cid, (
        f"⚙️ <b>Bot Configuration</b>\n\n"
        f"<b>Mode:</b> {mode}\n"
        f"<b>ONLY_CHANNEL:</b> {ONLY_CHANNEL}\n"
        f"<b>REPORT_ERRORS_TO_TG:</b> {REPORT_ERRORS_TO_TG}\n\n"
        f"<b>Universe:</b> {len(mkt.symbols)} symbols\n"
        f"<b>Primary recipients:</b> {len(PRIMARY_RECIPIENTS)}\n"
        f"<b>Allowed chats:</b> {len(ALLOWED_CHAT_IDS)}\n\n"
        f"<b>Poll intervals:</b>\n"
        f"  1H={POLL_1H_SEC}s · 4H={POLL_4H_SEC}s · "
        f"1D={POLL_1D_SEC}s · 1W={POLL_1W_SEC}s · 1M={POLL_1M_SEC}s\n\n"
        f"<b>RR minimum TP2:</b> Tier1 ≥ {RR_MIN_TIER1}  |  Tier2 ≥ {RR_MIN_TIER2}\n"
        f"<b>TP1 RR hard gate:</b> off (TP1 RR still displayed)\n"
        f"<b>Score floor:</b> Normal ≥ {MIN_SCORE_NORMAL}  |  Chop ≥ {MIN_SCORE_CHOP}\n"
        f"<b>Secondary candidate floor:</b> ≥ {SECONDARY_CANDIDATE_MIN_SCORE}\n"
        f"<b>4H trend alignment:</b> {'required' if REQUIRE_4H_TREND_ALIGNMENT else 'off'}\n"
        f"<b>TP reversal candle/volume:</b> "
        f"{'required' if REQUIRE_TP_REVERSAL_CONFIRM else 'optional'} / "
        f"{'required' if REQUIRE_TP_REVERSAL_VOLUME else 'optional'}\n"
        f"<b>LS reversal/volume:</b> "
        f"{'required' if REQUIRE_LS_REVERSAL_CONFIRM else 'optional'} / "
        f"{'required' if REQUIRE_LS_SWEEP_VOLUME else 'optional'}\n"
        f"<b>Post-SL lock:</b> {POST_SL_COOLDOWN_HOURS}h; repeated same-side "
        f"{REPEATED_SL_LOCK_HOURS}h after {MAX_CONSECUTIVE_SL_SAME_SIDE} stops\n"
        f"<b>Max idea duration:</b> {MAX_IDEA_DURATION_DAYS} days\n"
        f"<b>Setup context max:</b> {SETUP_CONTEXT_MAX_DAYS}d  "
        f"(legacy fresh: {SETUP_MAX_AGE_HOURS}h)\n"
        f"<b>Entry zone required:</b> {'yes' if ENTRY_ZONE_REQUIRED else 'no'}\n"
        f"<b>RR from current price:</b> {'yes' if RR_FROM_CURRENT_PRICE else 'no'}\n"
        f"<b>Liquidity Sweep max age:</b> {LIQUIDITY_SWEEP_MAX_AGE_HOURS}h\n"
        f"<b>LS entry retest gate:</b> after {LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS}h, "
        f"return ≤ {ENTRY_RETEST_MAX_AGE_HOURS}h\n"
        f"<b>Watchlist:</b> enabled\n"
        f"<b>Signal-eligible watchlist:</b> enabled\n"
        f"<b>Candidate selection:</b> enabled\n"
        f"<b>Dead candidate diagnostics:</b> enabled\n"
        f"<b>Candidate debug max:</b> {CANDIDATE_DEBUG_MAX}\n"
        f"<b>Candidate debug dedup:</b> enabled\n"
        f"<b>Post-confirmation TP/SL boundary:</b> enabled (Phase 8L.2)\n"
        f"<b>Persistent raw diagnostics:</b> {'enabled' if DIAGNOSTICS_DB_ENABLED else 'off'}\n"
        f"<b>Diagnostics DB:</b> <code>{html.escape(DIAGNOSTICS_DB_PATH)}</code>\n"
        f"<b>Raw outcome window:</b> {DIAGNOSTICS_OUTCOME_DAYS} days after entry (Phase 8L.4)\n"
        f"<b>Bybit private bridge:</b> {'GET-only enabled' if BYBIT_PRIVATE_READONLY_ENABLED else 'off'} (Phase 9A)\n"
        f"<b>Execution planner:</b> {'enabled' if EXECUTION_PLANNER_ENABLED else 'off'} (Phase 9B)\n"
        f"<b>Planner leverage:</b> {EXECUTION_PLANNER_LEVERAGE:.1f}x · "
        f"<b>equity reserve:</b> {EXECUTION_PLANNER_RESERVE_PCT:.0f}% · "
        f"<b>TP split:</b> 50/50\n"
        f"<b>Planner auto-send:</b> {'yes' if EXECUTION_PLANNER_AUTO_SEND else 'no'} · "
        f"<b>Bybit writes:</b> disabled by code\n"
        f"<b>Net economics:</b> {'enabled' if EXECUTION_ECONOMICS_ENABLED else 'off'} (Phase 9C) · "
        f"fees=Bybit account · funding=current-rate estimate\n"
        f"<b>Expiry execution policy:</b> net ≥ 0 exit; net < 0 → EXPIRED_WAIT_EXIT → SL or own net BE\n"
        f"<b>Shadow state:</b> <code>{html.escape(EXECUTION_SHADOW_STATE_PATH)}</code>\n"
        f"<b>API expiry reminders:</b> 30/21/14/7/1 days · check every "
        f"{max(3600, BYBIT_API_REMINDER_CHECK_SEC)}s"
    ))


async def _cmd_diag(app: web.Application, cid: int) -> None:
    """Compact real-time diagnostics snapshot including scan gate counters."""
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    poll_ago  = f"{now_s() - mkt.last_poll_ts}s ago" if mkt.last_poll_ts else "never"
    ready     = sum(1 for s in mkt.symbols if mkt.state[s].ready)
    active    = sum(1 for s in mkt.symbols if mkt.state[s].active_idea is not None)
    not_ready = [s for s in mkt.symbols if not mkt.state[s].ready][:10]
    btc_scanned = mkt.state["BTCUSDT"].last_scanned_ts if "BTCUSDT" in mkt.state else 0
    btc_scan_ago = (f"{now_s() - btc_scanned}s ago" if btc_scanned else "never")
    mode = "🧪 DRY RUN" if DRY_RUN_MODE else "✅ LIVE SIGNALS"
    nr_str = (", ".join(s.replace("USDT","") for s in not_ready)
              if not_ready else "—")

    d = mkt.diag_last    # last keepalive interval
    t = mkt.diag_total   # cumulative since startup
    pending_n = len(mkt.pending_setups)

    # Pending summary block
    if pending_n > 0:
        closest = sorted(mkt.pending_setups.values(), key=lambda p: p.distance_pct)[:5]
        p_lines = [f"<b>Closest pending ({min(pending_n,5)} of {pending_n}):</b>"]
        for i, p in enumerate(closest, 1):
            sym_s   = p.symbol.replace("USDT", "")
            setup_s = (p.setup_type
                       .replace("BREAKOUT_RETEST", "BR")
                       .replace("TREND_PULLBACK", "TP")
                       .replace("LIQUIDITY_SWEEP", "LS"))
            p_lines.append(
                f"  {i}. {sym_s} {p.side} {setup_s} "
                f"dist={p.distance_pct:.2f}% "
                f"px={p.current_price:.4f} "
                f"zone={p.entry_low:.4f}–{p.entry_high:.4f}"
            )
        pending_block = "\n".join(p_lines) + "\n\n"
    else:
        pending_block = ""

    # Dead-candidate summary block (Phase 8G)
    dead_n = len(mkt.candidate_debug)
    if dead_n > 0:
        from collections import Counter as _Ctr
        r_ctr   = _Ctr(_REASON_ABBREV.get(r.reason, r.reason) for r in mkt.candidate_debug)
        det_ctr = _Ctr(_SETUP_ABBREV.get(r.setup_type, r.setup_type) for r in mkt.candidate_debug)
        r_str   = "  ".join(f"{k}={v}" for k, v in r_ctr.most_common())
        det_str = "  ".join(f"{k}={v}" for k, v in sorted(det_ctr.items()))
        recent3 = list(reversed(mkt.candidate_debug))[:3]
        top3    = "\n".join(
            f"  {i}. {r.symbol.replace('USDT','')} {r.side} "
            f"{_SETUP_ABBREV.get(r.setup_type, r.setup_type)} "
            f"reason={_REASON_ABBREV.get(r.reason, r.reason)} "
            f"score={r.score} age={r.setup_age_h}h"
            for i, r in enumerate(recent3, 1)
        )
        matrix = dead_candidate_matrix(mkt.candidate_debug, limit=6)
        matrix_lines = "\n".join(
            f"  {det} × {reason} × {age}: {count}"
            for det, reason, age, count in matrix
        ) or "  —"
        dead_block = (
            f"<b>Recent dead candidates: {dead_n} unique stored</b>\n"
            f"Dead reasons: {r_str}\n"
            f"Dead detectors: {det_str}\n"
            f"<b>Dead matrix (detector × reason × age):</b>\n{matrix_lines}\n"
            f"Recent dead:\n{top3}\n\n"
        )
    else:
        dead_block = ""

    store = _diag_store(app)
    if store is not None:
        try:
            dbs = store.summary()
            db_block = (
                f"<b>Raw DB:</b> {dbs['total']} unique · "
                f"score_fail={dbs['score_fail']} · signals={dbs['signal_emitted']} · "
                f"WAIT={dbs['statuses'].get('WAITING_ENTRY',0)} · "
                f"ACTIVE={dbs['statuses'].get('ACTIVE',0)} · "
                f"DONE={dbs['statuses'].get('DONE',0)}\n\n"
            )
        except Exception as exc:
            _diag_warn(f"/diag DB summary failed: {type(exc).__name__}: {exc}")
            db_block = "<b>Raw DB:</b> unavailable ⚠️\n\n"
    else:
        db_block = ("<b>Raw DB:</b> disabled\n\n" if not DIAGNOSTICS_DB_ENABLED
                    else "<b>Raw DB:</b> unavailable ⚠️\n\n")

    await tg.send(cid, (
        f"🔬 <b>Diagnostics</b>\n\n"
        f"<b>Mode:</b> {mode}\n"
        f"<b>Last poll:</b> {poll_ago}  (#{mkt.poll_count})\n"
        f"<b>Ready:</b> {ready}/{len(mkt.symbols)}\n"
        f"<b>Active ideas:</b> {active}\n"
        f"<b>Pending setups:</b> {pending_n}\n"
        f"<b>BTC regime:</b> {_regime_emoji(mkt.btc_regime)} {mkt.btc_regime}\n"
        f"<b>BTC last scan:</b> {btc_scan_ago}\n"
        f"<b>Not ready (≤10):</b> {nr_str}\n\n"
        f"{pending_block}"
        f"{dead_block}"
        f"{db_block}"
        f"<b>Last-cycle scan (since keepalive reset):</b>\n"
        f"  checked={d.symbols_checked}  "
        f"not_ready={d.symbols_not_ready}  "
        f"lock={d.active_idea_lock}\n"
        f"  candidates: total={d.candidates_total}  "
        f"action={d.candidates_actionable}  "
        f"pend={d.candidates_pending}  "
        f"dead={d.candidates_dead}\n"
        f"  detector_none={d.detector_none}  "
        f"ctx_old={d.context_too_old}  "
        f"price_miss={d.price_missing}\n"
        f"  outside_zone={d.outside_entry_zone}  "
        f"hit_tp={d.already_hit_tp}  "
        f"hit_sl={d.already_hit_sl}\n"
        f"  tpsl_fail={d.tpsl_fail}  "
        f"rr_curr={d.rr_current_fail}  "
        f"ls_old={d.liquidity_sweep_too_old}  "
        f"entry_old={d.entry_retest_too_old}  "
        f"4h_conflict={d.fast_4h_conflict}  "
        f"sl_cooldown={d.post_sl_cooldown}  "
        f"reused_stop={d.reused_stopped_setup}  "
        f"secondary_low={d.secondary_score_fail}  "
        f"debug_dedup={d.candidate_debug_dedup}  "
        f"gate_fail={d.signal_gate_fail}\n"
        f"  actionable_ok={d.actionable_ok}  "
        f"new_idea={d.new_idea}  "
        f"errors={d.errors}\n\n"
        f"<b>Since startup (total):</b>\n"
        f"  checked={t.symbols_checked}  "
        f"candidates_total={t.candidates_total}  "
        f"detector_none={t.detector_none}\n"
        f"  ctx_old={t.context_too_old}\n"
        f"  outside_zone={t.outside_entry_zone}  "
        f"hit_tp={t.already_hit_tp}  "
        f"hit_sl={t.already_hit_sl}\n"
        f"  rr_curr={t.rr_current_fail}  "
        f"ls_old={t.liquidity_sweep_too_old}  "
        f"entry_old={t.entry_retest_too_old}  "
        f"4h_conflict={t.fast_4h_conflict}  "
        f"sl_cooldown={t.post_sl_cooldown}  "
        f"reused_stop={t.reused_stopped_setup}  "
        f"secondary_low={t.secondary_score_fail}  "
        f"debug_dedup={t.candidate_debug_dedup}  "
        f"gate_fail={t.signal_gate_fail}  "
        f"actionable_ok={t.actionable_ok}  "
        f"new={t.new_idea}\n\n"
        f"<b>Current gates (Phase 8D/8E/8H/8K/8L/8L.2/8L.4):</b>\n"
        f"  Context max: {SETUP_CONTEXT_MAX_DAYS}d  "
        f"(legacy fresh: {SETUP_MAX_AGE_HOURS}h)\n"
        f"  LS max age: {LIQUIDITY_SWEEP_MAX_AGE_HOURS}h  "
        f"(BR/TP not restricted)\n"
        f"  LS fresh entry retest: after {LS_ENTRY_RETEST_REQUIRED_AFTER_HOURS}h, "
        f"return ≤ {ENTRY_RETEST_MAX_AGE_HOURS}h\n"
        f"  Entry zone required: {'yes' if ENTRY_ZONE_REQUIRED else 'no'}\n"
        f"  RR from current price: {'yes' if RR_FROM_CURRENT_PRICE else 'no'}\n"
        f"  TP1 RR hard gate: off\n"
        f"  RR TP2 min: Tier1 {RR_MIN_TIER1} / Tier2 {RR_MIN_TIER2}\n"
        f"  Score floor: normal {MIN_SCORE_NORMAL} / chop {MIN_SCORE_CHOP}\n"
        f"  4H alignment: {'required' if REQUIRE_4H_TREND_ALIGNMENT else 'off'}\n"
        f"  Post-SL lock: {POST_SL_COOLDOWN_HOURS}h / repeated {REPEATED_SL_LOCK_HOURS}h\n"
        f"  Post-confirmation TP/SL boundary: enabled\n"
        f"  Watchlist: signal-eligible only"
    ))


async def _cmd_score(app: web.Application, cid: int, sym: str) -> None:
    tg:  Tg     = app["tg"]
    mkt: Market = app["mkt"]

    if not sym.endswith("USDT"):
        sym += "USDT"
    state = mkt.state.get(sym)
    if state is None or state.active_idea is None:
        await tg.send(cid, f"No active idea for <b>{sym}</b>.")
        return

    idea = state.active_idea
    breakdown = ""
    store = _diag_store(app)
    if store is not None and idea.setup_ts > 0:
        key = f"{sym}|{idea.setup_type}|{idea.side}|{int(idea.setup_ts)}"
        try:
            row = store.conn.execute(
                "SELECT score_components_first FROM raw_setups WHERE setup_key=?",
                (key,),
            ).fetchone()
            if row is not None:
                comps = json.loads(row["score_components_first"] or "{}")
                if comps:
                    breakdown = "\n" + "\n".join(
                        f"  +{int(points)} {html.escape(name.replace('_',' '))}"
                        for name, points in sorted(
                            comps.items(), key=lambda kv: (-int(kv[1]), kv[0])
                        )
                    )
        except Exception as exc:
            _diag_warn(f"/score breakdown failed {sym}: {type(exc).__name__}: {exc}")
    await tg.send(cid, (
        f"📐 <b>Score: {sym}</b>\n"
        f"Setup: {idea.setup_type.replace('_',' ')}\n"
        f"Score: {idea.setup_score}/100"
        f"{breakdown}"
    ))


# =============================================================================
# === 17. KEEPALIVE / WATCHDOG ===
# =============================================================================

async def keepalive_loop(app: web.Application) -> None:
    """Periodic log heartbeat with a compact bot health summary."""
    while True:
        await asyncio.sleep(KEEPALIVE_SEC)
        mkt: Market = app["mkt"]
        ready    = sum(1 for s in mkt.symbols if mkt.state[s].ready)
        active   = sum(1 for s in mkt.symbols if mkt.state[s].active_idea is not None)
        poll_ago = now_s() - mkt.last_poll_ts if mkt.last_poll_ts else -1
        d = mkt.diag_last
        matrix = dead_candidate_matrix(mkt.candidate_debug, limit=3)
        matrix_log = ";".join(
            f"{det}x{reason}x{age}={count}"
            for det, reason, age, count in matrix
        ) or "none"
        logger.info(
            f"Keepalive | Mode: {'DRY RUN' if DRY_RUN_MODE else 'LIVE'} | "
            f"BTC: {mkt.btc_regime} | "
            f"Ready: {ready}/{len(mkt.symbols)} | "
            f"Active ideas: {active} | "
            f"Pending: {len(mkt.pending_setups)} | "
            f"Dead debug: {len(mkt.candidate_debug)} | "
            f"Polls: {mkt.poll_count} | "
            f"Last poll: {poll_ago}s ago | "
            f"Ideas: {mkt.signal_stats['total']} "
            f"(TP2:{mkt.signal_stats['tp2_hit']} SL:{mkt.signal_stats['sl_hit']} "
            f"Amb:{mkt.signal_stats['ambiguous']}) | "
            f"Scan diag: checked={d.symbols_checked} "
            f"not_ready={d.symbols_not_ready} lock={d.active_idea_lock} "
            f"detector_none={d.detector_none} "
            f"cands: total={d.candidates_total} "
            f"action={d.candidates_actionable} "
            f"pend={d.candidates_pending} "
            f"dead={d.candidates_dead} | "
            f"ctx_old={d.context_too_old} "
            f"price_miss={d.price_missing} outside_zone={d.outside_entry_zone} "
            f"hit_tp={d.already_hit_tp} hit_sl={d.already_hit_sl} "
            f"tpsl_fail={d.tpsl_fail} rr_curr={d.rr_current_fail} "
            f"ls_old={d.liquidity_sweep_too_old} "
            f"entry_old={d.entry_retest_too_old} "
            f"4h_conflict={d.fast_4h_conflict} "
            f"sl_cooldown={d.post_sl_cooldown} "
            f"reused_stop={d.reused_stopped_setup} "
            f"secondary_low={d.secondary_score_fail} "
            f"debug_dedup={d.candidate_debug_dedup} "
            f"gate_fail={d.signal_gate_fail} actionable_ok={d.actionable_ok} "
            f"new={d.new_idea} errors={d.errors} | "
            f"Dead matrix(unique): {matrix_log}"
        )
        store = _diag_store(app)
        if store is not None:
            try:
                st = store.summary()
                det = st["detectors"]
                logger.info(
                    "DiagDB | "
                    f"raw={st['total']} "
                    f"BR={det.get('BR',{}).get('n',0)} "
                    f"TP={det.get('TP',{}).get('n',0)} "
                    f"LS={det.get('LS',{}).get('n',0)} | "
                    f"score_fail={st['score_fail']} signals={st['signal_emitted']} | "
                    f"waiting={st['statuses'].get('WAITING_ENTRY',0)} "
                    f"active={st['statuses'].get('ACTIVE',0)} "
                    f"done={st['statuses'].get('DONE',0)} | "
                    f"outcomes={st['outcomes']}"
                )
                brs = store.detector_stage_summary("BR")
                tps = store.detector_stage_summary("TP")
                logger.info(
                    "DeepDiag | "
                    f"BR raw-sides={brs.get('LONG',{}).get('raw_result',0)+brs.get('SHORT',{}).get('raw_result',0)} "
                    f"retest_fail={brs.get('LONG',{}).get('terminal_retest_fail',0)+brs.get('SHORT',{}).get('terminal_retest_fail',0)} | "
                    f"TP raw-sides={tps.get('LONG',{}).get('raw_result',0)+tps.get('SHORT',{}).get('raw_result',0)} "
                    f"duration_fail={tps.get('LONG',{}).get('terminal_pullback_duration_fail',0)+tps.get('SHORT',{}).get('terminal_pullback_duration_fail',0)} "
                    f"4h_fail={tps.get('LONG',{}).get('terminal_4h_close_confirm_fail',0)+tps.get('SHORT',{}).get('terminal_4h_close_confirm_fail',0)}"
                )
            except Exception as exc:
                _diag_warn(f"keepalive summary failed: {type(exc).__name__}: {exc}")
        # Reset last-cycle counters for the next keepalive window
        mkt.diag_last = ScanDiagnostics()


async def watchdog_loop(app: web.Application) -> None:
    """
    Exit the process if REST polling has stalled for STALL_EXIT_SEC seconds.
    The host process manager (Render, Docker restart policy) restarts the bot.
    A first poll cycle must complete before the watchdog starts checking
    (last_poll_ts == 0 is ignored).
    """
    while True:
        await asyncio.sleep(WATCHDOG_SEC)
        mkt: Market = app["mkt"]
        if mkt.last_poll_ts > 0 and now_s() - mkt.last_poll_ts > STALL_EXIT_SEC:
            logger.error(
                f"Poll stalled >{STALL_EXIT_SEC}s — exiting for host restart"
            )
            os._exit(1)


# =============================================================================
# === 18. APP STARTUP / CLEANUP ===
# =============================================================================

async def on_startup(app: web.Application) -> None:
    setup_logging(LOG_LEVEL)
    logger.info(
        "🚀 Starting CryptoBot v18 — Weekly Swing "
        "(Phases 3–6 active · Phase 7 dry-run/hardening · "
        "Phase 8A freshness/entry gate · Phase 8B.1 channel-safe send · "
        "Phase 8C gate diagnostics · Phase 8D actionable swing validation · "
        "Phase 8E pending setup watchlist · Phase 8F actionable candidate selection · "
        "Phase 8G dead candidate diagnostics · Phase 8H Liquidity Sweep recency gate · "
        "Phase 8I candidate debug dedup · Phase 8J TP/SL percentage ranges · "
        "Phase 8K fresh entry retest gate · Phase 8L signal-eligible watchlist · "
        "Phase 8L.1 temporal/lifecycle/quality hotfix · "
        "Phase 8L.2 post-confirmation timing/diagnostics hotfix · "
        "Phase 8L.3 signal-flow rollback · "
        "Phase 8L.4.3 persistent raw + BR/TP deep + BR shadow + TP stats analyzer · "
        "Phase 8M calibration review · Phase 9A Bybit RSA read-only bridge · Phase 9B minimum-size execution planner · Phase 9C net PnL + expiry safety shadow)"
    )

    # ── Startup safety warnings ───────────────────────────────────────────────
    if not TELEGRAM_TOKEN:
        logger.warning("⚠️  TELEGRAM_TOKEN is empty — Telegram sends will fail silently")
    if not get_broadcast_targets():
        logger.warning("⚠️  No broadcast targets configured (PRIMARY_RECIPIENTS and ALLOWED_CHAT_IDS are both empty)")
    if not DRY_RUN_MODE:
        logger.warning("⚠️  DRY_RUN_MODE=False — bot is in LIVE SIGNALS mode")

    http        = aiohttp.ClientSession()
    app["http"] = http
    app["tg"]   = Tg(TELEGRAM_TOKEN, http)
    app["rest"] = BybitRest(BYBIT_REST, http)

    # Phase 9A authenticated bridge is non-critical and GET-only.  Failure here
    # never blocks public market polling or the existing signal engine.
    app["bybit_private"] = None
    bybit_bridge_status: Dict[str, Any] = {"configured": False, "api_ok": False}
    if BYBIT_PRIVATE_READONLY_ENABLED:
        if not BYBIT_API_KEY:
            logger.warning("Phase 9A Bybit bridge disabled: BYBIT_API_KEY is empty")
        elif not BYBIT_PRIVATE_KEY_PATH:
            logger.warning("Phase 9A Bybit bridge disabled: BYBIT_PRIVATE_KEY_PATH is empty")
        else:
            private_client = BybitPrivateReadOnly(
                BYBIT_REST, http, BYBIT_API_KEY, BYBIT_PRIVATE_KEY_PATH, BYBIT_RECV_WINDOW
            )
            app["bybit_private"] = private_client
            bybit_bridge_status["configured"] = True
            try:
                snap = await private_client.health_snapshot()
                info = snap.get("api_key_info") or {}
                wallet = snap.get("wallet") or {}
                positions = snap.get("positions") or []
                errors = snap.get("errors") or {}
                bybit_bridge_status["api_ok"] = bool(info)
                bybit_bridge_status["errors"] = errors
                if info:
                    exp = bybit_api_expiry(info)
                    bybit_bridge_status["days_left"] = exp.get("days_left")
                if wallet:
                    bybit_bridge_status["equity"] = _safe_float(wallet.get("totalEquity"))
                bybit_bridge_status["positions"] = len(positions)
                logger.info(
                    "Phase 9A Bybit read-only bridge | "
                    f"api={'OK' if info else 'FAIL'} "
                    f"days={bybit_bridge_status.get('days_left', 'n/a')} "
                    f"equity={bybit_bridge_status.get('equity', 'n/a')} "
                    f"positions={len(positions)} "
                    f"partial_errors={list(errors)}"
                )
            except Exception as exc:
                bybit_bridge_status["errors"] = {"startup": f"{type(exc).__name__}: {exc}"}
                logger.warning(
                    f"Phase 9A Bybit bridge startup check failed: {type(exc).__name__}: {exc}"
                )

    # Phase 8L.4 diagnostics are deliberately non-critical: failure to open
    # the DB never prevents the trading bot from starting.
    app["diag_store"] = None
    if DIAGNOSTICS_DB_ENABLED:
        try:
            app["diag_store"] = DiagnosticStore(DIAGNOSTICS_DB_PATH)
            logger.info(
                f"Phase 8L.4.3 Diagnostic DB ready path={DIAGNOSTICS_DB_PATH} "
                f"outcome_days={DIAGNOSTICS_OUTCOME_DAYS}"
            )
        except Exception as exc:
            logger.warning(
                f"Phase 8L.4.3 Diagnostic DB disabled after open failure: "
                f"{type(exc).__name__}: {exc}"
            )

    if TELEGRAM_TOKEN:
        try:
            await app["tg"].delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.warning(f"delete_webhook failed, continuing startup: {e}")
    else:
        logger.warning("TELEGRAM_TOKEN empty — skipping delete_webhook")

    # 1. Validate universe (check all 15 symbols are active on Bybit)
    valid_syms = await validate_universe(app["rest"])

    # 2. Build Market (one SymbolState per symbol)
    mkt        = Market(
        symbols=valid_syms,
        state={sym: SymbolState() for sym in valid_syms},
    )
    app["mkt"] = mkt

    # 3. Preload all 5 TFs for all symbols; compute initial indicators and regime
    ready_count = await preload_all(app["rest"], mkt)

    # 4. Set BTC global regime from BTCUSDT state
    if "BTCUSDT" in mkt.state:
        mkt.btc_regime        = mkt.state["BTCUSDT"].regime
        mkt.btc_regime_reason = mkt.state["BTCUSDT"].regime_reason

    # 5. Start background tasks
    # runtime_state is a plain mutable dict used by report_error() and other
    # helpers that need to persist small values without mutating the app mapping
    # after startup (which triggers aiohttp DeprecationWarning).
    app["runtime_state"] = {
        "last_error_ts": 0,
        "bybit_bridge_status": bybit_bridge_status,
        "apikey_reminder_state": _load_apikey_reminder_state(
            BYBIT_API_REMINDER_STATE_PATH
        ),
        # Phase 9C persists the shadow execution lifecycle across redeploys so
        # EXPIRED_WAIT_EXIT and reserved test margin are not silently forgotten.
        "execution_shadow_reservations": _load_execution_shadow_state(
            EXECUTION_SHADOW_STATE_PATH
        ),
    }
    app["poll_task"]      = asyncio.create_task(poll_loop(app))
    app["tg_task"]        = asyncio.create_task(tg_loop(app))
    app["watchdog_task"]  = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))
    app["apikey_task"]    = None
    if isinstance(app.get("bybit_private"), BybitPrivateReadOnly):
        # Immediate threshold check, then periodic 6h/default checks.
        try:
            await maybe_send_apikey_expiry_reminder(app)
        except Exception as exc:
            logger.warning(
                f"Phase 9A initial expiry reminder check failed: {type(exc).__name__}: {exc}"
            )
        app["apikey_task"] = asyncio.create_task(apikey_reminder_loop(app))

    # 6. Startup notification to Telegram
    btc_e   = _regime_emoji(mkt.btc_regime)
    bullish = sum(1 for s in mkt.symbols if mkt.state[s].regime == "BULLISH")
    bearish = sum(1 for s in mkt.symbols if mkt.state[s].regime == "BEARISH")
    neutral = sum(1 for s in mkt.symbols if mkt.state[s].regime == "NEUTRAL")
    mode_line = "🧪 <b>DRY RUN</b>" if DRY_RUN_MODE else "✅ <b>LIVE SIGNALS</b>"

    for chat_id in get_broadcast_targets():
        with contextlib.suppress(Exception):
            await app["tg"].send(chat_id, (
                f"🟢 <b>CryptoBot v18 — Weekly Swing Online</b>\n\n"
                f"<b>Mode:</b> {mode_line}\n"
                f"<b>BTC Global Regime:</b> {btc_e} {mkt.btc_regime}\n"
                f"<i>{html.escape(mkt.btc_regime_reason)}</i>\n\n"
                f"<b>Universe:</b> {len(mkt.symbols)} symbols\n"
                f"<b>Ready:</b> {ready_count}/{len(mkt.symbols)}\n"
                f"<b>Regimes:</b> 🟢{bullish}  🔴{bearish}  🟡{neutral}\n\n"
                f"<b>Polling:</b> 1H/5m · 4H/15m · 1D/1h · 1W/4h · 1M/1d ✅\n"
                f"<b>Phase 3</b> detectors: BR + TP + LS active ✅\n"
                f"<b>Phase 4</b> TP/SL engine + RR gate: active ✅\n"
                f"<b>Phase 5</b> ActiveIdea lifecycle: active ✅\n"
                f"<b>Phase 6</b> Telegram signal formatting: active ✅\n"
                f"<b>Phase 7</b> dry-run/config/diag hardening: active ✅\n"
                f"<b>Phase 8A</b> entry-zone/current-price foundation: active ✅\n"
                f"<b>Phase 8B.1</b> channel-safe Telegram send: active ✅\n"
                f"<b>Phase 8C</b> gate diagnostics: active ✅\n"
                f"<b>Phase 8D</b> actionable swing validation: active ✅\n"
                f"<b>Phase 8E</b> watchlist / pending setups: active ✅\n"
                f"<b>Phase 8F</b> actionable candidate selection: active ✅\n"
                f"<b>Phase 8G</b> dead candidate diagnostics: active ✅\n"
                f"<b>Phase 8H</b> Liquidity Sweep recency gate: active ✅\n"
                f"<b>Phase 8I</b> candidate debug dedup: active ✅\n"
                f"<b>Phase 8J</b> TP/SL percentage ranges: active ✅\n"
                f"<b>Phase 8K</b> fresh entry retest gate: active ✅\n"
                f"<b>Phase 8L</b> signal-eligible watchlist: active ✅\n"
                f"<b>Phase 8L.1</b> temporal/lifecycle/quality hotfix: active ✅\n"
                f"<b>Phase 8L.2</b> post-confirmation timing + dead matrix + Tg polling logs: active ✅\n"
                f"<b>Phase 8L.3</b> Phase-8L signal-flow rollback: active ✅\n"
                f"<b>Phase 8L.4.3</b> persistent raw + BR/TP deep + BR shadow + TP stats analyzer: "
                f"{'active ✅' if _diag_store(app) is not None else 'unavailable ⚠️'}\n"
                f"<b>Phase 8M</b> calibration review / cohort readiness: "
                f"{'active ✅' if _diag_store(app) is not None else 'unavailable ⚠️'}\n"
                f"<b>Phase 9A</b> Bybit RSA read-only bridge: "
                f"{'connected ✅' if bybit_bridge_status.get('api_ok') else ('configured ⚠️' if bybit_bridge_status.get('configured') else 'off ⚪')}\n"
                f"<b>Phase 9B</b> minimum-size execution planner: "
                f"{'active ✅' if EXECUTION_PLANNER_ENABLED else 'off ⚪'} · "
                f"{EXECUTION_PLANNER_LEVERAGE:.1f}x plan · {EXECUTION_PLANNER_RESERVE_PCT:.0f}% reserve · 50/50 · no orders\n"
                f"<b>Phase 9C</b> net PnL + expiry safety shadow: "
                f"{'active ✅' if EXECUTION_ECONOMICS_ENABLED else 'off ⚪'} · "
                f"fees/funding estimates · EXPIRED_WAIT_EXIT · persistent shadow state\n\n"
                f"Commands: /status /regime /ideas /idea SYMBOL "
                f"/close SYMBOL /config /diag /apikey /bybit /plan SYMBOL /execshadow /statsdb /brtp /tpdiag /brshadow /calibration /watchlist /candidates"
    ))


async def on_cleanup(app: web.Application) -> None:
    for key in ("poll_task", "tg_task", "watchdog_task", "keepalive_task", "apikey_task"):
        task = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    store = _diag_store(app)
    if store is not None:
        with contextlib.suppress(Exception):
            store.close()
    if "http" in app:
        await app["http"].close()


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


def _selftest_phase_8l1_quality_hotfix() -> None:
    """Deterministic checks for the critical Phase 8L.1 safety helpers."""
    base_ts = 1_000_000_000_000
    idea = ActiveIdea(
        symbol="TESTUSDT", side="LONG", setup_type="LIQUIDITY_SWEEP", setup_score=80,
        entry_low=100.0, entry_high=101.0, stop_loss=95.0,
        tp1=105.0, tp2=110.0, rr_tp1=1.0, rr_tp2=2.0,
        status="ACTIVE", emitted_at=base_ts // 1000, expires_at=base_ts // 1000 + 86400,
        invalidation="test", setup_ts=base_ts - 3600000,
    )
    state = SymbolState(bars_1h=[
        (base_ts - 1800000, 100, 120, 90, 100, 1),  # contains emission: ignored
        (base_ts + 3600000, 100, 104, 99, 103, 1),
    ])
    assert len(_lifecycle_bars_after_emission(state, idea)) == 1

    ambiguous_bar: Bar = (base_ts + 3600000, 100, 111, 94, 100, 1)
    assert _idea_bar_hits(idea, ambiguous_bar) == (True, True, True)

    rr = SetupResult(
        setup_type="BREAKOUT_RETEST", side="LONG", score=80,
        entry_low=100, entry_high=101, stop_loss=95, tp1=105, tp2=110,
        rr_tp1=1.1, rr_tp2=2.1, invalidation="test", setup_ts=base_ts,
    )
    assert passes_rr_gate(rr, "BTCUSDT")
    assert passes_rr_gate(rr, "SOLUSDT")

    stopped = SymbolState(
        last_exit_ts=base_ts // 1000, last_exit_event="SL_HIT",
        last_stopped_side="LONG", post_sl_lock_until=base_ts // 1000 + 3600,
        stopped_setup_keys={(rr.setup_type, rr.setup_ts)},
    )
    assert validate_post_sl_reentry(stopped, rr)[1] == "reused_stopped_setup"

    # A 1D sweep cannot be confirmed by an intraday bar from the same day.
    old_flag = globals()["REQUIRE_LS_REVERSAL_CONFIRM"]
    globals()["REQUIRE_LS_REVERSAL_CONFIRM"] = False
    try:
        same_day = (base_ts + 4 * 3600000, 100, 103, 99, 102, 1)
        next_day = (base_ts + 86400000, 100, 103, 99, 102, 1)
        found = _find_sweep_confirmation(
            "LONG", base_ts, "1d", 101.0, [same_day, next_day], []
        )
        assert found is not None and found[0][B_TS] == next_day[B_TS]
    finally:
        globals()["REQUIRE_LS_REVERSAL_CONFIRM"] = old_flag


def _selftest_phase_8l2_post_confirmation_boundary() -> None:
    """Regression test: confirmation candle/pre-close movement must not kill a setup."""
    now = now_ms()
    hour = 3_600_000
    setup_open = now - 8 * hour
    result = SetupResult(
        setup_type="TREND_PULLBACK", side="LONG", score=80,
        entry_low=100.0, entry_high=102.0, stop_loss=95.0,
        tp1=106.0, tp2=110.0, rr_tp1=1.2, rr_tp2=2.0,
        invalidation="test", setup_ts=setup_open, setup_tf="4h",
    )

    state = SymbolState(bars_1h=[
        # These bars belong to the 4H confirmation candle.  Their extremes
        # happened before the setup was knowable and MUST be ignored.
        (setup_open + 1 * hour, 101.0, 120.0, 90.0, 101.0, 1.0),
        (setup_open + 3 * hour, 101.0, 108.0, 96.0, 101.0, 1.0),
        # First eligible bar starts exactly when the 4H confirmation closes.
        (setup_open + 4 * hour, 101.0, 103.0, 99.0, 101.0, 1.0),
        (setup_open + 5 * hour, 101.0, 104.0, 100.0, 101.0, 1.0),
    ])
    ok, reason = validate_actionable_setup(result, state, 101.0)
    assert ok and reason == "ok"

    # A genuine post-confirmation TP touch must still kill the candidate.
    state.bars_1h.append(
        (setup_open + 6 * hour, 101.0, 107.0, 100.0, 106.5, 1.0)
    )
    ok, reason = validate_actionable_setup(result, state, 101.0)
    assert not ok and reason == "already_hit_tp"

    # Diagnostic matrix should expose detector × reason × age.
    dbg = CandidateDebug(
        symbol="TESTUSDT", side="LONG", setup_type="TREND_PULLBACK",
        score=80, status="DEAD", reason="already_hit_tp",
        current_price=101.0, entry_low=100.0, entry_high=102.0,
        stop_loss=95.0, tp1=106.0, tp2=110.0, rr_tp1=1.2, rr_tp2=2.0,
        setup_ts=setup_open, setup_age_h=4, updated_at=now_s(),
        regime="BULLISH", btc_regime="BULLISH",
    )
    matrix = dead_candidate_matrix([dbg])
    assert matrix == [("TP", "hit_tp", "<24h", 1)]


def _selftest_phase_8l4_diagnostic_store() -> None:
    """In-memory regression test for raw dedup, score components and outcomes."""
    store = DiagnosticStore(":memory:")
    try:
        hour = 3_600_000
        base = (now_ms() // hour) * hour - 4 * hour
        state = SymbolState(
            bars_1h=[
                (base, 100.0, 101.0, 99.0, 100.0, 1.0),
                (base + hour, 100.0, 101.0, 99.0, 100.0, 1.0),
                # treated as forming at first observation; current px in zone
                (base + 2 * hour, 100.0, 101.0, 99.0, 100.5, 1.0),
            ]
        )
        state.regime = "BULLISH"
        raw = SetupResult(
            setup_type="BREAKOUT_RETEST", side="LONG", score=45,
            entry_low=99.0, entry_high=101.0, stop_loss=95.0,
            tp1=105.0, tp2=110.0, rr_tp1=1.0, rr_tp2=2.0,
            invalidation="test", setup_ts=base, setup_tf="1h",
            score_components={"factor_a": 20, "factor_b": 25},
        )
        store.record_detector_scan([raw], 55)
        trace = DetectorStageTrace("BR")
        trace.bump("LONG", "calls")
        trace.bump("LONG", "broken_swing", 2)
        trace.finish("LONG", "retest_fail")
        store.record_stage_traces("BTCUSDT", [trace])
        assert store.detector_stage_summary("BR")["LONG"]["calls"] == 1
        assert store.detector_stage_summary("BR")["LONG"]["terminal_retest_fail"] == 1
        key = store.upsert_raw_setup("BTCUSDT", state, "BULLISH", raw, raw, 55)
        # Same setup must update, not duplicate.
        store.upsert_raw_setup("BTCUSDT", state, "BULLISH", raw, raw, 55)
        store.update_gate(key, "DEAD", "score_floor_fail")
        assert store.summary()["total"] == 1
        assert store.summary()["score_fail"] == 1
        assert store.conn.execute(
            "SELECT COUNT(*) FROM setup_score_components WHERE setup_key=?", (key,)
        ).fetchone()[0] == 2

        # One newly closed post-observation bar reaches TP2 without touching SL.
        state.bars_1h = [
            (base, 100.0, 101.0, 99.0, 100.0, 1.0),
            (base + hour, 100.0, 101.0, 99.0, 100.0, 1.0),
            (base + 2 * hour, 100.0, 111.0, 99.0, 110.0, 1.0),
            (base + 3 * hour, 110.0, 111.0, 109.0, 110.0, 1.0),
        ]
        store.update_outcomes_for_symbol("BTCUSDT", state)
        row = store.conn.execute(
            "SELECT final_outcome,observation_status FROM raw_setups WHERE setup_key=?",
            (key,),
        ).fetchone()
        assert row["final_outcome"] == "TP2"
        assert row["observation_status"] == "DONE"
    finally:
        store.close()


def _selftest_phase_8l42_br_shadow_store() -> None:
    """Shadow rows must persist separately and never pollute raw-setups totals."""
    store = DiagnosticStore(":memory:")
    try:
        state = SymbolState()
        ts = 1_700_000_000_000
        # Two closed bars + one forming bar. First closed bar touches entry,
        # second reaches TP2 without touching shadow SL.
        state.bars_1h = [
            (ts, 100.0, 100.4, 99.8, 100.1, 1.0),
            (ts + 3_600_000, 100.1, 103.0, 100.0, 102.5, 1.0),
            (ts + 7_200_000, 102.5, 102.6, 102.4, 102.5, 1.0),
        ]
        r = SetupResult(
            setup_type="BREAKOUT_RETEST", side="LONG", score=65,
            entry_low=99.5, entry_high=100.5, stop_loss=99.25,
            tp1=101.0, tp2=102.0, rr_tp1=1.0, rr_tp2=2.0,
            invalidation="test", setup_ts=ts, setup_tf="1h",
            score_components={"test": 65},
        )
        sh = BRShadowCandidate(
            result=r, breakout_ts_ms=ts - 86_400_000, retest_ts_ms=ts,
            retest_tf="1h", key_level=100.0, original_stop_loss=99.7,
            shadow_stop_loss=99.25, geometry_gap_abs=0.2,
            geometry_gap_atr=0.1,
        )
        store.upsert_br_shadow("BTCUSDT", state, "BULLISH", sh)
        assert store.conn.execute("SELECT COUNT(*) FROM br_shadow_setups").fetchone()[0] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM raw_setups").fetchone()[0] == 0
        # Re-upsert dedups.
        store.upsert_br_shadow("BTCUSDT", state, "BULLISH", sh)
        assert store.conn.execute("SELECT COUNT(*) FROM br_shadow_setups").fetchone()[0] == 1
        x = store.br_shadow_summary()
        assert x["total"] == 1
    finally:
        store.close()


def _selftest_phase_8l43_tp_statistics_helpers() -> None:
    """Regression test for pullback-length and score-bucket outcome analytics."""
    c = {
        "trend_context_pass": 10,
        "pullback_len_1": 3,
        "pullback_len_2": 2,
        "pullback_len_8": 1,
    }
    line = _tp_pullback_length_line("LONG", c)
    assert "0D=4" in line and "1D=3" in line and "2D=2" in line and "8+D=1" in line

    raw = {
        "bucket_stats": {
            "<45": {
                "total": 2,
                "statuses": {"DONE": 1, "ACTIVE": 1},
                "outcomes": {"SL": 1},
            },
            "65-74": {
                "total": 3,
                "statuses": {"WAITING_ENTRY": 1, "ACTIVE": 1, "DONE": 1},
                "outcomes": {"TP2": 1},
            },
        }
    }
    lines = _tp_score_bucket_outcome_lines(raw)
    assert any("&lt;45" in x and "SL=1" in x for x in lines)
    assert any("65-74" in x and "TP2=1" in x for x in lines)



def _selftest_phase_8m_calibration_helpers() -> None:
    rows = [
        {
            "symbol": "AAAUSDT", "observation_status": "DONE",
            "final_outcome": "TP2", "regime_first": "NEUTRAL",
            "btc_regime_first": "BULLISH", "mfe_r": 2.4, "mae_r": 0.3,
            "rr_tp2": 2.1, "first_seen_ts": 1000, "entry_activated_ts": 4600,
            "final_outcome_ts": 11800, "raw_score_first": 68,
            "geometry_gap_atr": 0.18,
        },
        {
            "symbol": "BBBUSDT", "observation_status": "DONE",
            "final_outcome": "ENTRY_BAR_AMBIGUOUS", "regime_first": "NEUTRAL",
            "btc_regime_first": "BULLISH", "mfe_r": 1.0, "mae_r": 0.2,
            "rr_tp2": 2.0, "first_seen_ts": 2000, "entry_activated_ts": 5600,
            "final_outcome_ts": 5600, "raw_score_first": 58,
            "geometry_gap_atr": 0.24,
        },
        {
            "symbol": "AAAUSDT", "observation_status": "ACTIVE",
            "final_outcome": None, "regime_first": "BULLISH",
            "btc_regime_first": "BULLISH", "mfe_r": 0.8, "mae_r": 0.1,
            "rr_tp2": 2.2, "first_seen_ts": 3000, "entry_activated_ts": 6600,
            "final_outcome_ts": None, "raw_score_first": 68,
            "geometry_gap_atr": 0.31,
        },
    ]
    x = _cal_summary(rows)
    assert x["n"] == 3
    assert x["clean_done"] == 1
    assert x["ambiguous_done"] == 1
    assert x["outcomes"]["TP2"] == 1
    assert _cal_score_bucket(68) == "65-74"
    assert _cal_gap_bucket(0.18) == "0.10-0.20"
    assert _cal_gap_bucket(0.31) == ">0.30"
    grouped = _cal_group(rows, lambda r: _cal_score_bucket(r["raw_score_first"]))
    assert grouped["65-74"]["n"] == 2
    assert grouped["55-64"]["n"] == 1


def _selftest_phase_8l41_trace_nonintrusive() -> None:
    """Tracing must not change detector return values on identical state."""
    state = SymbolState()
    br_plain = detect_breakout_retest(state)
    br_trace = DetectorStageTrace("BR")
    br_traced = detect_breakout_retest(state, trace=br_trace)
    assert br_plain == br_traced == None
    tp_plain = detect_trend_pullback(state)
    tp_trace = DetectorStageTrace("TP")
    tp_traced = detect_trend_pullback(state, trace=tp_trace)
    assert tp_plain == tp_traced == None


def _selftest_phase_9a_readonly_bridge() -> None:
    """Deterministic expiry/reminder helper checks; no network or credentials."""
    info = {
        "deadlineDay": 30,
        "expiredAt": "2099-01-01T00:00:00Z",
        "createdAt": "2026-09-06T00:00:00Z",
        "ips": [],
    }
    exp = bybit_api_expiry(info)
    assert exp["status"] == "ACTIVE"
    assert exp["days_left"] == 30
    assert _select_apikey_reminder_threshold(30, set()) == 30
    assert _select_apikey_reminder_threshold(20, {30}) == 21
    assert _select_apikey_reminder_threshold(13, {30, 21}) == 14
    assert _select_apikey_reminder_threshold(6, {30, 21, 14}) == 7
    assert _select_apikey_reminder_threshold(1, {30, 21, 14, 7}) == 1
    assert _select_apikey_reminder_threshold(1, {30, 21, 14, 7, 1}) is None
    ip_info = {"deadlineDay": -2, "expiredAt": "1970-01-01T00:00:00Z", "ips": ["1.2.3.4"]}
    assert bybit_api_expiry(ip_info)["status"] == "ACTIVE_IP_BOUND"


def _selftest_phase_9b_execution_planner() -> None:
    """Minimum sizing must be 50/50-safe and respect reserve/margin."""
    idea = ActiveIdea(
        symbol="TESTUSDT",
        side="LONG",
        setup_type="LIQUIDITY_SWEEP",
        setup_score=80,
        entry_low=99.0,
        entry_high=101.0,
        stop_loss=95.0,
        tp1=105.0,
        tp2=110.0,
        rr_tp1=1.0,
        rr_tp2=2.0,
        status="ACTIVE",
        emitted_at=1,
        expires_at=2,
        invalidation="test",
        current_price_at_signal=100.0,
    )
    instrument = {
        "symbol": "TESTUSDT",
        "status": "Trading",
        "lotSizeFilter": {
            "minOrderQty": "0.01",
            "qtyStep": "0.01",
            "minNotionalValue": "5",
            "maxMktOrderQty": "1000",
        },
        "priceFilter": {"tickSize": "0.1"},
        "leverageFilter": {"maxLeverage": "10"},
    }
    wallet = {"totalEquity": "30", "totalAvailableBalance": "30"}

    p = calculate_execution_plan(idea, instrument, wallet, [], 0.0)
    assert p.status == "EXECUTABLE"
    assert p.tp1_qty == p.tp2_qty
    assert abs(p.qty - (p.tp1_qty + p.tp2_qty)) < 1e-12
    # Each 50% close leg is safe against the $5 notional floor at the
    # worst relevant price, so the total position is at least about $10.
    assert p.notional_usdt >= 10.0
    assert abs(p.leverage - 2.0) < 1e-9
    assert abs(p.reserve_usdt - 3.0) < 1e-9
    assert p.margin_required_usdt <= 27.0
    assert p.loss_at_sl_usdt > 0.0

    p2 = calculate_execution_plan(
        idea,
        instrument,
        wallet,
        [{"symbol": "TESTUSDT", "size": "1", "side": "Buy"}],
        0.0,
    )
    assert p2.status == "SKIPPED_ALREADY_OPEN"

    p3 = calculate_execution_plan(idea, instrument, wallet, [], 26.0)
    assert p3.status == "SKIPPED_NO_MARGIN"


def _selftest_phase_9c_net_economics() -> None:
    """Fee/funding math must be internally consistent and TP1 must not subsidise BE."""
    idea = ActiveIdea(
        symbol="TESTUSDT",
        side="LONG",
        setup_type="LIQUIDITY_SWEEP",
        setup_score=80,
        entry_low=99.0,
        entry_high=101.0,
        stop_loss=95.0,
        tp1=105.0,
        tp2=110.0,
        rr_tp1=1.0,
        rr_tp2=2.0,
        status="ACTIVE",
        emitted_at=1,
        expires_at=1 + 10 * 86400,
        invalidation="test",
        current_price_at_signal=100.0,
    )
    instrument = {
        "symbol": "TESTUSDT",
        "status": "Trading",
        "fundingInterval": 480,
        "lotSizeFilter": {
            "minOrderQty": "0.01",
            "qtyStep": "0.01",
            "minNotionalValue": "5",
            "maxMktOrderQty": "1000",
        },
        "priceFilter": {"tickSize": "0.1"},
        "leverageFilter": {"maxLeverage": "10"},
    }
    wallet = {"totalEquity": "30", "totalAvailableBalance": "30"}
    p = calculate_execution_plan(idea, instrument, wallet, [], 0.0)
    _apply_execution_economics(
        p,
        idea,
        instrument,
        {"fundingRate": "0.0001"},
        {"takerFeeRate": "0.00055", "makerFeeRate": "0.0002"},
    )
    assert p.economics_ready
    assert p.total_gross_profit_usdt > p.total_net_profit_usdt
    assert p.entry_fee_est_usdt > 0
    assert p.funding_cost_est_usdt > 0  # positive rate => LONG pays
    assert p.remaining_break_even_price > p.entry_price
    assert p.sl_net_pnl_usdt < -p.loss_at_sl_usdt

    # SHORT with positive funding should receive funding, not pay it.
    idea_s = ActiveIdea(
        symbol="TESTUSDT",
        side="SHORT",
        setup_type="LIQUIDITY_SWEEP",
        setup_score=80,
        entry_low=99.0,
        entry_high=101.0,
        stop_loss=105.0,
        tp1=95.0,
        tp2=90.0,
        rr_tp1=1.0,
        rr_tp2=2.0,
        status="ACTIVE",
        emitted_at=1,
        expires_at=1 + 10 * 86400,
        invalidation="test",
        current_price_at_signal=100.0,
    )
    ps = calculate_execution_plan(idea_s, instrument, wallet, [], 0.0)
    _apply_execution_economics(
        ps,
        idea_s,
        instrument,
        {"fundingRate": "0.0001"},
        {"takerFeeRate": "0.00055", "makerFeeRate": "0.0002"},
    )
    assert ps.funding_cost_est_usdt < 0
    # A sufficiently large funding credit can move a SHORT net break-even
    # slightly above entry, so direction alone is not a valid invariant.
    assert ps.remaining_break_even_price > 0


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",        handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    _selftest_pct_format_helpers()
    _selftest_entry_retest_helpers()
    _selftest_pending_signal_eligible_watchlist()
    _selftest_phase_8l1_quality_hotfix()
    _selftest_phase_8l2_post_confirmation_boundary()
    _selftest_phase_8l4_diagnostic_store()
    _selftest_phase_8l42_br_shadow_store()
    _selftest_phase_8l43_tp_statistics_helpers()
    _selftest_phase_8m_calibration_helpers()
    _selftest_phase_8l41_trace_nonintrusive()
    _selftest_phase_9a_readonly_bridge()
    _selftest_phase_9b_execution_planner()
    _selftest_phase_9c_net_economics()
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
