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
import contextlib
import html
import json
import logging
import os
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp import web

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

# ── RR minimums (applied to TP2) ──────────────────────────────────────────────
RR_MIN_TIER1 = float(os.getenv("RR_MIN_TIER1", "1.8"))   # BTC, ETH
RR_MIN_TIER2 = float(os.getenv("RR_MIN_TIER2", "2.0"))   # all others

# ── Idea lifecycle ─────────────────────────────────────────────────────────────
MAX_IDEA_DURATION_DAYS = int(os.getenv("MAX_IDEA_DURATION_DAYS", "10"))

# ── Setup scoring thresholds ───────────────────────────────────────────────────
MIN_SCORE_NORMAL               = int(os.getenv("MIN_SCORE_NORMAL",               "55"))
MIN_SCORE_CHOP                 = int(os.getenv("MIN_SCORE_CHOP",                 "85"))
LIQUIDITY_SWEEP_PRIORITY_SCORE = int(os.getenv("LIQUIDITY_SWEEP_PRIORITY_SCORE", "85"))

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
    last = app.setdefault("_last_error_ts", 0)
    if t - last < ERROR_REPORT_COOLDOWN_SEC:
        return
    app["_last_error_ts"] = t
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
                if r.status == 200:
                    return (await r.json()).get("result", [])
        except Exception:
            pass
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
                             #           | "EXPIRED" | "INVALIDATED"
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
    actionable_ok:          int = 0
    new_idea:               int = 0
    errors:                 int = 0


@dataclass
class PendingSetup:
    """
    A valid structural setup whose current price is outside the entry zone.
    Does NOT generate a trade signal — informational/watchlist only (Phase 8E).

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
            if side == "LONG":
                if bar[B_LOW] <= key_level + tolerance and bar[B_CLOSE] > key_level:
                    return (bar, tf_key)
            else:            # SHORT
                if bar[B_HIGH] >= key_level - tolerance and bar[B_CLOSE] < key_level:
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
) -> int:
    """
    Score the Breakout + Retest setup on a 0–90 scale.
    (BTC regime +10 is deferred: detector receives SymbolState, not Market.)

    Factors:
      +20  breakout volume > vol_sma × BREAKOUT_VOL_STRONG (1.5×)
      +15  retest candle is pin bar or engulfing in setup direction
      +15  1D EMA alignment (EMA20 > EMA50 for LONG, EMA20 < EMA50 for SHORT)
      +10  4H EMA20 > EMA50 for LONG (< EMA50 for SHORT)
      +10  retest occurred within 3 daily bars of the breakout
      +10  breakout close not overextended: distance from key_level <= 1%
      +10  retest bar volume < tf_vol_sma × 0.9 (healthy low-volume pullback)
            — compared to 4H vol SMA if retest on 4H, 1H vol SMA if retest on 1H
    """
    score = 0

    # +20: strong breakout volume
    if bo_bar[B_VOLUME] > vol_sma * BREAKOUT_VOL_STRONG:
        score += 20

    # +15: retest candle character
    if side == "LONG":
        if is_bullish_retest_candle(retest_bar, prev_retest):
            score += 15
    else:
        if is_bearish_retest_candle(retest_bar, prev_retest):
            score += 15

    # +15: 1D EMA alignment
    if state.ema20_1d > 0 and state.ema50_1d > 0:
        if side == "LONG" and state.ema20_1d > state.ema50_1d:
            score += 15
        elif side == "SHORT" and state.ema20_1d < state.ema50_1d:
            score += 15

    # +10: 4H EMA alignment
    if state.ema20_4h > 0 and state.ema50_4h > 0:
        if side == "LONG" and state.ema20_4h > state.ema50_4h:
            score += 10
        elif side == "SHORT" and state.ema20_4h < state.ema50_4h:
            score += 10

    # +10: retest within 3 daily bars
    # Deadline: bo_bar_ts (open of breakout day) + 4 days = 3 retest days + the breakout day
    if retest_bar[B_TS] < bo_bar_ts + 4 * 86_400_000:
        score += 10

    # +10: breakout close not overextended (within 1% of key_level)
    if key_level > 0:
        dist_pct = ((bo_bar[B_CLOSE] - key_level) / key_level if side == "LONG"
                    else (key_level - bo_bar[B_CLOSE]) / key_level)
        if dist_pct <= 0.01:
            score += 10

    # +10: retest bar volume is low (healthy pullback)
    # Method: compare retest bar volume to the vol SMA of the same TF.
    # If vol SMA is unavailable (< VOL_SMA_PERIOD bars), this factor is skipped.
    tf_vsma = vol_sma_4h if retest_tf == "4h" else vol_sma_1h
    if tf_vsma > 0 and retest_bar[B_VOLUME] < tf_vsma * 0.9:
        score += 10

    return score


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
    max_idx   = len(closed_1d) - 1
    min_idx   = SWING_LOOKBACK_1D + SWING_PROMINENCE_1D * 2
    # Only scan breakout bars that are recent enough for a retest to still be in window
    scan_floor = max(min_idx, max_idx - BREAKOUT_RETEST_MAX_BARS_1D)

    for bo_idx in range(max_idx, scan_floor - 1, -1):
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

        # ── Breakout candle quality ─────────────────────────────────────────────
        if bo_bar[B_VOLUME] < vol_sma * BREAKOUT_VOL_MIN:
            continue
        if side == "LONG":
            if not candle_closes_upper_pct(bo_bar, 0.40):
                continue
        else:
            if not candle_closes_lower_pct(bo_bar, 0.40):
                continue

        # ── Retest search ───────────────────────────────────────────────────────
        found = _find_retest(side, bo_bar[B_TS], key_level, atr, closed_4h, closed_1h)
        if found is None:
            continue
        retest_bar, retest_tf = found
        prev_retest = _prev_bar(
            closed_4h if retest_tf == "4h" else closed_1h,
            retest_bar[B_TS],
        )

        # ── Score ───────────────────────────────────────────────────────────────
        score = _score_breakout(
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
                continue
            tp1          = entry_mid - TP1_ATR_MULT_BREAKOUT * atr
            tp2          = entry_mid - TP2_ATR_MULT_BREAKOUT * atr
            invalidation = (
                f"Daily close above {key_level:.4f} "
                f"(broken support reverts to support)"
            )

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
        )

    return None


def detect_breakout_retest(state: SymbolState) -> Optional[SetupResult]:
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
        return None

    # Pre-compute TF-specific vol SMAs for retest-volume scoring
    vol_sma_4h = (calc_vol_sma(closed_4h, VOL_SMA_PERIOD)
                  if len(closed_4h) >= VOL_SMA_PERIOD else 0.0)
    vol_sma_1h = (calc_vol_sma(closed_1h, VOL_SMA_PERIOD)
                  if len(closed_1h) >= VOL_SMA_PERIOD else 0.0)

    _args = (state, closed_1d, closed_4h, closed_1h,
             atr, vol_sma, vol_sma_4h, vol_sma_1h)

    long_result  = _scan_breakout_side("LONG",  *_args)
    short_result = _scan_breakout_side("SHORT", *_args)

    # ── Selection: only one side present ─────────────────────────────────────
    if long_result is None:
        return short_result   # may also be None
    if short_result is None:
        return long_result

    # ── Both sides present: choose by regime then score ───────────────────────
    regime = state.regime
    if regime == "BULLISH":
        return long_result
    if regime == "BEARISH":
        return short_result
    # NEUTRAL (or unknown): prefer higher score; tie → None (ambiguous chop)
    if long_result.score > short_result.score:
        return long_result
    if short_result.score > long_result.score:
        return short_result
    return None  # equal scores in NEUTRAL — not a clean setup


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
) -> int:
    """
    Score the Trend Pullback setup on a 0–90 scale.
    (BTC regime +10 deferred: detector receives SymbolState, not Market.)

    Factors:
      +20  full EMA stack aligned
             LONG: EMA20 > EMA50 > EMA200 / SHORT: EMA20 < EMA50 < EMA200
             Skipped when EMA200 is zero (insufficient history).
      +15  pullback touches EMA20 precisely: closest bar's extreme within
             0.2×ATR of EMA20  (LONG: low;  SHORT: high)
      +15  4H confirmation candle is hammer or engulfing in the setup direction
      +15  average pullback bar volume < vol_sma20_1d × 0.9
             (weak, corrective move — healthy for a continuation setup)
      +10  reversal 4H candle volume > 4H vol SMA × PULLBACK_REVERSAL_VOL_MIN
      +10  1H EMA20 > EMA50 for LONG  /  EMA20 < EMA50 for SHORT
      +5   pullback duration is 3–5 bars (optimal; too short or too long is noisier)
    """
    score = 0

    # +20: full EMA stack
    if ema200 > 0:
        if side == "LONG" and ema20 > ema50 > ema200:
            score += 20
        elif side == "SHORT" and ema20 < ema50 < ema200:
            score += 20

    # +15: precise EMA20 touch (closest bar extreme within 0.2×ATR)
    precise = 0.2 * atr
    for bar in pullback_bars:
        if side == "LONG" and bar[B_LOW] <= ema20 + precise:
            score += 15
            break
        if side == "SHORT" and bar[B_HIGH] >= ema20 - precise:
            score += 15
            break

    # +15: 4H confirmation candle character
    prev_4h = _prev_bar(closed_4h, last_4h[B_TS])
    if side == "LONG":
        if is_bullish_retest_candle(last_4h, prev_4h):
            score += 15
    else:
        if is_bearish_retest_candle(last_4h, prev_4h):
            score += 15

    # +15: weak average pullback volume
    if vol_sma > 0 and pullback_bars:
        pb_vol_avg = sum(b[B_VOLUME] for b in pullback_bars) / len(pullback_bars)
        if pb_vol_avg < vol_sma * 0.9:
            score += 15

    # +10: strong reversal 4H candle volume
    vol_sma_4h = (calc_vol_sma(closed_4h, VOL_SMA_PERIOD)
                  if len(closed_4h) >= VOL_SMA_PERIOD else 0.0)
    if vol_sma_4h > 0 and last_4h[B_VOLUME] > vol_sma_4h * PULLBACK_REVERSAL_VOL_MIN:
        score += 10

    # +10: 1H EMA alignment
    if state.ema20_1h > 0 and state.ema50_1h > 0:
        if side == "LONG"  and state.ema20_1h > state.ema50_1h:
            score += 10
        elif side == "SHORT" and state.ema20_1h < state.ema50_1h:
            score += 10

    # +5: optimal pullback duration 3–5 bars
    if 3 <= len(pullback_bars) <= 5:
        score += 5

    # BTC regime (+10): deferred to can_signal (Phase 4)

    return score


def _scan_pullback_side(
    side: str,
    state: "SymbolState",
    closed_1d: List[Bar],
    closed_4h: List[Bar],
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
    if side == "LONG":
        if state.trend_1d != "UP":
            return None
    else:
        if state.trend_1d != "DOWN":
            return None

    ema20   = state.ema20_1d
    ema50   = state.ema50_1d
    ema200  = state.ema200_1d
    atr     = state.atr14_1d
    vol_sma = state.vol_sma20_1d

    # ── Pullback window ──────────────────────────────────────────────────────
    pullback_bars = _find_pullback_window(closed_1d, ema20, atr, side)
    if not (2 <= len(pullback_bars) <= 7):
        return None

    # ── EMA touch: EMA20 preferred, EMA50 as deeper fallback ────────────────
    if _is_ema_touch(pullback_bars, ema20, atr, side):
        pullback_type = "EMA20"
        if side == "LONG":
            entry_low  = ema20 - 0.2 * atr
            entry_high = ema20 + 0.3 * atr
        else:
            entry_high = ema20 + 0.2 * atr
            entry_low  = ema20 - 0.3 * atr
    elif _is_ema_touch(pullback_bars, ema50, atr, side):
        pullback_type = "EMA50"
        entry_low  = ema50 - 0.25 * atr
        entry_high = ema50 + 0.25 * atr
    else:
        return None

    entry_mid = (entry_low + entry_high) / 2.0

    # ── 4H confirmation ──────────────────────────────────────────────────────
    if not closed_4h:
        return None
    last_4h = closed_4h[-1]
    if side == "LONG"  and last_4h[B_CLOSE] <= state.ema20_4h:
        return None    # 4H hasn't closed back above EMA20_4H yet
    if side == "SHORT" and last_4h[B_CLOSE] >= state.ema20_4h:
        return None    # 4H hasn't closed back below EMA20_4H yet

    # ── SL: beyond the pullback extreme ─────────────────────────────────────
    if side == "LONG":
        sl_extreme = min(b[B_LOW]  for b in pullback_bars)
        stop_loss  = sl_extreme - 0.15 * atr
    else:
        sl_extreme = max(b[B_HIGH] for b in pullback_bars)
        stop_loss  = sl_extreme + 0.15 * atr

    # ── Geometry guard: SL must be outside the entry zone ───────────────────
    if side == "LONG"  and stop_loss >= entry_low:
        return None
    if side == "SHORT" and stop_loss <= entry_high:
        return None

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
    score = _score_pullback(
        side, state, pullback_bars, last_4h,
        pullback_type, ema20, ema50, ema200, atr, vol_sma,
        closed_4h,
    )

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
    )


def detect_trend_pullback(state: SymbolState) -> Optional[SetupResult]:
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
        return None

    _args = (state, closed_1d, closed_4h)
    long_result  = _scan_pullback_side("LONG",  *_args)
    short_result = _scan_pullback_side("SHORT", *_args)

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
    swept_level: float,
    closed_4h: List[Bar],
    closed_1h: List[Bar],
    exclude_ts: Optional[int] = None,
) -> Optional[Tuple[Bar, str]]:
    """
    Find the first 4H (preferred) or 1H bar that confirms the sweep reversal.

    Timing window: [sweep_bar_ts, sweep_bar_ts + 3 days).
    Condition:
      LONG  — bar close > swept_level
      SHORT — bar close < swept_level
    Returns (confirm_bar, tf_key) or None.
    Bars must be in chronological order (oldest first).

    exclude_ts: when set, skip the bar whose B_TS equals this value.
    Used when sweep_tf == "4h" to prevent the sweep candle from confirming
    itself (sweep candle already closes back inside level — that is the
    hard condition, not independent confirmation).
    """
    start_ts = sweep_bar_ts
    end_ts   = sweep_bar_ts + 3 * 86_400_000

    for tf_key, bars in (("4h", closed_4h), ("1h", closed_1h)):
        for bar in bars:
            ts = bar[B_TS]
            if ts < start_ts:
                continue
            if ts >= end_ts:
                break
            if exclude_ts is not None and ts == exclude_ts:
                continue
            if side == "LONG"  and bar[B_CLOSE] > swept_level:
                return (bar, tf_key)
            if side == "SHORT" and bar[B_CLOSE] < swept_level:
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
    level_in_short_lookback: bool,
) -> int:
    """
    Score the Liquidity Sweep setup on a 0–100 scale.
    (BTC regime +10 deferred to can_signal, Phase 4.)

    Factors:
      +20  clear sweep — always awarded; hard conditions already satisfied.
      +15  wick quality: sweep wick >= 50% of candle range.
      +15  confirmation candle is hammer/pin or engulfing in setup direction.
      +10  swept level is in short-lookback swing structure (recent, high-quality).
      +10  sweep distance <= 0.7 × ATR (tight, controlled).
      +10  sweep candle volume > vol_sma × SWEEP_VOL_MIN.
      +10  sweep candle volume > vol_sma × SWEEP_VOL_STRONG  (cumulative).
      +10  1H EMA20 > EMA50 for LONG  /  EMA20 < EMA50 for SHORT.
    """
    score = 20  # clear sweep always awarded

    rng = sweep_bar[B_HIGH] - sweep_bar[B_LOW]

    # +15: wick quality >= 50% of range
    if rng > 0:
        if side == "LONG":
            wick = min(sweep_bar[B_CLOSE], sweep_bar[B_OPEN]) - sweep_bar[B_LOW]
        else:
            wick = sweep_bar[B_HIGH] - max(sweep_bar[B_CLOSE], sweep_bar[B_OPEN])
        if wick >= 0.5 * rng:
            score += 15

    # +15: confirmation candle character (hammer/engulfing)
    if confirm_bar is not None:
        prev = (_prev_bar(closed_4h, confirm_bar[B_TS])
                if confirm_tf == "4h" else None)
        if side == "LONG"  and is_bullish_retest_candle(confirm_bar, prev):
            score += 15
        elif side == "SHORT" and is_bearish_retest_candle(confirm_bar, prev):
            score += 15

    # +10: level in short-lookback swing structure (recent/relevant)
    if level_in_short_lookback:
        score += 10

    # +10: controlled sweep distance <= 0.7 × ATR
    if side == "LONG":
        sweep_dist = abs(swept_level - sweep_bar[B_LOW])
    else:
        sweep_dist = abs(sweep_bar[B_HIGH] - swept_level)
    if sweep_dist <= 0.7 * atr:
        score += 10

    # +10 / +10: volume (cumulative when strong)
    if vol_sma > 0:
        if sweep_bar[B_VOLUME] > vol_sma * SWEEP_VOL_MIN:
            score += 10
        if sweep_bar[B_VOLUME] > vol_sma * SWEEP_VOL_STRONG:
            score += 10

    # +10: 1H EMA alignment
    if state.ema20_1h > 0 and state.ema50_1h > 0:
        if side == "LONG"  and state.ema20_1h > state.ema50_1h:
            score += 10
        elif side == "SHORT" and state.ema20_1h < state.ema50_1h:
            score += 10

    return score


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

    # ── 4H / 1H confirmation ──────────────────────────────────────────────────
    # When the sweep was found on a 4H bar, pass exclude_ts so the sweep candle
    # cannot confirm itself: its close-back-above/below the level satisfies the
    # hard condition for the sweep, but we require a *separate* bar to confirm.
    confirm = _find_sweep_confirmation(
        side, sweep_bar[B_TS], swept_level, closed_4h, closed_1h,
        exclude_ts=sweep_bar[B_TS] if sweep_tf == "4h" else None,
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
    score = _score_sweep(
        side, sweep_bar, swept_level, atr, vol_sma,
        state, confirm_bar, confirm_tf, closed_4h,
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
    True when rr_tp2 meets the tier-specific minimum.

      TIER 1 (BTCUSDT, ETHUSDT) : rr_tp2 >= RR_MIN_TIER1  (1.8)
      TIER 2 (all others)        : rr_tp2 >= RR_MIN_TIER2  (2.0)

    Returns False for None result, zero or negative rr_tp2.
    """
    if result is None or result.rr_tp2 <= 0.0:
        return False
    threshold = RR_MIN_TIER1 if symbol in TIER1_SYMBOLS else RR_MIN_TIER2
    return result.rr_tp2 >= threshold


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
    if result.setup_ts <= 0:
        return False
    return (now_ms() - result.setup_ts) <= SETUP_MAX_AGE_HOURS * 3_600_000


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
      3. already_hit_tp / sl  — any 4H/1D bar since setup_ts touched target/stop
      4. bad geometry          — current_price on the wrong side of stop_loss
      5. outside_entry_zone    — price outside [entry_low, entry_high]
                                 (only when ENTRY_ZONE_REQUIRED is True)
      6. ok

    Returns (True, "ok") or (False, reason_string).
    Reason strings correspond to ScanDiagnostics field names.
    """
    side = result.side

    # 1. Context age
    if result.setup_ts <= 0:
        return False, "context_too_old"
    age_ms = now_ms() - result.setup_ts
    if age_ms > SETUP_CONTEXT_MAX_DAYS * 86_400_000:
        return False, "context_too_old"

    # 2. Price availability
    if current_price <= 0.0:
        return False, "price_missing"

    # 3. Scan bars since setup_ts for TP/SL touches (Phase 8E: before zone check)
    #    Priority: TP2 hit > SL hit > TP1 hit (worst case surfaces first).
    for bars in (state.bars_4h, state.bars_1d):
        for bar in bars:
            if bar[B_TS] < result.setup_ts:
                continue
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
    age_h = int((now_ms() - result.setup_ts) / 3_600_000) if result.setup_ts > 0 else -1
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


# ── Phase 8G dead-candidate diagnostics helpers ───────────────────────────────

def add_candidate_debug(mkt: Market, item: CandidateDebug) -> None:
    """Append a CandidateDebug record and trim to CANDIDATE_DEBUG_MAX."""
    mkt.candidate_debug.append(item)
    excess = len(mkt.candidate_debug) - CANDIDATE_DEBUG_MAX
    if excess > 0:
        del mkt.candidate_debug[:excess]


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
    age_h = (
        int((now_ms() - result.setup_ts) / 3_600_000)
        if result.setup_ts > 0 else -1
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

# ── Phase 8F helpers ─────────────────────────────────────────────────────────

def collect_setup_candidates(state: SymbolState) -> List[SetupResult]:
    """
    Run all three detectors and return every candidate whose score meets the
    floor for the current regime.

    Score floor (per regime, same rule as run_setup_pipeline):
      NEUTRAL regime → score >= MIN_SCORE_CHOP  (85)
      Other regimes  → score >= MIN_SCORE_NORMAL (55)

    Returns at most 3 SetupResult objects (one per detector type).
    Order: [BREAKOUT_RETEST, TREND_PULLBACK, LIQUIDITY_SWEEP] (skipping None).
    """
    floor = MIN_SCORE_CHOP if state.regime == "NEUTRAL" else MIN_SCORE_NORMAL
    candidates: List[SetupResult] = []
    for detector in (detect_breakout_retest, detect_trend_pullback, detect_liquidity_sweep):
        r = detector(state)
        if r is not None and r.score >= floor:
            candidates.append(r)
    return candidates


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

    # 2. Current price
    px = get_current_price(state)

    # 3. Actionable validation
    valid, reason = validate_actionable_setup(result, state, px)
    if not valid:
        if reason == "outside_entry_zone":
            return CandidateEval(
                result, "PENDING", "outside_entry_zone",
                pending_ok=True, candidate_source=source
            )
        return CandidateEval(result, "DEAD", reason, candidate_source=source)

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

        # ── Pre-flight: symbol already has an active idea ─────────────────────
        if state.active_idea is not None:
            d.active_idea_lock += 1
            t.active_idea_lock += 1
            clear_pending_setup(mkt, sym, "active_idea")
            return

        # ── Phase 8F: collect all viable candidates ────────────────────────────
        candidates = collect_setup_candidates(state)
        if not candidates:
            d.detector_none += 1
            t.detector_none += 1
            clear_pending_setup(mkt, sym, "detector_none")
            return

        # ── Evaluate each candidate independently ─────────────────────────────
        evals: List[CandidateEval] = [
            evaluate_candidate(sym, state, mkt, c) for c in candidates
        ]

        # Update diagnostics — candidate-level (may exceed symbols_checked)
        for ev in evals:
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
                # Phase 8G: record dead candidate for /candidates command
                dbg = make_candidate_debug(sym, state, mkt, ev)
                if dbg is not None:
                    add_candidate_debug(mkt, dbg)

        # ── Prefer ACTIONABLE candidate ───────────────────────────────────────
        actionable = [ev.result for ev in evals if ev.status == "ACTIONABLE" and ev.result]
        if actionable:
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
            return

        # ── No actionable — check for pending (alive but outside zone) ────────
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

async def check_idea_lifecycle(
    sym: str,
    state: SymbolState,
    app: web.Application,
) -> None:
    """
    Phase 5: Check an active idea for TP1/TP2 hits, SL hit, and expiry.

    Bar source (uses potentially-forming candle — intentional, for live monitoring):
      bars_4h[-1] preferred; falls back to bars_1d[-1]; returns early if neither.

    Hit conditions:
      LONG:  TP1/TP2 hit if bar_high >= target;  SL hit if bar_low  <= stop_loss
      SHORT: TP1/TP2 hit if bar_low  <= target;  SL hit if bar_high >= stop_loss

    Priority on the same bar: SL > TP2 > TP1

    Status transitions:
      ACTIVE     → TP1_HIT  (idea stays open; tp1_hit stat incremented once)
      ACTIVE     → TP2_HIT  (idea closed; tp2_hit stat incremented)
      ACTIVE     → SL_HIT   (idea closed; sl_hit stat incremented)
      ACTIVE     → EXPIRED  (now >= expires_at; expired stat incremented)
      TP1_HIT    → TP2_HIT  (idea closed)
      TP1_HIT    → SL_HIT   (idea closed)
      TP1_HIT    → EXPIRED

    Phase 6 send_idea_update is active; lifecycle events are logged and
    dispatched to Telegram recipients.
    """
    idea: Optional[ActiveIdea] = state.active_idea
    if idea is None:
        return

    # ── Bar selection ─────────────────────────────────────────────────────────
    if state.bars_4h:
        bar = state.bars_4h[-1]
    elif state.bars_1d:
        bar = state.bars_1d[-1]
    else:
        return

    bar_high = bar[B_HIGH]
    bar_low  = bar[B_LOW]
    now      = now_s()
    mkt: Market = app["mkt"]

    # ── Hit detection ─────────────────────────────────────────────────────────
    if idea.side == "LONG":
        sl_hit  = bar_low  <= idea.stop_loss
        tp2_hit = bar_high >= idea.tp2
        tp1_hit = bar_high >= idea.tp1
    else:  # SHORT
        sl_hit  = bar_high >= idea.stop_loss
        tp2_hit = bar_low  <= idea.tp2
        tp1_hit = bar_low  <= idea.tp1

    expired = now >= idea.expires_at

    # ── Apply in priority order ───────────────────────────────────────────────
    # SL first
    if sl_hit and idea.status in ("ACTIVE", "TP1_HIT"):
        idea.status       = "SL_HIT"
        state.active_idea = None
        mkt.signal_stats["sl_hit"] += 1
        logger.info(
            f"IDEA SL_HIT  {sym} {idea.side} {idea.setup_type} "
            f"sl={idea.stop_loss:.4f}"
        )
        try:
            await send_idea_update(app, idea, "SL_HIT")
        except Exception as e:
            logger.warning(f"send_idea_update SL_HIT failed {sym}: {e}")
            await report_error(app, f"send_idea_update/{sym}/SL_HIT", e)
        return

    # TP2 next
    if tp2_hit and idea.status in ("ACTIVE", "TP1_HIT"):
        idea.status       = "TP2_HIT"
        state.active_idea = None
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

    # TP1 (only when still ACTIVE — not re-triggered if already TP1_HIT)
    if tp1_hit and idea.status == "ACTIVE":
        idea.status     = "TP1_HIT"
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
        return   # idea stays open

    # Expiry (covers both ACTIVE and TP1_HIT)
    if expired and idea.status in ("ACTIVE", "TP1_HIT"):
        idea.status       = "EXPIRED"
        state.active_idea = None
        mkt.signal_stats["expired"] += 1
        logger.info(
            f"IDEA EXPIRED {sym} {idea.side} {idea.setup_type}"
        )
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
        age_h       = (now_ms() - idea.setup_ts) // 3_600_000
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
        f"🛡 <b>Stop Loss:</b>   <code>{idea.stop_loss:.5f}</code>\n\n"
        f"🎯 <b>TP1:</b>  <code>{idea.tp1:.5f}</code>  "
        f"<i>(RR {idea.rr_tp1:.2f})</i>\n"
        f"🎯 <b>TP2:</b>  <code>{idea.tp2:.5f}</code>  "
        f"<i>(RR {idea.rr_tp2:.2f})</i>\n\n"
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
            f"<i>(RR {idea.rr_tp1:.2f})</i>\n\n"
            f"Idea remains active toward TP2."
        )
    if event == "TP2_HIT":
        return (
            f"{dry_prefix}✅ <b>TP2 HIT — Idea completed!</b>\n{header}\n\n"
            f"<b>TP2:</b> <code>{idea.tp2:.5f}</code>  "
            f"<i>(RR {idea.rr_tp2:.2f})</i>"
        )
    if event == "SL_HIT":
        return (
            f"{dry_prefix}❌ <b>STOP LOSS HIT</b>\n{header}\n\n"
            f"<b>SL:</b> <code>{idea.stop_loss:.5f}</code>"
        )
    if event == "EXPIRED":
        return (
            f"{dry_prefix}⏳ <b>IDEA EXPIRED</b>\n{header}\n\n"
            f"Max duration of {MAX_IDEA_DURATION_DAYS} days reached."
        )
    if event == "INVALIDATED":
        return f"{dry_prefix}🚫 <b>IDEA CLOSED / INVALIDATED</b>\n{header}"
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
    event: str,   # "TP1_HIT" | "TP2_HIT" | "SL_HIT" | "EXPIRED" | "INVALIDATED"
) -> None:
    """
    Send a lifecycle-event message via get_broadcast_targets().
    Returns silently when no targets are configured or tg is unavailable.
    """
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
                elif text in ("/watchlist", "/pending"):
                    await _cmd_watchlist(app, cid)
                elif text in ("/candidates", "/dead"):
                    await _cmd_candidates(app, cid)
        except Exception:
            await asyncio.sleep(5)


# ── Command handlers ──────────────────────────────────────────────────────────

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
        f"<b>Recent dead candidates:</b> {len(mkt.candidate_debug)}\n\n"
        f"<b>Ideas:</b> {stats['total']} total  "
        f"(L:{stats['long']} / S:{stats['short']})\n"
        f"TP1:{stats['tp1_hit']}  TP2:{stats['tp2_hit']}  "
        f"SL:{stats['sl_hit']}  Exp:{stats['expired']}\n\n"
        f"<b>Last poll:</b> {poll_ago}  (#{mkt.poll_count})\n"
        f"<b>Mode:</b> {'🧪 DRY RUN' if DRY_RUN_MODE else '✅ LIVE SIGNALS'}\n"
        f"<b>Phase:</b> 3 det · 4 RR · 5 lifecycle · 6 Tg · 7 dry-run · "
        f"8A entry gate · 8B.1 safe-send · 8C diag · 8D actionable · 8E watchlist · 8F candidates · 8G dead-diag"
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
            f"SL {idea.stop_loss:.4f}\n"
            f"   TP1 {idea.tp1:.4f} (RR {idea.rr_tp1:.2f}) | "
            f"TP2 {idea.tp2:.4f} (RR {idea.rr_tp2:.2f})\n"
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
        f"<b>Stop Loss:</b>  {idea.stop_loss:.5f}\n"
        f"<b>TP1:</b>        {idea.tp1:.5f}  (RR {idea.rr_tp1:.2f})\n"
        f"<b>TP2:</b>        {idea.tp2:.5f}  (RR {idea.rr_tp2:.2f})\n\n"
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
    "already_hit_tp":       "hit_tp",
    "already_hit_sl":       "hit_sl",
    "outside_entry_zone":   "outside_zone",
    "rr_current_fail":      "rr_curr",
    "signal_gate_fail":     "gate_fail",
    "tpsl_fail":            "tpsl_fail",
    "context_too_old":      "ctx_old",
    "price_missing":        "price_miss",
    "invalidated_since_setup": "invalidated",
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
        f"  total stored: {total}\n"
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
            f"SL: <code>{rec.stop_loss:.5f}</code>\n"
            f"TP1: <code>{rec.tp1:.5f}</code> (RR {rec.rr_tp1:.2f})  "
            f"TP2: <code>{rec.tp2:.5f}</code> (RR {rec.rr_tp2:.2f})\n"
            f"Setup age: {rec.setup_age_h}h | Updated: {age_ago}s ago"
        )

    if total > 10:
        lines.append(f"\n<i>Showing 10 of {total} stored records. "
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

    lines = [f"📌 <b>Pending Setups / Watchlist</b>  ({total} total)\n"]
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
            f"   SL: <code>{p.stop_loss:.5f}</code>  "
            f"TP1: <code>{p.tp1:.5f}</code> (RR {p.rr_tp1:.2f})  "
            f"TP2: <code>{p.tp2:.5f}</code> (RR {p.rr_tp2:.2f})\n"
            f"   Setup age: {p.setup_age_h}h  "
            f"Updated: {int(now_s()-p.updated_at)}s ago\n"
            f"   <i>Waiting for price to return to entry zone</i>"
        )

    if total > 10:
        lines.append(f"\n<i>Showing 10 of {total} pending setups.</i>")

    await tg.send(cid, "\n\n".join(lines))


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
        f"<b>RR minimum:</b> Tier1 ≥ {RR_MIN_TIER1}  |  Tier2 ≥ {RR_MIN_TIER2}\n"
        f"<b>Score floor:</b> Normal ≥ {MIN_SCORE_NORMAL}  |  Chop ≥ {MIN_SCORE_CHOP}\n"
        f"<b>Max idea duration:</b> {MAX_IDEA_DURATION_DAYS} days\n"
        f"<b>Setup context max:</b> {SETUP_CONTEXT_MAX_DAYS}d  "
        f"(legacy fresh: {SETUP_MAX_AGE_HOURS}h)\n"
        f"<b>Entry zone required:</b> {'yes' if ENTRY_ZONE_REQUIRED else 'no'}\n"
        f"<b>RR from current price:</b> {'yes' if RR_FROM_CURRENT_PRICE else 'no'}\n"
        f"<b>Watchlist:</b> enabled\n"
        f"<b>Candidate selection:</b> enabled\n"
        f"<b>Dead candidate diagnostics:</b> enabled\n"
        f"<b>Candidate debug max:</b> {CANDIDATE_DEBUG_MAX}"
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
        dead_block = (
            f"<b>Recent dead candidates: {dead_n} stored</b>\n"
            f"Dead reasons: {r_str}\n"
            f"Dead detectors: {det_str}\n"
            f"Recent dead:\n{top3}\n\n"
        )
    else:
        dead_block = ""

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
        f"gate_fail={t.signal_gate_fail}  "
        f"actionable_ok={t.actionable_ok}  "
        f"new={t.new_idea}\n\n"
        f"<b>Current gates (Phase 8D/8E):</b>\n"
        f"  Context max: {SETUP_CONTEXT_MAX_DAYS}d  "
        f"(legacy fresh: {SETUP_MAX_AGE_HOURS}h)\n"
        f"  Entry zone required: {'yes' if ENTRY_ZONE_REQUIRED else 'no'}\n"
        f"  RR from current price: {'yes' if RR_FROM_CURRENT_PRICE else 'no'}\n"
        f"  RR min: Tier1 {RR_MIN_TIER1} / Tier2 {RR_MIN_TIER2}\n"
        f"  Score floor: normal {MIN_SCORE_NORMAL} / chop {MIN_SCORE_CHOP}\n"
        f"  Watchlist: enabled"
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
    await tg.send(cid, (
        f"📐 <b>Score: {sym}</b>\n"
        f"Setup: {idea.setup_type.replace('_',' ')}\n"
        f"Score: {idea.setup_score}/100\n"
        f"<i>Detailed score breakdown is planned for a future diagnostics phase.</i>"
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
            f"(TP2:{mkt.signal_stats['tp2_hit']} SL:{mkt.signal_stats['sl_hit']}) | "
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
            f"gate_fail={d.signal_gate_fail} actionable_ok={d.actionable_ok} "
            f"new={d.new_idea} errors={d.errors}"
        )
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
        "Phase 8G dead candidate diagnostics)"
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
    app["poll_task"]      = asyncio.create_task(poll_loop(app))
    app["tg_task"]        = asyncio.create_task(tg_loop(app))
    app["watchdog_task"]  = asyncio.create_task(watchdog_loop(app))
    app["keepalive_task"] = asyncio.create_task(keepalive_loop(app))

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
                f"<b>Phase 8G</b> dead candidate diagnostics: active ✅\n\n"
                f"Commands: /status /regime /ideas /idea SYMBOL "
                f"/close SYMBOL /config /diag /watchlist /candidates"
    ))


async def on_cleanup(app: web.Application) -> None:
    for key in ("poll_task", "tg_task", "watchdog_task", "keepalive_task"):
        task = app.get(key)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    if "http" in app:
        await app["http"].close()


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",        handle_health)
    app.router.add_get("/healthz", handle_health)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
