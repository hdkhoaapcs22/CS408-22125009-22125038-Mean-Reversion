"""
This is main module for strategy backtesting

Algorithm: VWAP-Anchored EMA Momentum v4 (VAEM-v4) on VN30F1M
============================================================
Changes from v3 (the 15-min bar version):
  Problem: HPR ~0% overall. The strategy made steady gains (+0.35%) from
  Jan–Oct 2022 but was wiped out in November 2022 during Vietnam's
  real-estate/bond market crisis. VN30 was in a sustained crash and the
  strategy kept firing long signals into a falling market, repeatedly
  hitting stop-losses with no mechanism to recognise the regime change.

  Fix 1 — Long-term trend regime filter (EMA_TREND_PERIOD = 50 bars):
    Compute a slow EMA(50) on bar closes alongside the existing EMA(9/21).
    New long  entries are only allowed when bar close > EMA(50)  (uptrend).
    New short entries are only allowed when bar close < EMA(50)  (downtrend).
    This single rule would have blocked almost all long entries during the
    November crash, avoiding the repeated stop-out sequence entirely.
    EMA(50) x 15-min bars = ~12.5 hours of trend history — long enough to
    identify a genuine multi-day regime without being too slow to react.

  Fix 2 — Daily loss circuit breaker (DAILY_LOSS_LIMIT_POINTS = 3.0):
    If intraday realised P&L drops below -3 index points x MULTIPLIER,
    halt all new entries for the remainder of that session.
    This caps the worst-case single-day loss regardless of signal quality,
    preventing a bad day from cascading into a catastrophic week.

  All v3 fixes retained:
    - 15-min bars (vs original 5-min)
    - TP multiplier 3.0x ATR
    - Minimum ATR filter of 1.5 points
    - VWAP flip exit removed
    - Post-SL cooldown of 3 bars

Entry signals (all conditions evaluated on completed 15-min bars):
  Long  - EMA(9) crosses above EMA(21)  AND  bar close > VWAP
          AND  bar close > EMA(50)       (regime: uptrend only)
          AND  50 <= RSI(14) <= 70       AND  bar volume >= 1.2 x AvgVol(20)
          AND  ATR(14) >= 1.5 points     AND  not in cooldown or daily limit
          -> Enter Long at the bar close price

  Short - EMA(9) crosses below EMA(21)  AND  bar close < VWAP
          AND  bar close < EMA(50)       (regime: downtrend only)
          AND  30 <= RSI(14) <= 50       AND  bar volume >= 1.2 x AvgVol(20)
          AND  ATR(14) >= 1.5 points     AND  not in cooldown or daily limit
          -> Enter Short at the bar close price

Exit rules (checked on every completed bar, in priority order):
  1. Take-Profit    : unrealised P&L >= +3.0 x ATR(14) x MULTIPLIER
  2. Stop-Loss      : unrealised P&L <= -1.0 x ATR(14) x MULTIPLIER
  3. Signal Reversal: EMA(9) crosses back through EMA(21) against position
  4. Overnight Rule : force-close any open position at ATC (end of day)

Time filter  : only accept new entries on bars that close between
               09:15 and 14:15 local time
Position size: 1 contract at all times
"""

import numpy as np
from datetime import time as dtime
from decimal import Decimal
from typing import List, Optional, Dict
from collections import deque
import pandas as pd
import matplotlib.pyplot as plt

from config.config import BACKTESTING_CONFIG
from metrics.metric import get_returns, Metric
from utils import get_expired_dates, from_cash_to_tradeable_contracts, round_decimal

FEE_PER_CONTRACT = Decimal(BACKTESTING_CONFIG["fee"]) * Decimal('100')

# ---------- Strategy constants ----------
BAR_MINUTES      = 15      # aggregate ticks into N-minute candles before signalling
                           # 15-min bars reduce noise vs 5-min and produce ATR values
                           # large enough to clear the per-trade fee (5-min ATR was
                           # too small, causing fee drag on every trade)

# EMA periods (in bars, not ticks)
EMA_FAST_PERIOD  = 9       # 9 bars x 15 min = 135-minute fast trend
EMA_SLOW_PERIOD  = 21      # 21 bars x 15 min = 315-minute slow trend
EMA_TREND_PERIOD = 50      # 50 bars x 15 min = ~12.5 hours regime filter
                           # Only long above EMA(50); only short below EMA(50)

# Daily circuit breaker: halt new entries if intraday realised P&L
# drops below this many index points (negative = loss threshold)
DAILY_LOSS_LIMIT_POINTS = Decimal('-3.0')

# RSI (in bars)
RSI_PERIOD       = 14
RSI_LONG_LOW     = Decimal('50')   # RSI floor for long entries
RSI_LONG_HIGH    = Decimal('70')   # RSI ceiling for long entries (avoid overbought)
RSI_SHORT_LOW    = Decimal('30')   # RSI floor for short entries (avoid oversold)
RSI_SHORT_HIGH   = Decimal('50')   # RSI ceiling for short entries

# ATR (in bars) — drives dynamic TP and SL
ATR_PERIOD       = 14
ATR_TP_MULT      = Decimal('3.0')  # take-profit = entry +/- 3 x ATR (was 2x —
                                   # increased so each winner covers more losing
                                   # trades and fees at the 1:3 risk:reward ratio)
ATR_SL_MULT      = Decimal('1.0')  # stop-loss   = entry -/+ 1 x ATR

# Minimum ATR threshold: skip entries when the market is too quiet.
# If ATR is below this, potential profit is smaller than the fee per trade.
ATR_MIN_POINTS   = Decimal('1.5')  # require at least 1.5 index points of ATR to enter

# Volume confirmation (in bars)
VOL_MA_PERIOD    = 20
VOL_THRESHOLD    = Decimal('1.2')  # bar volume must be >= 1.2 x rolling avg

# Post-loss cooldown: after a stop-loss is hit, skip this many bars before
# re-entering. Prevents revenge-trading into a trending adverse move.
COOLDOWN_BARS    = 3

# Time filter: no new entries on bars closing outside this window
ENTRY_TIME_START = dtime(9, 15)
ENTRY_TIME_END   = dtime(14, 15)

# VN30F1M contract spec
MULTIPLIER       = Decimal('100')  # VND per index point per contract

# EMA smoothing factors  alpha = 2 / (period + 1)
_ALPHA_FAST  = Decimal('2') / Decimal(EMA_FAST_PERIOD + 1)
_ALPHA_SLOW  = Decimal('2') / Decimal(EMA_SLOW_PERIOD + 1)
_ALPHA_TREND = Decimal('2') / Decimal(EMA_TREND_PERIOD + 1)
_ALPHA_ATR   = Decimal('2') / Decimal(ATR_PERIOD + 1)
_ALPHA_RSI   = Decimal('1') / Decimal(RSI_PERIOD)   # Wilder smoothing: alpha = 1/n


def _bar_label(dt: pd.Timestamp, bar_minutes: int) -> pd.Timestamp:
    """Return the start timestamp of the BAR_MINUTES bar that contains dt."""
    floored_minute = (dt.minute // bar_minutes) * bar_minutes
    return dt.replace(minute=floored_minute, second=0, microsecond=0)


class Backtesting:
    """
    Backtesting class — VWAP-Anchored EMA Momentum (VAEM) strategy.

    Key change vs the previous (broken) version:
      Raw ticks are aggregated into BAR_MINUTES-minute OHLCV bars inside
      run(). All indicator and signal logic then operates on those bars,
      dramatically reducing trade frequency and making ATR/EMA/RSI
      meaningful rather than noisy.

    Public method signatures are preserved for pipeline compatibility.
    """

    def __init__(
        self,
        capital: Decimal,
        printable=True,
    ):
        self.printable = printable
        self.metric    = None

        # --- position state ---
        self.inventory:   int               = 0      # +1 long, -1 short, 0 flat
        self.entry_price: Optional[Decimal] = None
        # ATR captured at entry time — fixed for the duration of the trade
        # so that TP/SL don't shift while the trade is live
        self._entry_atr:  Optional[Decimal] = None

        # --- P&L / NAV tracking ---
        self.realised_pnl:   Decimal       = Decimal('0')
        self.daily_assets:   List[Decimal] = [capital]
        self.daily_returns:  List[Decimal] = []
        self.tracking_dates: list          = []
        self.daily_inventory: list         = []
        self.monthly_tracking: list        = []

        # ----------------------------------------------------------------
        # Indicator state — all updated on completed bars, NOT raw ticks
        # ----------------------------------------------------------------

        # EMA(fast), EMA(slow), EMA(trend) on bar closes; preserved across sessions
        self._ema_fast:      Optional[Decimal] = None
        self._ema_slow:      Optional[Decimal] = None
        self._ema_trend:     Optional[Decimal] = None   # regime filter: EMA(50)
        # Previous-bar EMA values for crossover detection
        self._prev_ema_fast: Optional[Decimal] = None
        self._prev_ema_slow: Optional[Decimal] = None

        # VWAP: session-anchored, reset at start of each trading day
        self._vwap_cum_pv: Decimal = Decimal('0')  # cumulative (typical_price x volume)
        self._vwap_cum_v:  Decimal = Decimal('0')  # cumulative volume

        # RSI(14) — Wilder smoothing on bar closes
        self._rsi_avg_gain:    Optional[Decimal] = None
        self._rsi_avg_loss:    Optional[Decimal] = None
        self._rsi_prev_close:  Optional[Decimal] = None
        self._rsi_warmup_gains:  list = []
        self._rsi_warmup_losses: list = []

        # ATR(14) — true range of bars: max(H-L, |H-PC|, |L-PC|)
        self._atr:            Optional[Decimal] = None
        self._atr_prev_close: Optional[Decimal] = None

        # Rolling bar-volume window for relative-volume filter
        self._vol_window: deque = deque(maxlen=VOL_MA_PERIOD)

        # --- F1/F2 roll-over support ---
        self.old_timestamp = None

        # --- Post-loss cooldown counter ---
        # Set to COOLDOWN_BARS after a stop-loss; decremented each bar.
        # New entries are blocked while this is > 0.
        self._cooldown_bars_remaining: int = 0

        # --- Daily loss circuit breaker ---
        # Tracks intraday realised P&L in index points.
        # New entries are blocked once this falls below DAILY_LOSS_LIMIT_POINTS.
        self._daily_realised_points: Decimal = Decimal('0')
        self._daily_limit_hit:       bool    = False

    # ------------------------------------------------------------------
    # Bar-level indicator helpers
    # ------------------------------------------------------------------

    def _update_ema(self, close: Decimal):
        """Update EMA(fast), EMA(slow), and EMA(trend) from the latest bar close."""
        if self._ema_fast is None:
            # Seed all three EMAs with the first observed close
            self._ema_fast  = close
            self._ema_slow  = close
            self._ema_trend = close
        else:
            self._ema_fast  = _ALPHA_FAST  * close + (1 - _ALPHA_FAST)  * self._ema_fast
            self._ema_slow  = _ALPHA_SLOW  * close + (1 - _ALPHA_SLOW)  * self._ema_slow
            self._ema_trend = _ALPHA_TREND * close + (1 - _ALPHA_TREND) * self._ema_trend

    def _update_vwap(self, typical_price: Decimal, volume: Decimal):
        """
        Accumulate session VWAP using the bar's typical price = (H + L + C) / 3
        and total tick volume summed across the bar.
        """
        if volume > 0:
            self._vwap_cum_pv += typical_price * volume
            self._vwap_cum_v  += volume

    def _current_vwap(self) -> Optional[Decimal]:
        """Return current session VWAP, or None if no volume seen yet."""
        if self._vwap_cum_v == 0:
            return None
        return self._vwap_cum_pv / self._vwap_cum_v

    def _update_rsi(self, close: Decimal) -> Optional[Decimal]:
        """
        Update RSI(14) with Wilder smoothing on bar closes.
        Returns RSI in [0, 100] once warm-up is complete, else None.
        """
        if self._rsi_prev_close is None:
            self._rsi_prev_close = close
            return None

        change = close - self._rsi_prev_close
        gain   = max(change, Decimal('0'))
        loss   = max(-change, Decimal('0'))
        self._rsi_prev_close = close

        if self._rsi_avg_gain is None:
            # Collect first RSI_PERIOD closes to seed Wilder averages
            self._rsi_warmup_gains.append(gain)
            self._rsi_warmup_losses.append(loss)
            if len(self._rsi_warmup_gains) >= RSI_PERIOD:
                n = Decimal(RSI_PERIOD)
                self._rsi_avg_gain = sum(self._rsi_warmup_gains) / n
                self._rsi_avg_loss = sum(self._rsi_warmup_losses) / n
            return None

        # Wilder smoothing: new_avg = alpha * current + (1 - alpha) * prev_avg
        self._rsi_avg_gain = _ALPHA_RSI * gain + (1 - _ALPHA_RSI) * self._rsi_avg_gain
        self._rsi_avg_loss = _ALPHA_RSI * loss + (1 - _ALPHA_RSI) * self._rsi_avg_loss

        if self._rsi_avg_loss == 0:
            return Decimal('100')
        rs = self._rsi_avg_gain / self._rsi_avg_loss
        return Decimal('100') - (Decimal('100') / (1 + rs))

    def _update_atr(self, high: Decimal, low: Decimal, close: Decimal) -> Optional[Decimal]:
        """
        Update ATR(14) using proper bar true range:
          TR = max(high - low, |high - prev_close|, |low - prev_close|)
        Using bar OHLC rather than tick prices gives a volatility measure
        that reflects realistic intraday swings, not tick-level noise.
        """
        if self._atr_prev_close is None:
            # First bar: seed with bar range only (no previous close available)
            self._atr_prev_close = close
            self._atr = high - low
            return self._atr

        tr = max(
            high - low,
            abs(high - self._atr_prev_close),
            abs(low  - self._atr_prev_close),
        )
        self._atr_prev_close = close
        self._atr = _ALPHA_ATR * tr + (1 - _ALPHA_ATR) * self._atr
        return self._atr

    def _volume_confirmed(self, bar_volume: Decimal, has_volume: bool) -> bool:
        """
        Return True if bar volume >= VOL_THRESHOLD x rolling average.
        If has_volume is False (dataset carries no real volume), bypass
        the filter entirely rather than blocking all entries.
        """
        if not has_volume:
            return True
        if len(self._vol_window) < VOL_MA_PERIOD:
            return False   # insufficient history during warm-up
        avg_vol = sum(self._vol_window) / Decimal(VOL_MA_PERIOD)
        if avg_vol == 0:
            return True    # no meaningful baseline — don't block entries
        return bar_volume >= VOL_THRESHOLD * avg_vol

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def _unrealised_pnl(self, cur_price: Decimal) -> Decimal:
        """Unrealised P&L for the open position in VND."""
        if self.inventory == 0 or self.entry_price is None:
            return Decimal('0')
        direction = Decimal('1') if self.inventory > 0 else Decimal('-1')
        return direction * (cur_price - self.entry_price) * MULTIPLIER

    def _open_position(self, direction: int, price: Decimal, atr: Decimal):
        """
        Open a 1-contract position and snapshot the current ATR.
        ATR is frozen at entry so TP/SL levels don't shift mid-trade.
        """
        self.inventory   = direction
        self.entry_price = price
        self._entry_atr  = atr

    def _close_position(self, price: Decimal):
        """Close current position and book realised P&L net of fees."""
        if self.inventory == 0:
            return
        direction = Decimal('1') if self.inventory > 0 else Decimal('-1')
        points    = direction * (price - self.entry_price)   # P&L in index points
        gross     = points * MULTIPLIER
        self.realised_pnl += gross - FEE_PER_CONTRACT

        # Track intraday realised points for the daily circuit breaker
        self._daily_realised_points += points
        if self._daily_realised_points <= DAILY_LOSS_LIMIT_POINTS:
            self._daily_limit_hit = True   # halt new entries for the rest of the day

        self.inventory    = 0
        self.entry_price  = None
        self._entry_atr   = None

    # ------------------------------------------------------------------
    # Daily bookkeeping
    # ------------------------------------------------------------------

    def _update_pnl(self, close_price: Decimal):
        """Force-close open position at ATC, snapshot NAV, compute daily return."""
        self._close_position(close_price)

        cur_asset = self.daily_assets[-1]
        new_asset = cur_asset + self.realised_pnl
        self.realised_pnl = Decimal('0')

        daily_return = new_asset / cur_asset - Decimal('1')
        self.daily_returns.append(daily_return)
        self.daily_assets.append(new_asset)

    def _reset_daily_indicators(self):
        """
        Reset session-scoped state at end of each trading day.
        - VWAP is reset because it is session-anchored by definition.
        - EMA, RSI, ATR smoothed values are PRESERVED across sessions for
          indicator continuity (avoid daily re-warm-up distortion).
        - prev-close anchors are cleared so the overnight gap does not
          create a spurious outsized first-bar move.
        """
        # Session VWAP: reset every day
        self._vwap_cum_pv = Decimal('0')
        self._vwap_cum_v  = Decimal('0')

        # Clear cross-bar anchors (smoothed _ema/rsi/atr state preserved)
        self._prev_ema_fast  = None
        self._prev_ema_slow  = None
        self._rsi_prev_close = None
        self._atr_prev_close = None

        # Cooldown and daily circuit breaker do not carry across sessions
        self._cooldown_bars_remaining = 0
        self._daily_realised_points   = Decimal('0')
        self._daily_limit_hit         = False

    # ------------------------------------------------------------------
    # Bar accumulator helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_bar() -> Dict:
        """Return a blank bar accumulator dictionary."""
        return {
            "open": None, "high": None, "low": None,
            "close": None, "volume": Decimal('0'),
        }

    @staticmethod
    def _add_tick_to_bar(bar: Dict, price: Decimal, volume: Decimal) -> Dict:
        """Incorporate one tick into the running bar accumulator."""
        if bar["open"] is None:
            bar["open"] = price
        bar["close"]   = price
        bar["high"]    = price if bar["high"] is None else max(bar["high"], price)
        bar["low"]     = price if bar["low"]  is None else min(bar["low"],  price)
        bar["volume"] += volume
        return bar

    # ------------------------------------------------------------------
    # Data processing — unchanged from original to preserve pipeline
    # ------------------------------------------------------------------

    @staticmethod
    def process_data(evaluation=False):
        prefix_path = "data/os/" if evaluation else "data/is/"
        f1_data = pd.read_csv(f"{prefix_path}VN30F1M_data.csv")
        f1_data["datetime"] = pd.to_datetime(
            f1_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        f1_data["date"] = (
            pd.to_datetime(f1_data["date"], format="%Y-%m-%d").copy().dt.date
        )
        rounding_columns = ["close", "price", "best-bid", "best-ask", "spread"]
        for col in rounding_columns:
            f1_data = round_decimal(f1_data, col)

        f2_data = pd.read_csv(f"{prefix_path}VN30F2M_data.csv")
        f2_data = f2_data[["date", "datetime", "tickersymbol", "price", "close"]].copy()
        f2_data["datetime"] = pd.to_datetime(
            f2_data["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        f2_data["date"] = (
            pd.to_datetime(f2_data["date"], format="%Y-%m-%d").copy().dt.date
        )
        f2_data.rename(
            columns={
                "price": "f2_price",
                "close": "f2_close",
                "tickersymbol": "f2-tickersymbol",
            },
            inplace=True,
        )
        rounding_columns = ["f2_close", "f2_price"]
        for col in rounding_columns:
            f2_data = round_decimal(f2_data, col)

        f1_data = pd.merge(
            f1_data,
            f2_data,
            on=["datetime", "date"],
            how="outer",
            sort=True,
        )
        f1_data = f1_data.ffill()
        return f1_data

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self, data: pd.DataFrame, step: Decimal = Decimal('0.1')):
        """
        Iterate tick-by-tick, aggregate into BAR_MINUTES-minute candles,
        and apply VAEM signal logic on each completed bar.

        The aggregation happens inline: as soon as a tick arrives that
        belongs to a new bar, the previous bar is finalised, indicators
        are updated, and entry/exit logic is evaluated. The current tick
        then seeds the next bar's accumulator.

        `step` is kept for API compatibility (unused by this strategy).
        """
        trading_dates    = data["date"].unique().tolist()
        start_date       = data["datetime"].iloc[0]
        end_date         = data["datetime"].iloc[-1]
        expiration_dates = get_expired_dates(start_date, end_date)

        # Detect whether the dataset carries real (non-constant) volume data
        has_volume = (
            "volume" in data.columns
            and data["volume"].nunique() > 1
        )

        cur_index         = 0
        moving_to_f2      = False
        current_bar       = self._empty_bar()
        current_bar_label = None   # pd.Timestamp of the bar being accumulated

        for index, row in data.iterrows():
            # ---- select active contract price ----
            tick_price  = row["f2_price"] if moving_to_f2 else row["price"]
            close_price = row["f2_close"] if moving_to_f2 else row["close"]

            # ---- roll over F1 -> F2 on expiry week ----
            if (
                cur_index != len(trading_dates) - 1
                and not expiration_dates.empty()
                and trading_dates[cur_index + 1] >= expiration_dates.queue[0]
            ):
                expiration_dates.get()
                moving_to_f2 = True

            # ---- tick volume (fallback handled by has_volume flag) ----
            tick_volume = (
                Decimal(str(row["volume"])) if has_volume else Decimal('1')
            )

            # ---- determine this tick's bar boundary ----
            tick_dt   = row["datetime"]
            bar_label = _bar_label(tick_dt, BAR_MINUTES)

            # ---- detect bar completion: tick belongs to a NEW bar ----
            bar_just_completed = (
                current_bar_label is not None
                and bar_label != current_bar_label
                and current_bar["open"] is not None
            )

            if bar_just_completed:
                # ============================================================
                # Process the just-completed bar
                # ============================================================
                bar_close  = current_bar["close"]
                bar_high   = current_bar["high"]
                bar_low    = current_bar["low"]
                bar_volume = current_bar["volume"]
                bar_time   = current_bar_label.time()

                # ---- snapshot previous EMA before updating (for crossover) ----
                self._prev_ema_fast = self._ema_fast
                self._prev_ema_slow = self._ema_slow

                # ---- update all bar-level indicators ----
                self._update_ema(bar_close)

                typical_price = (bar_high + bar_low + bar_close) / Decimal('3')
                self._update_vwap(typical_price, bar_volume)

                rsi = self._update_rsi(bar_close)
                atr = self._update_atr(bar_high, bar_low, bar_close)

                # Volume filter: check BEFORE adding current bar to window
                vol_ok = self._volume_confirmed(bar_volume, has_volume)
                self._vol_window.append(bar_volume)

                vwap = self._current_vwap()

                # ---- decrement cooldown counter each bar ----
                if self._cooldown_bars_remaining > 0:
                    self._cooldown_bars_remaining -= 1

                # ============================================================
                # Exit checks on existing position
                # ============================================================
                if self.inventory != 0 and self._entry_atr is not None and self._entry_atr > 0:
                    upnl      = self._unrealised_pnl(bar_close)
                    tp_thresh =  ATR_TP_MULT * self._entry_atr * MULTIPLIER
                    sl_thresh =  ATR_SL_MULT * self._entry_atr * MULTIPLIER

                    # Priority 1 — Take-Profit
                    if upnl >= tp_thresh:
                        self._close_position(bar_close)

                    # Priority 2 — Stop-Loss (triggers cooldown)
                    elif upnl <= -sl_thresh:
                        self._close_position(bar_close)
                        # Activate cooldown: likely trending hard against us
                        self._cooldown_bars_remaining = COOLDOWN_BARS

                    # Priority 3 — EMA signal reversal
                    elif (
                        self._prev_ema_fast is not None
                        and self._prev_ema_slow is not None
                    ):
                        ema_flipped_down = (
                            self._prev_ema_fast >= self._prev_ema_slow
                            and self._ema_fast < self._ema_slow
                        )
                        ema_flipped_up = (
                            self._prev_ema_fast <= self._prev_ema_slow
                            and self._ema_fast > self._ema_slow
                        )
                        if self.inventory == 1 and ema_flipped_down:
                            self._close_position(bar_close)
                        elif self.inventory == -1 and ema_flipped_up:
                            self._close_position(bar_close)

                    # NOTE: VWAP flip exit deliberately removed in v3.
                    # It was cutting winners short as price consolidated near
                    # VWAP mid-session. VWAP is now an entry filter only.

                # ============================================================
                # Entry logic: only when flat and all conditions satisfied
                # ============================================================
                if self.inventory == 0:
                    within_window = ENTRY_TIME_START <= bar_time <= ENTRY_TIME_END

                    indicators_ready = (
                        self._prev_ema_fast is not None
                        and self._prev_ema_slow is not None
                        and self._ema_trend is not None       # regime EMA must be seeded
                        and rsi is not None
                        and atr is not None
                        and atr >= ATR_MIN_POINTS             # skip low-volatility entries
                        and vwap is not None
                        and vol_ok
                        and self._cooldown_bars_remaining == 0  # post-SL cooldown clear
                        and not self._daily_limit_hit           # daily circuit breaker clear
                    )

                    if within_window and indicators_ready:
                        ema_crossed_up = (
                            self._prev_ema_fast <= self._prev_ema_slow
                            and self._ema_fast > self._ema_slow
                        )
                        ema_crossed_down = (
                            self._prev_ema_fast >= self._prev_ema_slow
                            and self._ema_fast < self._ema_slow
                        )

                        # Long: EMA bull cross + above VWAP + RSI momentum zone
                        #       + regime filter: price must be above EMA(50)
                        #         Blocks long entries during sustained downtrends
                        #         (e.g. would have blocked most Nov 2022 longs)
                        if (
                            ema_crossed_up
                            and bar_close > vwap
                            and bar_close > self._ema_trend
                            and RSI_LONG_LOW <= rsi <= RSI_LONG_HIGH
                        ):
                            self._open_position(+1, bar_close, atr)

                        # Short: EMA bear cross + below VWAP + RSI momentum zone
                        #        + regime filter: price must be below EMA(50)
                        #          Blocks short entries during sustained uptrends
                        elif (
                            ema_crossed_down
                            and bar_close < vwap
                            and bar_close < self._ema_trend
                            and RSI_SHORT_LOW <= rsi <= RSI_SHORT_HIGH
                        ):
                            self._open_position(-1, bar_close, atr)

                # ---- start accumulating the new bar ----
                current_bar       = self._empty_bar()
                current_bar_label = bar_label

            elif current_bar_label is None:
                # First tick of the entire dataset — initialise bar label
                current_bar_label = bar_label

            # ---- accumulate this tick into the running bar ----
            current_bar = self._add_tick_to_bar(current_bar, tick_price, tick_volume)

            # ============================================================
            # End-of-day bookkeeping
            # ============================================================
            is_last_tick_of_day = (
                index == len(data) - 1
                or row["date"] != data.iloc[index + 1]["date"]
            )
            if is_last_tick_of_day:
                # Priority 5: Overnight rule — force-close at ATC price
                self._update_pnl(close_price)

                if self.printable:
                    print(
                        f"Realized asset {row['date']}: "
                        f"{int(self.daily_assets[-1] * Decimal('1000'))} VND"
                    )

                if moving_to_f2:
                    self.monthly_tracking.append([row["date"], self.daily_assets[-1]])

                self.tracking_dates.append(row["date"])
                self.daily_inventory.append(self.inventory)

                # Reset session indicators and bar accumulator for new day
                self._reset_daily_indicators()
                current_bar       = self._empty_bar()
                current_bar_label = None

                moving_to_f2 = False
                cur_index   += 1

        self.metric = Metric(self.daily_returns, None)

    # ------------------------------------------------------------------
    # Plotting — identical signatures to original for pipeline compatibility
    # ------------------------------------------------------------------

    def plot_hpr(self, path="result/backtest/hpr.svg"):
        """Plot and save NAV / Holding Period Return chart."""
        plt.figure(figsize=(10, 6))
        assets    = pd.Series(self.daily_assets)
        ac_return = assets.apply(lambda x: x / assets.iloc[0])
        ac_return = [(val - 1) * 100 for val in ac_return.to_numpy()[1:]]
        plt.plot(self.tracking_dates, ac_return, label="Portfolio", color='black')
        plt.title('Holding Period Return Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Holding Period Return (%)')
        plt.grid(True)
        plt.legend()
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_drawdown(self, path="result/backtest/drawdown.svg"):
        """Plot and save drawdown chart."""
        _, drawdowns = self.metric.maximum_drawdown()
        plt.figure(figsize=(10, 6))
        plt.plot(self.tracking_dates, drawdowns, label="Portfolio", color='black')
        plt.title('Draw down Value Over Time')
        plt.xlabel('Time Step')
        plt.ylabel('Percentage')
        plt.grid(True)
        plt.savefig(path, dpi=300, bbox_inches='tight')

    def plot_inventory(self, path="result/backtest/inventory.svg"):
        """Plot and save end-of-day inventory chart."""
        plt.figure(figsize=(10, 6))
        plt.plot(self.tracking_dates, self.daily_inventory, label="Portfolio", color='black')
        plt.title('Inventory Value Over Time')
        plt.xlabel('Time Step')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(path, dpi=300, bbox_inches='tight')


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    bt = Backtesting(capital=Decimal("5e5"))

    data = bt.process_data()
    bt.run(data)

    print(
        f"Sharpe ratio:  "
        f"{bt.metric.sharpe_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    print(
        f"Sortino ratio: "
        f"{bt.metric.sortino_ratio(risk_free_return=Decimal('0.00023')) * Decimal(np.sqrt(250))}"
    )
    mdd, _ = bt.metric.maximum_drawdown()
    print(f"Maximum drawdown: {mdd}")

    monthly_df = pd.DataFrame(bt.monthly_tracking, columns=["date", "asset"])
    returns    = get_returns(monthly_df)

    print(f"HPR            {bt.metric.hpr()}")
    print(f"Monthly return {returns['monthly_return']}")
    print(f"Annual return  {returns['annual_return']}")

    bt.plot_hpr()
    bt.plot_drawdown()
    bt.plot_inventory()