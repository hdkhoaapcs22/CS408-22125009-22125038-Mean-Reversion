
"""Optimized live paper trading engine for PredictiveRSIMarketMaker.

Connects three components:
- PredictiveRSIMarketMaker: quote generation logic.
- PaperBrokerClient: FIX order management and execution reports.
- KafkaMarketDataClient: live quote stream.

Optimizations applied (vs new_algorithm.py):
1. Incremental indicator computation — O(1) per tick instead of O(N).
2. One-time CSV schema migration — cached flag avoids repeated disk I/O.
3. ThreadPoolExecutor for cancel orders — avoids per-cancel thread overhead.
"""

from __future__ import annotations

import asyncio
import csv
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, time as dt_time
import inspect
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from backtesting import PredictiveRSIMarketMaker

try:
    from paperbroker.client import PaperBrokerClient
    from paperbroker.market_data import KafkaMarketDataClient
except ImportError as exc:
    raise ImportError(
        "Could not import PaperBrokerClient/KafkaMarketDataClient. "
        "Install or expose your paper broker SDK before running."
    ) from exc


@dataclass
class RestingOrder:
    """Tracks a single resting limit order on the exchange."""
    cl_ord_id: str
    side: str
    price: float
    qty: int
    placed_at: float = 0.0


class IncrementalRSI:
    """Streaming RSI calculator — O(1) per new price."""

    def __init__(self, period: int = 30) -> None:
        self._period = period
        self._prev_close: Optional[float] = None
        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._warmup_gains: List[float] = []
        self._warmup_losses: List[float] = []

    def update(self, price: float) -> Optional[float]:
        if self._prev_close is None:
            self._prev_close = price
            return None

        delta = price - self._prev_close
        self._prev_close = price
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)

        if self._avg_gain is None:
            self._warmup_gains.append(gain)
            self._warmup_losses.append(loss)
            if len(self._warmup_gains) < self._period:
                return None
            self._avg_gain = sum(self._warmup_gains) / self._period
            self._avg_loss = sum(self._warmup_losses) / self._period
            self._warmup_gains.clear()
            self._warmup_losses.clear()
        else:
            n = self._period
            self._avg_gain = ((self._avg_gain * (n - 1)) + gain) / n
            self._avg_loss = ((self._avg_loss * (n - 1)) + loss) / n

        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def reset(self) -> None:
        self._prev_close = None
        self._avg_gain = None
        self._avg_loss = None
        self._warmup_gains.clear()
        self._warmup_losses.clear()


class IncrementalATR:
    """Streaming ATR calculator using simple difference (tick-level data)."""

    def __init__(self, period: int = 30) -> None:
        self._period = period
        self._prev_close: Optional[float] = None
        self._avg_tr: Optional[float] = None
        self._warmup: List[float] = []

    def update(self, price: float) -> Optional[float]:
        if self._prev_close is None:
            self._prev_close = price
            return None

        true_range = abs(price - self._prev_close)
        self._prev_close = price

        if self._avg_tr is None:
            self._warmup.append(true_range)
            if len(self._warmup) < self._period:
                return None
            self._avg_tr = sum(self._warmup) / self._period
            self._warmup.clear()
        else:
            n = self._period
            self._avg_tr = ((self._avg_tr * (n - 1)) + true_range) / n

        return self._avg_tr

    def reset(self) -> None:
        self._prev_close = None
        self._avg_tr = None
        self._warmup.clear()


class IncrementalADX:
    """Streaming ADX calculator (simplified for tick-level data where high=low=close)."""

    def __init__(self, period: int = 30) -> None:
        self._period = period
        self._prev_close: Optional[float] = None
        self._plus_dm_sum = 0.0
        self._minus_dm_sum = 0.0
        self._tr_sum = 0.0
        self._dx_warmup: List[float] = []
        self._avg_dx: Optional[float] = None
        self._window_prices: Deque[float] = deque(maxlen=period + 1)
        self._tick_count = 0

    def update(self, price: float) -> Optional[float]:
        if self._prev_close is None:
            self._prev_close = price
            self._window_prices.append(price)
            return None

        diff = price - self._prev_close
        up_move = diff
        down_move = -diff
        true_range = abs(diff)

        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0

        self._window_prices.append(price)
        self._prev_close = price
        self._tick_count += 1

        if self._tick_count < self._period:
            self._plus_dm_sum += plus_dm
            self._minus_dm_sum += minus_dm
            self._tr_sum += true_range
            return None

        if self._tick_count == self._period:
            self._plus_dm_sum += plus_dm
            self._minus_dm_sum += minus_dm
            self._tr_sum += true_range
        else:
            # Wilder smoothing approximation
            self._plus_dm_sum = self._plus_dm_sum - (self._plus_dm_sum / self._period) + plus_dm
            self._minus_dm_sum = self._minus_dm_sum - (self._minus_dm_sum / self._period) + minus_dm
            self._tr_sum = self._tr_sum - (self._tr_sum / self._period) + true_range

        if self._tr_sum == 0:
            return None

        plus_di = 100.0 * self._plus_dm_sum / self._tr_sum
        minus_di = 100.0 * self._minus_dm_sum / self._tr_sum
        di_sum = plus_di + minus_di
        if di_sum == 0:
            return None

        dx = abs(plus_di - minus_di) / di_sum * 100.0

        if self._avg_dx is None:
            self._dx_warmup.append(dx)
            if len(self._dx_warmup) < self._period:
                return None
            self._avg_dx = sum(self._dx_warmup) / self._period
            self._dx_warmup.clear()
        else:
            self._avg_dx = ((self._avg_dx * (self._period - 1)) + dx) / self._period

        return self._avg_dx

    def reset(self) -> None:
        self._prev_close = None
        self._plus_dm_sum = 0.0
        self._minus_dm_sum = 0.0
        self._tr_sum = 0.0
        self._dx_warmup.clear()
        self._avg_dx = None
        self._window_prices.clear()
        self._tick_count = 0


class IncrementalEMATrend:
    """Streaming EMA 12/26 crossover trend signal."""

    def __init__(self, fast: int = 12, slow: int = 26) -> None:
        self._fast_span = fast
        self._slow_span = slow
        self._fast_mult = 2.0 / (fast + 1)
        self._slow_mult = 2.0 / (slow + 1)
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._count = 0

    def update(self, price: float) -> Optional[int]:
        self._count += 1
        if self._ema_fast is None:
            self._ema_fast = price
            self._ema_slow = price
            return None

        self._ema_fast = (price - self._ema_fast) * self._fast_mult + self._ema_fast
        self._ema_slow = (price - self._ema_slow) * self._slow_mult + self._ema_slow

        if self._count < self._slow_span:
            return None
        return 1 if self._ema_fast >= self._ema_slow else -1

    def reset(self) -> None:
        self._ema_fast = None
        self._ema_slow = None
        self._count = 0


class LiveTradingEngine:
    """Live paper trading orchestrator with optimized indicator pipeline."""

    def __init__(self) -> None:
        load_dotenv(override=True)

        # --- Credentials & connection ---
        self.username = self._require_env_any("PAPER_USERNAME", "PAPERBROKER_USERNAME").strip()
        self.password = self._require_env_any("PAPER_PASSWORD", "PAPERBROKER_PASSWORD").strip()
        self.sub_account = self._require_env_any(
            "PAPER_ACCOUNT_ID_D1", "PAPERBROKER_SUB_ACCOUNT"
        ).strip()
        self.fix_host = self._require_env_any("PAPERBROKER_SOCKET_CONNECT_HOST", "SOCKET_HOST")
        self.fix_port = int(
            self._require_env_any("PAPERBROKER_SOCKET_CONNECT_PORT", "SOCKET_PORT")
        )
        self.sender_comp_id = self._require_env_any("PAPERBROKER_SENDER_COMP_ID", "SENDER_COMP_ID")

        # --- Trading parameters ---
        self.symbol = self._get_env_any("TARGET_SYMBOL", "PAPERBROKER_SYMBOL") or "VN30F1M"
        self.lot_size = int(os.getenv("PAPERBROKER_ORDER_QTY", "1"))
        self.contract_multiplier = float(os.getenv("PAPERBROKER_CONTRACT_MULTIPLIER", "100"))
        self.tick_size = float(os.getenv("PAPERBROKER_TICK_SIZE", "0.1"))
        self.max_position = int(os.getenv("PAPERBROKER_MAX_INVENTORY", "5"))
        spread_mult = float(os.getenv("PAPERBROKER_SPREAD_MULTIPLIER", "1.0"))
        self.spread_multiplier = spread_mult

        self.quote_engine = PredictiveRSIMarketMaker(spread_multiplier=spread_mult)

        # --- Capital & risk ---
        self.initial_capital = 500_000_000.0
        self.equity = self.initial_capital
        self.equity_anchor = self.initial_capital  # REST reconciliation anchor
        self.unrealized_anchor = 0.0
        self.kill_switch_threshold = 400_000_000.0
        self.stop_loss_pts = float(os.getenv("PAPERBROKER_STOP_LOSS_POINTS", "10.0"))
        self.is_halted = False
        self.is_midday_pause = False
        self._last_morning_flatten_date: Optional[datetime.date] = None
        self.morning_flatten_time = self._parse_time_env(
            "PAPERBROKER_MORNING_FLATTEN_START", dt_time(hour=11, minute=29, second=30)
        )

        # Fee disabled for paper trading visualization
        self.fee_per_contract = 0.0
        if self.fee_per_contract == 0.0:
            print("⚠️  WARNING: fee_per_contract=0. PnL is overstated!")

        # --- Position state ---
        self.position = 0
        self.entry_price: Optional[float] = None
        self.realized_pnl = 0.0
        self.market_price: Optional[float] = None
        self.trade_count = 0
        self.heal_count = 0
        self.signal_snapshot: Dict[str, Optional[float]] = {
            "rsi": None, "adx": None, "atr": None,
            "raw_atr_spread": None, "dynamic_spread": None,
        }

        # --- Price buffer ---
        self.tick_history: Deque[float] = deque(maxlen=300)
        self._last_tick_append_ts = 0.0
        self.warmup_ticks = 100

        # --- Incremental indicators (OPT #1) ---
        self._rsi_calc = IncrementalRSI(period=30)
        self._atr_calc = IncrementalATR(period=30)
        self._adx_calc = IncrementalADX(period=30)
        self._trend_calc = IncrementalEMATrend(fast=12, slow=26)
        self._last_rsi: Optional[float] = None
        self._last_atr: Optional[float] = None
        self._last_adx: Optional[float] = None
        self._last_trend: Optional[int] = None

        # --- Order tracking ---
        self.resting_orders: Dict[str, Optional[RestingOrder]] = {"BUY": None, "SELL": None}
        self.inflight_flush_orders: Dict[str, Dict[str, Any]] = {}
        self._flush_lock = threading.RLock()
        self.pending_fills: list[dict[str, Any]] = []
        self._pending_fills_lock = threading.RLock()

        # --- Thread pool for cancel I/O (OPT #3) ---
        self._cancel_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fix-cancel")

        self._startup_equity_done = False

        # --- Reversal flatten ---
        self.rev_flatten_cooldown_s = float(os.getenv("PAPERBROKER_REVERSAL_FLATTEN_COOLDOWN_SECONDS", "2"))
        self.rev_flatten_max_retries = int(os.getenv("PAPERBROKER_REVERSAL_FLATTEN_MAX_RETRIES", "3"))
        self.rev_flatten_lockout_s = float(os.getenv("PAPERBROKER_REVERSAL_FLATTEN_LOCKOUT_SECONDS", "20"))
        self.rev_flatten_state: Dict[str, Dict[str, float]] = {
            "BUY": {"failures": 0.0, "next_retry_ts": 0.0},
            "SELL": {"failures": 0.0, "next_retry_ts": 0.0},
        }
        self.rev_flatten_esc_ticks: Dict[str, int] = {"BUY": 0, "SELL": 0}
        self.rev_flatten_timeout_s = float(os.getenv("PAPERBROKER_REVERSAL_FLATTEN_TIMEOUT_SECONDS", "3"))

        # --- Requote & cooldown ---
        self.requote_interval_s = float(os.getenv("PAPERBROKER_REQUOTE_MIN_INTERVAL_SECONDS", "1.0"))
        self.modify_cooldown_s = float(os.getenv("PAPERBROKER_ORDER_MODIFY_COOLDOWN_SECONDS", "5.0"))
        self.cancel_backoff_s = float(os.getenv("PAPERBROKER_CANCEL_RETRY_BACKOFF_SECONDS", "5.0"))
        self._last_requote_ts: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}
        self._cancel_block_until: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}

        # --- Startup recovery ---
        self.recovery_enabled = (
            os.getenv("PAPERBROKER_STARTUP_RECOVERY_ENABLED", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self.recovery_max_cancels = int(os.getenv("PAPERBROKER_STARTUP_RECOVERY_MAX_CANCEL_ORDERS", "6"))
        self.recovery_cancel_timeout_s = float(os.getenv("PAPERBROKER_STARTUP_RECOVERY_CANCEL_TIMEOUT_SECONDS", "0.5"))

        # --- Reconciliation & analytics ---
        self.recon_interval_s = float(os.getenv("PAPERBROKER_RECONCILIATION_SECONDS", "600"))
        self.analytics_interval_s = float(os.getenv("PAPERBROKER_ANALYTICS_SECONDS", "10"))
        self.recon_probe_trades = (
            os.getenv("PAPERBROKER_RECON_PROBE_TRADE_COUNT", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._logged_api_caps = False
        self._last_analytics_reason: Optional[str] = None

        # --- Analytics history ---
        self.hist_timestamps: List[datetime] = []
        self.hist_equity: List[float] = []
        self.hist_position: List[int] = []
        self.hist_price: List[float] = []
        self.hist_elapsed: List[float] = []
        self.hist_rsi: List[Optional[float]] = []
        self.hist_adx: List[Optional[float]] = []
        self._hist_wall_ts: Optional[float] = None
        self._analytics_schema_migrated = False  # OPT #2: one-time flag
        self.analytics_csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "result", "papertrading", "analytics_history.csv",
        )
        self._load_analytics_history()

        # --- Clients ---
        self.fix_client = self._create_fix_client()
        self.kafka_client = self._create_kafka_client()
        self._bind_fix_events()

    # ── Env helpers ──────────────────────────────────────────────

    @staticmethod
    def _require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ValueError(f"Missing required env var: {name}")
        return value

    @staticmethod
    def _get_env_any(*names: str) -> Optional[str]:
        for name in names:
            value = os.getenv(name)
            if value:
                return value
        return None

    def _require_env_any(self, *names: str) -> str:
        value = self._get_env_any(*names)
        if value is None:
            raise ValueError(f"Missing required env var. Any of: {', '.join(names)}")
        return value

    @staticmethod
    def _as_dict(obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "__dict__"):
            return vars(obj)
        return {}

    @staticmethod
    def _parse_time_env(env_name: str, default: dt_time) -> dt_time:
        raw = os.getenv(env_name)
        if not raw:
            return default
        try:
            parts = [int(p) for p in raw.strip().split(":")]
            if len(parts) == 2:
                hour, minute = parts
                second = 0
            elif len(parts) == 3:
                hour, minute, second = parts
            else:
                raise ValueError("Expected HH:MM or HH:MM:SS")
            return dt_time(hour=hour, minute=minute, second=second)
        except Exception:
            print(f"Invalid {env_name}={raw!r}. Using default {default.strftime('%H:%M:%S')}.")
            return default

    # ── Client builders ──────────────────────────────────────────

    def _create_fix_client(self) -> Any:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(project_dir, "logs")
        os.makedirs(os.path.join(log_dir, "client_fix_messages"), exist_ok=True)

        kwargs = {
            "username": self.username,
            "password": self.password,
            "default_sub_account": self.sub_account,
            "socket_connect_host": self.fix_host,
            "socket_connect_port": self.fix_port,
            "sender_comp_id": self.sender_comp_id,
            "target_comp_id": self._get_env_any("PAPERBROKER_TARGET_COMP_ID", "TARGET_COMP_ID") or "SERVER",
            "rest_base_url": self._get_env_any("PAPERBROKER_REST_BASE_URL", "PAPER_REST_BASE_URL")
            or "https://papertrade.algotrade.vn/accounting",
            "log_dir": log_dir,
        }
        try:
            if "order_store_path" in inspect.signature(PaperBrokerClient.__init__).parameters:
                kwargs["order_store_path"] = None
        except (TypeError, ValueError):
            pass
        return PaperBrokerClient(**kwargs)

    def _create_kafka_client(self) -> Any:
        kwargs = {
            "bootstrap_servers": self._require_env("PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS"),
            "env_id": os.getenv("PAPERBROKER_ENV_ID", "real"),
            "merge_updates": True,
        }
        kafka_user = os.getenv("PAPERBROKER_KAFKA_USERNAME")
        kafka_pass = os.getenv("PAPERBROKER_KAFKA_PASSWORD")
        if kafka_user and kafka_user.strip().lower() != "username":
            kwargs["username"] = kafka_user.strip()
        if kafka_pass and kafka_pass.strip().lower() != "password":
            kwargs["password"] = kafka_pass.strip()
        return KafkaMarketDataClient(**kwargs)

    def _bind_fix_events(self) -> None:
        self.fix_client.on("fix:logon", self.on_logon)
        self.fix_client.on("fix:logout", self.on_logout)
        self.fix_client.on("fix:order:filled", self.on_fill)
        self.fix_client.on("fix:order:partial_fill", self.on_fill)
        self.fix_client.on("fix:order:canceled", self.on_cancel)
        self.fix_client.on("fix:order:rejected", self.on_reject)

    # ── Lifecycle ────────────────────────────────────────────────

    def start(self) -> None:
        print("Starting LiveTradingEngine...")
        if hasattr(self.fix_client, "connect"):
            self.fix_client.connect()
        elif hasattr(self.fix_client, "start"):
            self.fix_client.start()
        if hasattr(self.kafka_client, "connect"):
            self.kafka_client.connect()
        elif hasattr(self.kafka_client, "start"):
            self.kafka_client.start()

    async def start_async(self) -> None:
        print(f"Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Starting LiveTradingEngine (async)...")

        await self._call_method_async(self.fix_client, ("connect", "start"))

        print("Waiting for FIX authentication...")
        for _ in range(100):
            if hasattr(self.fix_client, "is_logged_on") and self.fix_client.is_logged_on():
                break
            await asyncio.sleep(0.1)

        if hasattr(self.fix_client, "is_logged_on") and not self.fix_client.is_logged_on():
            print("Authentication timeout. Check credentials.")
            await self.stop_async()
            return

        self._seed_startup_equity()
        if self.is_halted:
            print("Startup checks failed. Halted.")
            await self.stop_async()
            return

        self._sync_broker_position()
        await self._recover_stale_orders()

        tasks = [
            asyncio.create_task(self._run_dashboard(), name="dashboard"),
            asyncio.create_task(self._run_kafka(), name="kafka"),
            asyncio.create_task(self._run_strategy_loop(), name="strategy"),
            asyncio.create_task(self._run_reconciliation(), name="recon"),
            asyncio.create_task(self._run_analytics(), name="analytics"),
            asyncio.create_task(self._run_session_guard(), name="session-guard"),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.stop_async()

    async def stop_async(self) -> None:
        print("Stopping LiveTradingEngine...")
        self._cancel_pool.shutdown(wait=False)
        if hasattr(self.kafka_client, "stop"):
            result = self.kafka_client.stop()
            if inspect.isawaitable(result):
                await result
        if hasattr(self.fix_client, "disconnect"):
            self.fix_client.disconnect()
        elif hasattr(self.fix_client, "stop"):
            self.fix_client.stop()

    def stop(self) -> None:
        try:
            asyncio.run(self.stop_async())
        except RuntimeError:
            loop = asyncio.get_event_loop()
            loop.create_task(self.stop_async())

    # ── FIX event handlers ───────────────────────────────────────

    def on_logon(self, session_id: str, **kwargs) -> None:
        _ = kwargs
        print(f"FIX logon successful. Session: {session_id}")

    def on_logout(self, session_id: str, reason: Optional[str] = None, **kwargs) -> None:
        _ = kwargs
        print(f"FIX logout. Session: {session_id} | Reason: {reason or 'Normal'}")

    def on_cancel(self, orig_cl_ord_id: str, status: str, cum_qty: int = 0, **kwargs) -> None:
        _ = (status, cum_qty, kwargs)
        for side in ("BUY", "SELL"):
            if self.resting_orders.get(side) and self.resting_orders[side].cl_ord_id == orig_cl_ord_id:
                self.resting_orders[side] = None
                print(f"Order canceled: side={side}, id={orig_cl_ord_id}")

        with self._flush_lock:
            canceled_flush = self.inflight_flush_orders.pop(orig_cl_ord_id, None)
        if canceled_flush and str(canceled_flush.get("tag", "")) == "reversal_flatten":
            side_str = str(canceled_flush.get("side", "")).upper()
            if canceled_flush.get("escalate_after_cancel") and side_str in {"BUY", "SELL"}:
                self.rev_flatten_esc_ticks[side_str] = min(
                    self.rev_flatten_esc_ticks.get(side_str, 0) + 1, 10,
                )
                print(f"Reversal flatten {side_str} timed out; escalating to {self.rev_flatten_esc_ticks[side_str]} tick(s).")

    def on_reject(self, cl_ord_id: str, reason: str, status: str, **kwargs) -> None:
        _ = (status, kwargs)
        print(f"Order rejected: id={cl_ord_id}, reason={reason}")
        for side in ("BUY", "SELL"):
            if self.resting_orders.get(side) and self.resting_orders[side].cl_ord_id == cl_ord_id:
                self.resting_orders[side] = None
        with self._flush_lock:
            flush_order = self.inflight_flush_orders.pop(cl_ord_id, None)

        if flush_order and str(flush_order.get("tag", "")) == "reversal_flatten":
            rej_side = str(flush_order.get("side", "")).upper()
            if rej_side in {"BUY", "SELL"}:
                self._record_rev_flatten_failure(rej_side, reason)

    def on_fill(
        self, cl_ord_id: str, status: str, last_px: float, last_qty: int,
        cum_qty: Optional[int] = None, avg_px: Optional[float] = None, **kwargs,
    ) -> None:
        _ = (cum_qty, avg_px, kwargs)

        side = self._resolve_fill_side(cl_ord_id)
        if not side:
            print(f"Fill arrived early for {cl_ord_id}. Queuing.")
            with self._pending_fills_lock:
                self.pending_fills.append({
                    "cl_ord_id": cl_ord_id, "status": status,
                    "last_px": last_px, "last_qty": last_qty,
                })
            return

        fill_qty = int(last_qty)
        with self._flush_lock:
            tracked = self.inflight_flush_orders.get(cl_ord_id)
            if tracked is not None:
                tracked["filled_qty"] = int(tracked.get("filled_qty", 0)) + fill_qty

        self._apply_fill(side=side, qty=fill_qty, price=last_px)
        self.trade_count += fill_qty

        if status == "FILLED":
            if self.resting_orders.get(side) and self.resting_orders[side].cl_ord_id == cl_ord_id:
                self.resting_orders[side] = None
            with self._flush_lock:
                completed = self.inflight_flush_orders.pop(cl_ord_id, None)
            if completed and str(completed.get("tag", "")) == "reversal_flatten":
                done_side = str(completed.get("side", "")).upper()
                if done_side in {"BUY", "SELL"}:
                    self._clear_rev_flatten_state(done_side)

        self._recalc_equity()
        self._check_kill_switch()

    def _resolve_fill_side(self, cl_ord_id: str) -> Optional[str]:
        if self.resting_orders.get("BUY") and self.resting_orders["BUY"].cl_ord_id == cl_ord_id:
            return "BUY"
        if self.resting_orders.get("SELL") and self.resting_orders["SELL"].cl_ord_id == cl_ord_id:
            return "SELL"
        with self._flush_lock:
            flush_order = self.inflight_flush_orders.get(cl_ord_id)
        if flush_order is not None:
            return str(flush_order.get("side", "")).upper()
        return None

    # ── Market data callback ─────────────────────────────────────

    def on_quote(self, instrument: str, quote_snapshot: Any) -> None:
        _ = instrument
        price = self._parse_price(quote_snapshot)
        if price is None:
            return
        self.market_price = price
        now = time.time()
        if now - self._last_tick_append_ts >= 1.0:
            self.tick_history.append(price)
            self._last_tick_append_ts = now
            # OPT #1: update indicators incrementally
            self._last_rsi = self._rsi_calc.update(price)
            self._last_atr = self._atr_calc.update(price)
            self._last_adx = self._adx_calc.update(price)
            self._last_trend = self._trend_calc.update(price)

    # ── Main strategy loop ───────────────────────────────────────

    async def _run_strategy_loop(self) -> None:
        prev_price = None

        while not self.is_halted:
            await asyncio.sleep(0.1)
            self._check_rev_flatten_timeouts()

            # Resolve pending fills
            self._drain_pending_fills()

            if self.market_price is None or self.market_price == prev_price:
                continue
            prev_price = self.market_price

            self._recalc_equity()
            self._check_kill_switch()
            self._check_stop_loss()

            if self.is_halted:
                break
            if self.is_midday_pause or self._in_midday_break():
                continue
            if len(self.tick_history) < self.warmup_ticks:
                continue

            # OPT #1: use cached incremental indicator values
            indicators = self._get_cached_indicators()
            if indicators is None:
                continue

            self.signal_snapshot["rsi"] = indicators["rsi"]
            self.signal_snapshot["adx"] = indicators["adx"]
            self.signal_snapshot["atr"] = indicators["atr"]

            raw_spread = indicators["atr"] * self.spread_multiplier
            min_spread = 0.8
            fee_floor = (
                (2.0 * self.fee_per_contract) / self.contract_multiplier
                if self.contract_multiplier > 0 else 0.0
            )
            dynamic_spread = max(raw_spread, max(min_spread, fee_floor))
            self.signal_snapshot["raw_atr_spread"] = raw_spread
            self.signal_snapshot["dynamic_spread"] = dynamic_spread

            rsi = indicators["rsi"]
            is_oversold = rsi < 30
            is_overbought = rsi > 70

            # Reversal flatten: close position when RSI signals regime change
            if self.position < 0 and is_oversold:
                if not self._try_reversal_flatten("BUY"):
                    continue
                continue
            if self.position > 0 and is_overbought:
                if not self._try_reversal_flatten("SELL"):
                    continue
                continue

            # Normal market making quotes
            bid, ask = self.quote_engine.calculate_quotes(
                mid_price=self.market_price,
                current_inventory=self.position,
                rsi=indicators["rsi"],
                atr=indicators["atr"],
                dynamic_spread=dynamic_spread,
                trend_signal=indicators["trend_signal"],
                adx=indicators["adx"],
                max_inventory=self.max_position,
            )

            # Guard against crossed quotes
            if (
                self.market_price is not None and bid is not None and ask is not None
                and self._snap_price(bid) >= self._snap_price(ask)
            ):
                bid = self.market_price - self.tick_size
                ask = self.market_price + self.tick_size

            self._update_resting_order("BUY", bid)
            self._update_resting_order("SELL", ask)

    def _try_reversal_flatten(self, side: str) -> bool:
        """Attempt reversal flatten. Returns False if blocked/pending."""
        if self._has_pending_rev_flatten(side):
            return False
        blocked, msg = self._is_rev_flatten_blocked(side)
        if blocked:
            print(msg)
            return False
        direction = "covering short" if side == "BUY" else "dumping long"
        rsi = self.signal_snapshot.get("rsi")
        print(f"Signal reversal: {direction} (RSI={rsi:.2f})")
        self._cancel_all_resting()
        self._flag_analytics_event("reversal_flatten")
        self._send_aggressive_order(side=side, qty=abs(self.position), tag="reversal_flatten")
        return True

    def _drain_pending_fills(self) -> None:
        """Process fills that arrived before their order was tracked."""
        with self._pending_fills_lock:
            snapshot = list(self.pending_fills)
        if not snapshot:
            return

        resolved_ids: set[str] = set()
        for fill in snapshot:
            cid = str(fill.get("cl_ord_id", ""))
            side = self._resolve_fill_side(cid)
            if not side:
                continue

            print(f"Resolving delayed fill for {cid} on {side}")
            fqty = int(fill.get("last_qty", 0))
            fpx = float(fill.get("last_px", 0.0))
            fstatus = str(fill.get("status", ""))

            self._apply_fill(side=side, qty=fqty, price=fpx)
            self.trade_count += fqty

            if fstatus == "FILLED":
                if self.resting_orders.get(side) and self.resting_orders[side].cl_ord_id == cid:
                    self.resting_orders[side] = None
                with self._flush_lock:
                    completed = self.inflight_flush_orders.pop(cid, None)
                if completed and str(completed.get("tag", "")) == "reversal_flatten":
                    done_side = str(completed.get("side", "")).upper()
                    if done_side in {"BUY", "SELL"}:
                        self._clear_rev_flatten_state(done_side)

            resolved_ids.add(cid)

        if resolved_ids:
            with self._pending_fills_lock:
                self.pending_fills = [
                    f for f in self.pending_fills
                    if str(f.get("cl_ord_id", "")) not in resolved_ids
                ]
            self._recalc_equity()
            self._check_kill_switch()
            self._check_stop_loss()

    # ── Incremental indicators (OPT #1) ─────────────────────────

    def _get_cached_indicators(self) -> Optional[Dict[str, float]]:
        """Return latest indicator values from incremental calculators."""
        if (
            self._last_rsi is None
            or self._last_atr is None
            or self._last_adx is None
            or self._last_trend is None
        ):
            return None
        return {
            "rsi": self._last_rsi,
            "atr": self._last_atr,
            "adx": self._last_adx,
            "trend_signal": self._last_trend,
        }

    # ── Reconciliation loop ──────────────────────────────────────

    async def _run_reconciliation(self) -> None:
        while not self.is_halted:
            await asyncio.sleep(self.recon_interval_s)
            if self.is_halted:
                break

            print("\n" + "-" * 40)
            print("REST State Reconciliation")
            print("-" * 40)

            self._sync_broker_position()

            if self.recon_probe_trades:
                method, count = self._probe_trade_count()
                if count is not None:
                    print(f"Broker Trade Count: {count} (via {method})")
                    print(f"Local Trade Count:  {self.trade_count}")
                else:
                    print("Broker Trade Count: NOT AVAILABLE")

            broker_equity = self._fetch_broker_equity()
            if broker_equity is not None:
                self._anchor_equity(broker_equity)
                print(f"Reconciled Equity:  {self.equity:,.0f} VND")
                self._snapshot_analytics()
            else:
                print("Reconciled Equity:  FAILED TO PULL")
            print("-" * 40 + "\n")

    def _anchor_equity(self, broker_equity: float) -> None:
        """Re-anchor local equity tracking to broker REST value."""
        unrealized = self._calc_unrealized_pnl()
        self.equity_anchor = float(broker_equity)
        self.unrealized_anchor = unrealized
        self.realized_pnl = 0.0
        self.equity = float(broker_equity)

    # ── Analytics ────────────────────────────────────────────────

    def _snapshot_analytics(self) -> None:
        if self.market_price is None:
            return

        now = datetime.now()
        self.hist_timestamps.append(now)
        self.hist_equity.append(float(self.equity))
        self.hist_position.append(int(self.position))
        self.hist_price.append(float(self.market_price))
        rsi_val = self.signal_snapshot.get("rsi")
        adx_val = self.signal_snapshot.get("adx")
        self.hist_rsi.append(float(rsi_val) if rsi_val is not None else None)
        self.hist_adx.append(float(adx_val) if adx_val is not None else None)

        if self._hist_wall_ts is None:
            elapsed = self.hist_elapsed[-1] if self.hist_elapsed else 0.0
        else:
            delta = max(0.0, time.time() - self._hist_wall_ts)
            elapsed = (self.hist_elapsed[-1] if self.hist_elapsed else 0.0) + delta

        self.hist_elapsed.append(float(elapsed))
        self._hist_wall_ts = time.time()
        self._write_analytics_row(
            ts=now, elapsed=elapsed,
            rsi=self.hist_rsi[-1], adx=self.hist_adx[-1],
        )

    def _write_analytics_row(
        self, ts: datetime, elapsed: float,
        rsi: Optional[float], adx: Optional[float],
    ) -> None:
        self._last_analytics_reason = None

        # OPT #2: one-time schema migration
        if not self._analytics_schema_migrated:
            self._analytics_schema_migrated = True
            if os.path.exists(self.analytics_csv_path):
                try:
                    header = pd.read_csv(self.analytics_csv_path, nrows=0)
                    if "rsi" not in header.columns or "adx" not in header.columns:
                        df = pd.read_csv(self.analytics_csv_path)
                        if "rsi" not in df.columns:
                            df["rsi"] = pd.NA
                        if "adx" not in df.columns:
                            df["adx"] = pd.NA
                        df.to_csv(self.analytics_csv_path, index=False)
                except Exception as exc:
                    print(f"Schema migration failed: {exc}")

        row = [
            ts.isoformat(),
            float(self.equity),
            int(self.position),
            float(self.market_price) if self.market_price is not None else "",
            float(elapsed),
            float(rsi) if rsi is not None else "",
            float(adx) if adx is not None else "",
        ]
        columns = ["timestamp", "equity", "inventory", "price", "elapsed_seconds", "rsi", "adx"]

        os.makedirs(os.path.dirname(self.analytics_csv_path), exist_ok=True)
        write_header = not os.path.exists(self.analytics_csv_path)
        try:
            with open(self.analytics_csv_path, "a", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(columns)
                writer.writerow(row)
        except Exception as exc:
            print(f"Failed to write analytics: {exc}")

    async def _run_analytics(self) -> None:
        while not self.is_halted:
            await asyncio.sleep(self.analytics_interval_s)
            if self.is_halted:
                break
            if self.market_price is None:
                continue
            self._snapshot_analytics()

    async def _run_session_guard(self) -> None:
        """Enforce morning break and EOD liquidation (GMT+7)."""
        morning_start = self.morning_flatten_time
        morning_end = dt_time(hour=11, minute=30)
        afternoon_start = dt_time(hour=13, minute=0)
        eod_start = dt_time(hour=14, minute=29, second=30)
        eod_end = dt_time(hour=14, minute=30)
        tz = ZoneInfo("Asia/Ho_Chi_Minh")

        while True:
            await asyncio.sleep(1)
            if self.is_halted:
                break

            now_dt = datetime.now(tz)
            t = now_dt.time()
            today = now_dt.date()

            if morning_start <= t < morning_end and self._last_morning_flatten_date != today:
                print(f"\n{'#'*90}\nMORNING LIQUIDATION ({morning_start.strftime('%H:%M:%S')}-11:30)\n{'#'*90}\n")
                self._last_morning_flatten_date = today
                self._flag_analytics_event("morning_liquidation")
                self._cancel_all_resting()
                self._flatten_position(reason="morning_liquidation")
                self.is_midday_pause = True
                continue

            if morning_end <= t < afternoon_start:
                if not self.is_midday_pause:
                    print(f"\n{'#'*90}\nMIDDAY BREAK (11:30-13:00)\n{'#'*90}\n")
                    self.is_midday_pause = True
                    self._flag_analytics_event("morning_liquidation")
                    self._cancel_all_resting()
                    self._flatten_position(reason="morning_liquidation")
                continue

            if t >= afternoon_start and self.is_midday_pause:
                print(f"\n{'#'*90}\nAFTERNOON SESSION RESUMED\n{'#'*90}\n")
                self.is_midday_pause = False
                self.signal_snapshot["rsi"] = None
                self.signal_snapshot["adx"] = None
                # Reset incremental indicators for fresh afternoon session
                self._rsi_calc.reset()
                self._atr_calc.reset()
                self._adx_calc.reset()
                self._trend_calc.reset()
                self._last_rsi = None
                self._last_atr = None
                self._last_adx = None
                self._last_trend = None
                self._sync_broker_position()
                broker_eq = self._fetch_broker_equity()
                if broker_eq is not None:
                    self._anchor_equity(broker_eq)
                    print(f"Afternoon Equity Re-sync: {self.equity:,.0f} VND")

            if eod_start <= t < eod_end:
                print(f"\n{'#'*90}\nEOD LIQUIDATION (14:29:30-14:30)\n{'#'*90}\n")
                self.is_halted = True
                self._flag_analytics_event("eod_liquidation")
                self._cancel_all_resting()
                self._flatten_position(reason="eod_liquidation")
                break

            if t >= eod_end:
                print(f"\n{'#'*90}\nEOD HARD STOP (>=14:30)\n{'#'*90}\n")
                self.is_halted = True
                self._flag_analytics_event("eod_liquidation")
                self._cancel_all_resting()
                self._flatten_position(reason="eod_liquidation")
                break

    def _load_analytics_history(self) -> None:
        if not os.path.exists(self.analytics_csv_path):
            return
        try:
            df = pd.read_csv(self.analytics_csv_path)
        except Exception as exc:
            print(f"Failed to load analytics: {exc}")
            return

        required = {"timestamp", "equity", "inventory", "price", "elapsed_seconds"}
        if not required.issubset(set(df.columns)):
            print("Analytics history missing columns. Starting fresh.")
            return

        try:
            df = df.dropna(subset=["timestamp"]).copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.dropna(subset=["timestamp"]).copy()
        except Exception:
            return

        self.hist_timestamps = df["timestamp"].tolist()
        self.hist_equity = df["equity"].astype(float).tolist()
        self.hist_position = df["inventory"].astype(int).tolist()
        self.hist_price = df["price"].astype(float).tolist()
        self.hist_elapsed = df["elapsed_seconds"].astype(float).tolist()
        if "rsi" in df.columns:
            rsi_s = pd.to_numeric(df["rsi"], errors="coerce")
            self.hist_rsi = [float(v) if pd.notna(v) else None for v in rsi_s]
        else:
            self.hist_rsi = [None] * len(self.hist_timestamps)
        if "adx" in df.columns:
            adx_s = pd.to_numeric(df["adx"], errors="coerce")
            self.hist_adx = [float(v) if pd.notna(v) else None for v in adx_s]
        else:
            self.hist_adx = [None] * len(self.hist_timestamps)
        self._hist_wall_ts = None

    async def _run_dashboard(self) -> None:
        while not self.is_halted:
            await asyncio.sleep(10)
            if self.is_midday_pause or self._in_midday_break():
                continue

            px_str = f"{self.market_price:,.2f}" if self.market_price is not None else "N/A"
            if self.position > 0:
                pos_str = f"Long {self.position}"
            elif self.position < 0:
                pos_str = f"Short {abs(self.position)}"
            else:
                pos_str = "Flat 0"

            pnl = self.equity - self.initial_capital
            rsi = self.signal_snapshot.get("rsi")
            adx = self.signal_snapshot.get("adx")
            spr = self.signal_snapshot.get("dynamic_spread")
            raw_spr = self.signal_snapshot.get("raw_atr_spread")

            line = (
                f"[DASH] Px={px_str} | Pos={pos_str} | Eq={self.equity:,.0f} "
                f"| PnL={pnl:,.0f} | T={self.trade_count} "
                f"| RSI={self._fmt(rsi)} | ADX={self._fmt(adx)} "
                f"| ATRspr={self._fmt(raw_spr, 4)} | Spr={self._fmt(spr, 4)}"
            )
            self._log(line)

    # ── Price parsing ────────────────────────────────────────────

    def _parse_price(self, snapshot: Any) -> Optional[float]:
        if snapshot is None:
            return None
        if isinstance(snapshot, dict):
            price = snapshot.get("latest_matched_price")
            if price is None and isinstance(snapshot.get("data"), dict):
                price = snapshot["data"].get("latest_matched_price")
            return float(price) if price is not None else None
        if hasattr(snapshot, "latest_matched_price"):
            price = getattr(snapshot, "latest_matched_price")
            return float(price) if price is not None else None
        payload = self._as_dict(snapshot)
        price = payload.get("latest_matched_price")
        return float(price) if price is not None else None

    # ── Logging helpers ──────────────────────────────────────────

    @staticmethod
    def _fmt(value: Optional[float], digits: int = 2) -> str:
        return f"{value:.{digits}f}" if value is not None else "N/A"

    def _log(self, message: str) -> None:
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] {message}")

    @staticmethod
    def _vn_now() -> datetime:
        return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))

    def _in_midday_break(self) -> bool:
        t = self._vn_now().time()
        return dt_time(11, 30) <= t < dt_time(13, 0)

    def _log_limit_action(self, action: str, side: str, price: float, mkt_px: Optional[float]) -> None:
        self._log(
            f"[EXEC] {action} {side} Limit | Px={price:.2f} | "
            f"Mkt={self._fmt(mkt_px)} | Pos={self.position} | "
            f"RSI={self._fmt(self.signal_snapshot.get('rsi'))} | "
            f"Spr={self._fmt(self.signal_snapshot.get('dynamic_spread'), 4)}"
        )

    def _log_flush(self, reason: str, side: str, qty: int) -> None:
        self._log(
            f"[FLUSH] {reason} | {side} x{qty} | Pos={self.position} | "
            f"Mkt={self._fmt(self.market_price)} | Entry={self._fmt(self.entry_price)}"
        )

    # ── Order management ────────────────────────────────────────

    def _update_resting_order(self, side: str, target_price: Optional[float]) -> None:
        if self.is_halted:
            return

        now = time.time()
        existing = self.resting_orders.get(side)

        if now < self._cancel_block_until.get(side, 0.0):
            return
        if (
            existing is not None
            and self.requote_interval_s > 0
            and now - self._last_requote_ts.get(side, 0.0) < self.requote_interval_s
        ):
            return

        # No target → cancel existing
        if target_price is None:
            if existing is not None:
                self._submit_cancel(existing.cl_ord_id)
                self._cancel_block_until[side] = now + 2.0
            return

        # Position cap
        if side == "BUY" and self.position >= self.max_position:
            return
        if side == "SELL" and self.position <= -self.max_position:
            return

        # Block during active reversal flatten
        with self._flush_lock:
            is_flushing = any(
                str(o.get("tag", "")) == "reversal_flatten"
                for o in self.inflight_flush_orders.values()
            )
        if is_flushing:
            if existing is not None:
                self._submit_cancel(existing.cl_ord_id)
                self._cancel_block_until[side] = now + 2.0
            return

        snapped_price = self._snap_price(target_price)

        # Protect queue priority: skip micro-requotes
        if existing is not None:
            age = max(0.0, now - existing.placed_at)
            threshold = self.tick_size * 2
            if age > 10:
                threshold = self.tick_size / 2
            if abs(existing.price - snapped_price) < threshold:
                return

        # Modify cooldown
        if (
            existing is not None
            and self.modify_cooldown_s > 0
            and now - existing.placed_at < self.modify_cooldown_s
        ):
            return

        # Cancel existing before placing new
        if existing is not None:
            self._submit_cancel(existing.cl_ord_id)
            self._cancel_block_until[side] = now + 2.0
            return

        # Place new order
        self._log_limit_action("PLACE", side, snapped_price, self.market_price)
        cid = self._send_limit_order(side=side, qty=self.lot_size, price=snapped_price)
        if cid:
            self.resting_orders[side] = RestingOrder(
                cl_ord_id=cid, side=side, price=snapped_price,
                qty=self.lot_size, placed_at=now,
            )
            self._last_requote_ts[side] = now

    def _submit_cancel(self, cl_ord_id: str) -> bool:
        """Submit cancel via thread pool (OPT #3)."""
        def _do_cancel() -> None:
            try:
                self.fix_client.cancel_order(cl_ord_id=cl_ord_id)
            except TypeError:
                self.fix_client.cancel_order(cl_ord_id)
            except Exception as exc:
                print(f"Cancel failed for {cl_ord_id}: {exc}")
        try:
            self._cancel_pool.submit(_do_cancel)
            return True
        except Exception as exc:
            print(f"Failed to submit cancel for {cl_ord_id}: {exc}")
            return False

    def _send_limit_order(self, side: str, qty: int, price: float) -> str:
        if self._in_midday_break():
            return ""
        try:
            cid = self.fix_client.place_order(
                full_symbol=self.symbol, side=side.upper(),
                qty=qty, price=price, ord_type="LIMIT",
            )
            return str(cid) if cid is not None else ""
        except Exception as exc:
            print(f"Limit order failed: {exc}")
            return ""

    def _send_aggressive_order(self, side: str, qty: int, tag: str = "") -> str:
        """Emulate market order with aggressive limit (exchange rejects MARKET)."""
        if qty <= 0 or self.market_price is None:
            return ""
        if self._in_midday_break():
            return ""

        offset = self.tick_size * 10
        aggr_px = (
            self.market_price + offset if side.upper() == "BUY"
            else self.market_price - offset
        )
        snapped = self._snap_price(aggr_px)
        reason = tag or "marketable_flatten"
        self._log_flush(reason=reason, side=side.upper(), qty=qty)

        try:
            cid = self.fix_client.place_order(
                full_symbol=self.symbol, side=side.upper(),
                qty=qty, price=snapped, ord_type="LIMIT",
            )
            if cid is not None:
                cid_str = str(cid)
                with self._flush_lock:
                    self.inflight_flush_orders[cid_str] = {
                        "side": side.upper(), "qty": qty, "tag": tag,
                        "timestamp": time.time(), "price": snapped, "filled_qty": 0,
                    }
                return cid_str
            return ""
        except Exception as exc:
            print(f"Aggressive order failed: {exc}")
            return ""

    def _send_passive_flatten(self, side: str, qty: int, tag: str = "") -> str:
        if qty <= 0 or self.market_price is None:
            return ""
        side_up = side.upper()
        esc_ticks = self.rev_flatten_esc_ticks.get(side_up, 0)
        px = self.market_price
        if esc_ticks > 0:
            px += (self.tick_size * esc_ticks) if side_up == "BUY" else -(self.tick_size * esc_ticks)
        snapped = self._snap_price(px)
        try:
            cid = self.fix_client.place_order(
                full_symbol=self.symbol, side=side_up,
                qty=qty, price=snapped, ord_type="LIMIT",
            )
            if cid is not None:
                cid_str = str(cid)
                with self._flush_lock:
                    self.inflight_flush_orders[cid_str] = {
                        "side": side_up, "qty": qty, "tag": tag,
                        "timestamp": time.time(), "price": snapped,
                        "filled_qty": 0, "cancel_requested": False,
                        "escalate_after_cancel": False,
                    }
                return cid_str
            return ""
        except Exception as exc:
            print(f"Passive flatten failed: {exc}")
            return ""

    def _check_rev_flatten_timeouts(self) -> None:
        now = time.time()
        timed_out: List[str] = []

        with self._flush_lock:
            for cid, order in self.inflight_flush_orders.items():
                if str(order.get("tag", "")) != "reversal_flatten":
                    continue
                if order.get("cancel_requested"):
                    continue
                placed = float(order.get("timestamp", 0.0))
                if placed <= 0 or now - placed < self.rev_flatten_timeout_s:
                    continue
                filled = int(order.get("filled_qty", 0))
                total = int(order.get("qty", 0))
                if total > 0 and filled >= total:
                    continue
                timed_out.append(cid)

            for cid in timed_out:
                o = self.inflight_flush_orders.get(cid)
                if o:
                    o["cancel_requested"] = True
                    o["escalate_after_cancel"] = True

        for cid in timed_out:
            print(f"Rev flatten {cid} stale >{self.rev_flatten_timeout_s:.1f}s. Escalating.")
            self._submit_cancel(cid)

    # ── Fill processing ──────────────────────────────────────────

    def _apply_fill(self, side: str, qty: int, price: float) -> None:
        if qty <= 0:
            return
        px = float(price)

        # Heal broken invariant
        if self.position != 0 and self.entry_price is None:
            self.heal_count += 1
            print(f"CRITICAL: position/entry invariant broken. Healing with px={px:.2f}")
            self.entry_price = px

        signed = qty if side == "BUY" else -qty
        self.realized_pnl -= qty * self.fee_per_contract

        # Open new position
        if self.position == 0:
            self.position = signed
            self.entry_price = px
            return

        # Add to same direction
        if (self.position > 0 and signed > 0) or (self.position < 0 and signed < 0):
            abs_old = abs(self.position)
            abs_new = abs(signed)
            self.entry_price = (self.entry_price * abs_old + px * abs_new) / (abs_old + abs_new)
            self.position += signed
            return

        # Close / reverse
        closing = min(abs(self.position), abs(signed))
        if self.position > 0:
            self.realized_pnl += (px - self.entry_price) * closing * self.contract_multiplier
        else:
            self.realized_pnl += (self.entry_price - px) * closing * self.contract_multiplier

        if self.position > 0:
            self.position -= closing
        else:
            self.position += closing

        remainder = abs(signed) - closing
        if remainder > 0:
            self.position = -remainder if side == "SELL" else remainder
            self.entry_price = px
        elif self.position == 0:
            self.entry_price = None

    # ── Equity & risk ────────────────────────────────────────────

    def _recalc_equity(self) -> None:
        unrealized = self._calc_unrealized_pnl()
        delta = unrealized - self.unrealized_anchor
        self.equity = self.equity_anchor + self.realized_pnl + delta

    def _calc_unrealized_pnl(self) -> float:
        if self.position == 0 or self.entry_price is None or self.market_price is None:
            return 0.0
        if self.position > 0:
            return (self.market_price - self.entry_price) * abs(self.position) * self.contract_multiplier
        return (self.entry_price - self.market_price) * abs(self.position) * self.contract_multiplier

    def _check_kill_switch(self) -> None:
        if self.is_halted or self.equity >= self.kill_switch_threshold:
            return
        self._log("CRITICAL: Kill switch triggered. Equity below threshold.")
        self.is_halted = True
        self._flag_analytics_event("kill_switch")
        self._cancel_all_resting()
        self._flatten_position(reason="kill_switch")

    def _check_stop_loss(self) -> None:
        if self.is_halted or self.position == 0 or self.entry_price is None or self.market_price is None:
            return

        with self._flush_lock:
            already_flushing = any(
                str(o.get("tag", "")) in {"reversal_flatten", "stop_loss", "kill_switch", "risk_flatten", "eod_liquidation"}
                for o in self.inflight_flush_orders.values()
            )
        if already_flushing:
            return

        pts_against = (
            self.entry_price - self.market_price if self.position > 0
            else self.market_price - self.entry_price
        )
        if pts_against < self.stop_loss_pts:
            return

        self._log(f"STOP LOSS: position down {pts_against:.1f} pts. Flushing {abs(self.position)} contracts.")
        self._flag_analytics_event("stop_loss")
        self._cancel_all_resting()
        close_side = "SELL" if self.position > 0 else "BUY"
        self._send_aggressive_order(side=close_side, qty=abs(self.position), tag="stop_loss")

    def _flag_analytics_event(self, reason: str) -> None:
        self._last_analytics_reason = reason

    def _cancel_all_resting(self) -> None:
        now = time.time()
        for side in ("BUY", "SELL"):
            order = self.resting_orders.get(side)
            if order is None:
                continue
            if now < self._cancel_block_until.get(side, 0.0):
                continue
            self._submit_cancel(order.cl_ord_id)
            self._cancel_block_until[side] = now + 2.0

    def _flatten_position(self, reason: str = "risk_flatten") -> None:
        if self.position == 0:
            return
        side = "SELL" if self.position > 0 else "BUY"
        self._send_aggressive_order(side=side, qty=abs(self.position), tag=reason)

    # ── Reversal flatten helpers ─────────────────────────────────

    def _has_pending_rev_flatten(self, side: str) -> bool:
        side = side.upper()
        with self._flush_lock:
            return any(
                str(o.get("tag", "")) == "reversal_flatten" and str(o.get("side", "")) == side
                for o in self.inflight_flush_orders.values()
            )

    def _is_rev_flatten_blocked(self, side: str) -> Tuple[bool, str]:
        side = side.upper()
        state = self.rev_flatten_state.get(side)
        if state is None:
            return False, ""
        now = time.time()
        next_ts = float(state.get("next_retry_ts", 0.0))
        if now < next_ts:
            wait = int(max(1, next_ts - now))
            return True, f"Rev flatten retry blocked for {side}. Wait {wait}s."
        return False, ""

    def _record_rev_flatten_failure(self, side: str, reason: str) -> None:
        side = side.upper()
        state = self.rev_flatten_state.get(side)
        if state is None:
            return
        failures = int(state.get("failures", 0.0)) + 1
        state["failures"] = float(failures)
        if failures >= self.rev_flatten_max_retries:
            cooldown = self.rev_flatten_lockout_s
            print(f"Rev flatten failed {failures}x for {side}. Lockout {cooldown:.0f}s. Error: {reason}")
        else:
            cooldown = self.rev_flatten_cooldown_s
            print(f"Rev flatten failed for {side}. Retry in {cooldown:.0f}s. Error: {reason}")
        state["next_retry_ts"] = time.time() + cooldown

    def _clear_rev_flatten_state(self, side: str) -> None:
        side = side.upper()
        state = self.rev_flatten_state.get(side)
        if state is None:
            return
        state["failures"] = 0.0
        state["next_retry_ts"] = 0.0
        self.rev_flatten_esc_ticks[side] = 0

    # ── Startup ──────────────────────────────────────────────────

    def _seed_startup_equity(self) -> None:
        if self._startup_equity_done:
            return
        self._startup_equity_done = True
        print("\n" + "-" * 40)
        print("Account Identity Check")
        print("-" * 40)
        print(f"FIX SenderCompID: {self.sender_comp_id}")
        print(f"Sub-Account:      {self.sub_account}")

        try:
            sub_scope = (
                self.fix_client.use_sub_account(self.sub_account)
                if hasattr(self.fix_client, "use_sub_account") else nullcontext()
            )
            with sub_scope:
                payload = self.fix_client.get_account_balance()
            if isinstance(payload, dict) and payload.get("success") is False:
                raise ValueError(f"REST error: {payload.get('error')}")
            print("REST Mapping:     SUCCESS")
        except Exception as exc:
            acct = getattr(self.fix_client, "account_client", None)
            if acct:
                fix_id = getattr(acct, "ID", None)
                if fix_id:
                    print(f"REST fixAccountID: {fix_id}")
            print(f"REST Mapping:     FAILED ({exc})")
            print("CRITICAL: Halting to prevent ghost orders.")
            print("-" * 40 + "\n")
            self.is_halted = True
            self.equity = self.initial_capital
            return

        data = self._as_dict(payload)
        if not data and isinstance(payload, dict):
            data = payload

        eq_val: Optional[float] = None
        for src in (data, data.get("data", {})):
            if not isinstance(src, dict):
                continue
            for key in ("totalBalance", "equity", "current_equity", "net_asset_value", "total_equity"):
                if key in src and src[key] is not None:
                    try:
                        eq_val = float(src[key])
                        break
                    except (TypeError, ValueError):
                        continue
            if eq_val is not None:
                break

        if eq_val is None:
            print(f"WARNING: Could not parse equity. Using initial={self.initial_capital:,.0f}")
            self.equity = self.initial_capital
            print("-" * 40 + "\n")
            return

        self.initial_capital = eq_val
        self.equity_anchor = eq_val
        self.unrealized_anchor = 0.0
        self.equity = eq_val
        print(f"Startup Equity:   {self.equity:,.0f} VND")
        print("-" * 40 + "\n")

    def _sync_broker_position(self) -> None:
        """Fetch open positions from broker REST API."""
        print("-" * 40)
        print("Position Sync")
        print("-" * 40)

        self._log_api_caps_once()
        method, count = self._probe_trade_count()
        if count is not None:
            print(f"Broker Orders: {count} (via {method})")
        else:
            print("Broker Orders: NOT AVAILABLE")

        def _symbol_matches(target: str, candidate: Any) -> bool:
            t = str(target or "").strip().upper()
            c = str(candidate or "").strip().upper()
            if not t or not c:
                return False
            t_core = t.split(":")[-1]
            c_core = c.split(":")[-1]
            return c == t or c_core == t_core or c_core.endswith(t_core) or t_core.endswith(c_core)

        try:
            sub_scope = (
                self.fix_client.use_sub_account(self.sub_account)
                if hasattr(self.fix_client, "use_sub_account") else nullcontext()
            )
            with sub_scope:
                if hasattr(self.fix_client, "get_portfolio_by_sub"):
                    payload = self.fix_client.get_portfolio_by_sub(self.sub_account)
                elif hasattr(self.fix_client, "get_positions"):
                    payload = self.fix_client.get_positions()
                else:
                    print("WARNING: No position endpoint. Defaulting to Flat 0.")
                    self.position = 0
                    self.entry_price = None
                    print("-" * 40 + "\n")
                    return

            if isinstance(payload, dict) and payload.get("success") is False:
                raise ValueError(f"REST error: {payload.get('error')}")

            data = self._as_dict(payload)
            if not data and isinstance(payload, dict):
                data = payload

            positions: Any = []
            if isinstance(data, dict):
                if isinstance(data.get("items"), list):
                    positions = data["items"]
                elif isinstance(data.get("holdings"), list):
                    positions = data["holdings"]
                elif isinstance(data.get("data"), list):
                    positions = data["data"]
                elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("items"), list):
                    positions = data["data"]["items"]
                elif isinstance(data, list):
                    positions = data
            elif isinstance(data, list):
                positions = data

            found = False
            if isinstance(positions, list):
                for pos in positions:
                    if not isinstance(pos, dict):
                        continue
                    sym = pos.get("symbol") or pos.get("full_symbol") or pos.get("instrument") or pos.get("ticker") or ""
                    if not _symbol_matches(self.symbol, sym):
                        continue

                    try:
                        vol = int(pos.get("quantity", pos.get("volume", pos.get("qty", 0))) or 0)
                    except (TypeError, ValueError):
                        vol = 0

                    side_str = str(pos.get("side", "")).upper()
                    if side_str in {"SELL", "SHORT", "S", "2"}:
                        self.position = -abs(vol)
                    elif side_str in {"BUY", "LONG", "B", "1"}:
                        self.position = abs(vol)
                    else:
                        self.position = vol

                    avg_raw = pos.get("avgPrice", pos.get("average_price", pos.get("avg_px", pos.get("entry_price", 0.0))))
                    try:
                        avg = float(avg_raw or 0.0)
                    except (TypeError, ValueError):
                        avg = 0.0
                    self.entry_price = avg if avg > 0 else None
                    found = True

                    dir_str = "Short" if self.position < 0 else "Long"
                    print(f"Position Sync:  {dir_str} {abs(self.position)}")
                    entry_str = f"{self.entry_price:,.1f}" if self.entry_price else "N/A"
                    print(f"Entry Price:    {entry_str}")
                    break

            if not found:
                print("Position Sync:  Flat 0 (no open positions)")
                self.position = 0
                self.entry_price = None

        except Exception as exc:
            print(f"Position Sync:  FAILED ({exc})")
            self.position = 0
            self.entry_price = None

        print("-" * 40 + "\n")

    def _log_api_caps_once(self) -> None:
        if self._logged_api_caps:
            return
        self._logged_api_caps = True
        methods = sorted(
            n for n in dir(self.fix_client)
            if any(t in n.lower() for t in ("order", "trade", "execution"))
            and callable(getattr(self.fix_client, n, None))
        )
        print(f"SDK Methods: {', '.join(methods) if methods else 'NONE'}")

    def _probe_trade_count(self) -> Tuple[str, Optional[int]]:
        preferred = [
            "get_executions", "get_trades", "get_order_history", "get_orders",
            "list_executions", "list_trades", "list_orders",
        ]
        discovered = [
            n for n in dir(self.fix_client)
            if any(t in n.lower() for t in ("trade", "execution", "order"))
            and callable(getattr(self.fix_client, n, None))
        ]
        candidates: List[str] = []
        for n in preferred + sorted(discovered):
            if n not in candidates:
                candidates.append(n)

        for name in candidates:
            method = getattr(self.fix_client, name, None)
            if method is None or not callable(method):
                continue
            for arg_mode in ("none", "sub_account"):
                try:
                    sub_scope = (
                        self.fix_client.use_sub_account(self.sub_account)
                        if hasattr(self.fix_client, "use_sub_account") else nullcontext()
                    )
                    with sub_scope:
                        payload = method() if arg_mode == "none" else method(self.sub_account)
                except TypeError:
                    continue
                except Exception:
                    continue
                count = self._extract_count(payload)
                if count is not None:
                    return name, count
        return "", None

    @staticmethod
    def _extract_count(payload: Any) -> Optional[int]:
        if payload is None:
            return None
        if isinstance(payload, (list, tuple, set)):
            return len(payload)
        if not isinstance(payload, dict):
            return None
        if payload.get("success") is False:
            return None
        for key in ("trades", "executions", "orders", "items", "results", "records", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                return len(val)
            if isinstance(val, dict):
                for nk in ("items", "trades", "executions", "orders", "records"):
                    nv = val.get(nk)
                    if isinstance(nv, list):
                        return len(nv)
        return None

    def _fetch_broker_equity(self) -> Optional[float]:
        try:
            sub_scope = (
                self.fix_client.use_sub_account(self.sub_account)
                if hasattr(self.fix_client, "use_sub_account") else nullcontext()
            )
            with sub_scope:
                payload = self.fix_client.get_account_balance()
            if isinstance(payload, dict) and payload.get("success") is False:
                return None
        except Exception:
            return None

        data = self._as_dict(payload)
        if not data and isinstance(payload, dict):
            data = payload

        for key in ("totalBalance", "equity", "current_equity", "net_asset_value", "total_equity"):
            if key in data and data[key] is not None:
                try:
                    return float(data[key])
                except (TypeError, ValueError):
                    continue

        if isinstance(data.get("data"), dict):
            nested = data["data"]
            for key in ("totalBalance", "equity", "current_equity", "net_asset_value", "total_equity"):
                if key in nested and nested[key] is not None:
                    try:
                        return float(nested[key])
                    except (TypeError, ValueError):
                        continue
        return None

    # ── Startup recovery ─────────────────────────────────────────

    async def _recover_stale_orders(self) -> None:
        if not self.recovery_enabled:
            print("Startup recovery disabled.")
            return
        recover = getattr(self.fix_client, "recover_pending_orders", None)
        if recover is None:
            return
        try:
            result = recover()
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            print(f"Recovery failed: {exc}")
            return

        ids = self._extract_order_ids(result)
        if not ids:
            print("No stale orders to recover.")
            return
        if self.recovery_max_cancels > 0 and len(ids) > self.recovery_max_cancels:
            ids = ids[:self.recovery_max_cancels]

        print(f"Recovering {len(ids)} stale orders...")
        timeouts = 0
        for cid in ids:
            ok = await self._cancel_with_timeout(cid)
            if not ok:
                timeouts += 1
        if timeouts:
            print(f"Recovery: {timeouts} cancel timeouts.")

    async def _cancel_with_timeout(self, cl_ord_id: str) -> bool:
        timeout = self.recovery_cancel_timeout_s
        if timeout <= 0:
            self._submit_cancel(cl_ord_id)
            return True
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._submit_cancel, cl_ord_id), timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            print(f"Cancel timeout for {cl_ord_id} after {timeout:.2f}s.")
            return False

    @staticmethod
    def _extract_order_ids(payload: Any) -> List[str]:
        ids: List[str] = []
        if payload is None:
            return ids
        if isinstance(payload, str):
            return [payload]
        if isinstance(payload, (list, tuple, set)):
            for item in payload:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    cid = item.get("cl_ord_id") or item.get("ClOrdID") or item.get("id")
                    if cid:
                        ids.append(str(cid))
            return ids
        if isinstance(payload, dict):
            for key in ("cl_ord_id", "ClOrdID", "id"):
                v = payload.get(key)
                if v:
                    ids.append(str(v))
            for key in ("orders", "pending_orders", "items", "data"):
                nested = payload.get(key)
                if isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, str):
                            ids.append(item)
                        elif isinstance(item, dict):
                            cid = item.get("cl_ord_id") or item.get("ClOrdID") or item.get("id")
                            if cid:
                                ids.append(str(cid))
        return list(dict.fromkeys(ids))

    # ── Kafka consumer ───────────────────────────────────────────

    async def _run_kafka(self) -> None:
        print(f"Subscribing to {self.symbol}...")
        try:
            await self.kafka_client.subscribe(self.symbol, self.on_quote)
            result = self.kafka_client.start()
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            print(f"Kafka stopped: {exc}")
            self.is_halted = True
            raise

    @staticmethod
    async def _call_method_async(client: Any, names: Tuple[str, ...]) -> None:
        for name in names:
            method = getattr(client, name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                await result
            return

    def _snap_price(self, value: float) -> float:
        if self.tick_size <= 0:
            return float(value)
        steps = round(float(value) / self.tick_size)
        return round(steps * self.tick_size, 6)


async def main() -> None:
    engine = LiveTradingEngine()
    await engine.start_async()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt. Shutting down.")
