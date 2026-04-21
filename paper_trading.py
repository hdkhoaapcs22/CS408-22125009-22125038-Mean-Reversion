#!/usr/bin/env python3
"""
Live Trading Engine: SMA(1000) Momentum Crossover

Adapted from synchronous backtest to asynchronous, event-driven live trading.
"""

import os
import sys
import asyncio
import logging
from collections import deque
from datetime import datetime, time
from decimal import Decimal
from typing import Optional
from threading import Event as ThreadEvent
from dotenv import load_dotenv
from paperbroker.client import PaperBrokerClient
from paperbroker.market_data import KafkaMarketDataClient, QuoteSnapshot

# Add parent directory to path if needed for your environment
# sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ---------- Strategy Constants ----------
SMA_WINDOW = 10
TP_POINTS = 3.0
SL_POINTS = 2.0
END_OF_DAY_CLOSE = time(14, 29, 55)  # 5 seconds before ATC

class LiveSMABot:
    def __init__(self, symbol: str, account: str):
        self.symbol = symbol
        self.account = account
        
        # --- Strategy State ---
        self.inventory = 0           # +1 for Long, -1 for Short, 0 for Flat
        self.entry_price = 0.0
        self.pending_inventory: Optional[int] = None # Add this line
        self.price_window = deque(maxlen=SMA_WINDOW)
        self.prev_price: Optional[float] = None
        self.prev_sma: Optional[float] = None
        
        # --- Execution State ---
        self.pending_order_id: Optional[str] = None
        self.pending_order_time: Optional[datetime] = None
        self.order_canceled = ThreadEvent()
        
        # --- Clients ---
        self._init_clients()

    def _init_clients(self):
        """Initialize both FIX and Redis clients."""
        # 1. FIX Client (Execution)
        self.fix = PaperBrokerClient(
            default_sub_account=self.account,
            username=os.getenv("PAPER_USERNAME", "BL01"),
            password=os.getenv("PAPER_PASSWORD", "123"),
            rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://localhost:9090"),
            socket_connect_host=os.getenv("SOCKET_HOST", "localhost"),
            socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
            sender_comp_id=os.getenv("SENDER_COMP_ID", "cross-FIX"),
            target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
            console=False,
            order_store_path="orders.db" # Crash recovery
        )
        
        # Wire up FIX events
        self.fix.on("fix:order:filled", self.on_order_filled)
        self.fix.on("fix:order:canceled", self.on_order_failed)
        self.fix.on("fix:order:rejected", self.on_order_failed)

        # 2. Redis Client (Market Data)
        self.md = KafkaMarketDataClient(
        bootstrap_servers=os.getenv("PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS"),
        username=os.getenv("PAPERBROKER_KAFKA_USERNAME"),
        password=os.getenv("PAPERBROKER_KAFKA_PASSWORD"),
        env_id=os.getenv("PAPERBROKER_ENV_ID"),
        merge_updates=True  # Full snapshots
    )

    # ------------------------------------------------------------------
    # FIX Event Handlers (Execution Callbacks)
    # ------------------------------------------------------------------
    
    def on_order_filled(self, cl_ord_id, status, last_px, last_qty, **kw):
        """Handle execution fills."""
        if cl_ord_id != self.pending_order_id:
            return 

        logger.info(f"✅ ORDER FILLED: {last_qty} @ {last_px}")
        
        # Apply the pending inventory state
        if self.pending_inventory is not None:
            if self.pending_inventory != 0:
                self.inventory = self.pending_inventory
                self.entry_price = last_px # Use the actual filled price!
            else:
                logger.info("🎯 Position Closed. Flat.")
                self.inventory = 0
                self.entry_price = 0.0
                
        self.pending_inventory = None
        self.pending_order_id = None

    def on_order_failed(self, cl_ord_id, **kw):
        """Handle rejected/canceled orders by freeing the lock."""
        if cl_ord_id == self.pending_order_id:
            logger.warning("⚠️ Order failed or canceled. Freeing lock.")
            self.pending_order_id = None

    # ------------------------------------------------------------------
    # Market Data Callback (Strategy Engine)
    # ------------------------------------------------------------------

    def on_quote_update(self, instrument: str, quote: QuoteSnapshot):
        logger.info(
            f"TICK EVAL | Price: {quote.latest_matched_price} | SMA: {sum(self.price_window) / SMA_WINDOW:.2f} | Window: {len(self.price_window)} | "
            f"PrevPx: {self.prev_price} | PrevSMA: {self.prev_sma if self.prev_sma else 0:.2f} | "
            f"Pos: {self.inventory} | Lock: {self.pending_order_id}"
        )

        """Tick-by-Tick Evaluation."""
        tick_price = quote.latest_matched_price
        if tick_price is None:
            return

        # 1. Update rolling window
        self.price_window.append(tick_price)
        if len(self.price_window) < SMA_WINDOW:
            logger.info(f"⏳ Warming up... {len(self.price_window)}/{SMA_WINDOW} ticks collected.")
            return # Warming up

        cur_sma = sum(self.price_window) / SMA_WINDOW

        # --- DIAGNOSTIC LOGGING ---
        # Log the exact state on every tick

        # Skip evaluation if we are currently waiting for an order to fill
        if self.pending_order_id is not None:
            # Check if order has been stuck for more than 10 seconds
            elapsed_sec = (datetime.now() - self.pending_order_time).total_seconds()
            
            if elapsed_sec > 120:
                logger.warning(f"⚠️ Order {self.pending_order_id[:8]} stuck for {elapsed_sec:.1f}s. Canceling and unlocking!")
                self.fix.cancel_order(self.pending_order_id)
                if not self.order_canceled.wait(timeout=10):
                    logger.warning("⚠️ Timeout waiting for cancellation confirmation")
                
                # Force free the lock and clear the pending intent
                self.pending_order_id = None
                self.pending_order_time = None
                self.pending_inventory = None
            else:
                logger.info("⏳ Waiting for pending order to fill. Skipping signal evaluation.")
            
            self._update_prev_state(tick_price, cur_sma)
            return

        # 2. Check End-Of-Day (EOD) Force Close
        current_time = datetime.now().time()
        if current_time >= END_OF_DAY_CLOSE:
            logger.info(f"⏰ Current time {current_time} >= EOD close time {END_OF_DAY_CLOSE}.")
            if self.inventory != 0:
                logger.info("🚨 EOD TRIGGERED. Closing positions.")
                self.close_position(quote)
            return # Do not open new positions

        # 3. Check Take-Profit & Stop-Loss
        if self.inventory != 0:
            logger.info(f"📊 Evaluating TP/SL | Entry: {self.entry_price} | Current: {tick_price}")
            unrealized_pnl = self.inventory * (tick_price - self.entry_price)
            
            if unrealized_pnl >= TP_POINTS:
                logger.info(f"💰 TAKE PROFIT Hit! PnL: +{unrealized_pnl:.1f} pts")
                self.close_position(quote)
                self._update_prev_state(tick_price, cur_sma)
                return
                
            if unrealized_pnl <= -SL_POINTS:
                logger.info(f"🛑 STOP LOSS Hit! PnL: {unrealized_pnl:.1f} pts")
                self.close_position(quote)
                self._update_prev_state(tick_price, cur_sma)
                return

        # 4. Check Entry Signals (Crossover)
        if self.inventory == 0 and self.prev_price is not None and self.prev_sma is not None:
            logger.info(f"🔍 Checking for crossover signals | PrevPx: {self.prev_price} | PrevSMA: {self.prev_sma:.2f} | CurPx: {tick_price} | CurSMA: {cur_sma:.2f}")
            # Buy signal: prev < SMA and cur >= SMA
            if self.prev_price < self.prev_sma and tick_price >= cur_sma:
                logger.info("📈 BUY Signal (Bullish Crossover)")
                self.open_position("BUY", quote)
                
            # Sell signal: prev > SMA and cur <= SMA
            elif self.prev_price > self.prev_sma and tick_price <= cur_sma:
                logger.info("📉 SELL Signal (Bearish Crossover)")
                self.open_position("SELL", quote)

        logger.info(f"State Update | Price: {tick_price} | SMA: {cur_sma:.2f} | Pos: {self.inventory}")
        self._update_prev_state(tick_price, cur_sma)

    def _update_prev_state(self, price, sma):
        self.prev_price = price
        self.prev_sma = sma

    # ------------------------------------------------------------------
    # Execution Helpers
    # ------------------------------------------------------------------

    def open_position(self, side: str, quote: QuoteSnapshot):
        """Place an aggressive limit order to open a position."""
        # Use current price +/- 1.0 pt slippage to ensure instant fill without hitting extreme bounds
        slippage = 1.0
        limit_price = quote.latest_matched_price + slippage if side == "BUY" else quote.latest_matched_price - slippage
        limit_price = round(limit_price, 1) # Ensure VN30F tick precision
        
        try:
            order_id = self.fix.place_order(
                full_symbol=self.symbol,
                side=side,
                qty=1,
                price=limit_price,
                ord_type="LIMIT"
            )
            self.pending_order_id = order_id
            self.pending_order_time = datetime.now()
            
            # Record INTENT, do not update actual inventory yet
            self.pending_inventory = 1 if side == "BUY" else -1 
            
            logger.info(f"📤 Sent {side} order: {order_id[:8]} at {limit_price}")
        except Exception as e:
            logger.error(f"Failed to place open order: {e}")

    def close_position(self, quote: QuoteSnapshot):
        """Place an aggressive limit order to close the current position."""
        side = "SELL" if self.inventory == 1 else "BUY"
        
        slippage = 1.0
        limit_price = quote.latest_matched_price - slippage if side == "SELL" else quote.latest_matched_price + slippage
        limit_price = round(limit_price, 1)
        
        try:
            order_id = self.fix.place_order(
                full_symbol=self.symbol,
                side=side,
                qty=1,
                price=limit_price,
                ord_type="LIMIT"
            )
            self.pending_order_id = order_id
            self.pending_order_time = datetime.now()
            
            # Record INTENT to go flat
            self.pending_inventory = 0 
            
            logger.info(f"📤 Sent CLOSE ({side}) order: {order_id[:8]} at {limit_price}")
        except Exception as e:
            logger.error(f"Failed to place close order: {e}")

    # ------------------------------------------------------------------
    # Main Loop
    # ------------------------------------------------------------------

    async def run(self):
        """Start the live trading engine."""
        logger.info("🔌 Connecting to FIX Engine...")
        self.fix.connect()
        
        if not self.fix.wait_until_logged_on(timeout=10):
            logger.error(f"❌ Logon failed: {self.fix.last_logon_error()}")
            return

        logger.info("✅ FIX Logged On. Checking pending orders...")
        pending = self.fix.recover_pending_orders()
        for p in pending:
            logger.warning(f"🧹 Canceling orphan order: {p['cl_ord_id']}")
            try:
                self.fix.cancel_order(p['cl_ord_id'])
            except TimeoutError as e:
                logger.error(f"⚠️ Timeout canceling {p['cl_ord_id']}. It may already be closed/expired on the server.")
            except Exception as e:
                logger.error(f"⚠️ Failed to cancel {p['cl_ord_id']}: {e}")

        logger.info(f"📡 Subscribing to Market Data for {self.symbol}...")
        await self.md.subscribe(self.symbol, self.on_quote_update)
        await self.md.start()
        
        logger.info("🚀 Live Trading Engine Running. Waiting for ticks...")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏹️ Shutting down by user...")
        finally:
            await self.md.close()
            os._exit(0)  # Handle QuickFIX segfault as learned in Example 1

if __name__ == "__main__":
    load_dotenv()
    
    symbol = os.getenv("VN30F1M", "HNXDS:VN30F2605",)
    account = os.getenv("PAPER_ACCOUNT_ID", "main")
    
    bot = LiveSMABot(symbol=symbol, account=account)
    asyncio.run(bot.run())