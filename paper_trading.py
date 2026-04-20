#!/usr/bin/env python3
"""
Live Trading Engine: SMA Momentum Crossover (v2)

Adapted from backtesting-v2.py to asynchronous, event-driven live trading.
Key improvements over v1:
- SMA window persists across sessions (not reset daily)
- After TP/SL exit, immediately checks for crossover re-entry on the same tick
- Trade counter for monitoring execution frequency
"""

import os
import sys
import asyncio
import logging
from collections import deque
from datetime import datetime, time
from decimal import Decimal
from typing import Optional

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
SMA_WINDOW = 1000
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
        self.price_window = deque(maxlen=SMA_WINDOW)
        self.prev_price: Optional[float] = None
        self.prev_sma: Optional[float] = None
        
        # --- Trade Counter ---
        self.total_trades: int = 0   # count of completed round-trip trades
        
        # --- Execution State ---
        self.pending_order_id: Optional[str] = None
        self.last_quote_timestamp: Optional[str] = None
        
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
            # order_store_path="live_orders.db" # Crash recovery
        )
        
        # Wire up FIX events
        self.fix.on("fix:order:filled", self.on_order_filled)
        self.fix.on("fix:order:canceled", self.on_order_failed)
        self.fix.on("fix:order:rejected", self.on_order_failed)

        
        self.md = KafkaMarketDataClient(
            bootstrap_servers=os.getenv('PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS'),
            username=os.getenv('PAPERBROKER_KAFKA_USERNAME'),
            password=os.getenv('PAPERBROKER_KAFKA_PASSWORD'),
            env_id=os.getenv('PAPERBROKER_KAFKA_ENV_ID'),
            merge_updates=True
        )

    # ------------------------------------------------------------------
    # FIX Event Handlers (Execution Callbacks)
    # ------------------------------------------------------------------
    
    def on_order_filled(self, cl_ord_id, status, last_px, last_qty, **kw):
        """Handle execution fills."""
        if cl_ord_id != self.pending_order_id:
            return # Ignore manual trades or old orders

        logger.info(f"✅ ORDER FILLED: {last_qty} @ {last_px}")
        
        # Determine if this was an entry or an exit
        if self.inventory == 0:
            # We were flat, so this is a new entry
            pass # inventory/entry_price already set in open_position()
        else:
            # We had a position, so this must be a close
            self.total_trades += 1
            logger.info(f"🎯 Position Closed. Flat. (Total trades: {self.total_trades})")
            self.inventory = 0
            self.entry_price = 0.0
            
        self.pending_order_id = None # Free the lock

    def on_order_failed(self, cl_ord_id, **kw):
        """Handle rejected/canceled orders by freeing the lock."""
        if cl_ord_id == self.pending_order_id:
            logger.warning("⚠️ Order failed or canceled. Freeing lock.")
            self.pending_order_id = None

    # ------------------------------------------------------------------
    # Market Data Callback (Strategy Engine)
    # ------------------------------------------------------------------

    def on_quote_update(self, instrument: str, quote: QuoteSnapshot):
        # --- DIAGNOSTIC LOGGING ---
        logger.info(
            f"TICK EVAL | Price: {quote.latest_matched_price} | SMA: {sum(self.price_window) / SMA_WINDOW:.2f} | Window: {len(self.price_window)} | "
            f"PrevPx: {self.prev_price} | PrevSMA: {self.prev_sma if self.prev_sma else 0:.2f} | "
            f"Pos: {self.inventory} | Lock: {self.pending_order_id} | Trades: {self.total_trades}"
        )
        
        """Tick-by-Tick Evaluation — v2 logic with persistent SMA and immediate re-entry."""
        tick_price = quote.latest_matched_price
        if tick_price is None:
            return

        # 1. Update rolling window
        self.price_window.append(tick_price)
        if len(self.price_window) < SMA_WINDOW:
            return # Warming up

        cur_sma = sum(self.price_window) / SMA_WINDOW

        # Skip evaluation if we are currently waiting for an order to fill
        if self.pending_order_id is not None:
            self._update_prev_state(tick_price, cur_sma)
            return

        # 2. Check End-Of-Day (EOD) Force Close
        current_time = datetime.now().time()
        if current_time >= END_OF_DAY_CLOSE:
            if self.inventory != 0:
                logger.info("🚨 EOD TRIGGERED. Closing positions.")
                self.close_position(quote)
            self._update_prev_state(tick_price, cur_sma)
            return # Do not open new positions near EOD

        # 3. Check Take-Profit & Stop-Loss
        #    After TP/SL close, we do NOT return — we fall through to check
        #    for crossover re-entry on the same tick (v2 improvement).
        if self.inventory != 0:
            unrealized_pnl = self.inventory * (tick_price - self.entry_price)
            
            if unrealized_pnl >= TP_POINTS:
                logger.info(f"💰 TAKE PROFIT Hit! PnL: +{unrealized_pnl:.1f} pts")
                self.close_position(quote)
                self._update_prev_state(tick_price, cur_sma)
                return  # Must wait for fill before re-entry
                
            if unrealized_pnl <= -SL_POINTS:
                logger.info(f"🛑 STOP LOSS Hit! PnL: {unrealized_pnl:.1f} pts")
                self.close_position(quote)
                self._update_prev_state(tick_price, cur_sma)
                return  # Must wait for fill before re-entry

        # 4. Check Entry Signals (Crossover)
        if self.inventory == 0 and self.prev_price is not None and self.prev_sma is not None:
            # Buy signal: prev < SMA and cur >= SMA
            if self.prev_price < self.prev_sma and tick_price >= cur_sma:
                logger.info("📈 BUY Signal (Bullish Crossover)")
                self.open_position("BUY", quote)
                
            # Sell signal: prev > SMA and cur <= SMA
            elif self.prev_price > self.prev_sma and tick_price <= cur_sma:
                logger.info("📉 SELL Signal (Bearish Crossover)")
                self.open_position("SELL", quote)

        self._update_prev_state(tick_price, cur_sma)

    def _update_prev_state(self, price, sma):
        self.prev_price = price
        self.prev_sma = sma

    # ------------------------------------------------------------------
    # Execution Helpers
    # ------------------------------------------------------------------

    def open_position(self, side: str, quote: QuoteSnapshot):
        """Place an aggressive limit order to open a position."""
        # Use ceiling for BUY, floor for SELL to ensure instant fill (like a market order)
        limit_price = quote.ceiling_price if side == "BUY" else quote.floor_price
        
        try:
            order_id = self.fix.place_order(
                full_symbol=self.symbol,
                side=side,
                qty=1,
                price=limit_price,
                ord_type="LIMIT"
            )
            self.pending_order_id = order_id
            self.inventory = 1 if side == "BUY" else -1
            self.entry_price = quote.latest_matched_price # Track estimated entry
            logger.info(f"📤 Sent {side} order: {order_id[:8]} at {limit_price}")
        except Exception as e:
            logger.error(f"Failed to place open order: {e}")

    def close_position(self, quote: QuoteSnapshot):
        """Place an aggressive limit order to close the current position."""
        side = "SELL" if self.inventory == 1 else "BUY"
        limit_price = quote.floor_price if side == "SELL" else quote.ceiling_price
        
        try:
            order_id = self.fix.place_order(
                full_symbol=self.symbol,
                side=side,
                qty=1,
                price=limit_price,
                ord_type="LIMIT"
            )
            self.pending_order_id = order_id
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
        # pending = self.fix.recover_pending_orders()
        # for p in pending:
        #     logger.warning(f"🧹 Canceling orphan order: {p['cl_ord_id']}")
        #     self.fix.cancel_order(p['cl_ord_id'])

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
    account = os.getenv("PAPER_ACCOUNT_ID_D1", "D1")
    
    bot = LiveSMABot(symbol=symbol, account=account)
    asyncio.run(bot.run())