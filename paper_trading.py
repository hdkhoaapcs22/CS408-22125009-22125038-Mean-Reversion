import os
import asyncio
from collections import deque
from decimal import Decimal

# Assume these are imported from your actual broker/data client modules
# from paper_broker import PaperBrokerClient
# from market_data import KafkaMarketDataClient

# ---------- Strategy constants ----------
SMA_WINDOW   = 1000
TP_POINTS    = Decimal('3')
SL_POINTS    = Decimal('2')
SYMBOL       = "VN30F1M" # Assuming this is your target symbol

class SMACrossoverStrategy:
    def __init__(self, fix_client):
        self.fix = fix_client
        
        # Position state
        self.inventory: int = 0          # +1 = long, -1 = short, 0 = flat
        self.entry_price = None          # Decimal price at which we entered
        
        # Order state lock to prevent spamming orders while one is in flight
        self.pending_order = False       
        self.expected_action = None      # "OPEN_LONG", "OPEN_SHORT", "CLOSE"

        # Rolling window
        self._price_window = deque(maxlen=SMA_WINDOW)
        self._prev_price = None
        self._prev_sma = None

    def _current_sma(self):
        if len(self._price_window) < SMA_WINDOW:
            return None
        return sum(self._price_window) / Decimal(SMA_WINDOW)

    def _unrealised_pnl(self, cur_price: Decimal) -> Decimal:
        if self.inventory == 0 or self.entry_price is None:
            return Decimal('0')
        direction = Decimal('1') if self.inventory > 0 else Decimal('-1')
        return direction * (cur_price - self.entry_price) # Returns PnL in Index Points

    def on_quote(self, instrument, quote):
        if not quote.latest_matched_price:
            return

        tick_price = Decimal(str(quote.latest_matched_price))

        # Update rolling SMA window
        self._price_window.append(tick_price)
        cur_sma = self._current_sma()

        # Wait until we have enough ticks to calculate SMA
        if cur_sma is not None and self._prev_price is not None and self._prev_sma is not None:
            prev_price = self._prev_price
            prev_sma   = self._prev_sma

            # 1. Check Take-Profit / Stop-Loss on an existing position
            if self.inventory != 0 and not self.pending_order:
                upnl = self._unrealised_pnl(tick_price)
                if upnl >= TP_POINTS or upnl <= -SL_POINTS:
                    print(f"Triggering Exit! Unrealized PnL: {upnl} points")
                    self._close_position()

            # 2. Check for entry signals if we are flat
            if self.inventory == 0 and not self.pending_order:
                # Buy signal: prev < SMA and cur >= SMA
                if prev_price < prev_sma and tick_price >= cur_sma:
                    print(f"Buy Signal! Price: {tick_price}, SMA: {cur_sma}")
                    self._open_position(1)
                
                # Sell signal: prev > SMA and cur <= SMA
                elif prev_price > prev_sma and tick_price <= cur_sma:
                    print(f"Sell Signal! Price: {tick_price}, SMA: {cur_sma}")
                    self._open_position(-1)

        # Store for next tick
        self._prev_price = tick_price
        self._prev_sma   = cur_sma

    def _open_position(self, direction: int):
        self.pending_order = True
        self.expected_action = "OPEN_LONG" if direction == 1 else "OPEN_SHORT"
        side = "BUY" if direction == 1 else "SELL"
        
        # The backtest specifies Limit Buy at ceiling / Limit sell at floor.
        # If your API supports LIMIT orders, change ord_type to "LIMIT" and pass the calculated price.
        # For execution guarantees mimicking your snippet, we default to MARKET.
        try:
            order_id = self.fix.place_order(
                full_symbol=SYMBOL,
                side=side,
                qty=1, 
                ord_type="MARKET"   
            )
            print(f"[{side}] Open Order Placed: {order_id}")
        except Exception as e:
            print(f"Failed to place open order: {e}")
            self.pending_order = False

    def _close_position(self):
        self.pending_order = True
        self.expected_action = "CLOSE"
        side = "SELL" if self.inventory > 0 else "BUY"
        
        try:
            order_id = self.fix.place_order(
                full_symbol=SYMBOL,
                side=side,
                qty=1, 
                ord_type="MARKET"   
            )
            print(f"[{side}] Close Order Placed: {order_id}")
        except Exception as e:
            print(f"Failed to place close order: {e}")
            self.pending_order = False

    # --- FIX Callbacks ---
    def on_order_filled(self, cl_ord_id, last_px):
        """Called when FIX confirms the order is filled."""
        last_px = Decimal(str(last_px))
        self.pending_order = False
        
        if self.expected_action == "OPEN_LONG":
            self.inventory = 1
            self.entry_price = last_px
            print(f"Fill confirmed. Long 1 @ {last_px}")
            
        elif self.expected_action == "OPEN_SHORT":
            self.inventory = -1
            self.entry_price = last_px
            print(f"Fill confirmed. Short 1 @ {last_px}")
            
        elif self.expected_action == "CLOSE":
            print(f"Fill confirmed. Position closed @ {last_px}. PnL booked.")
            self.inventory = 0
            self.entry_price = None


async def main():
    # 1. Initialize FIX Client
    fix = PaperBrokerClient(
        default_sub_account=os.getenv("PAPER_ACCOUNT_ID_D1", "D1"),
        username=os.getenv("PAPER_USERNAME", "BL01"),
        password=os.getenv("PAPER_PASSWORD"),
        rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://localhost:9090"),
        socket_connect_host=os.getenv("SOCKET_HOST", "localhost"),
        socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id=os.getenv("SENDER_COMP_ID", "cross-FIX"),
        target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
        order_store_path="orders.db",
        console=False,
    )

    # 2. Initialize Strategy
    trader = SMACrossoverStrategy(fix_client=fix)

    # 3. Define FIX Listeners to route to the strategy
    def on_accepted(cl_ord_id, **kw):
        print(f"Order {cl_ord_id} accepted by broker.")

    def on_filled(cl_ord_id, last_px, last_qty, **kw):
        trader.on_order_filled(cl_ord_id, last_px)

    fix.on("fix:order:accepted", on_accepted)
    fix.on("fix:order:filled", on_filled)

    # Connect to FIX
    fix.connect()
    fix.wait_until_logged_on(timeout=10)
    print("FIX Logged on successfully.")

    # 4. Initialize Kafka Data Feed
    kafka = KafkaMarketDataClient(
        bootstrap_servers=os.getenv("PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS"),
        username=os.getenv("PAPERBROKER_KAFKA_USERNAME"),
        password=os.getenv("PAPERBROKER_KAFKA_PASSWORD"),
        env_id=os.getenv("ENV_ID", "DEFAULT"),
        merge_updates=True
    )

    # Route Kafka quotes directly to the strategy's tick processor
    def on_kafka_quote(instrument, quote):
        trader.on_quote(instrument, quote)

    await kafka.subscribe(SYMBOL, on_kafka_quote)
    
    print("Starting Kafka Feed. Listening for ticks...")
    await kafka.start()

if __name__ == "__main__":
    asyncio.run(main())