#!/usr/bin/env python3
"""
Example 06 - Market Data Subscribe (RAW Mode)

Demonstrates real-time quote updates via Redis pub/sub in RAW mode.

RAW MODE: Only shows fields that changed in each update.
          Unchanged fields will be None.
          This is for low-level developers who want to see exact changes.

Usage:
    python examples/06_market_data_subscribe.py

Press Ctrl+C to stop.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from paperbroker.market_data import RedisMarketDataClient, QuoteSnapshot


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# Track updates
class UpdateTracker:
    """Track quote update statistics."""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.update_count = 0
        self.instruments = set()
        self.last_prices = {}
    
    def on_update(self, instrument: str, quote: QuoteSnapshot):
        """Handle quote update."""
        self.update_count += 1
        self.instruments.add(quote.instrument)
        
        # Track price change
        prev_price = self.last_prices.get(quote.instrument)
        self.last_prices[quote.instrument] = quote.latest_matched_price
        
        # Print update
        self.print_update(quote, prev_price)
    
    def print_update(self, quote: QuoteSnapshot, prev_price: Optional[float] = None):
        """Print quote update."""
        print(f"\n{'=' * 80}")
        print(f"🔔 UPDATE #{self.update_count} - {quote.instrument}")
        print(f"   Time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        print('─' * 80)
        
        # Latest trade
        if quote.latest_matched_price:
            price = quote.latest_matched_price
            change_symbol = ""
            
            if prev_price:
                change = price - prev_price
                pct_change = (change / prev_price) * 100
                
                if change > 0:
                    change_symbol = f"  ⬆️  +{change:,.2f} (+{pct_change:.2f}%)"
                elif change < 0:
                    change_symbol = f"  ⬇️  {change:,.2f} ({pct_change:.2f}%)"
                else:
                    change_symbol = "  ➡️  No change"
            
            print(f"   💰 Price:    {price:>12,.2f}{change_symbol}")
            
            if quote.latest_matched_quantity:
                print(f"   📦 Quantity: {quote.latest_matched_quantity:>12,.0f}")
        
        # Bid/Ask
        if quote.bid_price_1 is not None or quote.ask_price_1 is not None:
            print("\n   📊 Order Book:")
            
            if quote.bid_price_1 is not None:
                bid_qty = quote.bid_quantity_1 if quote.bid_quantity_1 is not None else 0
                print(
                    f"      Bid: {quote.bid_price_1:>12,.2f}  "
                    f"x {bid_qty:>10,.0f}"
                )
            
            if quote.ask_price_1 is not None:
                ask_qty = quote.ask_quantity_1 if quote.ask_quantity_1 is not None else 0
                print(
                    f"      Ask: {quote.ask_price_1:>12,.2f}  "
                    f"x {ask_qty:>10,.0f}"
                )
            
            if quote.spread:
                print(f"      Spread: {quote.spread:>10,.2f}", end="")
                if quote.spread_bps:
                    print(f" ({quote.spread_bps:.2f} bps)")
                else:
                    print()
        
        # Mid price
        if quote.mid_price:
            print(f"\n   🎯 Mid:      {quote.mid_price:>12,.2f}")
        
        # Reference
        if quote.ref_price:
            print(f"   📌 Ref:      {quote.ref_price:>12,.2f}")
        
        # Session stats
        if quote.total_matched_quantity:
            print(f"\n   📈 Total Volume: {quote.total_matched_quantity:>12,.0f}")
        
        print('=' * 80)
    
    def print_summary(self):
        """Print session summary."""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n\n" + "=" * 80)
        print("📊 SESSION SUMMARY")
        print("=" * 80)
        print(f"   Duration:    {duration:.1f} seconds")
        print(f"   Updates:     {self.update_count}")
        print(f"   Instruments: {len(self.instruments)}")
        
        if self.update_count > 0 and duration > 0:
            rate = self.update_count / duration
            print(f"   Update Rate: {rate:.2f} updates/sec")
        
        if self.instruments:
            print(f"\n   Tracked Instruments:")
            for inst in sorted(self.instruments):
                price = self.last_prices.get(inst)
                if price:
                    print(f"      {inst}: {price:,.2f}")
        
        print("=" * 80)


async def main():
    """Main function demonstrating subscribe mode."""
    
    # Load environment variables
    load_dotenv()
    
    # Configuration
    REDIS_HOST = os.getenv('MARKET_REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('MARKET_REDIS_PORT', '6379'))
    REDIS_PASSWORD = os.getenv('MARKET_REDIS_PASSWORD')
    
    # Instrument to subscribe
    INSTRUMENT = 'HNXDS:VN30F2602'
    
    print("=" * 80)
    print("EXAMPLE 06: MARKET DATA SUBSCRIBE (RAW MODE)")
    print("=" * 80)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Instrument: {INSTRUMENT}")
    print()
    print("📌 RAW MODE: Only changed fields are shown")
    print("   Unchanged fields = None (not displayed)")
    print("   Perfect for low-level developers")
    print()
    print("Press Ctrl+C to stop...")
    print("=" * 80)
    
    # Create client in RAW mode (merge_updates=False)
    client = RedisMarketDataClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        merge_updates=False,  # RAW mode: only show changed fields
    )
    
    # Create tracker
    tracker = UpdateTracker()
    
    try:
        # Subscribe to instrument
        logger.info(f"Subscribing to {INSTRUMENT}...")
        await client.subscribe(INSTRUMENT, tracker.on_update)
        
        logger.info("✅ Subscription active. Waiting for updates...")
        logger.info("   (This will run until you press Ctrl+C)")
        
        # Run forever
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    
    finally:
        # Print summary
        tracker.print_summary()
        
        # Cleanup
        logger.info("Closing connection...")
        await client.close()
        logger.info("✅ Closed")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    
    sys.exit(0)
