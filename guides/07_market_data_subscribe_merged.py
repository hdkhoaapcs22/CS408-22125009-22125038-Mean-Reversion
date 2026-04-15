"""
Example 07 - Market Data Subscribe (MERGED Mode)

This example demonstrates REAL-TIME market data subscription using Redis pub/sub
with MERGED mode enabled.

MERGED MODE:
- Always shows FULL snapshot of all fields
- Unchanged fields are filled with previous cached values
- Perfect for application developers who need complete data
- Easier to work with - no need to track state yourself

Compare with Example 06 (RAW mode):
- Example 06: Only changed fields, others = None (low-level)
- Example 07: Full snapshot every time (application-level)

Usage:
    python examples/07_market_data_subscribe_merged.py
"""

import asyncio
import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from paperbroker.market_data import RedisMarketDataClient, QuoteSnapshot

# Load environment
load_dotenv()

# Configuration
REDIS_HOST = os.getenv("MARKET_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("MARKET_REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("MARKET_REDIS_PASSWORD")
INSTRUMENT = "HNXDS:VN30F2603"


class UpdateTracker:
    """Tracks market data updates for display."""

    def __init__(self):
        self.update_count = 0
        self.last_price: Optional[float] = None
        self.instruments: set[str] = set()

    def get_price_arrow(self, current_price: Optional[float]) -> str:
        """Return arrow indicator for price movement."""
        if current_price is None or self.last_price is None:
            return "•"

        if current_price > self.last_price:
            return "↑"
        elif current_price < self.last_price:
            return "↓"
        else:
            return "="

    def update(self, instrument: str, quote: QuoteSnapshot):
        """Update tracking info."""
        self.update_count += 1
        self.instruments.add(instrument)

        # Track price changes
        price_arrow = self.get_price_arrow(quote.latest_matched_price)
        self.last_price = quote.latest_matched_price

        # Display update
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        print("\n" + "─" * 80)
        print(
            f"📊 Update #{self.update_count} | {timestamp} | "
            f"{instrument} {price_arrow}"
        )
        print("─" * 80)

        # Latest Price (ALWAYS shown in merged mode)
        if quote.latest_matched_price is not None:
            print(
                f"\n   💰 Latest: {quote.latest_matched_price:>12,.2f}  "
                f"({price_arrow} from previous)"
            )

        # Bid/Ask (ALWAYS shown in merged mode)
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

            # Spread calculation
            if quote.spread is not None:
                spread_bps = quote.spread_bps if quote.spread_bps is not None else 0
                print(f"      Spread: {quote.spread:>10,.2f}  ({spread_bps:.2f} bps)")

        # Session Info (ALWAYS shown in merged mode)
        session_parts = []
        if quote.open_price is not None:
            session_parts.append(f"Open: {quote.open_price:>10,.2f}")
        if quote.highest_price is not None:
            session_parts.append(f"High: {quote.highest_price:>10,.2f}")
        if quote.lowest_price is not None:
            session_parts.append(f"Low: {quote.lowest_price:>10,.2f}")

        if session_parts:
            print("\n   📈 Session:")
            for part in session_parts:
                print(f"      {part}")

        # Trading Activity (ALWAYS shown in merged mode)
        activity_parts = []
        if quote.total_matched_quantity is not None:
            activity_parts.append(f"Volume: {quote.total_matched_quantity:>12,.0f}")

        if activity_parts:
            print("\n   📦 Trading Activity:")
            for part in activity_parts:
                print(f"      {part}")

        # Summary footer
        print("\n   📝 Note: ALL fields shown above are complete snapshots")
        print("      (unchanged fields filled with cached values)")


async def handle_quote_update(instrument: str, quote: QuoteSnapshot):
    """Callback for quote updates."""
    tracker.update(instrument, quote)


async def main():
    """Main entry point."""
    print("=" * 80)
    print("EXAMPLE 07: MARKET DATA SUBSCRIBE (MERGED MODE)")
    print("=" * 80)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Instrument: {INSTRUMENT}")
    print()
    print("🔄 MERGED MODE: Full snapshot every update")
    print("   All fields always populated (cached values used)")
    print("   Perfect for application developers")
    print()
    print("Press Ctrl+C to stop...")
    print("=" * 80)

    # Create client in MERGED mode (merge_updates=True)
    client = RedisMarketDataClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        merge_updates=True,  # MERGED mode: always show full snapshot
    )

    try:
        # Subscribe (client will auto-connect)
        print(f"\n🔔 Subscribing to {INSTRUMENT}...")
        await client.subscribe(INSTRUMENT, handle_quote_update)

        # Keep running
        print("\n⏳ Listening for updates...\n")
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        # Display summary
        print("\n" + "=" * 80)
        print("📊 SESSION SUMMARY")
        print("=" * 80)
        print(f"   Total Updates: {tracker.update_count}")

        if tracker.instruments:
            print("\n   Tracked Instruments:")
            for inst in sorted(tracker.instruments):
                print(f"      - {inst}")

        print("\n   Mode: MERGED (full snapshots with cached values)")
        print("=" * 80)

        # Cleanup
        await client.close()
        print("\n✅ Client closed gracefully")


# Initialize tracker
tracker = UpdateTracker()

# Run
if __name__ == "__main__":
    asyncio.run(main())
