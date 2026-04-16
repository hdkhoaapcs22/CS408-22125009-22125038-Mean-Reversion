#!/usr/bin/env python3
"""
Example 11 - Market Data via Kafka

Demonstrates real-time market data streaming via Kafka consumer.

Features:
- Subscribe to instrument updates
- Background consumer thread
- Cached quote access
- Full snapshot mode (merge_updates=True)

Usage:
    python examples/11_market_data_kafka.py

Press Ctrl+C to stop.

Required environment variables:
    PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS - Kafka bootstrap servers
    PAPERBROKER_KAFKA_USERNAME - SASL username  
    PAPERBROKER_KAFKA_PASSWORD - SASL password
    PAPERBROKER_ENV_ID - Environment ID (e.g., 'real', 'test')
    
Topic format: {env_id}.{exchange}.{symbol}
    Example: 'real.HNXDS.VN30F2602' for instrument 'HNXDS:VN30F2602'
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from paperbroker.market_data import KafkaMarketDataClient, QuoteSnapshot

# Load environment
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class QuoteTracker:
    """Track and display quote updates."""
    
    def __init__(self):
        self.update_count = 0
        self.instruments = set()
        self.last_prices = {}
    
    def on_quote(self, instrument: str, quote: QuoteSnapshot):
        """Handle quote update."""
        self.update_count += 1
        self.instruments.add(instrument)
        
        # Track price change
        prev_price = self.last_prices.get(instrument)
        current_price = quote.latest_matched_price
        self.last_prices[instrument] = current_price
        
        # Display update
        self.print_quote(instrument, quote, prev_price)
    
    def print_quote(self, instrument: str, quote: QuoteSnapshot, prev_price=None):
        """Pretty print quote update."""
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        print(f"\n{'=' * 80}")
        print(f"🔔 UPDATE #{self.update_count} | {timestamp} | {instrument}")
        print('─' * 80)
        
        # Latest trade
        if quote.latest_matched_price is not None:
            price = quote.latest_matched_price
            change_str = ""
            
            if prev_price and prev_price != price:
                change = price - prev_price
                pct = (change / prev_price) * 100
                arrow = "⬆️" if change > 0 else "⬇️"
                change_str = f"  {arrow} {change:+.2f} ({pct:+.2f}%)"
            
            print(f"  💰 Latest Price:  {price:>12,.2f}{change_str}")
            
            if quote.latest_matched_quantity:
                print(f"  📦 Last Qty:      {quote.latest_matched_quantity:>12,.0f}")
        
        # Order Book (Top 3 levels)
        print("\n  📊 Order Book:")
        print(f"     {'Bid':>15}  {'Qty':>10}  |  {'Ask':>15}  {'Qty':>10}")
        print("     " + "-" * 60)
        
        for i in range(1, 4):
            bid_p = getattr(quote, f'bid_price_{i}', None)
            bid_q = getattr(quote, f'bid_quantity_{i}', None)
            ask_p = getattr(quote, f'ask_price_{i}', None)
            ask_q = getattr(quote, f'ask_quantity_{i}', None)
            
            bid_str = f"{bid_p:>15,.2f}" if bid_p else f"{'---':>15}"
            bid_q_str = f"{bid_q:>10,.0f}" if bid_q else f"{'---':>10}"
            ask_str = f"{ask_p:>15,.2f}" if ask_p else f"{'---':>15}"
            ask_q_str = f"{ask_q:>10,.0f}" if ask_q else f"{'---':>10}"
            
            print(f"  {i}: {bid_str}  {bid_q_str}  |  {ask_str}  {ask_q_str}")
        
        # Spread
        if quote.spread is not None:
            print(f"\n  📐 Spread: {quote.spread:.2f} ({quote.spread_bps:.1f} bps)")
        
        # Reference prices
        if quote.ref_price:
            print(f"\n  📈 Ref: {quote.ref_price:,.2f} | "
                  f"High: {quote.highest_price or 'N/A'} | "
                  f"Low: {quote.lowest_price or 'N/A'}")
        
        # Volume
        if quote.total_matched_quantity:
            print(f"  📊 Total Volume: {quote.total_matched_quantity:,.0f}")


async def main():
    """Main function."""
    print("=" * 80)
    print("EXAMPLE 11: MARKET DATA VIA KAFKA")
    print("=" * 80)
    
    # Check environment
    bootstrap_servers = os.getenv('PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS')
    username = os.getenv('PAPERBROKER_KAFKA_USERNAME')
    password = os.getenv('PAPERBROKER_KAFKA_PASSWORD')
    # env_id = os.getenv('PAPERBROKER_ENV_ID')
    env_id = "real"
    
    if not bootstrap_servers:
        print("\n❌ Error: PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS not set in .env")
        print("   Required variables:")
        print("   - PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS")
        print("   - PAPERBROKER_KAFKA_USERNAME")
        print("   - PAPERBROKER_KAFKA_PASSWORD")
        print("   - PAPERBROKER_ENV_ID")
        return
    
    if not env_id:
        print("\n❌ Error: PAPERBROKER_ENV_ID not set in .env")
        print("   This is required for topic naming (e.g., 'real', 'test')")
        return
    
    print(f"\nKafka: {bootstrap_servers}")
    print(f"Username: {username}")
    print(f"Env ID: {env_id}")
    
    # Instrument to subscribe
    instrument = os.getenv('VN30F1M', 'HNXDS:VN30F2604')
    expected_topic = f"{env_id}.{instrument.replace(':', '.')}"
    print(f"Instrument: {instrument}")
    print(f"Topic: {expected_topic}")
    print("=" * 80)
    
    # Create client
    print("\n📡 Connecting to Kafka...")
    client = KafkaMarketDataClient(
        bootstrap_servers=bootstrap_servers,
        username=username,
        password=password,
        env_id=env_id,
        merge_updates=True  # Full snapshots
    )
    
    # Create tracker
    tracker = QuoteTracker()
    
    # Subscribe
    print(f"📌 Subscribing to {instrument}...")
    await client.subscribe(instrument, tracker.on_quote)
    
    # Start consumer
    print("🚀 Starting Kafka consumer...")
    await client.start()
    print("✅ Consumer started! Waiting for messages...")
    print("\nPress Ctrl+C to stop.\n")
    
    try:
        # Keep running and show periodic stats
        while True:
            await asyncio.sleep(10)
            print(f"\n📊 Stats: {tracker.update_count} updates, "
                  f"{len(tracker.instruments)} instruments")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping...")
    
    finally:
        await client.stop()
        print("✅ Consumer stopped")
        print(f"\n📊 Final Stats:")
        print(f"   Total updates: {tracker.update_count}")
        print(f"   Instruments: {tracker.instruments}")


if __name__ == '__main__':
    asyncio.run(main())
