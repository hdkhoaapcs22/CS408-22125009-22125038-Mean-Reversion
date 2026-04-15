#!/usr/bin/env python3
"""
Example 05 - Market Data Query Mode

Demonstrates direct quote queries using Redis GET.
Simple, synchronous-style data fetching.

Usage:
    python examples/05_market_data_query.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
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


async def main():
    """Main function demonstrating query mode."""
    
    # Load environment variables
    load_dotenv()
    
    # Configuration
    REDIS_HOST = os.getenv('MARKET_REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('MARKET_REDIS_PORT', '6379'))
    REDIS_PASSWORD = os.getenv('MARKET_REDIS_PASSWORD')
    
    # Instruments to query
    instruments = [
        'HNXDS:VN30F2511',
        'HSX:VNM',
        'HSX:HPG',
        'HSX:VCB',
    ]
    
    print("=" * 80)
    print("EXAMPLE 05: MARKET DATA QUERY MODE")
    print("=" * 80)
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Instruments: {', '.join(instruments)}")
    print("=" * 80)
    
    # Create client
    client = RedisMarketDataClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
    )
    
    try:
        # Query each instrument
        for instrument in instruments:
            print(f"\n{'─' * 80}")
            print(f"📊 Querying: {instrument}")
            print('─' * 80)
            
            quote = await client.query(instrument)
            
            if quote:
                print_quote(quote)
            else:
                print(f"⚠️  No data available for {instrument}")
        
        print(f"\n{'=' * 80}")
        print("✅ Query complete!")
        print("=" * 80)
    
    finally:
        await client.close()


def print_quote(quote: QuoteSnapshot) -> None:
    """
    Pretty print quote information.
    
    Args:
        quote: QuoteSnapshot to display
    """
    # Latest trade
    if quote.latest_matched_price:
        print(f"  💰 Latest Trade:")
        print(f"     Price:    {quote.latest_matched_price:>12,.2f}")
        print(f"     Quantity: {quote.latest_matched_quantity:>12,.0f}")
    
    # Best bid/ask
    if quote.bid_price_1 or quote.ask_price_1:
        print(f"\n  📈 Best Bid/Ask:")
        
        if quote.bid_price_1:
            print(f"     Bid:  {quote.bid_price_1:>12,.2f}  x  {quote.bid_quantity_1:>10,.0f}")
        
        if quote.ask_price_1:
            print(f"     Ask:  {quote.ask_price_1:>12,.2f}  x  {quote.ask_quantity_1:>10,.0f}")
        
        if quote.spread:
            print(f"     Spread: {quote.spread:>10,.2f}")
            if quote.spread_bps:
                print(f"     (Spread: {quote.spread_bps:>10.2f} bps)")
    
    # Level 2
    if quote.bid_price_2 or quote.ask_price_2:
        print(f"\n  📊 Level 2:")
        if quote.bid_price_2:
            print(f"     Bid2: {quote.bid_price_2:>12,.2f}  x  {quote.bid_quantity_2:>10,.0f}")
        if quote.ask_price_2:
            print(f"     Ask2: {quote.ask_price_2:>12,.2f}  x  {quote.ask_quantity_2:>10,.0f}")
    
    # Reference prices
    if quote.ref_price:
        print(f"\n  📌 Reference:")
        print(f"     Ref:     {quote.ref_price:>12,.2f}")
        if quote.ceiling_price:
            print(f"     Ceiling: {quote.ceiling_price:>12,.2f}")
        if quote.floor_price:
            print(f"     Floor:   {quote.floor_price:>12,.2f}")
    
    # Session stats
    if quote.open_price or quote.highest_price or quote.lowest_price:
        print(f"\n  📈 Session Stats:")
        if quote.open_price:
            print(f"     Open:    {quote.open_price:>12,.2f}")
        if quote.highest_price:
            print(f"     High:    {quote.highest_price:>12,.2f}")
        if quote.lowest_price:
            print(f"     Low:     {quote.lowest_price:>12,.2f}")
        if quote.total_matched_quantity:
            print(f"     Volume:  {quote.total_matched_quantity:>12,.0f}")
    
    # Timestamp
    if quote.datetime_str:
        print(f"\n  🕐 Timestamp: {quote.datetime_str}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
