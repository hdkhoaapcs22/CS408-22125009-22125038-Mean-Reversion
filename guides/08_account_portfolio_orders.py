#!/usr/bin/env python3
"""
Example 08 - Account Portfolio & Orders

Demonstrates:
- get_portfolio_by_sub(): Get current positions with PnL
- get_orders(): Get orders in date range

Usage:
    python examples/08_account_portfolio_orders.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from paperbroker.client import PaperBrokerClient

# Load environment
load_dotenv()

# Configuration from .env
REST_BASE_URL = os.getenv("PAPER_REST_BASE_URL", "http://localhost:9090")
USERNAME = os.getenv("PAPER_USERNAME", "BL01")
PASSWORD = os.getenv("PAPER_PASSWORD", "123")
SUB_ACCOUNT = os.getenv("PAPER_ACCOUNT_ID_D1", "D1")
SOCKET_HOST = os.getenv("SOCKET_HOST", "localhost")
SOCKET_PORT = int(os.getenv("SOCKET_PORT", "5001"))
SENDER_COMP_ID = os.getenv("SENDER_COMP_ID", "BL-FIX")
TARGET_COMP_ID = os.getenv("TARGET_COMP_ID", "SERVER")


def print_separator(char="=", width=80):
    """Print separator line."""
    print(char * width)


def print_section(title: str):
    """Print section header."""
    print()
    print_separator()
    print(f"  {title}")
    print_separator()


def format_number(value, decimals=2):
    """Format number with commas and decimals."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(value)


def main():
    """Main entry point."""
    print_separator()
    print("EXAMPLE 08: ACCOUNT PORTFOLIO & ORDERS")
    print_separator()
    print(f"REST API: {REST_BASE_URL}")
    print(f"Username: {USERNAME}")
    print(f"Sub-Account: {SUB_ACCOUNT}")
    print_separator()

    if not USERNAME or not PASSWORD:
        print("\n❌ Error: PAPER_USERNAME and PAPER_PASSWORD required in .env")
        return

    # Create PaperBroker client (FIX + REST)
    print("\n🔌 Connecting to PaperBroker...")
    client = PaperBrokerClient(
        default_sub_account=SUB_ACCOUNT,
        username=USERNAME,
        password=PASSWORD,
        rest_base_url=REST_BASE_URL,
        socket_connect_host=SOCKET_HOST,
        socket_connect_port=SOCKET_PORT,
        sender_comp_id=SENDER_COMP_ID,
        target_comp_id=TARGET_COMP_ID,
        console=False,  # Disable verbose FIX logs
    )

    # Connect and wait for logon
    client.connect()
    if not client.wait_until_logged_on(timeout=10):
        error = client.last_logon_error()
        print(f"❌ Logon failed: {error}")
        return

    print("✅ Successfully connected!")

    # =================================================================
    # 1. GET PORTFOLIO
    # =================================================================
    print_section("📊 PORTFOLIO (Current Positions + Pending)")

    # Use wrapper method from PaperBrokerClient (uses default sub-account)
    portfolio = client.get_portfolio_by_sub()

    if portfolio.get("success"):
        items = portfolio.get("items", [])
        print(f"\nFound {len(items)} instrument(s):")
        print()

        if items:
            # Header
            print(f"{'Instrument':<15} {'Quantity':>12} {'Avg Price':>12} {'Total Cost':>15} "
                  f"{'Current Price':>15} {'Market Value':>15} {'PnL':>15}")
            print("-" * 115)

            total_cost = 0
            total_market_value = 0
            total_pnl = 0

            for item in items:
                instrument = item.get("instrument", "")
                quantity = item.get("quantity", 0)
                avg_price = item.get("avgPrice")  # Server returns avgPrice
                cost = item.get("totalCost", 0)
                current_price = item.get("currentPrice")
                market_value = item.get("marketValue")
                pnl = item.get("pnl")

                # Track totals
                try:
                    total_cost += float(cost or 0)
                    if market_value is not None:
                        total_market_value += float(market_value)
                    if pnl is not None:
                        total_pnl += float(pnl)
                except (ValueError, TypeError):
                    pass

                # Format PnL with color
                pnl_str = format_number(pnl)
                if pnl is not None:
                    try:
                        if float(pnl) > 0:
                            pnl_str = f"🟢 +{pnl_str}"
                        elif float(pnl) < 0:
                            pnl_str = f"🔴 {pnl_str}"
                        else:
                            pnl_str = f"   {pnl_str}"
                    except (ValueError, TypeError):
                        pass

                print(f"{instrument:<15} "
                      f"{format_number(quantity, 0):>12} "
                      f"{format_number(avg_price):>12} "
                      f"{format_number(cost):>15} "
                      f"{format_number(current_price):>15} "
                      f"{format_number(market_value):>15} "
                      f"{pnl_str:>15}")

            # Summary
            print("-" * 115)
            print(f"{'TOTAL':<15} {'':<12} {'':<12} "
                  f"{format_number(total_cost):>15} "
                  f"{'':<15} "
                  f"{format_number(total_market_value):>15} "
                  f"{format_number(total_pnl):>15}")

            # PnL percentage
            if total_cost > 0 and total_pnl != 0:
                pnl_pct = (total_pnl / total_cost) * 100
                print()
                print(f"📈 Total PnL: {format_number(total_pnl)} "
                      f"({format_number(pnl_pct)}%)")
        else:
            print("No positions found.")
    else:
        print(f"❌ Failed to get portfolio: {portfolio}")

    # =================================================================
    # 2. GET ORDERS (Last 7 Days)
    # =================================================================
    print_section("📜 ORDERS (Last 7 Days)")

    # Calculate date range
    today = datetime.now().date()
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    print(f"\nDate Range: {start_date} to {end_date}")
    print()

    # Use wrapper method from PaperBrokerClient
    orders = client.get_orders(start_date, end_date)

    if orders.get("success"):
        items = orders.get("items", [])
        print(f"Found {len(items)} order(s):")
        print()

        if items:
            # Display orders
            for i, order in enumerate(items, 1):
                print(f"\n{'─' * 80}")
                print(f"Order #{i}")
                print(f"{'─' * 80}")

                # Basic info
                print(f"Order ID:       {order.get('orderId', 'N/A')}")
                print(f"Client Order:   {order.get('clOrdId', 'N/A')}")
                print(f"Symbol:         {order.get('symbol', 'N/A')}")
                print(f"Exchange:       {order.get('securityExchange', 'N/A')}")

                # Side
                side_code = order.get("side", "")
                side_text = "BUY" if side_code == "1" else "SELL" if side_code == "2" else side_code
                print(f"Side:           {side_text}")

                # Quantities & Prices
                print(f"Order Qty:      {format_number(order.get('orderQty'), 0)}")
                print(f"Filled Qty:     {format_number(order.get('cumQty'), 0)}")
                print(f"Leaves Qty:     {format_number(order.get('leavesQty'), 0)}")
                print(f"Price:          {format_number(order.get('price'))}")
                print(f"Avg Fill Price: {format_number(order.get('avgPx'))}")

                # Status
                status = order.get("ordStatus", "N/A")
                status_text = order.get("statusText", "")
                print(f"Status:         {status} ({status_text})")

                # Dates
                print(f"Order Date:     {order.get('orderDate', 'N/A')}")
                print(f"Last Update:    {order.get('lastUpdateDate', 'N/A')}")

                # Flags
                flags = []
                if order.get("isCancelled"):
                    flags.append("CANCELLED")
                if order.get("isExecuting"):
                    flags.append("EXECUTING")
                if flags:
                    print(f"Flags:          {', '.join(flags)}")

            print(f"\n{'─' * 80}")
        else:
            print("No orders found in the date range.")
    else:
        print(f"❌ Failed to get orders: {orders}")

    print()
    print_separator()
    print("✅ Example completed")
    print_separator()
    
    # Note: We don't call client.disconnect() to avoid QuickFIX cleanup issues
    # The process will terminate cleanly on its own


if __name__ == "__main__":
    main()
