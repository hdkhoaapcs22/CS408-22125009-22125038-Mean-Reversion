#!/usr/bin/env python3
"""
Example 09 - Account Transactions & Order Mapping

Demonstrates:
- get_transactions_by_date(): Get transaction history (fills/executions)
- Mapping transactions to orders via orderId
- Analyzing transaction patterns and fees
- Calculating total PnL and trade statistics

Key Concepts:
- Transaction = Individual fill/execution
- Order = Intent to buy/sell (can have multiple transactions)
- Transaction.orderId links back to Order.orderId

Usage:
    python examples/09_account_transactions.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
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
    print("EXAMPLE 09: ACCOUNT TRANSACTIONS & ORDER MAPPING")
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
    # 1. GET TRANSACTIONS (Last 7 Days)
    # =================================================================
    print_section("📜 TRANSACTIONS (Last 7 Days)")

    # Calculate date range
    today = datetime.now().date()
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    print(f"\nDate Range: {start_date} to {end_date}")
    print()

    # Get transactions
    transactions_result = client.get_transactions_by_date(start_date, end_date)

    if not transactions_result.get("success"):
        print(f"❌ Failed to get transactions: {transactions_result}")
        return

    transactions = transactions_result.get("items", [])
    print(f"Found {len(transactions)} transaction(s)")

    if not transactions:
        print("\nNo transactions found in the date range.")
        print_separator()
        print("✅ Example completed (no data to display)")
        print_separator()
        return

    # =================================================================
    # 2. DISPLAY TRANSACTIONS
    # =================================================================
    print()
    print(f"{'#':<4} {'Transaction ID':<38} {'Order ID':<38} {'Symbol':<18} {'Type':<6} {'Qty':>8} {'Price':>12} {'Fee':>12}")
    print("-" * 150)

    for i, txn in enumerate(transactions, 1):
        print(f"{i:<4} "
              f"{txn.get('transactionId', 'N/A'):<38} "
              f"{txn.get('orderId', 'N/A'):<38} "
              f"{txn.get('symbol', 'N/A'):<18} "
              f"{txn.get('type', 'N/A'):<6} "
              f"{format_number(txn.get('quantity'), 0):>8} "
              f"{format_number(txn.get('price')):>12} "
              f"{format_number(txn.get('totalFee')):>12}")

    # =================================================================
    # 3. TRANSACTION STATISTICS
    # =================================================================
    print_section("📊 TRANSACTION STATISTICS")

    total_buy_qty = 0
    total_sell_qty = 0
    total_buy_cost = 0
    total_sell_cost = 0
    total_fees = 0
    total_pnl = 0

    for txn in transactions:
        txn_type = txn.get('type', '')
        qty = float(txn.get('quantity', 0))
        cost = float(txn.get('totalCost', 0))
        fee = float(txn.get('totalFee', 0))
        pnl = float(txn.get('pnl', 0))

        total_fees += fee
        total_pnl += pnl

        if txn_type == 'BUY':
            total_buy_qty += qty
            total_buy_cost += cost
        elif txn_type == 'SELL':
            total_sell_qty += qty
            total_sell_cost += cost

    print(f"\nBuy Transactions:")
    print(f"  Total Quantity: {format_number(total_buy_qty, 0)}")
    print(f"  Total Cost: {format_number(total_buy_cost)} VND")
    if total_buy_qty > 0:
        print(f"  Average Price: {format_number(total_buy_cost / total_buy_qty)} VND")

    print(f"\nSell Transactions:")
    print(f"  Total Quantity: {format_number(total_sell_qty, 0)}")
    print(f"  Total Revenue: {format_number(total_sell_cost)} VND")
    if total_sell_qty > 0:
        print(f"  Average Price: {format_number(total_sell_cost / total_sell_qty)} VND")

    print(f"\nOverall:")
    print(f"  Total Fees: {format_number(total_fees)} VND")
    print(f"  Total PnL: {format_number(total_pnl)} VND")
    print(f"  Net Trading: {format_number(total_sell_cost - total_buy_cost - total_fees)} VND")

    # =================================================================
    # 4. MAP TRANSACTIONS TO ORDERS
    # =================================================================
    print_section("🔗 TRANSACTION-TO-ORDER MAPPING")

    # Get orders for the same date range
    orders_result = client.get_orders(start_date, end_date)

    if not orders_result.get("success"):
        print(f"❌ Failed to get orders: {orders_result}")
        return

    orders = orders_result.get("items", [])
    print(f"\nFound {len(orders)} order(s) in the same period")

    # Build order lookup by orderId
    order_lookup = {order.get('orderId'): order for order in orders}

    # Group transactions by orderId
    txns_by_order = defaultdict(list)
    for txn in transactions:
        order_id = txn.get('orderId')
        if order_id:
            txns_by_order[order_id].append(txn)

    print(f"\nMapping {len(transactions)} transactions to {len(txns_by_order)} unique orders:")
    print()

    # Display mapping
    for order_id, txns in txns_by_order.items():
        order = order_lookup.get(order_id, {})
        
        print(f"{'─' * 80}")
        print(f"Order ID: {order_id}")
        
        if order:
            print(f"  Symbol: {order.get('symbol', 'N/A')}")
            print(f"  Client Order ID: {order.get('clOrdId', 'N/A')}")
            
            side_code = order.get('side', '')
            side_text = 'BUY' if side_code == '1' else 'SELL' if side_code == '2' else side_code
            print(f"  Side: {side_text}")
            
            print(f"  Order Quantity: {format_number(order.get('orderQty'), 0)}")
            print(f"  Filled Quantity: {format_number(order.get('cumQty'), 0)}")
            print(f"  Average Fill Price: {format_number(order.get('avgPx'))}")
            print(f"  Status: {order.get('ordStatus', 'N/A')}")
        else:
            print(f"  ⚠️  Order details not found (may be outside date range)")
        
        print(f"\n  Transactions ({len(txns)}):")
        
        for i, txn in enumerate(txns, 1):
            print(f"    {i}. {txn.get('type'):<4} "
                  f"{format_number(txn.get('quantity'), 0):>6} @ "
                  f"{format_number(txn.get('price')):>12} VND | "
                  f"Fee: {format_number(txn.get('totalFee')):>10} | "
                  f"Time: {txn.get('sysTimestamp', 'N/A')}")
        
        print()

    # =================================================================
    # 5. ANALYSIS: ORDERS WITH MULTIPLE FILLS
    # =================================================================
    print_section("🔍 ORDERS WITH MULTIPLE FILLS (Partial Executions)")

    multi_fill_orders = [(order_id, txns) for order_id, txns in txns_by_order.items() if len(txns) > 1]

    if multi_fill_orders:
        print(f"\nFound {len(multi_fill_orders)} order(s) with multiple fills:")
        print()

        for order_id, txns in multi_fill_orders:
            order = order_lookup.get(order_id, {})
            symbol = order.get('symbol', 'N/A') if order else 'N/A'
            
            print(f"Order {order.get('clOrdId', order_id[:8])}... ({symbol}):")
            print(f"  Total Fills: {len(txns)}")
            
            total_qty = sum(float(txn.get('quantity', 0)) for txn in txns)
            total_cost = sum(float(txn.get('totalCost', 0)) for txn in txns)
            avg_price = total_cost / total_qty if total_qty > 0 else 0
            
            print(f"  Total Quantity: {format_number(total_qty, 0)}")
            print(f"  Weighted Avg Price: {format_number(avg_price)}")
            
            print(f"  Fill Sequence:")
            for i, txn in enumerate(txns, 1):
                print(f"    Fill #{i}: {format_number(txn.get('quantity'), 0)} @ "
                      f"{format_number(txn.get('price'))} at {txn.get('sysTimestamp', 'N/A')}")
            
            print()
    else:
        print("\nNo orders with multiple fills found.")
        print("All orders were filled in a single execution.")

    # =================================================================
    # 6. SYMBOL BREAKDOWN
    # =================================================================
    print_section("📈 BREAKDOWN BY SYMBOL")

    # Group by symbol
    by_symbol = defaultdict(lambda: {
        'buy_qty': 0,
        'sell_qty': 0,
        'buy_cost': 0,
        'sell_cost': 0,
        'fees': 0,
        'count': 0
    })

    for txn in transactions:
        symbol = txn.get('symbol', 'UNKNOWN')
        stats = by_symbol[symbol]
        
        txn_type = txn.get('type', '')
        qty = float(txn.get('quantity', 0))
        cost = float(txn.get('totalCost', 0))
        fee = float(txn.get('totalFee', 0))
        
        stats['count'] += 1
        stats['fees'] += fee
        
        if txn_type == 'BUY':
            stats['buy_qty'] += qty
            stats['buy_cost'] += cost
        elif txn_type == 'SELL':
            stats['sell_qty'] += qty
            stats['sell_cost'] += cost

    print()
    print(f"{'Symbol':<20} {'Txns':>6} {'Buy Qty':>10} {'Sell Qty':>10} {'Fees':>15} {'Net':>15}")
    print("-" * 90)

    for symbol in sorted(by_symbol.keys()):
        stats = by_symbol[symbol]
        net = stats['sell_cost'] - stats['buy_cost'] - stats['fees']
        
        print(f"{symbol:<20} "
              f"{stats['count']:>6} "
              f"{format_number(stats['buy_qty'], 0):>10} "
              f"{format_number(stats['sell_qty'], 0):>10} "
              f"{format_number(stats['fees']):>15} "
              f"{format_number(net):>15}")

    print()
    print_separator()
    print("✅ Example completed")
    print_separator()
    
    print("\nKey Takeaways:")
    print("  • Transactions represent actual fills/executions")
    print("  • Each transaction links to an order via orderId")
    print("  • Orders can have multiple transactions (partial fills)")
    print("  • Transaction data includes fees, PnL, and timestamps")
    print("  • Useful for detailed trade analysis and reconciliation")


if __name__ == "__main__":
    main()
