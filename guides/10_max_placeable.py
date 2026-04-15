#!/usr/bin/env python3
"""
Example 10 - Maximum Placeable Quantity Calculator

Demonstrates:
- get_max_placeable(): Calculate max order size before placing
- Account for margin requirements, fees, and available cash
- Compare BUY vs SELL max quantities
- Test different price levels and symbols
- Risk management and position sizing

Key Concepts:
- Max Placeable = Maximum quantity you can order given current cash
- Considers: Free cash, margin requirements, trading fees, current positions
- Different for BUY (need cash) vs SELL (might have position or unlimited)
- Critical for risk management and avoiding rejected orders

Usage:
    python examples/10_max_placeable.py
"""

import os
import sys
from pathlib import Path
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
DERIVATIVE_SYMBOL = os.getenv("VN30F1M", "HNXDS:VN30F2602")
EQUITY_SYMBOL = os.getenv("EQUITY", "HSX:MWG")
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


def format_number(value, decimals=0):
    """Format number with commas and decimals."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(value)


def display_max_placeable(symbol: str, price: float, side: str, result: dict):
    """Display max placeable result in formatted way."""
    if not result.get("success"):
        print(f"❌ Failed to get max placeable: {result}")
        return

    max_qty = result.get("maxQty", 0)
    per_unit_cost = result.get("perUnitCost", 0)
    remain_cash = result.get("remainCash", 0)
    unlimited = result.get("unlimited", False)

    print(f"\n{side} {symbol} @ {format_number(price)} VND:")
    print(f"  {'─' * 60}")
    
    if unlimited:
        print(f"  ✨ UNLIMITED - This account can place orders without limit")
    else:
        print(f"  📊 Maximum Quantity: {format_number(max_qty)} units")
    
    print(f"  💰 Available Cash: {format_number(remain_cash)} VND")
    print(f"  💸 Cost per Unit: {format_number(per_unit_cost)} VND")
    
    if not unlimited and max_qty > 0:
        total_cost = float(max_qty) * float(per_unit_cost)
        print(f"  📈 Max Order Value: {format_number(total_cost)} VND")
        
        # Calculate what percentage of cash this uses
        if remain_cash > 0:
            usage_pct = (total_cost / float(remain_cash)) * 100
            print(f"  📉 Cash Usage: {format_number(usage_pct, 1)}%")


def main():
    """Main entry point."""
    print_separator()
    print("EXAMPLE 10: MAXIMUM PLACEABLE QUANTITY CALCULATOR")
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

    # Get current cash balance for reference
    cash_info = client.get_cash_balance()
    current_cash = cash_info.get('remainCash', 0)
    print(f"\n💰 Current Available Cash: {format_number(current_cash)} VND")

    # =================================================================
    # 1. DERIVATIVES - VN30 Futures
    # =================================================================
    print_section("📊 DERIVATIVES: VN30 Futures")

    derivative_symbol = DERIVATIVE_SYMBOL
    derivative_price = 1985

    print(f"\nSymbol: {derivative_symbol}")
    print(f"Testing at price: {format_number(derivative_price)} VND")

    # Test BUY
    result = client.get_max_placeable(derivative_symbol, derivative_price, "BUY")
    display_max_placeable(derivative_symbol, derivative_price, "BUY", result)

    # Test SELL
    result = client.get_max_placeable(derivative_symbol, derivative_price, "SELL")
    display_max_placeable(derivative_symbol, derivative_price, "SELL", result)

    # =================================================================
    # 2. EQUITIES - Stock Market
    # =================================================================
    print_section("📈 EQUITIES: Stock Market")

    equity_symbol = EQUITY_SYMBOL
    equity_price = 90000  # Example: MWG at 90,000 VND per share

    print(f"\nSymbol: {equity_symbol}")
    print(f"Testing at price: {format_number(equity_price)} VND")

    # Test BUY
    result = client.get_max_placeable(equity_symbol, equity_price, "BUY")
    display_max_placeable(equity_symbol, equity_price, "BUY", result)

    # Test SELL
    result = client.get_max_placeable(equity_symbol, equity_price, "SELL")
    display_max_placeable(equity_symbol, equity_price, "SELL", result)

    # =================================================================
    # 3. PRICE SENSITIVITY ANALYSIS
    # =================================================================
    print_section("💹 PRICE SENSITIVITY ANALYSIS")

    print("\nHow max quantity changes with different prices:")
    print(f"(Testing BUY orders for {derivative_symbol})\n")

    test_prices = [1935, 1960, 1985, 2010, 2035]
    
    print(f"{'Price':>12} {'Max Qty':>12} {'Per Unit Cost':>18} {'Total Value':>18}")
    print("─" * 65)

    for test_price in test_prices:
        result = client.get_max_placeable(derivative_symbol, test_price, "BUY")
        
        if result.get("success"):
            max_qty = result.get("maxQty", 0)
            per_unit = result.get("perUnitCost", 0)
            
            if result.get("unlimited"):
                print(f"{format_number(test_price):>12} {'UNLIMITED':>12} {format_number(per_unit):>18} {'UNLIMITED':>18}")
            else:
                total_value = float(max_qty) * float(per_unit)
                print(f"{format_number(test_price):>12} "
                      f"{format_number(max_qty):>12} "
                      f"{format_number(per_unit):>18} "
                      f"{format_number(total_value):>18}")
        else:
            print(f"{format_number(test_price):>12} {'ERROR':>12}")

    # =================================================================
    # 4. BUY vs SELL COMPARISON
    # =================================================================
    print_section("⚖️  BUY vs SELL COMPARISON")

    print(f"\nComparing max quantities for {derivative_symbol} at {format_number(derivative_price)} VND:\n")

    buy_result = client.get_max_placeable(derivative_symbol, derivative_price, "BUY")
    sell_result = client.get_max_placeable(derivative_symbol, derivative_price, "SELL")

    print(f"{'Side':<6} {'Max Qty':>12} {'Per Unit Cost':>18} {'Reason'}")
    print("─" * 60)

    if buy_result.get("success"):
        buy_qty = buy_result.get("maxQty", 0)
        buy_cost = buy_result.get("perUnitCost", 0)
        buy_unlimited = buy_result.get("unlimited", False)
        
        if buy_unlimited:
            print(f"{'BUY':<6} {'UNLIMITED':>12} {format_number(buy_cost):>18} Unlimited account")
        else:
            print(f"{'BUY':<6} {format_number(buy_qty):>12} {format_number(buy_cost):>18} Limited by cash")

    if sell_result.get("success"):
        sell_qty = sell_result.get("maxQty", 0)
        sell_cost = sell_result.get("perUnitCost", 0)
        sell_unlimited = sell_result.get("unlimited", False)
        
        if sell_unlimited:
            print(f"{'SELL':<6} {'UNLIMITED':>12} {format_number(sell_cost):>18} Unlimited account")
        else:
            print(f"{'SELL':<6} {format_number(sell_qty):>12} {format_number(sell_cost):>18} Limited by position/margin")

    # Analysis
    print("\n💡 Analysis:")
    if buy_result.get("success") and sell_result.get("success"):
        buy_qty = float(buy_result.get("maxQty", 0))
        sell_qty = float(sell_result.get("maxQty", 0))
        
        if buy_result.get("unlimited") or sell_result.get("unlimited"):
            print("   • Account has unlimited trading capability")
        elif buy_qty > sell_qty:
            diff = buy_qty - sell_qty
            print(f"   • BUY has {format_number(diff)} more capacity than SELL")
            print(f"   • BUY limited by available cash")
            print(f"   • SELL may be limited by current short position or margin")
        elif sell_qty > buy_qty:
            diff = sell_qty - buy_qty
            print(f"   • SELL has {format_number(diff)} more capacity than BUY")
            print(f"   • SELL may benefit from existing long position")
            print(f"   • BUY limited by available cash")
        else:
            print(f"   • BUY and SELL have equal capacity: {format_number(buy_qty)}")

    # =================================================================
    # 5. RISK MANAGEMENT EXAMPLE
    # =================================================================
    print_section("🛡️  RISK MANAGEMENT EXAMPLE")

    print("\nUsing max placeable for safe order sizing:\n")

    target_price = 1985
    max_result = client.get_max_placeable(derivative_symbol, target_price, "BUY")

    if max_result.get("success"):
        max_qty = float(max_result.get("maxQty", 0))
        
        print(f"Scenario: Want to BUY {derivative_symbol} @ {format_number(target_price)}")
        print(f"")
        print(f"✅ Maximum Safe Quantity: {format_number(max_qty)}")
        print(f"")
        print(f"Recommended Position Sizing:")
        
        # Conservative: 25% of max
        conservative = max_qty * 0.25
        print(f"  🟢 Conservative (25%): {format_number(conservative)} contracts")
        
        # Moderate: 50% of max
        moderate = max_qty * 0.5
        print(f"  🟡 Moderate (50%):     {format_number(moderate)} contracts")
        
        # Aggressive: 75% of max
        aggressive = max_qty * 0.75
        print(f"  🟠 Aggressive (75%):   {format_number(aggressive)} contracts")
        
        # Maximum: 100% of max
        print(f"  🔴 Maximum (100%):     {format_number(max_qty)} contracts")
        
        print(f"\n💡 Best Practice:")
        print(f"   • Always check max placeable BEFORE placing order")
        print(f"   • Use conservative sizing (25-50%) for risk management")
        print(f"   • Leave buffer for price fluctuations")
        print(f"   • Account for margin calls and volatility")

    # =================================================================
    # 6. PRACTICAL WORKFLOW
    # =================================================================
    print_section("🔄 PRACTICAL WORKFLOW")

    print("\nStep-by-step order placement workflow:\n")

    workflow_symbol = derivative_symbol
    workflow_price = 1985
    workflow_side = "BUY"
    desired_qty = 20

    print(f"Goal: {workflow_side} {desired_qty} contracts of {workflow_symbol} @ {format_number(workflow_price)}\n")

    # Step 1: Check max placeable
    print("Step 1️⃣: Check maximum placeable quantity")
    max_result = client.get_max_placeable(workflow_symbol, workflow_price, workflow_side)
    
    if max_result.get("success"):
        max_qty = float(max_result.get("maxQty", 0))
        print(f"         ✓ Max placeable: {format_number(max_qty)} contracts")
        
        # Step 2: Validate order size
        print("\nStep 2️⃣: Validate order size")
        if max_result.get("unlimited"):
            print(f"         ✓ Unlimited account - order size OK")
            can_place = True
        elif desired_qty <= max_qty:
            print(f"         ✓ Desired {desired_qty} <= Max {format_number(max_qty)} - OK to proceed")
            can_place = True
        else:
            print(f"         ✗ Desired {desired_qty} > Max {format_number(max_qty)} - REDUCE SIZE!")
            print(f"         ⚠️  Recommended: {format_number(max_qty)} contracts")
            can_place = False
        
        # Step 3: Calculate costs
        print("\nStep 3️⃣: Calculate total cost")
        per_unit_cost = float(max_result.get("perUnitCost", 0))
        order_qty = min(desired_qty, max_qty) if not max_result.get("unlimited") else desired_qty
        total_cost = order_qty * per_unit_cost
        
        print(f"         Quantity: {format_number(order_qty)}")
        print(f"         Cost per unit: {format_number(per_unit_cost)} VND")
        print(f"         Total cost: {format_number(total_cost)} VND")
        
        # Step 4: Check cash availability
        print("\nStep 4️⃣: Verify cash availability")
        remain_cash = float(max_result.get("remainCash", 0))
        print(f"         Available: {format_number(remain_cash)} VND")
        print(f"         Required:  {format_number(total_cost)} VND")
        
        if total_cost <= remain_cash:
            print(f"         ✓ Sufficient cash")
        else:
            print(f"         ✗ Insufficient cash!")
        
        # Step 5: Place order (simulated)
        print("\nStep 5️⃣: Place order (simulated)")
        if can_place:
            print(f"         ✓ Ready to place order:")
            print(f"           Symbol: {workflow_symbol}")
            print(f"           Side: {workflow_side}")
            print(f"           Quantity: {format_number(order_qty)}")
            print(f"           Price: {format_number(workflow_price)}")
            print(f"           # client.place_order(...)")
        else:
            print(f"         ✗ Cannot place order - adjust quantity")

    print()
    print_separator()
    print("✅ Example completed")
    print_separator()
    
    print("\nKey Takeaways:")
    print("  • Always check max_placeable BEFORE placing orders")
    print("  • Max quantity differs for BUY vs SELL")
    print("  • Account for margin, fees, and current positions")
    print("  • Use conservative position sizing for risk management")
    print("  • Validate order size to avoid rejections")
    print("  • perUnitCost includes all fees and margin requirements")


if __name__ == "__main__":
    main()
