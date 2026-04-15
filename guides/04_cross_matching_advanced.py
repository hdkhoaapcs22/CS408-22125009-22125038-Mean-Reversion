"""
Cross-Matching Derivative Example (D1 vs D2) - Advanced Order Scenarios

Demonstrates:
- Full fill: One order matches completely in a single fill
- Partial fills: One order filled incrementally over multiple events
- Focus on D1 (main trader), D2 as matching tool

Scenarios:
1. Scenario A: D1 sells 5 contracts → D2 buys 5 contracts → FULL FILL (1 event)
2. Scenario B: D1 sells 5 contracts → D2 buys in chunks (1+1+1+2) → PARTIAL FILLS (4 events)

Cross-Matching Logic:
- D1 (Derivative 1): Main trading account
- D2 (Derivative 2): Matching counterparty (unconditional matching with D1)
- D2 is the tool to execute D1's orders
"""

import os
import sys
import time
import logging
from pathlib import Path
from threading import Event as ThreadEvent
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from paperbroker.client import PaperBrokerClient

load_dotenv()

# Setup logger for this example
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class D1OrderTracker:
    """
    Track D1 orders with focus on fill patterns.

    Features:
    - Monitors only D1 orders (ignores D2)
    - Tracks full vs partial fills
    - Records cumulative quantities
    - Detailed fill history
    """

    def __init__(self):
        self.orders = {}  # {cl_ord_id: order_info}
        self.filled_event = ThreadEvent()
        self.target_orders = []  # D1 orders we're tracking

    def register_order(self, cl_ord_id, qty):
        """Register a D1 order for tracking."""
        self.orders[cl_ord_id] = {
            "status": "NEW",
            "qty_ordered": qty,
            "cum_qty": 0,  # Cumulative filled
            "avg_price": 0,
            "fills": [],  # [{fill_num, qty, price}]
        }
        self.target_orders.append(cl_ord_id)
        logger.info(
            f"   📋 Registered for tracking: {cl_ord_id[:8]}... ({qty} contracts)"
        )

    def on_order_filled(self, cl_ord_id, status, last_px, last_qty, cum_qty=None, **kw):
        """Handle fill events for D1 orders."""
        # Only process D1 orders
        if cl_ord_id not in self.target_orders:
            return

        order = self.orders[cl_ord_id]
        order["status"] = status
        order["avg_price"] = last_px

        # Update cumulative quantity
        if cum_qty is not None:
            order["cum_qty"] = cum_qty
        else:
            order["cum_qty"] += last_qty

        # Record this fill
        fill_num = len(order["fills"]) + 1
        order["fills"].append({"fill_num": fill_num, "qty": last_qty, "price": last_px})

        # Determine fill type
        is_fully_filled = order["cum_qty"] >= order["qty_ordered"]
        fill_type = "✅ FULL FILL" if is_fully_filled else "🔸 PARTIAL FILL"

        logger.info(
            f"      {fill_type} #{fill_num}: {last_qty} contracts @ {last_px:,.0f} VND | "
            f"Progress: {order['cum_qty']}/{order['qty_ordered']}"
        )

        # Check if all orders complete
        if self._all_orders_complete():
            self.filled_event.set()

    def _all_orders_complete(self):
        """Check if all D1 orders are fully filled."""
        for cl_ord_id in self.target_orders:
            order = self.orders[cl_ord_id]
            if order["cum_qty"] < order["qty_ordered"]:
                return False
        return True

    def on_order_accepted(self, cl_ord_id, status, **kw):
        """Handle order accepted events."""
        if cl_ord_id in self.target_orders:
            logger.info(f"   ✓ Order accepted: {cl_ord_id[:8]}...")

    def on_order_rejected(self, cl_ord_id, reason, **kw):
        """Handle order rejected events."""
        if cl_ord_id in self.target_orders:
            logger.error(f"   ✗ Order rejected: {cl_ord_id[:8]}... - {reason}")

    def print_summary(self):
        """Print detailed summary of D1 orders."""
        logger.info("\n" + "=" * 80)
        logger.info("📊 D1 ORDER EXECUTION SUMMARY")
        logger.info("=" * 80)

        total_orders = len(self.target_orders)
        total_fills = sum(len(self.orders[oid]["fills"]) for oid in self.target_orders)

        logger.info(f"\nTotal D1 Orders: {total_orders}")
        logger.info(f"Total Fill Events: {total_fills}\n")

        for idx, cl_ord_id in enumerate(self.target_orders, 1):
            order = self.orders[cl_ord_id]
            logger.info(f"Order #{idx}: {cl_ord_id[:8]}...")
            logger.info(f"  Status: {order['status']}")
            logger.info(f"  Ordered Quantity: {order['qty_ordered']} contracts")
            logger.info(f"  Filled Quantity: {order['cum_qty']} contracts")
            logger.info(f"  Number of Fills: {len(order['fills'])}")
            logger.info(f"  Average Price: {order['avg_price']:,.0f} VND")

            if order["fills"]:
                logger.info(f"  Fill History:")
                for fill in order["fills"]:
                    logger.info(
                        f"    Fill #{fill['fill_num']}: "
                        f"{fill['qty']} contracts @ {fill['price']:,.0f} VND"
                    )
            logger.info("")

        logger.info("=" * 80)


def run_scenario_a_full_fill(client, account_d1, account_d2, tracker, symbol, price):
    """
    Scenario A: Full Fill
    D1 sells 5 contracts → D2 buys 5 contracts → Complete in 1 fill
    """
    logger.info("\n" + "🔷" * 40)
    logger.info("SCENARIO A: FULL FILL (5 contracts → 1 fill event)")
    logger.info("🔷" * 40)

    qty = 5

    logger.info(f"\n1️⃣  D1 places SELL order: {qty} contracts @ {price:,.0f} VND")
    try:
        with client.use_sub_account(account_d1):
            order_d1 = client.place_order(
                full_symbol=symbol, side="SELL", qty=qty, price=price, ord_type="LIMIT"
            )
        tracker.register_order(order_d1, qty)
    except Exception as e:
        logger.error(f"   Failed: {e}")
        return False

    time.sleep(0.5)

    logger.info(f"\n2️⃣  D2 matches with BUY order: {qty} contracts @ {price:,.0f} VND")
    try:
        with client.use_sub_account(account_d2):
            client.place_order(
                full_symbol=symbol, side="BUY", qty=qty, price=price, ord_type="LIMIT"
            )
    except Exception as e:
        logger.error(f"   Failed: {e}")
        return False

    logger.info("\n⏳ Waiting for fill...")
    if not tracker.filled_event.wait(timeout=5):
        logger.warning("   ⚠️ Timeout!")
        return False

    logger.info("\n✅ Scenario A completed!")
    return True


def run_scenario_b_partial_fills(client, account_d1, account_d2, tracker, symbol, price):
    """
    Scenario B: Multiple Orders with Independent Fills
    D1 places 3 separate SELL orders → D2 matches each → 3 fill events

    Strategy: Place multiple D1 orders at SAME price with time priority.
    Server matches using FIFO (First In First Out) at the same price level.
    This demonstrates tracking multiple independent order executions.
    """
    logger.info("\n" + "🔶" * 40)
    logger.info("SCENARIO B: MULTIPLE ORDERS (3 orders → 3 independent fills)")
    logger.info("🔶" * 40)

    # Strategy: Place 3 separate D1 orders at SAME price
    # Using same price + time priority ensures predictable matching order
    orders = [
        {"qty": 2, "price": price},
        {"qty": 2, "price": price},
        {"qty": 1, "price": price},
    ]

    # Reset tracker
    tracker.filled_event.clear()

    logger.info(
        f"\n1️⃣  D1 places {len(orders)} SELL orders (same price, time-priority):"
    )
    d1_orders = []
    for i, order_spec in enumerate(orders, 1):
        qty = order_spec["qty"]
        px = order_spec["price"]
        logger.info(f"   Order {i}: {qty} contracts @ {px:,.0f} VND")
        try:
            with client.use_sub_account(account_d1):
                order_id = client.place_order(
                    full_symbol=symbol, side="SELL", qty=qty, price=px, ord_type="LIMIT"
                )
            tracker.register_order(order_id, qty)
            d1_orders.append((order_id, qty, px))
            time.sleep(0.5)  # Increased delay between D1 orders
        except Exception as e:
            logger.error(f"   Failed: {e}")
            return False

    time.sleep(1.0)  # Wait for all D1 orders to be accepted

    logger.info("\n2️⃣  D2 matches each order individually (FIFO time-priority):")

    for i, (order_id, qty, px) in enumerate(d1_orders, 1):
        logger.info(
            f"\n   Match {i}/{len(d1_orders)}: D2 buys {qty} contracts @ {px:,.0f} VND"
        )
        try:
            with client.use_sub_account(account_d2):
                client.place_order(
                    full_symbol=symbol, side="BUY", qty=qty, price=px, ord_type="LIMIT"
                )
            time.sleep(0.8)
        except Exception as e:
            logger.error(f"   Failed: {e}")
            return False

    logger.info("\n⏳ Waiting for all fills...")
    if not tracker.filled_event.wait(timeout=15):
        logger.warning("   ⚠️ Timeout!")
        return False

    logger.info("\n✅ Scenario B completed!")
    return True


def main():
    """Execute advanced cross-matching scenarios."""

    symbol = os.getenv("VN30F1M", "HNXDS:VN30F2511")
    price = 1985.0
    
    # Sub-account IDs
    account_d1 = os.getenv("PAPER_ACCOUNT_ID_D1", "D1")
    account_d2 = os.getenv("PAPER_ACCOUNT_ID_D2", "D2")

    logger.info("=" * 80)
    logger.info("🎯 ADVANCED DERIVATIVE CROSS-MATCHING: D1 Order Focus")
    logger.info("=" * 80)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Price: {price:,.0f} VND")
    logger.info(f"Sub-Accounts: {account_d1} (seller) and {account_d2} (buyer)")
    logger.info("Focus: D1 orders (D2 is matching tool)")
    logger.info("=" * 80)

    # Initialize tracker for D1 orders
    tracker = D1OrderTracker()

    # Create single client - will use different sub_account_id for each order
    client = PaperBrokerClient(
        default_sub_account=account_d1,  # Default to D1
        username=os.getenv("PAPER_USERNAME", "BL01"),
        password=os.getenv("PAPER_PASSWORD", "123"),
        rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://100.97.83.110:9090"),
        socket_connect_host=os.getenv("SOCKET_HOST", "100.97.83.110"),
        socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id=os.getenv('SENDER_COMP_ID', 'cross-FIX'),
        target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
        console=False,
    )

    try:
        # Subscribe to order events
        client.on("fix:order:filled", tracker.on_order_filled)
        client.on("fix:order:accepted", tracker.on_order_accepted)
        client.on("fix:order:rejected", tracker.on_order_rejected)

        # Connect
        logger.info("\n🔌 Connecting...")
        client.connect()

        if not client.wait_until_logged_on(timeout=10):
            logger.error(f"Logon failed: {client.last_logon_error()}")
            return

        logger.info("✅ Successfully logged on!\n")

        # Check initial balance for D1
        with client.use_sub_account(account_d1):
            balance = client.get_cash_balance()
        logger.info(f"💰 D1 Initial Balance: {balance.get('remainCash', 0):,.0f} VND\n")

        # Run Scenario A: Full Fill
        if not run_scenario_a_full_fill(client, account_d1, account_d2, tracker, symbol, price):
            logger.error("Scenario A failed!")
            return

        time.sleep(2)  # Brief pause between scenarios

        # Run Scenario B: Multiple Orders
        if not run_scenario_b_partial_fills(
            client, account_d1, account_d2, tracker, symbol, price
        ):
            logger.error("Scenario B failed!")
            return

        # Print comprehensive summary
        tracker.print_summary()

        # Check final balance for D1
        with client.use_sub_account(account_d1):
            balance_after = client.get_cash_balance()
        logger.info(
            f"\n💰 D1 Final Balance: {balance_after.get('remainCash', 0):,.0f} VND"
        )

        logger.info("\n" + "=" * 80)
        logger.info("🎉 ALL SCENARIOS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info("\nKey Observations:")
        logger.info("  • Scenario A: 1 large order → 1 fill event (full execution)")
        logger.info("  • Scenario B: 3 separate orders → 3 fill events (FIFO matching)")
        logger.info("  • Same price → time-priority matching (first order fills first)")
        logger.info("  • D2 acts as matching tool for D1 strategy")
        logger.info("  • Event-driven tracking provides real-time visibility")

    finally:
        logger.info("\n✅ Example completed!")
        os._exit(0)


if __name__ == "__main__":
    main()
