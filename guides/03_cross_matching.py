"""
Cross-Matching Derivative Example (D1 vs D2) - Advanced Order Scenarios

Demonstrates:
- Multi-account event-based trading
- Full fills vs Partial fills
- Multiple orders management
- Real-time order fill tracking via events
- Cross-matching mechanism between sub-accounts

Cross-Matching Logic:
- D1 (Derivative 1): Main trading account - places orders
- D2 (Derivative 2): Matching counterparty - can match UNCONDITIONALLY with D1
- When D1 SELLS and D2 BUYS at same price -> INSTANT MATCH
- D2 is used as a tool to execute D1's strategy

Scenarios Tested:
1. Full Fill: D1 sells 5 contracts, D2 buys 5 contracts -> Complete match
2. Partial Fills: D1 sells 5 contracts, D2 buys in smaller chunks (1+1+1+2) -> 4 partial fills

Note: This demonstrates realistic order execution patterns where orders
may be filled incrementally rather than all at once.
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

# Setup logger for this example (separate from library)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class CrossMatchTracker:
    """
    Track cross-matching using events (no polling!).

    This tracker demonstrates event-driven order monitoring:
    - Receives real-time notifications when orders are accepted/filled/rejected
    - No need to poll server for order status
    - Thread-safe event signaling for synchronization
    """

    def __init__(self):
        self.orders = {}
        self.filled_event = ThreadEvent()
        self.fill_count = 0

    def on_order_filled(self, cl_ord_id, status, last_px, last_qty, **kw):
        """Event handler for filled orders."""
        logger.info(f"🔔 ORDER FILLED EVENT: {cl_ord_id[:8]}...")

        # Store order info
        if cl_ord_id not in self.orders:
            self.orders[cl_ord_id] = {"status": status, "qty_filled": 0, "avg_price": 0}

        order = self.orders[cl_ord_id]
        order["status"] = status
        order["qty_filled"] = last_qty
        order["avg_price"] = last_px

        self.fill_count += 1
        logger.info(
            f"✅ Order FILLED: {cl_ord_id[:8]}... | "
            f"Qty: {last_qty} @ Price: {last_px:,.0f}"
        )

        # Signal completion if both orders filled
        if self.fill_count >= 2:
            logger.info("🎯 Both orders filled - cross-match complete!")
            self.filled_event.set()

    def on_order_accepted(self, cl_ord_id, status, **kw):
        """Event handler for accepted orders."""
        logger.info(f"� Order ACCEPTED: {cl_ord_id[:8]}... (waiting for fill)")

    def on_order_rejected(self, cl_ord_id, reason, **kw):
        """Event handler for rejected orders."""
        logger.error(f"❌ Order REJECTED: {cl_ord_id[:8]}... | Reason: {reason}")


def main():
    """
    Cross-matching demonstration for derivatives.

    Scenario:
    1. D1 places SELL order at price X
    2. D2 places BUY order at same price X
    3. Orders match IMMEDIATELY (D2 can match unconditionally with D1)
    4. Both accounts receive fill notifications via events

    This demonstrates paper trading cross-account settlement without
    needing real market conditions.
    """

    # Order tracker for event-based monitoring
    tracker = CrossMatchTracker()

    # Derivative symbol (VN30 Futures - must include exchange prefix)
    symbol = os.getenv("VN30F1M", "HNXDS:VN30F2511")
    
    # Sub-account IDs
    account_d1 = os.getenv("PAPER_ACCOUNT_ID_D1", "D1")
    account_d2 = os.getenv("PAPER_ACCOUNT_ID_D2", "D2")

    logger.info("=" * 80)
    logger.info("🎯 Derivative Cross-Matching Demo: D1 (SELL) vs D2 (BUY)")
    logger.info("=" * 80)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Sub-Accounts: {account_d1} (seller) and {account_d2} (buyer)")
    logger.info("Mechanism: D2 can match unconditionally with D1")
    logger.info("=" * 80)

    # Create single client - will use different sub_account_id for each order
    # Note: console=False means library only shows WARNING/ERROR,
    #       DEBUG/INFO still goes to log file
    client = PaperBrokerClient(
        default_sub_account=account_d1,  # Default to D1
        username=os.getenv("PAPER_USERNAME", "cross_fix"),
        password=os.getenv("PAPER_PASSWORD", "123"),
        rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://100.97.83.110:9090"),
        socket_connect_host=os.getenv("SOCKET_HOST", "100.97.83.110"),
        socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id=os.getenv('SENDER_COMP_ID', 'cross-FIX'),
        target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
        console=False,  # Only show warnings/errors in console
    )

    try:
        # Subscribe to order events (event-based, no polling!)
        client.on("fix:order:filled", tracker.on_order_filled)
        client.on("fix:order:accepted", tracker.on_order_accepted)
        client.on("fix:order:rejected", tracker.on_order_rejected)

        # Connect client
        logger.info("🔌 Connecting...")
        client.connect()

        # Wait for logon
        logger.info("⏳ Waiting for logon...")

        if not client.wait_until_logged_on(timeout=10):
            logger.error(f"❌ Logon failed: {client.last_logon_error()}")
            return

        logger.info("✅ Successfully logged on!")

        # Show initial balances for both sub-accounts
        with client.use_sub_account(account_d1):
            balance_d1 = client.get_cash_balance()
        with client.use_sub_account(account_d2):
            balance_d2 = client.get_cash_balance()

        logger.info("\n💰 Initial Balances:")
        logger.info(f"   D1: {balance_d1.get('remainCash', 0):,.0f} VND")
        logger.info(f"   D2: {balance_d2.get('remainCash', 0):,.0f} VND")

        # Place orders (D1 SELL, D2 BUY at same price)
        price = 1985.0  # VN30 futures price
        qty = 1  # 1 contract

        logger.info("\n📤 Placing Cross-Match Orders:")
        logger.info(f"   Symbol: {symbol}")
        logger.info(f"   Price: {price:,.0f}")
        logger.info(f"   Quantity: {qty} contract(s)")
        logger.info("\n   Strategy: D1 SELLS → D2 BUYS → INSTANT MATCH")

        # D1 sells first (creates order in book)
        logger.info("\n   [1/2] D1 placing SELL order...")
        try:
            with client.use_sub_account(account_d1):
                order_d1 = client.place_order(
                    full_symbol=symbol,
                    side="SELL",
                    qty=qty,
                    price=price,
                    ord_type="LIMIT"
                )
            logger.info(f"         Order ID: {order_d1[:8]}...")
        except Exception as e:
            logger.error(f"         ❌ Failed to place D1 order: {e}")
            logger.exception("Full traceback:")
            return

        # Small delay to ensure D1 order is in book
        time.sleep(0.5)

        # D2 buys (will match IMMEDIATELY with D1's order)
        logger.info("\n   [2/2] D2 placing BUY order...")
        try:
            with client.use_sub_account(account_d2):
                order_d2 = client.place_order(
                    full_symbol=symbol,
                    side="BUY",
                    qty=qty,
                    price=price,
                    ord_type="LIMIT"
                )
            logger.info(f"         Order ID: {order_d2[:8]}...")
        except Exception as e:
            logger.error(f"         ❌ Failed to place D2 order: {e}")
            logger.exception("Full traceback:")
            return        # Wait for fills (event-based, not polling!)
        logger.info("\n⏳ Waiting for cross-match (event-driven, zero-latency)...")

        if tracker.filled_event.wait(timeout=10):
            logger.info("\n" + "=" * 80)
            logger.info("🎉 CROSS-MATCH COMPLETED!")
            logger.info("=" * 80)

            # Show final results
            logger.info("\n📊 Trade Summary:")
            for cl_ord_id, order in tracker.orders.items():
                logger.info(
                    f"   Order {cl_ord_id[:8]}... | "
                    f"Status: {order['status']:8s} | "
                    f"Filled: {order['qty_filled']} @ {order['avg_price']:,.0f} VND"
                )

            # Show updated balances
            with client.use_sub_account(account_d1):
                balance_d1_after = client.get_cash_balance()
            with client.use_sub_account(account_d2):
                balance_d2_after = client.get_cash_balance()

            logger.info("\n💰 Final Balances:")
            logger.info(f"   D1: {balance_d1_after.get('remainCash', 0):,.0f} VND")
            logger.info(f"   D2: {balance_d2_after.get('remainCash', 0):,.0f} VND")

            logger.info("\n✨ Event-driven trading completed successfully!")
            logger.info("   No polling required - instant notification via events")

        else:
            logger.warning("\n⚠️ Timeout waiting for fills (10 seconds)")
            logger.warning("   This may indicate:")
            logger.warning("   - Server connectivity issues")
            logger.warning("   - Matching engine problems")
            logger.warning("   - Check server logs for details")

    finally:
        # Disconnect cleanly
        logger.info("\n✅ Example completed!")
        os._exit(0)

        # Note: Using os._exit() to avoid QuickFIX cleanup segfault
        # This is a known issue with QuickFIX Python bindings
        os._exit(0)


if __name__ == "__main__":
    main()
