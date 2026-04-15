"""
Order Cancellation Example - Event-Based Design

Demonstrates:
- Place a limit order
- Track order status with events
- Cancel the order before it fills
- Recover pending orders after restart (v0.2.4+)
- Clean error handling
"""

import os
import sys
import time
import logging
from pathlib import Path
from threading import Event as ThreadEvent
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from paperbroker.client import PaperBrokerClient

# Load environment variables
load_dotenv()

# Setup logger for this example (separate from library logger)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def on_order_accepted(tracker, cl_ord_id, status, **kwargs):
    """Event handler for order accepted."""
    if cl_ord_id == tracker.order_id:
        tracker.status = status
        logger.info(f"✅ Order accepted: {cl_ord_id[:8]}... (Status: {status})")
        tracker.order_accepted.set()


def on_order_canceled(tracker, orig_cl_ord_id, status, **kwargs):
    """Event handler for order canceled."""
    if orig_cl_ord_id == tracker.order_id:
        tracker.status = status
        logger.info(f"✅ Order canceled: {orig_cl_ord_id[:8]}... (Status: {status})")
        tracker.order_canceled.set()


def on_order_rejected(tracker, cl_ord_id, reason, **kwargs):
    """Event handler for order rejected."""
    if cl_ord_id == tracker.order_id:
        logger.error(f"❌ Order rejected: {cl_ord_id[:8]}... - {reason}")


def main():
    """Order cancellation demonstration."""

    # Configuration
    symbol = os.getenv("VN30F1M", "HNXDS:VN30F2603")
    account = os.getenv("PAPER_ACCOUNT_ID_D1", "main")
    price =  1500.0  # Use a price unlikely to match
    qty = 1
    delay_seconds = 5  # Wait before canceling

    logger.info("=" * 70)
    logger.info("ORDER CANCELLATION EXAMPLE")
    logger.info("=" * 70)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Account: {account}")
    logger.info(f"Strategy: Place → Wait {delay_seconds}s → Cancel")
    logger.info("=" * 70)

    # Order tracker
    class OrderTracker:
        def __init__(self):
            self.order_accepted = ThreadEvent()
            self.order_canceled = ThreadEvent()
            self.order_id = None
            self.status = None

    tracker = OrderTracker()

    # Create client (order_store_path enables SQLite persistence for crash recovery)
    client = PaperBrokerClient(
        default_sub_account=account,
        username=os.getenv("PAPER_USERNAME", "BL01"),
        password=os.getenv("PAPER_PASSWORD", "123"),
        rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://100.97.83.110:9090"),
        socket_connect_host=os.getenv("SOCKET_HOST", "100.97.83.110"),
        socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id=os.getenv("SENDER_COMP_ID", "BL-FIX"),
        target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
        console=False,  # Only show WARNING/ERROR in console, DEBUG/INFO go to file only
        order_store_path="orders.db",  # SQLite persistence (set None to disable)
    )

    # Subscribe to events (clean event-based design)
    client.on("fix:order:accepted", lambda **kw: on_order_accepted(tracker, **kw))
    client.on("fix:order:canceled", lambda **kw: on_order_canceled(tracker, **kw))
    client.on("fix:order:rejected", lambda **kw: on_order_rejected(tracker, **kw))

    # Connect (non-blocking)
    logger.info("\n🔌 Connecting to PaperBroker...")
    client.connect()

    try:
        # Wait for logon (with timeout)
        if not client.wait_until_logged_on(timeout=10):
            error = client.last_logon_error()
            logger.error(f"❌ Logon failed: {error}")
            return

        logger.info("✅ Successfully logged on!")

        # Step 0 (v0.2.4+): Recover any pending orders from previous session
        pending = client.recover_pending_orders()
        if pending:
            logger.info(f"\n⚠️ Found {len(pending)} pending order(s) from previous session:")
            for p in pending:
                logger.info(
                    f"  {p['cl_ord_id']}: {p['side']} {p['qty']}x "
                    f"{p['symbol']} @ {p['price']} (status: {p['status']})"
                )
                try:
                    client.cancel_order(p["cl_ord_id"])
                    logger.info(f"  → Cancel request sent for {p['cl_ord_id']}")
                except Exception as e:
                    logger.warning(f"  → Could not cancel {p['cl_ord_id']}: {e}")
            time.sleep(2)  # Wait for cancel confirmations

        # Check balance
        cash = client.get_cash_balance()
        logger.info(f"💰 Available cash: {cash.get('remainCash', 0):,.0f} VND\n")

        # Step 1: Place order
        logger.info("=" * 70)
        logger.info("STEP 1: PLACE ORDER")
        logger.info("=" * 70)
        logger.info(f"Placing BUY order: {qty} contracts @ {price:,.0f} VND")

        order_id = client.place_order(
            full_symbol=symbol,
            side="BUY",
            qty=qty,
            price=price,
            ord_type="LIMIT"
        )
        tracker.order_id = order_id
        logger.info(f"Order ID: {order_id[:16]}...")

        # Wait for order acceptance
        if not tracker.order_accepted.wait(timeout=10):
            logger.error("❌ Timeout waiting for order acceptance")
            return

        # Step 2: Wait before canceling
        logger.info(f"\n⏳ Waiting {delay_seconds} seconds before canceling...")
        time.sleep(delay_seconds)

        # Step 3: Cancel order
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: CANCEL ORDER")
        logger.info("=" * 70)
        logger.info(f"Canceling order: {order_id[:16]}...")

        client.cancel_order(order_id)
        logger.info("Cancel request sent")

        # Wait for cancellation confirmation
        if not tracker.order_canceled.wait(timeout=10):
            logger.warning("⚠️ Timeout waiting for cancellation confirmation")
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("📊 SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Order ID: {order_id[:16]}...")
        logger.info(f"Final Status: {tracker.status or 'UNKNOWN'}")
        logger.info(f"Symbol: {symbol}")
        logger.info("Side: SELL")
        logger.info(f"Quantity: {qty}")
        logger.info(f"Price: {price:,.0f} VND")
        logger.info("=" * 70)

        logger.info("\n✅ Example completed!")

    finally:
        # Note: Using os._exit() to avoid QuickFIX cleanup segfault
        # This is a known issue with QuickFIX Python bindings
        os._exit(0)


if __name__ == "__main__":
    main()
