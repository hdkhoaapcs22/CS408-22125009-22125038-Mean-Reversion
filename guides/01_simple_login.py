"""
Simple Login Example - Event-Based Design

Demonstrates:
- Clean event-based login flow
- Automatic connection management with context manager
- Error handling
"""

import os
import sys
import logging
from pathlib import Path
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


def on_logon(session_id, **kw):
    """Event handler for successful logon."""
    logger.info(f"✅ FIX session established: {session_id}")


def on_logout(session_id, reason=None, **kw):
    """Event handler for logout."""
    logger.info(f"👋 FIX session closed: {session_id}, reason: {reason}")


def on_reject(reason, msg_type, **kw):
    """Event handler for rejected messages."""
    logger.error(f"❌ Message rejected - Type: {msg_type}, Reason: {reason}")


def main():
    """Simple login demonstration."""

    # Create client
    # Config file auto-generated from connection parameters
    client = PaperBrokerClient(
        default_sub_account=os.getenv("PAPER_ACCOUNT_ID_D1", "D1"),
        username=os.getenv("PAPER_USERNAME", "BL01"),
        password=os.getenv("PAPER_PASSWORD", "123"),
        rest_base_url=os.getenv("PAPER_REST_BASE_URL", "http://localhost:9090"),
        socket_connect_host=os.getenv("SOCKET_HOST", "localhost"),
        socket_connect_port=int(os.getenv("SOCKET_PORT", "5001")),
        sender_comp_id=os.getenv("SENDER_COMP_ID", "cross-FIX"),
        target_comp_id=os.getenv("TARGET_COMP_ID", "SERVER"),
        console=True,  # Only show WARNING/ERROR in console, DEBUG/INFO go to file only
    )

    # Subscribe to events (clean event-based design)
    client.on("fix:logon", on_logon)
    client.on("fix:logout", on_logout)
    client.on("fix:reject", on_reject)

    # Connect (non-blocking)
    logger.info("🔌 Connecting to PaperBroker...")
    client.connect()

    try:
        # Wait for logon (with timeout)
        if client.wait_until_logged_on(timeout=10):
            logger.info("✅ Successfully logged on!")

            # Get account info via REST API
            cash = client.get_cash_balance()
            logger.info(f"💰 Available cash: {cash.get('remainCash', 0):,.0f} VND")

            total = client.get_account_balance()
            logger.info(f"💰 Total balance: {total.get('totalBalance', 0):,.0f} VND")

        else:
            error = client.last_logon_error()
            logger.error(f"❌ Logon failed: {error}")
            return

        # Keep connection alive for a moment
        logger.info("⏳ Staying connected for 5 seconds...")
        import time

        time.sleep(5)

    finally:
        # Note: Using os._exit() to avoid QuickFIX cleanup segfault
        # This is a known issue with QuickFIX Python bindings
        logger.info("✅ Example completed!")
        os._exit(0)


if __name__ == "__main__":
    main()
