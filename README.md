# CS408 — SMA Momentum Crossover Live Trading Bot

**APCS | CS408: Computation Finance**
Student IDs: 22125009 · 22125038

---

## Overview

This project implements a **live, tick-by-tick trading bot** for the **VN30 Index Futures (VN30F1M)** market using a Simple Moving Average (SMA) momentum crossover strategy. The bot connects to the [AlgoTrade PaperBroker](https://papertrade.algotrade.vn) platform via FIX protocol for order execution and Kafka for real-time market data.

---

## Strategy

### Hypothesis

When the tick price crosses **above** the SMA, it signals the beginning of a short-term uptrend. When it crosses **below**, a downtrend is expected. The bot exploits these momentum shifts with disciplined take-profit and stop-loss rules.

### Target Market

**VN30 Index Futures — VN30F1M (Front-month contract)**

### Entry Signals

A **Limit Order** is placed immediately upon signal detection:

| Signal | Condition |
|---|---|
| **Open Long** | $P_{t-1} < SMA_t\text{-}1$ **and** $P_t \geq SMA_t$ |
| **Open Short** | $P_{t-1} > SMA_t\text{-}1$ **and** $P_t \leq SMA_t$ |

> The SMA window is configurable via `SMA_WINDOW` (default: **10 ticks** for live testing).

### Exit Rules

| Rule | Condition | Action |
|---|---|---|
| **Take-Profit** | Unrealized P&L ≥ **+3 points** | Close position |
| **Stop-Loss** | Unrealized P&L ≤ **−2 points** | Close position |
| **End-of-Day** | Current time ≥ **14:29:55** | Force-close all positions (no overnight holds) |

### Position Sizing

**1 contract** per position. Maximum 1 open position at a time.

---

## Architecture

```
paper_trading.py
├── LiveSMABot
│   ├── _init_clients()         — Connects FIX (execution) + Kafka (market data)
│   ├── on_quote_update()       — Tick handler: SMA calc → signal eval → order routing
│   ├── on_order_filled()       — FIX fill callback: confirms inventory state
│   ├── on_order_failed()       — FIX reject/cancel callback: frees order lock
│   ├── open_position()         — Places aggressive limit order to enter
│   ├── close_position()        — Places aggressive limit order to exit
│   └── run()                   — Async main loop: FIX logon → MD subscribe → event loop
```

### Key Design Decisions

- **Pending-order lock** — Only one live order at a time. New signals are suppressed while an order is outstanding.
- **Pending inventory intent** — Inventory state is only updated on confirmed fill (`on_order_filled`), never on order placement.
- **Stuck-order watchdog** — Orders unconfirmed for **120 seconds** are automatically canceled and the lock is freed.
- **Crash recovery** — Orphaned orders from a previous session are loaded from `orders.db` and canceled on startup.
- **Aggressive limit orders** — Entry/exit prices include **±1.0 point slippage** to maximize fill probability without crossing circuit limits.

---

## Project Structure

```
.
├── paper_trading.py          # Live trading bot (this file)
├── new_paper_trading.py      # Experimental / WIP version
├── backtesting.py            # Historical backtest engine
├── optimization.py           # Parameter optimization
├── evaluation.py             # Strategy evaluation metrics
├── data_loader.py            # Historical data loader
├── price_util.py             # Price utility helpers
├── utils.py                  # General utilities
├── config/
│   └── config.py             # Configuration constants
├── database/
│   ├── data_service.py
│   └── query.py
├── metrics/
│   └── metric.py
├── guides/                   # PaperBroker API usage examples
├── data/                     # Historical data (see Setup below)
├── orders.db                 # Auto-generated order store (crash recovery)
├── .env.example              # Environment variable template
└── CS408_Trading_Algorithm.md  # Strategy specification document
```

---

## Setup & Installation

### Prerequisites

- Python 3.9+
- A virtual environment (recommended)
- Access credentials for the AlgoTrade PaperBroker platform

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Install the PaperBroker client

Download the Python client `.whl` package from the [PaperBroker docs page](https://papertrade.algotrade.vn/docs/), place it in the project root, then install:

```bash
pip install paperbroker_client-0.2.4-py3-none-any.whl
```

> Rename the filename above to match the version you downloaded.

### 3. Set up historical data (for backtesting)

Download the `is` and `os` data folders from [Google Drive](https://drive.google.com/drive/folders/181d7JcfHilIvviLgEuaDt2VqwZLYnYUF), then place them inside a `data/` folder at the project root:

```
data/
├── is/
└── os/
```

### 4. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials (available in the private/class channel):

```bash
cp .env.example .env
```

```ini
PAPER_ACCOUNT_ID=D1
PAPER_USERNAME=<username>
PAPER_PASSWORD=<password>
SENDER_COMP_ID=<sender_comp_id>
TARGET_COMP_ID=SERVER
SOCKET_HOST=papertrade.algotrade.vn
SOCKET_PORT=5001
PAPER_REST_BASE_URL=https://papertrade.algotrade.vn/accounting
PAPERBROKER_KAFKA_BOOTSTRAP_SERVERS=<kafka_bootstrap_servers>
PAPERBROKER_KAFKA_USERNAME=<kafka_username>
PAPERBROKER_KAFKA_PASSWORD=<kafka_password>
PAPERBROKER_ENV_ID=<env_id>
VN30F1M=HNXDS:VN30F2605
```

---

## Running the Bot

```bash
python paper_trading.py
```

The bot will:
1. Connect and log on to the FIX engine
2. Cancel any orphaned orders from a previous session
3. Subscribe to live market data via Kafka
4. Begin evaluating ticks and trading autonomously
5. Shut down cleanly on `Ctrl+C`

---

## Strategy Parameters

All tunable parameters are defined at the top of `paper_trading.py`:

| Parameter | Default | Description |
|---|---|---|
| `SMA_WINDOW` | `10` | Rolling tick window for SMA calculation |
| `TP_POINTS` | `3.0` | Take-profit threshold (points) |
| `SL_POINTS` | `2.0` | Stop-loss threshold (points) |
| `END_OF_DAY_CLOSE` | `14:29:55` | Force-close time (5 s before ATC) |

> The original research strategy uses `SMA_WINDOW = 1000`. The default is set to `10` for faster warm-up during live testing sessions.

---

## Logging

The bot logs every tick evaluation with full state context:

```
[10:00:01] [INFO] TICK EVAL | Price: 1285.0 | SMA: 1284.50 | Window: 10/10 | Pos: 0 | Lock: None
[10:00:02] [INFO] 📈 BUY Signal (Bullish Crossover)
[10:00:02] [INFO] 📤 Sent BUY order: a3f1bc92 at 1286.0
[10:00:02] [INFO] ✅ ORDER FILLED: 1 @ 1285.5
```
