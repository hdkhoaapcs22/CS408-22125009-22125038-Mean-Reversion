# Trading Algorithm

**APCS | CS408: Computation Finance**

---

## Group Information

- **Group Name:** **********************\_\_\_\_**********************
- **Member 1:** **********************\_\_\_\_**********************
- **Member 2:** **********************\_\_\_\_**********************
- **Member 3:** **********************\_\_\_\_**********************

---

## Checklist

- [ ] Position Entry Point
- [ ] Target Market
- [ ] Take-Profit Point
- [ ] Stop-Loss Point
- [ ] Position Sizing
- [ ] Trading Tactics

---

## Hypothesis

High-quality, large-cap stocks in the VN-Diamond index experience temporary price dislocations due to broader market noise or retail panic. Because these assets have robust fundamentals and persistent institutional demand (having reached their Foreign Ownership Limit), when their price deviates significantly below their historical 20-day moving average, they have a statistically high probability of **"reverting to the mean"** (bouncing back) rather than continuing downward.

While mean-reversion captures profits during choppy or panic-driven markets, high-quality VN-Diamond stocks frequently enter prolonged, sustained uptrends driven by institutional buying. By implementing a **Momentum strategy**, the algorithm capitalizes on these strong upward trends, buying into established strength and holding until the macroeconomic or institutional momentum breaks.

---

## Target Market

**Top constituent stocks of the VN-Diamond Index**

---

## Strategy Group

- [x] Momentum
- [x] Mean-Reversion

---

## When to Open Positions

### Position Sizing — Unified Pool

The **500 million VND** capital is divided into **3 equal tranches of ~166.6 million VND**. The algorithm handles a maximum of **3 concurrent positions**, regardless of which signal triggered them.

### Position Entry Point

The algorithm scans daily and triggers a **Buy signal** if a stock meets **either** of the following conditions:

|       | Condition             | Criteria                                                                                                       |
| ----- | --------------------- | -------------------------------------------------------------------------------------------------------------- |
| **A** | Mean-Reversion Signal | Daily closing price drops **below the Lower Bollinger Band** AND the **14-day RSI < 30**                       |
| **B** | Momentum Signal       | **20-day SMA crosses above 50-day SMA** (Golden Cross) AND daily volume exceeds **150% of the 20-day average** |

### Trading Tactics

Execute a **Market Buy order** during the **ATC (At The Close) session** to ensure the daily candle and technical indicators are fully confirmed before committing capital.

---

## When to Close Positions

### Take-Profit Point _(Dependent on Entry Trigger)_

| Entry Condition                  | Take-Profit Rule                                                                          |
| -------------------------------- | ----------------------------------------------------------------------------------------- |
| **Condition A** (Mean-Reversion) | Sell when price crosses **back above the 20-day SMA** (mean is restored)                  |
| **Condition B** (Momentum)       | Sell when daily closing price **drops and closes below the 20-day SMA** (trend is broken) |

### Stop-Loss Point _(Universal)_

- 🔴 **Hard Stop:** Sell immediately if the price drops **7% or more** below the entry price (evaluated from **T+2.5 settlement** onwards) to protect against fundamental breakdowns.
- ⏱️ **Time Stop** _(Condition A only):_ Liquidate if the price hasn't reverted to the 20-day SMA within **15 trading days** to free up stagnant capital.

### Position Sizing

Liquidate **100%** of the specific tranche to return the ~166.6 million VND back to the available capital pool.

### Trading Tactics

Execute a **Market Sell order** during **continuous matching or ATC** to prioritize immediate exit and prevent further drawdown.

---

---

# Trading Algorithm — Example

**APCS | CS408: Computation Finance**

---

## Group Information

- **Group Name:** **********************\_\_\_\_**********************
- **Member 1:** **********************\_\_\_\_**********************
- **Member 2:** **********************\_\_\_\_**********************
- **Member 3:** **********************\_\_\_\_**********************

---

## Checklist

- [ ] Position Entry Point
- [ ] Target Market
- [ ] Take-Profit Point
- [ ] Stop-Loss Point
- [ ] Position Sizing
- [ ] Trading Tactics

---

## Hypothesis

When the price starts to rise again after a downtrend, the price line crossing **over** the SMA signals an uptrend that is expected to continue in the short term. Conversely, when the price starts to fall after an uptrend, a drop **below** the SMA signals the beginning of a downtrend.

---

## Target Market

**VN30 Index Future Contract – VN30F1M**

---

## Strategy Group

- [x] Momentum
- [ ] Mean-Reversion
- [ ] Grid
- [ ] Scalping
- [ ] Market-Making

---

## When to Open Positions

### Variables

- **Independent variable:** VN30F1M price by ticks
- **Dependent variable:**

$$SMA(1000)_t = \frac{P_t + P_{t-1} + P_{t-2} + \cdots + P_{t-999}}{1000}$$

> Where $P_t$ is the current price of VN30F1M, and $P_{t-1}$ is the previous tick price.

### Entry Signals

Place a **Limit Order (LO)** to buy at the ceiling price or sell at the floor price as soon as a signal appears:

| Signal                 | Condition                                                  |
| ---------------------- | ---------------------------------------------------------- |
| **Open Buy position**  | $P_{t-1} < SMA(1000)_{t-1}$ **and** $P_t \geq SMA(1000)_t$ |
| **Open Sell position** | $P_{t-1} > SMA(1000)_{t-1}$ **and** $P_t \leq SMA(1000)_t$ |

### Position Sizing

Trade **01 contract** per position.

---

## When to Close Positions

| Rule               | Condition                         | Action                                                    |
| ------------------ | --------------------------------- | --------------------------------------------------------- |
| **Take-Profit**    | Unrealized Profit ≥ **+3 points** | Place order to close position                             |
| **Stop-Loss**      | Unrealized Profit ≤ **−2 point**  | Place order to close position                             |
| **Overnight Rule** | Any open position at ATC session  | Close with an **ATC order** — no positions held overnight |
