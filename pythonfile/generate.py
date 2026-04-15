"""
Stock Market Event Generator → Azure Event Hubs
------------------------------------------------
Simulates 3 event streams:
  1. STREAMING  — live tick data (price/volume every second)
  2. CDC         — portfolio DB change events (INSERT/UPDATE/DELETE)
  3. BATCH OHLCV — end-of-day candle data bursts

Setup:
    pip install azure-eventhub faker

Usage:
    python stock_event_generator.py
    # Then pick a mode when prompted
"""

import json
import time
import os
import random
import threading
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv  # <--- Add this

# Load the variables from the .env file
load_dotenv() 

# Now these will actually find the values
CONNECTION_STRING = os.getenv('CONNECTION_STRING')
EVENT_HUB_NAME    = os.getenv('EVENT_HUBNAME')       # your Event Hub name

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

STOCKS = ["AAPL", "MSFT", "GOOGL", "TSLA", "INFY", "RELIANCE", "TCS", "HDFC"]

BASE_PRICES = {
    "AAPL": 185.0, "MSFT": 415.0, "GOOGL": 175.0, "TSLA": 245.0,
    "INFY": 18.5,  "RELIANCE": 2900.0, "TCS": 3800.0, "HDFC": 1650.0,
}

# Shared mutable state so CDC events reference realistic prices
current_prices = dict(BASE_PRICES)
sequence_counter = 0
sequence_lock = threading.Lock()


def next_seq() -> int:
    global sequence_counter
    with sequence_lock:
        sequence_counter += 1
        return sequence_counter


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jitter(price: float, pct: float = 0.005) -> float:
    """Random walk within ±pct of current price."""
    change = price * pct * random.uniform(-1, 1)
    return round(price + change, 4)


def send_batch(producer: EventHubProducerClient, events: list[dict], label: str):
    """Send a list of dicts as a single Event Hub batch."""
    with producer:
        batch = producer.create_batch()
        for ev in events:
            batch.add(EventData(json.dumps(ev)))
        producer.send_batch(batch)
    print(f"  [SENT] {len(events)} {label} event(s)")


def make_producer() -> EventHubProducerClient:
    return EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )


# ─────────────────────────────────────────────
# MODE 1 — STREAMING TICK DATA
# ─────────────────────────────────────────────

def build_tick_event(symbol: str) -> dict:
    price = jitter(current_prices[symbol])
    current_prices[symbol] = price
    return {
        "event_type":  "TICK",
        "sequence_id": next_seq(),
        "timestamp":   now_iso(),
        "symbol":      symbol,
        "price":       price,
        "bid":         round(price - random.uniform(0.01, 0.05), 4),
        "ask":         round(price + random.uniform(0.01, 0.05), 4),
        "volume":      random.randint(100, 5000),
        "exchange":    random.choice(["NSE", "BSE", "NASDAQ", "NYSE"]),
    }


def run_streaming(interval_sec: float = 1.0, total_ticks: int = 30):
    """
    Sends one tick per symbol every `interval_sec`.
    Runs for `total_ticks` rounds (each round = all symbols).
    """
    print(f"\n[STREAMING] Sending tick data — {total_ticks} rounds, {interval_sec}s interval")
    print(f"            Symbols: {STOCKS}\n")

    for tick_num in range(1, total_ticks + 1):
        events = [build_tick_event(sym) for sym in STOCKS]

        producer = make_producer()
        with producer:
            batch = producer.create_batch()
            for ev in events:
                batch.add(EventData(json.dumps(ev)))
            producer.send_batch(batch)

        prices_str = " | ".join(f"{e['symbol']} {e['price']}" for e in events)
        print(f"  Tick {tick_num:02d} → {prices_str}")
        time.sleep(interval_sec)

    print("\n[STREAMING] Done.")


# ─────────────────────────────────────────────
# MODE 2 — CDC PORTFOLIO CHANGE EVENTS
# ─────────────────────────────────────────────

CDC_OPERATIONS = ["INSERT", "UPDATE", "DELETE"]
USERS = [f"user_{i:03d}" for i in range(1, 11)]


def build_cdc_event(op: str = None) -> dict:
    op     = op or random.choices(CDC_OPERATIONS, weights=[4, 5, 1])[0]
    symbol = random.choice(STOCKS)
    qty    = random.randint(1, 200)
    price  = current_prices[symbol]

    before = None
    after  = None

    if op == "INSERT":
        after = {
            "user_id":  random.choice(USERS),
            "symbol":   symbol,
            "quantity": qty,
            "avg_cost": round(price * random.uniform(0.95, 1.05), 4),
            "status":   "open",
        }
    elif op == "UPDATE":
        old_qty = qty
        new_qty = qty + random.randint(-50, 50)
        before  = {"symbol": symbol, "quantity": old_qty, "status": "open"}
        after   = {"symbol": symbol, "quantity": max(1, new_qty), "status": "open"}
    elif op == "DELETE":
        before  = {"symbol": symbol, "quantity": qty, "status": "closed"}

    return {
        "event_type":   "CDC",
        "sequence_id":  next_seq(),
        "timestamp":    now_iso(),
        "db_table":     "portfolio.holdings",
        "operation":    op,
        "transaction_id": f"txn_{random.randint(100000, 999999)}",
        "before":       before,
        "after":        after,
        "source": {
            "db":      "portfolio_db",
            "schema":  "portfolio",
            "table":   "holdings",
            "lsn":     f"{random.randint(1000000, 9999999)}",  # log sequence number
        },
    }


def run_cdc(total_events: int = 20, interval_sec: float = 0.5):
    """
    Sends CDC events simulating portfolio DB changes.
    """
    print(f"\n[CDC] Sending {total_events} CDC portfolio change events, {interval_sec}s apart\n")

    for i in range(1, total_events + 1):
        event = build_cdc_event()
        op    = event["operation"]
        sym   = (event["after"] or event["before"] or {}).get("symbol", "?")

        producer = make_producer()
        with producer:
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)

        print(f"  CDC {i:02d} → seq={event['sequence_id']:04d}  op={op:<6}  symbol={sym}")
        time.sleep(interval_sec)

    print("\n[CDC] Done.")


# ─────────────────────────────────────────────
# MODE 3 — BATCH OHLCV END-OF-DAY
# ─────────────────────────────────────────────

def build_ohlcv_event(symbol: str, date_str: str) -> dict:
    base  = current_prices[symbol]
    open_ = round(base * random.uniform(0.98, 1.02), 4)
    close = round(base * random.uniform(0.97, 1.03), 4)
    high  = round(max(open_, close) * random.uniform(1.001, 1.02), 4)
    low   = round(min(open_, close) * random.uniform(0.98, 0.999), 4)
    vol   = random.randint(500_000, 10_000_000)

    return {
        "event_type":    "OHLCV",
        "sequence_id":   next_seq(),
        "timestamp":     now_iso(),
        "date":          date_str,
        "symbol":        symbol,
        "open":          open_,
        "high":          high,
        "low":           low,
        "close":         close,
        "volume":        vol,
        "vwap":          round((high + low + close) / 3, 4),
        "trades_count":  random.randint(10_000, 200_000),
        "source":        "EOD_BATCH",
    }


def run_ohlcv_batch(num_days: int = 5):
    """
    Sends one day's OHLCV candle for every symbol, for `num_days` days.
    Simulates an ADF-triggered end-of-day batch job.
    """
    from datetime import timedelta
    today = datetime.now(timezone.utc).date()

    print(f"\n[BATCH OHLCV] Sending {num_days} days × {len(STOCKS)} symbols\n")

    for day_offset in range(num_days, 0, -1):
        date_str = str(today - timedelta(days=day_offset))
        events   = [build_ohlcv_event(sym, date_str) for sym in STOCKS]

        producer = make_producer()
        with producer:
            batch = producer.create_batch()
            for ev in events:
                batch.add(EventData(json.dumps(ev)))
            producer.send_batch(batch)

        print(f"  Date {date_str} → {len(STOCKS)} OHLCV records sent")
        time.sleep(0.3)

    print("\n[BATCH OHLCV] Done.")


# ─────────────────────────────────────────────
# MODE 4 — ALL THREE TOGETHER (mixed stream)
# ─────────────────────────────────────────────

def run_mixed(rounds: int = 10):
    """
    Each round sends a mix: ticks for all symbols, 2 CDC events, 1 OHLCV candle.
    Simulates a realistic multi-feed pipeline.
    """
    from datetime import timedelta
    today = datetime.now(timezone.utc).date()

    print(f"\n[MIXED] Sending {rounds} rounds of mixed events (TICK + CDC + OHLCV)\n")

    for r in range(1, rounds + 1):
        events = []

        # Tick events for all symbols
        for sym in STOCKS:
            events.append(build_tick_event(sym))

        # 2 CDC portfolio changes
        events.append(build_cdc_event())
        events.append(build_cdc_event())

        # 1 OHLCV candle (today - 1)
        events.append(build_ohlcv_event(
            random.choice(STOCKS),
            str(today - timedelta(days=1))
        ))

        producer = make_producer()
        with producer:
            batch = producer.create_batch()
            for ev in events:
                batch.add(EventData(json.dumps(ev)))
            producer.send_batch(batch)

        types = [e["event_type"] for e in events]
        print(f"  Round {r:02d} → {len(events)} events  {types}")
        time.sleep(1.0)

    print("\n[MIXED] Done.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Stock Market Event Generator → Azure Event Hubs")
    print("=" * 55)
    print()
    print("  [1] Streaming tick data   (live prices, 1s interval)")
    print("  [2] CDC portfolio changes (INSERT / UPDATE / DELETE)")
    print("  [3] Batch OHLCV           (end-of-day candles)")
    print("  [4] Mixed stream          (all three together)")
    print()

    choice = input("Select mode [1/2/3/4]: ").strip()

    if choice == "1":
        ticks    = int(input("  How many tick rounds? [default 30]: ").strip() or 30)
        interval = float(input("  Interval in seconds?  [default 1.0]: ").strip() or 1.0)
        run_streaming(interval_sec=interval, total_ticks=ticks)

    elif choice == "2":
        total    = int(input("  How many CDC events? [default 20]: ").strip() or 20)
        interval = float(input("  Interval in seconds? [default 0.5]: ").strip() or 0.5)
        run_cdc(total_events=total, interval_sec=interval)

    elif choice == "3":
        days = int(input("  How many days of OHLCV? [default 5]: ").strip() or 5)
        run_ohlcv_batch(num_days=days)

    elif choice == "4":
        rounds = int(input("  How many rounds? [default 10]: ").strip() or 10)
        run_mixed(rounds=rounds)

    else:
        print("Invalid choice. Exiting.")


if __name__ == "__main__":
    main()
