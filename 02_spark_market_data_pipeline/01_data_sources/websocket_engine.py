import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import websockets
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")

BINANCE_WS_URI = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
ALPACA_WS_URI = "wss://stream.data.alpaca.markets/v2/iex"

TOP_30_CRYPTOS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "TRXUSDT", "LTCUSDT", "BCHUSDT",
    "UNIUSDT", "ATOMUSDT", "XLMUSDT", "NEARUSDT", "ICPUSDT",
    "APTUSDT", "FILUSDT", "ETCUSDT", "VETUSDT", "LDOUSDT",
    "OPUSDT", "STXUSDT", "INJUSDT", "RNDRUSDT", "TIAUSDT"
]

US_STOCK_WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "XOM", "OXY"]


async def stream_binance_crypto() -> None:
    """Stream selected Binance crypto mini-tickers into PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to PostgreSQL for crypto stream: {e}")
        return

    async with websockets.connect(BINANCE_WS_URI) as websocket:
        print("Connected to Binance crypto stream.")

        while True:
            try:
                response = await websocket.recv()
                data = json.loads(response)

                count = 0
                for item in data:
                    symbol = item["s"]

                    if symbol in TOP_30_CRYPTOS:
                        price = float(item["c"])
                        timestamp = datetime.fromtimestamp(item["E"] / 1000.0, tz=timezone.utc)

                        cursor.execute(
                            """
                            INSERT INTO realtime_prices (window_start, symbol, avg_price, type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (symbol, type, window_start) DO NOTHING
                            """,
                            (timestamp, symbol, price, "crypto")
                        )
                        count += 1

                if count > 0:
                    conn.commit()
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserted {count} crypto ticks.")

            except Exception as e:
                print(f"Error in crypto stream: {e}")
                await asyncio.sleep(5)


async def stream_alpaca_stocks() -> None:
    """Stream selected Alpaca US stock trades into PostgreSQL."""
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("Alpaca credentials are missing in .env.")
        return

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Failed to connect to PostgreSQL for stock stream: {e}")
        return

    async with websockets.connect(ALPACA_WS_URI) as websocket:
        auth_message = {
            "action": "auth",
            "key": ALPACA_API_KEY,
            "secret": ALPACA_SECRET_KEY
        }
        await websocket.send(json.dumps(auth_message))
        auth_response = await websocket.recv()
        print(f"Alpaca auth response: {auth_response}")

        sub_message = {
            "action": "subscribe",
            "trades": US_STOCK_WATCHLIST
        }
        await websocket.send(json.dumps(sub_message))
        sub_response = await websocket.recv()
        print(f"Alpaca subscribe response: {sub_response}")
        print("Connected to Alpaca US stock stream.")

        while True:
            try:
                response = await websocket.recv()
                data = json.loads(response)

                count = 0
                for item in data:
                    if item.get("T") == "t":
                        symbol = item.get("S")
                        price = float(item.get("p"))
                        timestamp = datetime.now(timezone.utc)

                        cursor.execute(
                            """
                            INSERT INTO realtime_prices (window_start, symbol, avg_price, type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (symbol, type, window_start) DO NOTHING
                            """,
                            (timestamp, symbol, price, "us_stock")
                        )
                        count += 1

                if count > 0:
                    conn.commit()
                    print(f"Inserted {count} US stock trades.")

            except Exception as e:
                print(f"Error in stock stream: {e}")
                await asyncio.sleep(5)


async def main() -> None:
    """Run crypto and US stock streams concurrently."""
    print("Starting market websocket engine...")

    await asyncio.gather(
        stream_binance_crypto(),
        stream_alpaca_stocks()
    )


if __name__ == "__main__":
    asyncio.run(main())