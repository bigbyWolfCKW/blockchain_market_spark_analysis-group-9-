import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_data")


try:
    producer = KafkaProducer(
        bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Connected to Kafka successfully (HK and index pipeline).")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    producer = None


indices = [
    "^HSI",
    "^HSCE",
    "^HSTECH",
    "000001.SS",
    "^TWII",
    "^N225"
]

hk_stocks = [
    "0700.HK", "09988.HK", "03690.HK", "0005.HK", "1299.HK", "0941.HK", "0883.HK", "0388.HK",
    "0011.HK", "0857.HK", "3988.HK", "0939.HK", "1398.HK", "0016.HK", "0267.HK", "1928.HK",
    "0001.HK", "0002.HK", "0003.HK", "0066.HK", "1093.HK", "1109.HK", "2020.HK", "2269.HK",
    "2318.HK", "2388.HK", "2628.HK", "0012.HK", "0017.HK", "0101.HK", "0288.HK", "0386.HK",
    "0688.HK", "0762.HK", "0823.HK", "0968.HK", "1113.HK", "1177.HK", "1378.HK", "1810.HK",
    "1997.HK", "2007.HK", "2313.HK", "2319.HK", "2600.HK", "2899.HK", "3692.HK", "3968.HK",
    "6098.HK", "6690.HK", "9618.HK", "9633.HK", "9888.HK", "9999.HK", "0020.HK", "0135.HK",
    "0144.HK", "0151.HK", "0241.HK", "0257.HK", "0268.HK", "0285.HK", "0315.HK", "0322.HK",
    "0338.HK", "0347.HK", "0358.HK", "0460.HK", "0522.HK", "0586.HK", "0669.HK", "0772.HK",
    "0813.HK", "0836.HK", "0868.HK", "0914.HK", "0960.HK", "0981.HK", "0992.HK", "1024.HK",
    "1038.HK", "1044.HK", "1088.HK", "1171.HK", "1193.HK", "1209.HK", "1336.HK", "1347.HK",
    "1772.HK", "1801.HK", "1833.HK", "1876.HK", "1898.HK", "1919.HK", "2015.HK", "2038.HK"
]

all_tickers = indices + hk_stocks


def run_hk_streamer() -> None:
    print(f"Starting HK stocks and index streamer ({len(all_tickers)} assets)...")

    while True:
        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_str}] Fetching latest prices...")

        success_count = 0

        for ticker_symbol in all_tickers:
            try:
                ticker = yf.Ticker(ticker_symbol)
                price = ticker.fast_info["lastPrice"]

                asset_type = "index" if ticker_symbol in indices else "hk_stock"
                current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                record = {
                    "symbol": ticker_symbol,
                    "price": round(float(price), 3),
                    "timestamp": current_time,
                    "type": asset_type
                }

                if producer:
                    producer.send(KAFKA_TOPIC, record)

                success_count += 1
                print(f"[Sent] {ticker_symbol}: ${price:.2f} ({asset_type})")

                time.sleep(0.2)

            except Exception as e:
                print(f"Failed to fetch {ticker_symbol}: {e}")

        if producer:
            producer.flush()

        print(f"Completed one cycle. Sent {success_count} price records. Next cycle starts in 10 seconds.")
        time.sleep(10)


if __name__ == "__main__":
    try:
        run_hk_streamer()
    except KeyboardInterrupt:
        print("\nHK market streamer stopped safely.")
        if producer:
            producer.close()