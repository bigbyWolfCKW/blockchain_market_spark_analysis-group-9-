import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

required_env_vars = [
    "POSTGRES_HOST",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_PORT",
    "JDBC_JAR_PATH",
]

missing_vars = [key for key in required_env_vars if not os.getenv(key)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

JDBC_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

JDBC_JAR = os.getenv("JDBC_JAR_PATH")


CRYPTO_META = {
    "BTCUSDT": {"base": "BTC", "quote": "USDT", "category": "major"},
    "ETHUSDT": {"base": "ETH", "quote": "USDT", "category": "major"},
    "BNBUSDT": {"base": "BNB", "quote": "USDT", "category": "exchange"},
    "SOLUSDT": {"base": "SOL", "quote": "USDT", "category": "layer1"},
    "XRPUSDT": {"base": "XRP", "quote": "USDT", "category": "payment"},
    "DOGEUSDT": {"base": "DOGE", "quote": "USDT", "category": "meme"},
    "ADAUSDT": {"base": "ADA", "quote": "USDT", "category": "layer1"},
    "TRXUSDT": {"base": "TRX", "quote": "USDT", "category": "layer1"},
    "AVAXUSDT": {"base": "AVAX", "quote": "USDT", "category": "layer1"},
    "LINKUSDT": {"base": "LINK", "quote": "USDT", "category": "oracle"},
    "DOTUSDT": {"base": "DOT", "quote": "USDT", "category": "layer1"},
    "MATICUSDT": {"base": "MATIC", "quote": "USDT", "category": "layer2"},
    "SHIBUSDT": {"base": "SHIB", "quote": "USDT", "category": "meme"},
}


def insert_crypto_summary(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO crypto_summary (summary_time, symbol, metric_type, metric_value)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()


def insert_crypto_group_summary(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO crypto_group_summary (summary_time, group_key, metric_type, metric_value)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()


def parse_symbol_info(symbol):
    meta = CRYPTO_META.get(symbol)
    if meta:
        return meta["base"], meta["quote"], meta["category"]

    if symbol.endswith("USDT"):
        return symbol[:-4], "USDT", "other"
    if symbol.endswith("BTC"):
        return symbol[:-3], "BTC", "other"
    if symbol.endswith("ETH"):
        return symbol[:-3], "ETH", "other"

    return symbol, "UNKNOWN", "unknown"


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoAdvancedRDDAnalysis")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    sc = spark.sparkContext

    meta_bc = sc.broadcast(CRYPTO_META)

    missing_meta_acc = sc.accumulator(0)
    bad_price_acc = sc.accumulator(0)
    total_rows_acc = sc.accumulator(0)

    df = spark.read.jdbc(
        url=JDBC_URL,
        table="realtime_prices",
        properties=JDBC_PROPERTIES,
    )

    cutoff = datetime.now() - timedelta(hours=1)

    crypto_df = df.filter(
        (col("type") == "crypto") &
        (col("window_start") >= cutoff)
    )

    def enrich_row(row):
        total_rows_acc.add(1)

        symbol = row["symbol"]
        price = row["avg_price"]
        ts = row["window_start"]

        if price is None:
            bad_price_acc.add(1)
            return None

        meta = meta_bc.value.get(symbol)
        if meta:
            base = meta["base"]
            quote = meta["quote"]
            category = meta["category"]
        else:
            missing_meta_acc.add(1)
            base, quote, category = parse_symbol_info(symbol)

        return (symbol, float(price), ts, base, quote, category)

    base_rdd = (
        crypto_df.select("symbol", "avg_price", "window_start")
        .rdd
        .map(enrich_row)
        .filter(lambda x: x is not None)
    )

    base_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    row_count = base_rdd.count()
    print(f"Crypto rows found: {row_count}")
    print(f"Total rows scanned: {total_rows_acc.value}")
    print(f"Rows with invalid price: {bad_price_acc.value}")
    print(f"Rows with missing metadata: {missing_meta_acc.value}")

    if row_count == 0:
        print("No crypto data found in the last 1 hour.")
        base_rdd.unpersist()
        spark.stop()
        return

    avg_price_rdd = (
        base_rdd.map(lambda x: (x[0], (x[1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    max_price_rdd = (
        base_rdd.map(lambda x: (x[0], x[1]))
        .reduceByKey(lambda a, b: a if a > b else b)
    )

    min_price_rdd = (
        base_rdd.map(lambda x: (x[0], x[1]))
        .reduceByKey(lambda a, b: a if a < b else b)
    )

    latest_price_rdd = (
        base_rdd.map(lambda x: (x[0], (x[2], x[1])))
        .reduceByKey(lambda a, b: a if a[0] > b[0] else b)
        .mapValues(lambda x: x[1])
    )

    price_range_rdd = (
        base_rdd.map(lambda x: (x[0], (x[1], x[1])))
        .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])))
        .mapValues(lambda x: x[1] - x[0])
    )

    momentum_rdd = (
        avg_price_rdd.join(latest_price_rdd)
        .mapValues(lambda x: x[1] - x[0])
    )

    summary_time = datetime.now()
    symbol_rows = []

    for symbol, value in avg_price_rdd.collect():
        symbol_rows.append((summary_time, symbol, "avg_price", float(value)))

    for symbol, value in max_price_rdd.collect():
        symbol_rows.append((summary_time, symbol, "max_price", float(value)))

    for symbol, value in min_price_rdd.collect():
        symbol_rows.append((summary_time, symbol, "min_price", float(value)))

    for symbol, value in latest_price_rdd.collect():
        symbol_rows.append((summary_time, symbol, "latest_price", float(value)))

    for symbol, value in price_range_rdd.collect():
        symbol_rows.append((summary_time, symbol, "price_range", float(value)))

    for symbol, value in momentum_rdd.collect():
        symbol_rows.append((summary_time, symbol, "momentum", float(value)))

    insert_crypto_summary(symbol_rows)
    print(f"Inserted {len(symbol_rows)} rows into crypto_summary.")

    avg_price_by_category_rdd = (
        base_rdd.map(lambda x: (x[5], (x[1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    count_by_quote_rdd = (
        base_rdd.map(lambda x: ((x[4], x[0]), 1))
        .reduceByKey(lambda a, b: 1)
        .map(lambda x: (x[0][0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    symbol_category_rdd = (
        base_rdd.map(lambda x: (x[0], x[5]))
        .reduceByKey(lambda a, b: a)
    )

    category_momentum_rdd = (
        symbol_category_rdd.join(momentum_rdd)
        .map(lambda x: (x[1][0], (x[1][1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    group_rows = []

    for group_key, value in avg_price_by_category_rdd.collect():
        group_rows.append((summary_time, group_key, "avg_price_by_category", float(value)))

    for group_key, value in count_by_quote_rdd.collect():
        group_rows.append((summary_time, group_key, "symbol_count_by_quote", float(value)))

    for group_key, value in category_momentum_rdd.collect():
        group_rows.append((summary_time, group_key, "avg_momentum_by_category", float(value)))

    insert_crypto_group_summary(group_rows)
    print(f"Inserted {len(group_rows)} rows into crypto_group_summary.")

    top_movers = momentum_rdd.takeOrdered(10, key=lambda x: -x[1])
    top_volatile = price_range_rdd.takeOrdered(10, key=lambda x: -x[1])

    print("\nTop 10 movers:")
    for item in top_movers:
        print(item)

    print("\nTop 10 most volatile symbols:")
    for item in top_volatile:
        print(item)

    base_rdd.unpersist()
    spark.stop()


if __name__ == "__main__":
    while True:
        try:
            print("\nStarting a new crypto Spark batch...")
            main()
            print("Batch completed. Sleeping for 300 seconds...")
            time.sleep(300)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Batch failed: {e}")
            time.sleep(60)