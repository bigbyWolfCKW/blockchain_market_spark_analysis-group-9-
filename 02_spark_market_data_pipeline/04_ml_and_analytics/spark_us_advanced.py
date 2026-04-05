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

US_META = {
    "AAPL": {"sector": "tech"},
    "MSFT": {"sector": "tech"},
    "NVDA": {"sector": "semiconductor"},
    "TSLA": {"sector": "ev"},
    "AMZN": {"sector": "ecommerce"},
    "META": {"sector": "social_media"},
    "GOOGL": {"sector": "internet"},
    "JPM": {"sector": "banking"},
    "V": {"sector": "payments"},
    "UNH": {"sector": "healthcare"},
    "XOM": {"sector": "energy"},
    "OXY": {"sector": "energy"},
}


def insert_us_summary(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO us_stock_summary (summary_time, symbol, metric_type, metric_value)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()


def insert_sector_summary(rows):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO us_sector_summary (summary_time, group_key, metric_type, metric_value)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()


def main():
    spark = (
        SparkSession.builder
        .appName("USStockAdvancedRDDAnalysis")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    sc = spark.sparkContext

    meta_bc = sc.broadcast(US_META)

    missing_meta_acc = sc.accumulator(0)
    bad_price_acc = sc.accumulator(0)
    total_rows_acc = sc.accumulator(0)

    df = spark.read.jdbc(
        url=JDBC_URL,
        table="realtime_prices",
        properties=JDBC_PROPERTIES,
    )

    cutoff = datetime.now() - timedelta(hours=1)

    us_df = df.filter(
        (col("type") == "us_stock") &
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
            sector = meta["sector"]
        else:
            missing_meta_acc.add(1)
            sector = "unknown"

        return (symbol, float(price), ts, sector)

    base_rdd = (
        us_df.select("symbol", "avg_price", "window_start")
        .rdd
        .map(enrich_row)
        .filter(lambda x: x is not None)
    )

    base_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    row_count = base_rdd.count()
    print(f"US stock rows found: {row_count}")
    print(f"Total rows scanned: {total_rows_acc.value}")
    print(f"Rows with invalid price: {bad_price_acc.value}")
    print(f"Rows with missing metadata: {missing_meta_acc.value}")

    if row_count == 0:
        print("No US stock data found in the last 1 hour.")
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

    insert_us_summary(symbol_rows)
    print(f"Inserted {len(symbol_rows)} rows into us_stock_summary.")

    avg_price_by_sector_rdd = (
        base_rdd.map(lambda x: (x[3], (x[1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    symbol_sector_rdd = (
        base_rdd.map(lambda x: (x[0], x[3]))
        .reduceByKey(lambda a, b: a)
    )

    avg_momentum_by_sector_rdd = (
        symbol_sector_rdd.join(momentum_rdd)
        .map(lambda x: (x[1][0], (x[1][1], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    symbol_count_by_sector_rdd = (
        base_rdd.map(lambda x: ((x[3], x[0]), 1))
        .reduceByKey(lambda a, b: 1)
        .map(lambda x: (x[0][0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    sector_rows = []

    for group_key, value in avg_price_by_sector_rdd.collect():
        sector_rows.append((summary_time, group_key, "avg_price_by_sector", float(value)))

    for group_key, value in avg_momentum_by_sector_rdd.collect():
        sector_rows.append((summary_time, group_key, "avg_momentum_by_sector", float(value)))

    for group_key, value in symbol_count_by_sector_rdd.collect():
        sector_rows.append((summary_time, group_key, "symbol_count_by_sector", float(value)))

    insert_sector_summary(sector_rows)
    print(f"Inserted {len(sector_rows)} rows into us_sector_summary.")

    top_movers = momentum_rdd.takeOrdered(10, key=lambda x: -x[1])
    top_volatile = price_range_rdd.takeOrdered(10, key=lambda x: -x[1])

    print("\nTop 10 US movers:")
    for item in top_movers:
        print(item)

    print("\nTop 10 most volatile US symbols:")
    for item in top_volatile:
        print(item)

    base_rdd.unpersist()
    spark.stop()


if __name__ == "__main__":
    while True:
        try:
            print("\nStarting a new US stock Spark batch...")
            main()
            print("Batch completed. Sleeping for 300 seconds...")
            time.sleep(300)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Batch failed: {e}")
            time.sleep(60)