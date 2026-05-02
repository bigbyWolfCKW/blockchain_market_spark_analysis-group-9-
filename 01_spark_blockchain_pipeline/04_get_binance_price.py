import requests
import pandas as pd
import time
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from loguru import logger
from pathlib import Path

DATA_FOLDER = Path("./data")
OUTPUT_FOLDER = Path("./output")


def get_binance_history(symbol="BTCUSDT", interval="1d", start_str="2021-12-31"):

    # Convert string to millisecond timestamp
    start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines"

    all_data = []
    current_start = start_ts

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "limit": 1000
        }

        response = requests.get(url, params=params).json()

        if not response or len(response) == 0:
            break

        all_data.extend(response)

        # Set next start time to 1ms after the last received close time
        current_start = response[-1][6] + 1

        # Stop if we've reached the current date
        if current_start > int(time.time() * 1000):
            break

        # Avoid hitting rate limits
        time.sleep(0.1)

    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
    ])

    # Clean up formatting
    df['date'] = pd.to_datetime(df['open_time'], unit='ms').dt.strftime('%Y-%m-%d')
    return df[['date', 'close']]


def get_binance_spark():
    logger.info(f"=== Reading BTCUSDT Price from Binance API ===")
    binance_pd = get_binance_history()

    spark = (
        SparkSession.builder
        .appName("load-aws-btc-transactions")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    logger.info("=== Building AWS BTC daily transaction summary ===")
    binance_spark = spark.createDataFrame(binance_pd)

    # Cast price to double for math operations
    binance_spark = binance_spark.withColumn("btc_price", binance_spark["close"].cast(DoubleType())) \
        .drop("close")

    # Check the data
    binance_spark.show(5)

if __name__ == "__main__":
    get_binance_spark()
