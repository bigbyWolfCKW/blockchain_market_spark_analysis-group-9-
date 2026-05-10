import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Binance hourly market Parquet from monthly archive CSV files."
    )
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("build-binance-hourly-market-parquet")
        .getOrCreate()
    )

    raw_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "false")
        .csv(f"{args.input_path}/*.csv")
        .toDF(*COLUMNS)
    )

    market_df = (
        raw_df
        .withColumn("open_time_raw", F.col("open_time").cast("long"))
        .withColumn("close_time_raw", F.col("close_time").cast("long"))
        .withColumn(
            "open_time_ms",
            F.when(F.col("open_time_raw") > F.lit(10_000_000_000_000), F.col("open_time_raw") / 1000)
            .otherwise(F.col("open_time_raw"))
        )
        .withColumn(
            "close_time_ms",
            F.when(F.col("close_time_raw") > F.lit(10_000_000_000_000), F.col("close_time_raw") / 1000)
            .otherwise(F.col("close_time_raw"))
        )
        .withColumn("hour_bucket", F.to_timestamp(F.from_unixtime(F.col("open_time_ms") / 1000)))
        .withColumn("close_time_ts", F.to_timestamp(F.from_unixtime(F.col("close_time_ms") / 1000)))

        .withColumn("symbol", F.lit(args.symbol))
        .withColumn("open", F.col("open").cast("double"))
        .withColumn("high", F.col("high").cast("double"))
        .withColumn("low", F.col("low").cast("double"))
        .withColumn("close", F.col("close").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
        .withColumn("quote_volume", F.col("quote_volume").cast("double"))
        .withColumn("trade_count", F.col("trade_count").cast("long"))
        .withColumn("taker_buy_base_volume", F.col("taker_buy_base_volume").cast("double"))
        .withColumn("taker_buy_quote_volume", F.col("taker_buy_quote_volume").cast("double"))
        .select(
            "hour_bucket",
            "close_time_ts",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        )
        .dropDuplicates(["symbol", "hour_bucket"])
        .orderBy("hour_bucket")
    )

    print("=== Binance hourly market preview ===")
    market_df.show(20, truncate=False)

    print("=== Binance hourly market summary ===")
    market_df.select(
        F.count("*").alias("row_count"),
        F.min("hour_bucket").alias("min_hour"),
        F.max("hour_bucket").alias("max_hour"),
    ).show(truncate=False)

    print(f"=== Writing Binance hourly market Parquet to {args.output_path} ===")
    (
        market_df.write
        .mode("overwrite")
        .parquet(args.output_path)
    )

    print("=== Completed Binance hourly market Parquet build ===")
    spark.stop()


if __name__ == "__main__":
    main()
