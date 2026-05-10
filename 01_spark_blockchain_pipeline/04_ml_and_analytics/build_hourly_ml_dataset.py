import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build hourly BTC ML dataset from blockchain and market Parquet data."
    )
    parser.add_argument(
        "--blockchain-path",
        default="output/hourly_features_3y_cluster",
    )
    parser.add_argument(
        "--market-path",
        default="output/market_btcusdt_1h_3y",
    )
    parser.add_argument(
        "--output-path",
        default="output/hourly_ml_dataset_3y",
    )
    parser.add_argument(
        "--volatility-quantile",
        type=float,
        default=0.75,
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("build-hourly-btc-ml-dataset")
        .getOrCreate()
    )

    blockchain_df = spark.read.parquet(args.blockchain_path)

    market_df = (
        spark.read.parquet(args.market_path)
        .select(
            "hour_bucket",
            "symbol",
            F.col("open").alias("market_open"),
            F.col("high").alias("market_high"),
            F.col("low").alias("market_low"),
            F.col("close").alias("market_close"),
            F.col("volume").alias("market_volume"),
            F.col("quote_volume").alias("market_quote_volume"),
            F.col("trade_count").alias("market_trade_count"),
        )
        .where(F.col("symbol") == "BTCUSDT")
    )

    joined_df = (
        blockchain_df
        .join(market_df, on="hour_bucket", how="inner")
        .orderBy("hour_bucket")
    )

    window_by_time = Window.orderBy("hour_bucket")
    rolling_24h = Window.orderBy("hour_bucket").rowsBetween(-23, 0)

    labeled_df = (
        joined_df
        .withColumn("close_lag_1h", F.lag("market_close", 1).over(window_by_time))
        .withColumn("close_lag_6h", F.lag("market_close", 6).over(window_by_time))
        .withColumn("close_lag_24h", F.lag("market_close", 24).over(window_by_time))
        .withColumn("market_return_1h", F.log(F.col("market_close") / F.col("close_lag_1h")))
        .withColumn("market_return_6h", F.log(F.col("market_close") / F.col("close_lag_6h")))
        .withColumn("market_return_24h", F.log(F.col("market_close") / F.col("close_lag_24h")))
        .withColumn("market_abs_return_1h", F.abs(F.col("market_return_1h")))
        .withColumn("market_abs_return_6h", F.abs(F.col("market_return_6h")))
        .withColumn("market_abs_return_24h", F.abs(F.col("market_return_24h")))
        .withColumn("market_volume_24h_avg", F.avg("market_volume").over(rolling_24h))
        .withColumn("market_trade_count_24h_avg", F.avg("market_trade_count").over(rolling_24h))
        .withColumn("next_24h_close", F.lead("market_close", 24).over(window_by_time))
        .withColumn(
            "next_24h_log_return",
            F.log(F.col("next_24h_close") / F.col("market_close")),
        )
        .withColumn(
            "next_24h_direction",
            F.when(F.col("next_24h_close") > F.col("market_close"), F.lit(1.0))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("next_24h_abs_return", F.abs(F.col("next_24h_log_return")))
    )

    volatility_threshold = labeled_df.approxQuantile(
        "next_24h_abs_return",
        [args.volatility_quantile],
        0.001,
    )[0]

    labeled_df = (
        labeled_df
        .withColumn(
            "next_24h_volatility_spike",
            F.when(
                F.col("next_24h_abs_return") >= F.lit(volatility_threshold),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .dropna(
            subset=[
                "market_return_1h",
                "market_return_6h",
                "market_return_24h",
                "market_abs_return_1h",
                "market_abs_return_6h",
                "market_abs_return_24h",
                "market_volume_24h_avg",
                "market_trade_count_24h_avg",
            ]
        )
    )

    print("=== Joined hourly ML dataset summary ===")
    labeled_df.select(
        F.count("*").alias("row_count"),
        F.min("hour_bucket").alias("min_hour"),
        F.max("hour_bucket").alias("max_hour"),
        F.avg("next_24h_direction").alias("direction_positive_rate"),
        F.avg("next_24h_volatility_spike").alias("volatility_spike_rate"),
    ).show(truncate=False)

    print(f"=== Volatility threshold: {volatility_threshold} ===")

    print("=== Hourly ML dataset preview ===")
    labeled_df.select(
        "hour_bucket",
        "tx_count",
        "total_fee",
        "total_input_value",
        "is_anomaly",
        "market_close",
        "next_24h_close",
        "next_24h_direction",
        "next_24h_log_return",
        "next_24h_volatility_spike",
    ).show(20, truncate=False)

    print(f"=== Writing hourly ML dataset to {args.output_path} ===")
    (
        labeled_df.write
        .mode("overwrite")
        .parquet(args.output_path)
    )

    print("=== Completed hourly ML dataset build ===")
    spark.stop()


if __name__ == "__main__":
    main()
