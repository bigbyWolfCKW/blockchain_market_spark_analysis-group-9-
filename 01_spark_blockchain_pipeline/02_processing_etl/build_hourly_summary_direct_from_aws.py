from datetime import datetime, timedelta
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def make_date_paths(base: str, start_date: str, num_days: int) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    paths = []

    for i in range(num_days):
        d = start + timedelta(days=i)
        day_str = d.strftime("%Y-%m-%d")
        paths.append(f"{base}date={day_str}/")

    return paths


def add_zscore(df, col_name: str, window_spec):
    avg_col = f"{col_name}_24h_avg"
    std_col = f"{col_name}_24h_std"
    z_col = f"{col_name}_24h_zscore"

    return (
        df.withColumn(avg_col, F.avg(F.col(col_name)).over(window_spec))
        .withColumn(std_col, F.stddev(F.col(col_name)).over(window_spec))
        .withColumn(
            z_col,
            F.when(
                F.col(std_col).isNull() | (F.col(std_col) == 0),
                F.lit(0.0),
            ).otherwise((F.col(col_name) - F.col(avg_col)) / F.col(std_col)),
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build AWS BTC hourly summary directly from S3."
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-03-20",
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to read from AWS public blockchain data",
    )

    parser.add_argument(
        "--base-path",
        type=str,
        default="s3a://aws-public-blockchain/v1.0/btc/transactions/",
        help="AWS public blockchain BTC transactions base path",
    )

    parser.add_argument(
        "--output-path",
        type=str,
        default="./output/hourly_features",
        help="Output path for hourly blockchain features",
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("build-aws-btc-hourly-features")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    paths = make_date_paths(args.base_path, args.start_date, args.days)

    print("=== Reading AWS BTC transaction parquet paths ===")
    for p in paths:
        print(p)

    df = spark.read.parquet(*paths)

    df = (
        df.withColumn("block_timestamp", F.to_timestamp("block_timestamp"))
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("block_timestamp")))
        .withColumn("fee", F.col("fee").cast("double"))
        .withColumn("input_value", F.col("input_value").cast("double"))
        .withColumn("output_value", F.col("output_value").cast("double"))
        .withColumn("size", F.col("size").cast("double"))
    )

    hourly_df = (
        df.groupBy("hour_bucket")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.avg("size"), 8).alias("avg_tx_size"),
            F.max("size").alias("max_tx_size"),
            F.round(F.avg("fee"), 8).alias("avg_fee"),
            F.max("fee").alias("max_fee"),
            F.round(F.sum("fee"), 8).alias("total_fee"),
            F.round(F.avg("input_value"), 8).alias("avg_input_value"),
            F.round(F.sum("input_value"), 8).alias("total_input_value"),
            F.round(F.avg("output_value"), 8).alias("avg_output_value"),
            F.round(F.sum("output_value"), 8).alias("total_output_value"),
        )
        .orderBy("hour_bucket")
    )

    rolling_24h = Window.orderBy("hour_bucket").rowsBetween(-23, 0)

    hourly_df = add_zscore(hourly_df, "tx_count", rolling_24h)
    hourly_df = add_zscore(hourly_df, "total_fee", rolling_24h)
    hourly_df = add_zscore(hourly_df, "total_input_value", rolling_24h)

    hourly_df = hourly_df.withColumn(
        "is_anomaly",
        (
            (F.abs(F.col("tx_count_24h_zscore")) > 2.0)
            | (F.abs(F.col("total_fee_24h_zscore")) > 2.0)
            | (F.abs(F.col("total_input_value_24h_zscore")) > 2.0)
        ),
    )

    print("=== Hourly feature preview ===")
    hourly_df.show(20, truncate=False)

    print(f"=== Writing hourly features to {args.output_path} ===")
    (
        hourly_df.write
        .mode("overwrite")
        .parquet(args.output_path)
    )

    print("=== Completed hourly feature generation ===")

    spark.stop()


if __name__ == "__main__":
    main()
