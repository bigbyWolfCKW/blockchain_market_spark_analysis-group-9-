from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    spark = (
        SparkSession.builder
        .appName("build-aws-btc-features")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/processed/aws_btc_daily_summary"
    output_path = "data/processed/aws_btc_features"

    print(f"=== Reading AWS daily summary from {input_path} ===")
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Input parquet not found: {e}")
        spark.stop()
        return

    required_cols = {"date", "tx_count"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        print(f"Missing required columns: {sorted(missing_cols)}")
        spark.stop()
        return

    print("=== Building AWS BTC features ===")

    window_7d = Window.orderBy("date").rowsBetween(-6, 0)
    window_lag = Window.orderBy("date")

    df_features = (
        df
        .withColumn("tx_count_7d_avg", F.round(F.avg("tx_count").over(window_7d), 0))
        .withColumn("tx_count_lag_1", F.lag("tx_count", 1).over(window_lag))
        .withColumn(
            "tx_count_daily_change_pct",
            F.when(
                F.col("tx_count_lag_1").isNull() | (F.col("tx_count_lag_1") == 0),
                None
            ).otherwise(
                F.round(
                    ((F.col("tx_count") - F.col("tx_count_lag_1")) / F.col("tx_count_lag_1")) * 100,
                    2
                )
            )
        )
        .withColumn(
            "is_anomaly",
            F.when(F.col("tx_count") > (F.col("tx_count_7d_avg") * 1.3), True).otherwise(False)
        )
    )

    print("=== Feature preview ===")
    df_features.select(
        "date",
        "tx_count",
        "tx_count_lag_1",
        "tx_count_daily_change_pct",
        "tx_count_7d_avg",
        "is_anomaly"
    ).show(50, truncate=False)

    print(f"=== Saving AWS BTC features to {output_path} ===")
    df_features.write.mode("overwrite").parquet(output_path)
    print("Save completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()