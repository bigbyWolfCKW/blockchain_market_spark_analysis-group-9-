from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("build-daily-summary-from-aws")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/raw/aws_btc_transactions"
    output_path = "data/processed/aws_btc_daily_summary"

    print(f"=== Reading raw AWS BTC transactions from {input_path} ===")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Failed to read input parquet: {e}")
        spark.stop()
        return

    if "date" not in df.columns:
        print("Error: 'date' column not found.")
        spark.stop()
        return

    print("=== Building AWS daily transaction summary ===")
    daily_summary = (
        df.groupBy("date")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.avg("fee"), 0).alias("avg_fee"),
            F.max("fee").alias("max_fee"),
            F.round(F.avg("size"), 0).alias("avg_tx_size"),
            F.max("size").alias("max_tx_size"),
            F.sum("input_value").alias("total_input_value")
        )
        .orderBy("date")
    )

    daily_summary.show(50, truncate=False)

    print(f"=== Saving AWS daily summary to {output_path} ===")
    daily_summary.write.mode("overwrite").parquet(output_path)
    print("Save completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()