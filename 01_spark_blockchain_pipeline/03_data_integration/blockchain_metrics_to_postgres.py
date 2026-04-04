import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


def main():
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("backfill-blockchain-metrics-to-postgres")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/processed/aws_btc_features"

    print(f"=== Reading AWS BTC features from {input_path} ===")
    try:
        df_hist = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Input parquet not found: {e}")
        spark.stop()
        return

    required_cols = {
        "date",
        "tx_count",
        "tx_count_7d_avg",
        "tx_count_daily_change_pct",
        "is_anomaly"
    }
    missing_cols = required_cols - set(df_hist.columns)
    if missing_cols:
        print(f"Missing required columns: {sorted(missing_cols)}")
        spark.stop()
        return

    df_to_upload = df_hist.select(
        F.col("date").cast(TimestampType()).alias("time_bucket"),
        F.col("tx_count"),
        F.col("tx_count_7d_avg"),
        F.col("tx_count_daily_change_pct"),
        F.col("is_anomaly")
    )

    print("=== Preview of blockchain metrics backfill data ===")
    df_to_upload.show(20, truncate=False)

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_driver = os.getenv("DB_DRIVER", "org.postgresql.Driver")

    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Missing one or more required DB environment variables.")

    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    print("=== Writing blockchain metrics to PostgreSQL table: live_blockchain_metrics ===")

    try:
        df_to_upload.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "live_blockchain_metrics") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", db_driver) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        print("Backfill completed successfully.")

    except Exception as e:
        print(f"Write failed: {e}")

    spark.stop()


if __name__ == "__main__":
    main()