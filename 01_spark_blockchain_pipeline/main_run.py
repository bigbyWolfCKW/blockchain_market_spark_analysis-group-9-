from datetime import datetime, timedelta
import argparse
from pyspark.sql import SparkSession


def make_date_paths(base: str, start_date: str, num_days: int) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    paths = []

    for i in range(num_days):
        d = start + timedelta(days=i)
        day_str = d.strftime("%Y-%m-%d")
        paths.append(f"{base}date={day_str}/")

    return paths


def main():
    parser = argparse.ArgumentParser(description="Load AWS BTC raw transactions")
    parser.add_argument("--start-date", type=str, default="2026-01-01")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("load-aws-btc-transactions")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.720"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        )
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    base_s3_path = "s3a://aws-public-blockchain/v1.0/btc/transactions/"
    output_path = "data/raw/aws_btc_transactions"

    print(f"=== Reading AWS BTC transactions from {args.start_date} for {args.days} days ===")
    paths = make_date_paths(base_s3_path, args.start_date, args.days)

    try:
        df = spark.read.parquet(*paths)
    except Exception as e:
        print(f"Failed to read AWS BTC data: {e}")
        spark.stop()
        return

    print("=== Raw AWS transaction sample ===")
    df.show(10, truncate=False)

    print(f"=== Saving raw AWS transactions to {output_path} ===")
    df.write.mode("overwrite").parquet(output_path)
    print("Save completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()