import argparse
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", default="GBTRegressor")
    parser.add_argument("--input-base-path", default="output/hourly_realized_volatility_regression_results")
    parser.add_argument("--table", default="ml_btc_hourly_realized_volatility_predictions")
    args = parser.parse_args()

    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("upload-hourly-realized-volatility-predictions")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_driver = os.getenv("DB_DRIVER", "org.postgresql.Driver")

    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Missing one or more required DB environment variables.")

    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    input_path = f"{args.input_base_path}/next_24h_realized_volatility_{args.model_name}_predictions"
    print(f"=== Reading {input_path} ===")

    result_df = (
        spark.read.parquet(input_path)
        .withColumn("model_name", F.lit(args.model_name))
        .withColumn("created_at", F.current_timestamp())
        .orderBy("hour_bucket")
    )

    result_df.show(20, truncate=False)

    print(f"=== Writing to PostgreSQL table: {args.table} ===")
    result_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", args.table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", db_driver) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print("Upload completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
