import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("fuse-blockchain-market-postgres")
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
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": db_driver
    }

    blockchain_table = "live_blockchain_metrics"
    market_table = "crypto_summary"
    target_table = "blockchain_market_fusion"

    print(f"=== Reading PostgreSQL table: {blockchain_table} ===")
    chain_df = spark.read.jdbc(
        url=db_url,
        table=blockchain_table,
        properties=db_properties
    )

    print(f"=== Reading PostgreSQL table: {market_table} ===")
    market_df = spark.read.jdbc(
        url=db_url,
        table=market_table,
        properties=db_properties
    )

    # Build a common join key from both tables
    # Assumption:
    # - blockchain table uses time_bucket
    # - market table uses date
    chain_df = chain_df.withColumn(
        "join_date",
        F.date_format("time_bucket", "yyyy-MM-dd")
    )

    market_df = market_df.withColumn(
        "join_date",
        F.date_format("date", "yyyy-MM-dd")
    )

    # Optional: keep only BTC rows if the market table contains many symbols
    if "symbol" in market_df.columns:
        market_df = market_df.filter(F.col("symbol") == "BTC")

    print("=== Running blockchain + market fusion join ===")
    fusion_df = chain_df.join(
        market_df,
        on="join_date",
        how="inner"
    )

    print("=== Fusion preview ===")
    fusion_df.show(10, truncate=False)

    print(f"=== Writing fused data to PostgreSQL table: {target_table} ===")
    try:
        fusion_df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", target_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", db_driver) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        print("Fusion completed successfully.")

    except Exception as e:
        print(f"Write failed: {e}")

    spark.stop()


if __name__ == "__main__":
    main()