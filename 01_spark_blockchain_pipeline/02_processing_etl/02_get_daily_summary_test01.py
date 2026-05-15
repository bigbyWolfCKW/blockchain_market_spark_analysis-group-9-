from pyspark.sql import SparkSession, functions as F
from loguru import logger
from pathlib import Path
import time

DATA_FOLDER = Path("./data")
OUTPUT_FOLDER = Path("./output")

def get_daily_summary():
    spark = (
        SparkSession.builder
        .appName("load-aws-btc-transactions")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    logger.info(f"=== Reading raw AWS BTC transactions from {DATA_FOLDER} ===")
    df = spark.read.option("basePath", str(DATA_FOLDER)).parquet(str(Path(DATA_FOLDER, f"chain=btc")))

    logger.info("=== Building AWS BTC daily transaction summary ===")
    daily_summary = (
        df.groupBy(F.col("s3_date").alias("date"))
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.avg("size"), 8).alias("avg_tx_size"),
            F.max("size").alias("max_tx_size"),
            F.round(F.avg("fee"), 8).alias("avg_fee"),
            F.max("fee").alias("max_fee"),
            F.sum("fee").alias("total_fee"),
            F.sum("input_value").alias("total_input_value")
        )
        .orderBy("date")
    )

    daily_summary_output = Path(OUTPUT_FOLDER, "daily_summary")
    logger.info(f"=== Saving AWS daily summary to {daily_summary_output} ===")
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    daily_summary.write.mode("overwrite").parquet(str(daily_summary_output))
    end = time.perf_counter()
    logger.info("Save completed successfully.")
    logger.info(f"Runtime: {end - start:.4f} seconds.")
    spark.stop()

if __name__ == "__main__":
    get_daily_summary()