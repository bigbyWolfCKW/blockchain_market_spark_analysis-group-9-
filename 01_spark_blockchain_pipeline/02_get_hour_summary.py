from pyspark.sql import SparkSession, functions as F
from loguru import logger
from pathlib import Path

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

    logger.info("=== Building AWS BTC hour transaction summary ===")
    hour_summary = (
        df.withColumn("hour", F.hour("block_timestamp"))
        .withColumn("date", F.to_date("block_timestamp"))
        .groupBy("date", "hour")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.avg("size"), 8).alias("avg_tx_size"),
            F.max("size").alias("max_tx_size"),
            F.round(F.avg("fee"), 8).alias("avg_fee"),
            F.max("fee").alias("max_fee"),
            F.sum("fee").alias("total_fee"),
            F.sum("input_value").alias("total_input_value")
        )
        .orderBy("date","hour")
    )
    hour_summary.show(100, truncate=False)

    hour_summary_output = Path(OUTPUT_FOLDER, "hour_summary")
    logger.info(f"=== Saving AWS hour summary to {hour_summary_output} ===")
    hour_summary_output.mkdir(parents=True, exist_ok=True)
    hour_summary.write.mode("overwrite").parquet(str(hour_summary_output))
    logger.info("Save completed successfully.")
    spark.stop()

if __name__ == "__main__":
    get_daily_summary()