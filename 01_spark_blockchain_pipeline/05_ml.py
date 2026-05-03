from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from loguru import logger
from pathlib import Path

DATA_FOLDER = Path("./data")
OUTPUT_FOLDER = Path("./output")

def main():
    logger.info("=== Building Daily Features and BTCUSDT data ===")
    spark = (
        SparkSession.builder
        .appName("load-aws-btc-transactions")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    logger.info(f"=== Reading Daily Features from {OUTPUT_FOLDER} ===")
    df_features = spark.read.option("basePath", str(OUTPUT_FOLDER)).parquet(str(Path(OUTPUT_FOLDER, "daily_features")))

    logger.info(f"=== Reading BTCUSDT from {OUTPUT_FOLDER} ===")
    df_btcusdt = spark.read.option("basePath", str(OUTPUT_FOLDER)).parquet(str(Path(OUTPUT_FOLDER, "BTCUSDT")))

    final_df = df_features.join(df_btcusdt, on="date", how="left")
    final_df.show(50, truncate=False)
    spark.stop()
    return

if __name__ == "__main__":
    main()