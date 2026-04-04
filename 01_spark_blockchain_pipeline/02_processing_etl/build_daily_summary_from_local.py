from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as spark_sum, avg, max as spark_max, min as spark_min


def main():
    spark = (
        SparkSession.builder
        .appName("build-daily-summary-from-local")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/raw/blocks.csv"
    output_path = "data/processed/blockchain_daily_summary"

    print(f"=== Reading local block CSV from {input_path} ===")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    clean_df = (
        df
        .withColumn("event_time", col("event_time").cast("timestamp"))
        .withColumn("day", to_date(col("day")))
        .withColumn("height", col("height").cast("long"))
        .withColumn("n_tx", col("n_tx").cast("long"))
        .withColumn("size", col("size").cast("long"))
    )

    print("=== Building local daily blockchain summary ===")
    daily_df = clean_df.groupBy("day").agg(
        count("*").alias("daily_block_count"),
        spark_sum("n_tx").alias("daily_total_n_tx"),
        avg("n_tx").alias("daily_avg_n_tx"),
        spark_max("n_tx").alias("daily_max_n_tx"),
        avg("size").alias("daily_avg_block_size"),
        spark_min("height").alias("min_height"),
        spark_max("height").alias("max_height")
    ).orderBy("day")

    daily_df.show(50, truncate=False)

    print(f"=== Saving local daily summary to {output_path} ===")
    daily_df.write.mode("overwrite").parquet(output_path)
    print("Save completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()