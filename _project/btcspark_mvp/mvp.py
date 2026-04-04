# mvp.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg

spark = (SparkSession.builder
         .appName("btcspark-mvp")
         .getOrCreate())

# 假設 blocks.csv 欄位包含：time(Unix timestamp 或 ISO), height, n_tx
blocks = (spark.read.option("header", True)
          .option("inferSchema", True)
          .csv("data/blocks.csv"))

# 如果 time 係 ISO 直接 to_date；如果係 unix seconds 你要改成 from_unixtime
blocks = blocks.withColumn("day", to_date(col("time")))

daily = (blocks.groupBy("day")
         .agg(count("*").alias("blocks"),
              avg(col("n_tx")).alias("avg_tx_per_block"))
         .orderBy("day"))

daily.show(30, truncate=False)
daily.coalesce(1).write.mode("overwrite").parquet("out/daily.parquet")
daily.coalesce(1).write.mode("overwrite").option("header", True).csv("out/daily_csv")

spark.stop()