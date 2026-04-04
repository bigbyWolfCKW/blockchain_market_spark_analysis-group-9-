from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg, sum as Fsum, stddev_pop, desc

spark = SparkSession.builder.appName("btcspark-mvp-plus").getOrCreate()

blocks = (spark.read.option("header", True)
          .option("inferSchema", True)
          .csv("data/blocks.csv"))

blocks = blocks.withColumn("day", to_date(col("time")))

daily = (blocks.groupBy("day")
         .agg(count("*").alias("blocks"),
              avg(col("n_tx")).alias("avg_tx_per_block"),
              Fsum(col("n_tx")).alias("tx_total"),
              stddev_pop(col("n_tx")).alias("std_tx_per_block"))
         .orderBy("day"))

daily.show(50, truncate=False)

daily.coalesce(1).write.mode("overwrite").parquet("out/daily_plus.parquet")
daily.coalesce(1).write.mode("overwrite").option("header", True).csv("out/daily_plus_csv")

# Top blocks by n_tx (simple "anomaly-ish" view)
top_blocks = (blocks.select("day", "height", "n_tx")
              .orderBy(desc("n_tx"))
              .limit(20))

top_blocks.show(20, truncate=False)
top_blocks.coalesce(1).write.mode("overwrite").option("header", True).csv("out/top_blocks_csv")

spark.stop()