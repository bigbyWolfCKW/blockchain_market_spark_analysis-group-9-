from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from db import raw_ticks_col, clusters_col, predictions_col

spark = SparkSession.builder.appName("MiniStockStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 5003)
    .load()
)

parts = split(raw["value"], ",")

parsed = raw.select(
    parts.getItem(0).alias("time"),
    parts.getItem(1).alias("stock_code"),
    parts.getItem(2).cast("double").alias("price"),
    parts.getItem(3).cast("double").alias("volume"),
    parts.getItem(4).cast("double").alias("amount"),
)

def process_batch(df, batch_id):
    pdf = df.toPandas()
    if pdf.empty:
        return

    rows = pdf.to_dict(orient="records")

    raw_ticks_col.insert_many(rows)

    cluster_docs = []
    prediction_docs = []

    for row in rows:
        stock_code = row["stock_code"]
        price = row["price"]

        cluster_label = 1 if price > 100 else 0
        predicted_price = round(price * 1.001, 4)

        cluster_docs.append({
            "time": row["time"],
            "stock_code": stock_code,
            "cluster": cluster_label
        })

        prediction_docs.append({
            "time": row["time"],
            "stock_code": stock_code,
            "predicted_price": predicted_price
        })

    if cluster_docs:
        clusters_col.insert_many(cluster_docs)

    if prediction_docs:
        predictions_col.insert_many(prediction_docs)

    print(f"Batch {batch_id} inserted into MongoDB")

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .start()
)

query.awaitTermination()