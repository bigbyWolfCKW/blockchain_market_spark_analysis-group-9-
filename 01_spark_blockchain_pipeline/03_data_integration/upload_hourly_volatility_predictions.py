import os

from dotenv import load_dotenv
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("upload-hourly-volatility-predictions")
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

    input_path = (
        "output/hourly_classifier_results_v3/"
        "next_24h_volatility_spike_GBTClassifier_predictions"
    )
    prediction_table = "ml_btc_hourly_volatility_predictions"

    print(f"=== Reading predictions from {input_path} ===")
    predictions = spark.read.parquet(input_path)

    result_df = (
        predictions
        .withColumn("spike_probability", vector_to_array("probability")[1])
        .select(
            F.col("hour_bucket"),
            F.col("market_close"),
            F.col("label").alias("actual_volatility_spike"),
            F.col("prediction").alias("predicted_volatility_spike"),
            F.col("spike_probability"),
        )
        .withColumn("model_name", F.lit("GBTClassifier"))
        .withColumn("created_at", F.current_timestamp())
        .orderBy("hour_bucket")
    )

    print("=== Upload preview ===")
    result_df.show(20, truncate=False)

    print(f"=== Writing to PostgreSQL table: {prediction_table} ===")
    try:
        result_df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", prediction_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", db_driver) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

        print("Prediction results written successfully.")

    except Exception as e:
        print(f"Write failed: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
