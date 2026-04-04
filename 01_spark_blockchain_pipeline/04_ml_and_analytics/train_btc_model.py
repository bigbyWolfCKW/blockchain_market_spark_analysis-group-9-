import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def main():
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("train-btc-model")
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

    source_table = "blockchain_market_fusion"
    prediction_table = "ml_btc_predictions"

    print(f"=== Reading PostgreSQL table: {source_table} ===")
    df = spark.read.jdbc(
        url=db_url,
        table=source_table,
        properties=db_properties
    )

    # Pick a target column depending on what exists in your market table
    price_target = None
    for candidate in ["btc_avg_price", "close", "Close", "adj_close", "Adj Close"]:
        if candidate in df.columns:
            price_target = candidate
            break

    if price_target is None:
        print("No supported target price column was found.")
        spark.stop()
        return

    # Candidate feature columns based on your new blockchain feature pipeline
    candidate_features = [
        "tx_count",
        "tx_count_7d_avg",
        "tx_count_lag_1",
        "tx_count_daily_change_pct",
        "is_anomaly"
    ]

    feature_cols = [c for c in candidate_features if c in df.columns]

    if len(feature_cols) < 2:
        print(f"Not enough usable feature columns found. Available: {feature_cols}")
        spark.stop()
        return

    print(f"=== Using target column: {price_target} ===")
    print(f"=== Using feature columns: {feature_cols} ===")

    # Convert boolean anomaly flag to numeric if needed
    if "is_anomaly" in feature_cols:
        df = df.withColumn(
            "is_anomaly_numeric",
            F.when(F.col("is_anomaly") == True, 1.0).otherwise(0.0)
        )
        feature_cols = [c for c in feature_cols if c != "is_anomaly"] + ["is_anomaly_numeric"]

    # Drop nulls only for required columns
    required_cols = feature_cols + [price_target]
    df_clean = df.select(*required_cols, *[c for c in ["time_bucket", "date", "join_date"] if c in df.columns]).dropna()

    total_rows = df_clean.count()
    if total_rows < 10:
        print(f"Not enough clean rows for training. Rows available: {total_rows}")
        spark.stop()
        return

    print(f"=== Clean training rows available: {total_rows} ===")
    df_clean.show(10, truncate=False)

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    # Keep one time column for output if available
    time_col = None
    for candidate in ["time_bucket", "date", "join_date"]:
        if candidate in df_clean.columns:
            time_col = candidate
            break

    select_cols = ["features", F.col(price_target).alias("label")]
    if time_col:
        ml_data = assembler.transform(df_clean).select(
            F.col(time_col).alias("event_time"),
            "features",
            F.col(price_target).alias("label")
        )
    else:
        ml_data = assembler.transform(df_clean).select(
            "features",
            F.col(price_target).alias("label")
        )

    print("=== ML dataset preview ===")
    ml_data.show(10, truncate=False)

    train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)

    test_count = test_data.count()
    if test_count == 0:
        print("Test split is empty. Try adding more data.")
        spark.stop()
        return

    print("=== Training Random Forest regressor ===")
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=20,
        seed=42
    )

    model = rf.fit(train_data)
    print("Model training completed successfully.")

    predictions = model.transform(test_data)

    print("=== Prediction preview ===")
    preview_cols = ["label", "prediction"]
    if "event_time" in predictions.columns:
        preview_cols = ["event_time"] + preview_cols
    predictions.select(*preview_cols).show(10, truncate=False)

    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Test RMSE: ${rmse:.2f}")

    if "event_time" in predictions.columns:
        result_df = predictions.select(
            F.col("event_time"),
            F.col("label").alias("actual_price"),
            F.col("prediction").alias("predicted_price")
        )
    else:
        result_df = predictions.select(
            F.col("label").alias("actual_price"),
            F.col("prediction").alias("predicted_price")
        )

    result_df = result_df.withColumn(
        "error_usd",
        F.abs(F.col("actual_price") - F.col("predicted_price"))
    ).withColumn(
        "accuracy_pct",
        100.0 - (F.col("error_usd") / F.col("actual_price") * 100.0)
    )

    print(f"=== Writing prediction results to PostgreSQL table: {prediction_table} ===")
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