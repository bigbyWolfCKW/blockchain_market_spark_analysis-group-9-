from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
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
    final_df = final_df.withColumn(
        "is_anomaly_numeric",
        F.when(F.col("is_anomaly") == True, 1.0).otherwise(0.0)
    )
    final_df.show(5, truncate=False)

    # Candidate feature columns based on your new blockchain feature pipeline
    candidate_features = [
        "tx_count",
        "tx_count_7d_avg",
        "tx_count_lag_1",
        "tx_count_daily_change_pct",
        "tx_count_7d_zscore",
        "is_anomaly_numeric",
    ]

    target_column = ['daily_return_t+1']

    logger.info(f"=== Using target column: {candidate_features} ===")
    logger.info(f"=== Using feature columns: {target_column} ===")

    ml_dataset = final_df.select(candidate_features+target_column).dropna()

    assembler = VectorAssembler(
        inputCols=candidate_features,
        outputCol="features"
    )
    ml_dataset_transform = assembler.transform(ml_dataset)

    logger.info("=== ML dataset preview ===")
    ml_dataset_transform.show(10, truncate=False)

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features",
                            withStd=True, withMean=False)
    scaler_model = scaler.fit(ml_dataset_transform)
    ml_dataset_trasform_scaled = scaler_model.transform(ml_dataset_transform)
    train_df = ml_dataset_trasform_scaled.limit(int(ml_dataset_trasform_scaled.count() * 0.8))
    test_df = ml_dataset_trasform_scaled.subtract(train_df)

    regression = LinearRegression(featuresCol="scaled_features", labelCol=target_column[0])
    lr_model = regression.fit(train_df)

    predictions = lr_model.transform(test_df)
    evaluator = RegressionEvaluator(labelCol=target_column[0], predictionCol="prediction", metricName="r2")
    logger.info(f"R-Squared on Test Data: {evaluator.evaluate(predictions):.4f}")

    # train_data, test_data = ml_dataset_trasform.randomSplit([0.8, 0.2], seed=42)
    # logger.info("=== Training Random Forest regressor ===")
    # Regressor = RandomForestRegressor(
    #     featuresCol="features",
    #     labelCol=target_column[0],
    #     numTrees=20,
    #     seed=42
    # )
    # model = Regressor.fit(train_data)
    # predictions = model.transform(test_data)
    # logger.info("=== Model training completed successfully. ===")
    #
    # logger.info("=== Prediction preview ===")
    # preview_cols = [target_column[0], "prediction"]
    # predictions.select(preview_cols).show(10, truncate=False)
    # evaluator = RegressionEvaluator(
    #     labelCol=target_column[0],
    #     predictionCol="prediction",
    #     metricName="rmse"
    # )
    # rmse = evaluator.evaluate(predictions)
    # logger.info(f"Test RMSE: ${rmse:.2f}")

    spark.stop()
    return

if __name__ == "__main__":
    main()