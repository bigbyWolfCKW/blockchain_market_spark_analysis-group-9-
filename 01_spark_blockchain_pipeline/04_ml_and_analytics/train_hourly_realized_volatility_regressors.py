import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


FEATURE_COLS = [
    "tx_count", "avg_tx_size", "max_tx_size", "avg_fee", "max_fee",
    "total_fee", "avg_input_value", "total_input_value",
    "avg_output_value", "total_output_value",
    "tx_count_24h_avg", "tx_count_24h_zscore",
    "total_fee_24h_avg", "total_fee_24h_zscore",
    "total_input_value_24h_avg", "total_input_value_24h_zscore",
    "market_volume", "market_quote_volume", "market_trade_count",
    "is_anomaly_numeric",
    "market_return_1h", "market_return_6h", "market_return_24h",
    "market_abs_return_1h", "market_abs_return_6h", "market_abs_return_24h",
    "market_volume_24h_avg", "market_trade_count_24h_avg",
]


def add_realized_volatility_target(df):
    window_by_time = Window.orderBy("hour_bucket")

    out = df
    for k in range(1, 25):
        out = out.withColumn(f"future_return_{k}h", F.lead("market_return_1h", k).over(window_by_time))

    squared_sum = None
    for k in range(1, 25):
        term = F.pow(F.col(f"future_return_{k}h"), 2)
        squared_sum = term if squared_sum is None else squared_sum + term

    out = out.withColumn("next_24h_realized_volatility", F.sqrt(squared_sum))
    return out.drop(*[f"future_return_{k}h" for k in range(1, 25)])


def evaluate(predictions):
    return {
        "rmse": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(predictions),
        "mae": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae").evaluate(predictions),
        "r2": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2").evaluate(predictions),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default="output/hourly_ml_dataset_3y_v2")
    parser.add_argument("--output-path", default="output/hourly_realized_volatility_regression_results")
    parser.add_argument("--risk-quantile", type=float, default=0.75)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("train-hourly-realized-volatility-regressors").getOrCreate()

    df = (
        spark.read.parquet(args.input_path)
        .withColumn("is_anomaly_numeric", F.when(F.col("is_anomaly") == True, 1.0).otherwise(0.0))
    )
    df = add_realized_volatility_target(df)

    required = FEATURE_COLS + ["hour_bucket", "market_close", "next_24h_realized_volatility"]
    clean_df = df.select(*required).dropna()

    window_by_time = Window.orderBy("hour_bucket")
    indexed_df = clean_df.withColumn("row_num", F.row_number().over(window_by_time))

    total_rows = indexed_df.count()
    split_row = int(total_rows * 0.8)

    train_df = indexed_df.where(F.col("row_num") <= split_row)
    test_df = indexed_df.where(F.col("row_num") > split_row)

    risk_threshold = train_df.approxQuantile("next_24h_realized_volatility", [args.risk_quantile], 0.001)[0]

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")

    models = [
        ("RandomForestRegressor", RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=120, maxDepth=8, seed=42)),
        ("GBTRegressor", GBTRegressor(featuresCol="features", labelCol="label", maxIter=80, maxDepth=5, seed=42)),
    ]

    results = []

    for model_name, regressor in models:
        print(f"=== Training {model_name} ===")

        train_ml = train_df.withColumnRenamed("next_24h_realized_volatility", "label")
        test_ml = test_df.withColumnRenamed("next_24h_realized_volatility", "label")

        pipeline = Pipeline(stages=[assembler, regressor])
        model = pipeline.fit(train_ml)
        predictions = model.transform(test_ml)

        metrics = evaluate(predictions)

        pred_out = (
            predictions
            .withColumn("actual_realized_volatility_pct", F.col("label") * 100.0)
            .withColumn("predicted_realized_volatility_pct", F.col("prediction") * 100.0)
            .withColumn("high_volatility_threshold", F.lit(risk_threshold))
            .withColumn("actual_high_volatility", F.when(F.col("label") >= risk_threshold, 1.0).otherwise(0.0))
            .withColumn("predicted_high_risk", F.when(F.col("prediction") >= risk_threshold, 1.0).otherwise(0.0))
            .select(
                "hour_bucket",
                "market_close",
                F.col("label").alias("actual_realized_volatility"),
                F.col("prediction").alias("predicted_realized_volatility"),
                "actual_realized_volatility_pct",
                "predicted_realized_volatility_pct",
                "high_volatility_threshold",
                "actual_high_volatility",
                "predicted_high_risk",
            )
        )

        pred_path = f"{args.output_path}/next_24h_realized_volatility_{model_name}_predictions"
        pred_out.write.mode("overwrite").parquet(pred_path)

        result = {
            "model": model_name,
            "label": "next_24h_realized_volatility",
            "train_rows": train_df.count(),
            "test_rows": test_df.count(),
            "feature_count": len(FEATURE_COLS),
            "risk_threshold": risk_threshold,
            **metrics,
        }
        results.append(result)
        print(result)

    results_df = spark.createDataFrame(results)
    results_df.orderBy("rmse").show(truncate=False)
    results_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.output_path}/summary")

    spark.stop()


if __name__ == "__main__":
    main()
