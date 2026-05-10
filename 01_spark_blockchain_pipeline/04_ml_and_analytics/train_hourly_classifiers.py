import argparse
from typing import Iterable

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


FEATURE_COLS = [
    "tx_count",
    "avg_tx_size",
    "max_tx_size",
    "avg_fee",
    "max_fee",
    "total_fee",
    "avg_input_value",
    "total_input_value",
    "avg_output_value",
    "total_output_value",
    "tx_count_24h_avg",
    "tx_count_24h_zscore",
    "total_fee_24h_avg",
    "total_fee_24h_zscore",
    "total_input_value_24h_avg",
    "total_input_value_24h_zscore",
    "market_volume",
    "market_quote_volume",
    "market_trade_count",
    "is_anomaly_numeric",
    "market_return_1h",
    "market_return_6h",
    "market_return_24h",
    "market_abs_return_1h",
    "market_abs_return_6h",
    "market_abs_return_24h",
    "market_volume_24h_avg",
    "market_trade_count_24h_avg",

]


def add_row_index(df):
    window_by_time = Window.orderBy("hour_bucket")
    return df.withColumn("row_num", F.row_number().over(window_by_time))


def evaluate_predictions(predictions, label_col: str) -> dict[str, float]:
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="accuracy",
    )
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="f1",
    )
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )

    return {
        "accuracy": accuracy_evaluator.evaluate(predictions),
        "f1": f1_evaluator.evaluate(predictions),
        "auc": auc_evaluator.evaluate(predictions),
    }

def positive_class_metrics(predictions, label_col: str) -> dict[str, float]:
    counts = (
        predictions
        .select(
            F.col(label_col).cast("double").alias("label"),
            F.col("prediction").cast("double").alias("prediction"),
        )
        .groupBy("label", "prediction")
        .count()
        .collect()
    )

    matrix = {(row["label"], row["prediction"]): row["count"] for row in counts}

    tp = matrix.get((1.0, 1.0), 0)
    fp = matrix.get((0.0, 1.0), 0)
    tn = matrix.get((0.0, 0.0), 0)
    fn = matrix.get((1.0, 0.0), 0)

    precision_positive = tp / (tp + fp) if (tp + fp) else 0.0
    recall_positive = tp / (tp + fn) if (tp + fn) else 0.0
    f1_positive = (
        2 * precision_positive * recall_positive / (precision_positive + recall_positive)
        if (precision_positive + recall_positive)
        else 0.0
    )

    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision_positive": precision_positive,
        "recall_positive": recall_positive,
        "f1_positive": f1_positive,
    }


def majority_baseline(train_df, test_df, label_col: str) -> float:
    majority_label = (
        train_df.groupBy(label_col)
        .count()
        .orderBy(F.desc("count"))
        .first()[label_col]
    )

    baseline_df = test_df.withColumn("baseline_prediction", F.lit(float(majority_label)))

    correct = baseline_df.where(F.col(label_col) == F.col("baseline_prediction")).count()
    total = baseline_df.count()

    return correct / total if total else 0.0


def build_models(label_col: str):
    return [
        (
            "LogisticRegression",
            LogisticRegression(
                featuresCol="scaled_features",
                labelCol=label_col,
                maxIter=50,
                regParam=0.01,
                elasticNetParam=0.0,
            ),
        ),
        (
            "RandomForestClassifier",
            RandomForestClassifier(
                featuresCol="features",
                labelCol=label_col,
                numTrees=80,
                maxDepth=6,
                seed=42,
            ),
        ),
        (
            "GBTClassifier",
            GBTClassifier(
                featuresCol="features",
                labelCol=label_col,
                maxIter=50,
                maxDepth=4,
                seed=42,
            ),
        ),
    ]


def train_for_label(df, label_col: str, output_path: str) -> list[dict]:
    required_cols = FEATURE_COLS + [label_col, "hour_bucket", "market_close"]

    clean_df = (
        df
        .select(*required_cols)
        .dropna(subset=FEATURE_COLS + [label_col])
    )

    indexed_df = add_row_index(clean_df)
    total_rows = indexed_df.count()
    split_row = int(total_rows * 0.8)

    train_df = indexed_df.where(F.col("row_num") <= split_row)
    test_df = indexed_df.where(F.col("row_num") > split_row)

    baseline_accuracy = majority_baseline(train_df, test_df, label_col)

    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features",
        handleInvalid="skip",
    )

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=False,
    )

    results = []

    for model_name, classifier in build_models(label_col):
        print(f"=== Training {model_name} for {label_col} ===")

        stages = [assembler]

        if model_name == "LogisticRegression":
            stages.append(scaler)

        stages.append(classifier)

        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        metrics = evaluate_predictions(predictions, label_col)
        positive_metrics = positive_class_metrics(predictions, label_col)

        result = {
            "label": label_col,
            "model": model_name,
            "train_rows": train_df.count(),
            "test_rows": test_df.count(),
            "feature_count": len(FEATURE_COLS),
            "positive_rate_train": train_df.agg(F.avg(label_col)).first()[0],
            "positive_rate_test": test_df.agg(F.avg(label_col)).first()[0],
            "baseline_accuracy": baseline_accuracy,
            **metrics,
            **positive_metrics,
        }
        results.append(result)

        print(result)

        prediction_output = f"{output_path}/{label_col}_{model_name}_predictions"
        (
            predictions
            .select(
                "hour_bucket",
                "market_close",
                F.col(label_col).alias("label"),
                "prediction",
                "probability",
            )
            .write
            .mode("overwrite")
            .parquet(prediction_output)
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train hourly BTC direction and volatility classifiers."
    )
    parser.add_argument(
        "--input-path",
        default="output/hourly_ml_dataset_3y",
    )
    parser.add_argument(
        "--output-path",
        default="output/hourly_classifier_results",
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("train-hourly-btc-classifiers")
        .getOrCreate()
    )

    df = (
        spark.read.parquet(args.input_path)
        .withColumn(
            "is_anomaly_numeric",
            F.when(F.col("is_anomaly") == True, F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    )

    all_results = []
    for label_col in ["next_24h_direction", "next_24h_volatility_spike"]:
        all_results.extend(train_for_label(df, label_col, args.output_path))

    results_df = spark.createDataFrame(all_results)

    print("=== Classification results ===")
    results_df.orderBy("label", F.desc("auc")).show(truncate=False)

    (
        results_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{args.output_path}/summary")
    )

    print(f"=== Results written to {args.output_path} ===")
    spark.stop()


if __name__ == "__main__":
    main()
