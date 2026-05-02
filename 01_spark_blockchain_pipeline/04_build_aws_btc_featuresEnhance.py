from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path
from loguru import logger
import pandas as pd

DATA_FOLDER = Path("./data")
OUTPUT_FOLDER = Path("./output")

def main():
    spark = (
        SparkSession.builder
        .appName("build-aws-btc-features")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")


    logger.info(f"=== Reading AWS daily summary from {DATA_FOLDER} ===")
    df = spark.read.option("basePath", str(OUTPUT_FOLDER)).parquet(str(Path(OUTPUT_FOLDER, "daily_summary")))


    logger.info("=== Building richer AWS BTC features ===")

    window_lag = Window.orderBy("date")
    window_3d = Window.orderBy("date").rowsBetween(-2, 0)
    window_7d = Window.orderBy("date").rowsBetween(-6, 0)
    window_14d = Window.orderBy("date").rowsBetween(-13, 0)
    window_30d = Window.orderBy("date").rowsBetween(-29, 0)

    df_features = (
        df
        # Lags
        .withColumn("tx_count_lag_1", F.lag("tx_count", 1).over(window_lag))
        .withColumn("tx_count_lag_7", F.lag("tx_count", 7).over(window_lag))

        # Rolling averages
        .withColumn("tx_count_3d_avg", F.round(F.avg("tx_count").over(window_3d), 2))
        .withColumn("tx_count_7d_avg", F.round(F.avg("tx_count").over(window_7d), 2))
        .withColumn("tx_count_14d_avg", F.round(F.avg("tx_count").over(window_14d), 2))
        .withColumn("tx_count_30d_avg", F.round(F.avg("tx_count").over(window_30d), 2))

        # Rolling std
        .withColumn("tx_count_7d_std", F.round(F.stddev("tx_count").over(window_7d), 2))

        # Absolute changes
        .withColumn("tx_count_change_1d", F.col("tx_count") - F.col("tx_count_lag_1"))
        .withColumn("tx_count_change_7d", F.col("tx_count") - F.col("tx_count_lag_7"))

        # Percentage changes
        .withColumn(
            "tx_count_daily_change_pct",
            F.when(
                F.col("tx_count_lag_1").isNull() | (F.col("tx_count_lag_1") == 0),
                None
            ).otherwise(
                F.round(
                    ((F.col("tx_count") - F.col("tx_count_lag_1")) / F.col("tx_count_lag_1")) * 100,
                    2
                )
            )
        )
        .withColumn(
            "tx_count_7d_change_pct",
            F.when(
                F.col("tx_count_lag_7").isNull() | (F.col("tx_count_lag_7") == 0),
                None
            ).otherwise(
                F.round(
                    ((F.col("tx_count") - F.col("tx_count_lag_7")) / F.col("tx_count_lag_7")) * 100,
                    2
                )
            )
        )

        # Ratio against moving average
        .withColumn(
            "tx_count_vs_7d_avg_ratio",
            F.when(
                F.col("tx_count_7d_avg").isNull() | (F.col("tx_count_7d_avg") == 0),
                None
            ).otherwise(
                F.round(F.col("tx_count") / F.col("tx_count_7d_avg"), 4)
            )
        )

        # Z-score
        .withColumn(
            "tx_count_7d_zscore",
            F.when(
                F.col("tx_count_7d_std").isNull() | (F.col("tx_count_7d_std") == 0),
                None
            ).otherwise(
                F.round(
                    (F.col("tx_count") - F.col("tx_count_7d_avg")) / F.col("tx_count_7d_std"),
                    2
                )
            )
        )

        # Calendar features
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("month", F.month("date"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False)
        )

        # Simple anomaly flags
        .withColumn(
            "is_high_spike",
            F.when(F.col("tx_count_vs_7d_avg_ratio") > 1.3, True).otherwise(False)
        )
        .withColumn(
            "is_low_drop",
            F.when(F.col("tx_count_vs_7d_avg_ratio") < 0.7, True).otherwise(False)
        )
        .withColumn(
            "is_anomaly",
            F.when(F.col("tx_count_7d_zscore") > 2.0, True)
             .when(F.col("tx_count_7d_zscore") < -2.0, True)
             .otherwise(False)
        )
    )

    logger.info("=== Feature preview ===")
    df_features.select(
        "date",
        "tx_count",
        "tx_count_lag_1",
        "tx_count_lag_7",
        "tx_count_3d_avg",
        "tx_count_7d_avg",
        "tx_count_7d_std",
        "tx_count_daily_change_pct",
        "tx_count_7d_change_pct",
        "tx_count_vs_7d_avg_ratio",
        "tx_count_7d_zscore",
        "day_of_week",
        "is_weekend",
        "is_high_spike",
        "is_low_drop",
        "is_anomaly"
    ).show(50, truncate=False)

    logger.info(f"=== Saving richer AWS BTC features to {OUTPUT_FOLDER} ===")
    output_path = Path(OUTPUT_FOLDER, "daily_features")
    df_features.write.mode("overwrite").parquet(str(output_path))
    logger.info(f"=== Saving pandas dataframe to {OUTPUT_FOLDER} ===")
    pandas_df = df.toPandas()
    pandas_df['date'] = pd.to_datetime(pandas_df['date'])
    pandas_df = pandas_df.sort_values('date')
    pandas_df.to_csv(Path(OUTPUT_FOLDER,"daily_features.csv"))
    logger.info("Save completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()