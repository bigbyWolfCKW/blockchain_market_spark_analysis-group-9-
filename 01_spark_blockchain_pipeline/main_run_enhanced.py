import argparse
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def run_command(name: str, command: list[str]) -> None:
    print("\n" + "=" * 80)
    print(f"Running step: {name}")
    print("Command:", " ".join(command))
    print("=" * 80)

    result = subprocess.run(command, cwd=BASE_DIR)

    if result.returncode != 0:
        print(f"ERROR: Step failed: {name}")
        sys.exit(result.returncode)

    print(f"Completed step: {name}")


def run_python_step(name: str, relative_path: str, extra_args: list[str] | None = None) -> None:
    extra_args = extra_args or []
    script_path = BASE_DIR / relative_path

    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        sys.exit(1)

    run_command(name, [sys.executable, str(script_path)] + extra_args)


def run_spark_cluster_step(name: str, app_path: str, extra_args: list[str] | None = None) -> None:
    extra_args = extra_args or []

    command = [
        "docker", "exec", "-i", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        app_path,
    ] + extra_args

    run_command(name, command)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run enhanced hourly Spark volatility risk pipeline"
    )

    parser.add_argument("--skip-docker-up", action="store_true")
    parser.add_argument("--skip-market-parquet", action="store_true")
    parser.add_argument("--skip-ml-dataset", action="store_true")
    parser.add_argument("--skip-classifier", action="store_true")
    parser.add_argument("--skip-realized-vol", action="store_true")
    parser.add_argument("--skip-upload-classifier", action="store_true")
    parser.add_argument("--skip-upload-realized-vol", action="store_true")

    parser.add_argument(
        "--full-rebuild-blockchain",
        action="store_true",
        help="Run full 3-year AWS blockchain hourly aggregation on Spark cluster. This may take over 1 hour."
    )

    args = parser.parse_args()

    if not args.skip_docker_up:
        run_command(
            "Start Spark Standalone Cluster",
            ["docker", "compose", "-f", "docker-compose.spark.yml", "up", "-d"],
        )

    if args.full_rebuild_blockchain:
        run_spark_cluster_step(
            "Build 3-Year Hourly Blockchain Features",
            "/app/01_spark_blockchain_pipeline/02_processing_etl/build_hourly_summary_direct_from_aws.py",
            [
                "--start-date", "2023-04-20",
                "--days", "1095",
                "--output-path", "/app/output/hourly_features_3y_cluster",
            ],
        )
    else:
        print("Skipping full blockchain rebuild. Using prepared output/hourly_features_3y_cluster.")

    if not args.skip_market_parquet:
        run_spark_cluster_step(
            "Build Binance Hourly Market Parquet",
            "/app/02_spark_market_data_pipeline/02_processing_etl/build_binance_hourly_parquet.py",
            [
                "--input-path", "/app/data/binance/BTCUSDT/1h/raw_csv",
                "--output-path", "/app/output/market_btcusdt_1h_3y",
                "--symbol", "BTCUSDT",
            ],
        )

    if not args.skip_ml_dataset:
        run_spark_cluster_step(
            "Build Hourly ML Dataset",
            "/app/01_spark_blockchain_pipeline/04_ml_and_analytics/build_hourly_ml_dataset.py",
            [
                "--blockchain-path", "/app/output/hourly_features_3y_cluster",
                "--market-path", "/app/output/market_btcusdt_1h_3y",
                "--output-path", "/app/output/hourly_ml_dataset_3y_v2",
                "--volatility-quantile", "0.75",
            ],
        )

    if not args.skip_classifier:
        run_spark_cluster_step(
            "Train Hourly Volatility Classifiers",
            "/app/01_spark_blockchain_pipeline/04_ml_and_analytics/train_hourly_classifiers.py",
            [
                "--input-path", "/app/output/hourly_ml_dataset_3y_v2",
                "--output-path", "/app/output/hourly_classifier_results_v3",
            ],
        )

    if not args.skip_realized_vol:
        run_spark_cluster_step(
            "Train Hourly Realized Volatility Regressors",
            "/app/01_spark_blockchain_pipeline/04_ml_and_analytics/train_hourly_realized_volatility_regressors.py",
            [
                "--input-path", "/app/output/hourly_ml_dataset_3y_v2",
                "--output-path", "/app/output/hourly_realized_volatility_regression_results",
            ],
        )

    if not args.skip_upload_classifier:
        run_command(
            "Upload Classification Predictions to PostgreSQL",
            [
                "spark-submit",
                "--packages", "org.postgresql:postgresql:42.7.3",
                "01_spark_blockchain_pipeline/03_data_integration/upload_hourly_volatility_predictions.py",
            ],
        )

    if not args.skip_upload_realized_vol:
        run_command(
            "Upload Realized Volatility Predictions to PostgreSQL",
            [
                "spark-submit",
                "--packages", "org.postgresql:postgresql:42.7.3",
                "01_spark_blockchain_pipeline/03_data_integration/upload_hourly_realized_volatility_predictions.py",
                "--model-name", "GBTRegressor",
            ],
        )

    print("\n" + "=" * 80)
    print("Enhanced hourly Spark volatility risk pipeline completed successfully.")
    print("=" * 80)


if __name__ == "__main__":
    main()