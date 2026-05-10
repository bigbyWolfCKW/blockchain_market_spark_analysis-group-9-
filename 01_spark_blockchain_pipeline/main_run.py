import argparse
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def run_step(name: str, relative_path: str, extra_args: list[str] | None = None):
    extra_args = extra_args or []
    script_path = BASE_DIR / relative_path

    print("\n" + "=" * 80)
    print(f"Running step: {name}")
    print(f"Script: {script_path}")
    print("=" * 80)

    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        sys.exit(1)

    command = [sys.executable, str(script_path)] + extra_args

    result = subprocess.run(
        command,
        cwd=BASE_DIR
    )

    if result.returncode != 0:
        print(f"ERROR: Step failed: {name}")
        sys.exit(result.returncode)

    print(f"Completed step: {name}")


def main():
    parser = argparse.ArgumentParser(
        description="Run legacy daily Spark blockchain pipeline"
    )

    parser.add_argument(
        "--skip-raw",
        action="store_true",
        help="Skip AWS raw BTC transaction loading"
    )

    parser.add_argument(
        "--skip-summary",
        action="store_true",
        help="Skip daily summary building"
    )

    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip enhanced feature engineering"
    )

    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip blockchain metrics backfill to PostgreSQL"
    )

    parser.add_argument(
        "--skip-fusion",
        action="store_true",
        help="Skip blockchain-market fusion"
    )

    parser.add_argument(
        "--skip-model",
        action="store_true",
        help="Skip BTC prediction model training"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-01-01",
        help="Start date for AWS BTC raw transaction loading"
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to load from AWS BTC public dataset"
    )

    args = parser.parse_args()

    if not args.skip_raw:
        run_step(
            name="Load AWS BTC Raw Transactions",
            relative_path="01_data_sources/load_aws_btc_transactions.py",
            extra_args=[
                "--start-date", args.start_date,
                "--days", str(args.days)
            ]
        )
    else:
        print("Skipping raw AWS BTC transaction loading.")

    if not args.skip_summary:
        run_step(
            name="Build AWS Daily Summary",
            relative_path="02_processing_etl/build_daily_summary_from_aws.py"
        )
    else:
        print("Skipping daily summary building.")

    if not args.skip_features:
        run_step(
            name="Build Enhanced AWS BTC Features",
            relative_path="02_processing_etl/build_aws_btc_featuresEnhance.py"
        )
    else:
        print("Skipping enhanced feature engineering.")

    if not args.skip_postgres:
        run_step(
            name="Backfill Blockchain Metrics to PostgreSQL",
            relative_path="03_data_integration/blockchain_metrics_to_postgres.py"
        )
    else:
        print("Skipping PostgreSQL blockchain metrics backfill.")

    if not args.skip_fusion:
        run_step(
            name="Fuse Blockchain and Market Data",
            relative_path="03_data_integration/fuse_blockchain_market_postgres.py"
        )
    else:
        print("Skipping blockchain-market fusion.")

    if not args.skip_model:
        run_step(
            name="Train BTC Prediction Model",
            relative_path="04_ml_and_analytics/train_btc_model.py"
        )
    else:
        print("Skipping BTC prediction model training.")

    print("\n" + "=" * 80)  
    print("Legacy daily Spark blockchain pipeline completed successfully.")
    print("=" * 80)


if __name__ == "__main__":
    main()