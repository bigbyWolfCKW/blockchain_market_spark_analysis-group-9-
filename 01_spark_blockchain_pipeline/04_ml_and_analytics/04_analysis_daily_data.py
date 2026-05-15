from pyspark.sql import SparkSession, functions as F
from loguru import logger
from pathlib import Path
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

DATA_FOLDER = Path("./data")
OUTPUT_FOLDER = Path("./output")

def daily_analysis():
    spark = (
        SparkSession.builder
        .appName("load-aws-btc-transactions")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    logger.info(f"=== Reading Daily Summary from {OUTPUT_FOLDER} ===")
    df = spark.read.option("basePath", str(OUTPUT_FOLDER)).parquet(str(Path(OUTPUT_FOLDER, "daily_summary")))
    pandas_df = df.toPandas()
    pandas_df['date'] = pd.to_datetime(pandas_df['date'])
    pandas_df = pandas_df.sort_values('date')

    logger.info(f"=== Saving pandas dataframe to {OUTPUT_FOLDER} ===")
    pandas_df.to_csv(Path(OUTPUT_FOLDER,"daily_summary.csv"))

    # 2. Create the figure and the first axis (ax1)
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # --- PRIMARY AXIS: TX COUNT ---
    color1 = 'tab:blue'
    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('Transaction Count', color=color1, fontsize=12, fontweight='bold')
    ax1.plot(pandas_df['date'], pandas_df['tx_count'], color=color1, marker='o', linewidth=2, label='TX Count')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.grid(True, linestyle='--', alpha=0.5)  # Add a grid to the primary axis

    # --- SECONDARY AXIS: FEES ---
    # Instantiate a second axes that shares the same x-axis
    ax2 = ax1.twinx()

    color2 = 'tab:orange'
    ax2.set_ylabel('Total Fee (BTC)', color=color2, fontsize=12, fontweight='bold')
    ax2.plot(pandas_df['date'], pandas_df['total_fee'], color=color2, marker='s', linewidth=2, label='Total Fee')
    ax2.tick_params(axis='y', labelcolor=color2)

    # --- FINAL POLISHING ---
    plt.title('BTC Transaction Count and Fee', fontsize=16, pad=15)

    # Rotating date labels on the shared X-axis
    fig.autofmt_xdate()

    # Since we have two axes, a standard legend only shows one.
    # Here is the trick to combine them:
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    plt.tight_layout()
    plt.savefig(Path(OUTPUT_FOLDER, "daily_btc.png"), dpi=300)
    logger.info("Plot successfully saved as daily_btc.png")
    spark.stop()

if __name__ == "__main__":
    daily_analysis()