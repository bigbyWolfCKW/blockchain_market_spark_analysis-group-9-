# 📈 blockchain & market data Spark analysis

## 📝 Project Overview
This is an end-to-end Data Engineering and Machine Learning pipeline designed for high-throughput financial data. It features a decoupled architecture with two independent data producers: an Apache Spark pipeline for blockchain on-chain data, and an asynchronous Python streaming engine for real-time market data (Crypto & Stocks). 

Both pipelines feed into a centralized PostgreSQL database, serving as the single source of truth for a real-time Grafana monitoring dashboard.

##  Architecture
* **Pipeline 1:** `Apache Spark` / `PySpark` for batch processing historical blockchain data, ETL, and training a Random Forest Machine Learning model to predict BTC price movements.
* **Pipeline 2:** `asyncio` & `WebSockets` (Binance API for Crypto, Alpaca API for US Stocks) for real-time market data ingestion.
* **Storage & UI:** `PostgreSQL` (Optimized with time-series indexing) + `Grafana` (Real-time tracking and ML prediction overlay).

## 🗂️ Project Structure
This repository contains the **Spark Blockchain & ML Pipeline **, organized with a clean orchestrator pattern:

```text
BLOCKCHAIN/
│
├── 01_spark_blockchain_pipeline/
│   ├── 01_data_sources/
│   │   ├── load_aws_btc_transactions.py
│   │   └── load_local_blocks_csv.py
│   │
│   ├── 02_processing_etl/
│   │   ├── build_aws_btc_features.py
│   │   ├── build_aws_btc_featuresEnhance.py
│   │   ├── build_daily_summary_from_aws.py
│   │   └── build_daily_summary_from_local.py
│   │
│   ├── 03_data_integration/
│   │   ├── blockchain_metrics_to_postgres.py
│   │   └── fuse_blockchain_market_postgres.py
│   │
│   ├── 04_ml_and_analytics/
│   │   └── train_btc_model.py
│   │
│   ├── main_run.py
│   └── readme.md
│
├── data/
├── .env
└── .gitignore
```
## How to Run

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd BLOCKCHAIN
```
### 2. Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```
### 3. Install dependencies
```bash
pip install -r requirements.txt
```
### 4. Set environment variables
Create a .env file in the project root:
```bash
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=org.postgresql.Driver
```
### 5. Run the pipeline
```bash
python 01_spark_blockchain_pipeline/main_run.py --days 30
```
