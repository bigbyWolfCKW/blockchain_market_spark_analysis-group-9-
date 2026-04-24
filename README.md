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

### 1. Prerequisite
This repo is designed to be executed in Linux or WSL2 environment, with specific Java SDK, Apache Spark and python version.
```bash
Java JDK 21.0.10
Apache Spark 4.1.1
Python 3.11
```
### 2. Environment Setup
Install JDK 21
```bash
sudo apt update
sudo apt install openjdk-21-jdk -y
```
Install Spark 4.1.1
```bash
# 1. Create the target directory
mkdir -p $HOME/spark
# 2. Download spark and unzip into target directory
curl -sL "https://archive.apache.org/dist/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz" | tar -xz -C $HOME/spark --strip-components=1
```
Install UV (for environment management)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Install Python 3.11 via UV
```bash
uv python install 3.11
```
Update Linux environment path
```bash
# 1) open bashrc
vim ~/.bashrc

# 2) Add these lines at the end of the file

## Spark
export SPARK_HOME=$HOME/spark
export PATH=$SPARK_HOME/bin:$PATH
export PATH=$SPARK_HOME/sbin:$PATH

## Pyspark
## Replace \<repo folder> as the folder name of the repo, e.g blockchain
export PYSPARK_PYTHON=$HOME/<repo folder>/.venv/bin/python3
export PYSPARK_DRIVER_PYTHON=$HOME/<repo folder>/.venv/bin/python3

## UV cache
export UV_CACHE_DIR=$HOME/.cache/uv
```

### 3. Clone the repository into specific folder
This git command will clone the repo into a folder named as blockchain. You can freely change the name of folder.
```bash
git clone https://github.com/bigbyWolfCKW/blockchain_market_spark_analysis-group-9-.git blockchain
```

### 4. Create virtual environment and install python dependencies via UV
```bash
cd blockchain
uv sync
```
### 5. List of dependencies
```bash
"numpy>=2.4.4"
"pandas>=3.0.2"
"psycopg2-binary>=2.9.12"
"pyspark>=4.1.1"
"python-dotenv>=1.2.2"
"requests>=2.33.1"
"websocket-client>=1.9.0"
"websockets>=16.0"
```
### 6. Set environment variables
Create a .env file in the project root:
```bash
DB_HOST=your_host
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=org.postgresql.Driver
```
### 7. Run the pipeline
```bash
python 01_spark_blockchain_pipeline/main_run.py --days 30
```