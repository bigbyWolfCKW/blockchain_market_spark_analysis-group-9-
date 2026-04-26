from datetime import date, timedelta
import argparse
from pathlib import Path
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from loguru import logger

PATH_MAPPING = {
    "btc": "v1.0/btc/transactions/",
    "eth": "v1.0/eth/transactions/",
}
DATA_FOLDER = Path("./data")


def get_aws_files(bucket_name, chain, start_date, days):
    logger.info(f"Get AWS Parquest files for {bucket_name}: chain={chain}, start date={start_date} for days={days}")
    for day in range(days):
        download_date = start_date + timedelta(days=day)
        aws_path = f"{PATH_MAPPING.get(chain)}date={download_date.strftime('%Y-%m-%d')}/"
        local_path = Path(f"{DATA_FOLDER}", f"chain={chain}", f"s3_date={download_date.strftime('%Y-%m-%d')}")
        local_path.mkdir(parents=True, exist_ok=True)

        # Setup S3 client for public data
        s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
        bucket = s3.Bucket(bucket_name)

        # Configure parallel transfer (Helpful for WSL2 to saturate the pipe)
        transfer_config = TransferConfig(
            multipart_threshold=1024 * 10, # 10MB
            max_concurrency=20,
            use_threads=True
        )
        logger.info(f"Getting files for {download_date}")
        for obj in bucket.objects.filter(Prefix=aws_path):
            logger.info(f'Getting {obj.key}')
            if obj.key.endswith('.parquet'):
                local_file = local_path / Path(obj.key).name
                bucket.download_file(obj.key, local_file, Config=transfer_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load AWS BTC raw transactions")
    parser.add_argument("--bucket_name", type=str, default="aws-public-blockchain")
    parser.add_argument("--chain", type=str, default="btc", choices=["btc", "eth"])
    parser.add_argument("--start_date", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()
    get_aws_files(**vars(args))