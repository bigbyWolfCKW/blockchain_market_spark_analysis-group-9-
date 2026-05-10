import argparse
import io
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def month_range(start_month: str, end_month: str) -> list[str]:
    current = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    months = []

    while current <= end:
        months.append(current.strftime("%Y-%m"))

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def download_month(symbol: str, interval: str, month: str, max_retries: int = 5) -> pd.DataFrame | None:
    url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{month}.zip"

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Downloading {url}")
            response = requests.get(url, timeout=120)

            if response.status_code == 404:
                print(f"Missing archive for {month}, skipping.")
                return None

            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                csv_names = [name for name in zf.namelist() if name.endswith(".csv")]
                if not csv_names:
                    print(f"No CSV found in {url}, skipping.")
                    return None

                with zf.open(csv_names[0]) as f:
                    return pd.read_csv(f, header=None, names=COLUMNS)

        except requests.RequestException as exc:
            wait_sec = min(2 ** attempt, 30)
            print(f"Download failed for {month} attempt {attempt}/{max_retries}: {exc}")
            print(f"Retrying in {wait_sec}s...")
            time.sleep(wait_sec)

    raise RuntimeError(f"Failed to download archive for {month}")


def build_archive_parquet(
    symbol: str,
    interval: str,
    start_month: str,
    end_month: str,
    output_path: str,
    sleep_sec: float,
) -> None:
    frames = []

    for month in month_range(start_month, end_month):
        df = download_month(symbol, interval, month)

        if df is not None and not df.empty:
            frames.append(df)
            print(f"Loaded {month}: {len(df)} rows")

        time.sleep(sleep_sec)

    if not frames:
        print("No data loaded.")
        return

    df_all = pd.concat(frames, ignore_index=True)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    for col in numeric_cols:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

    df_all["trade_count"] = pd.to_numeric(df_all["trade_count"], errors="coerce").astype("Int64")
    df_all["open_time"] = pd.to_numeric(df_all["open_time"], errors="coerce").astype("Int64")
    df_all["close_time"] = pd.to_numeric(df_all["close_time"], errors="coerce").astype("Int64")

    df_all["hour_bucket"] = pd.to_datetime(df_all["open_time"], unit="ms")
    df_all["close_time_ts"] = pd.to_datetime(df_all["close_time"], unit="ms")
    df_all["symbol"] = symbol

    df_all = (
        df_all[
            [
                "hour_bucket",
                "close_time_ts",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "trade_count",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
            ]
        ]
        .drop_duplicates(subset=["symbol", "hour_bucket"])
        .sort_values("hour_bucket")
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output, index=False)

    print("Archive backfill completed.")
    print(f"Rows written: {len(df_all)}")
    print(f"Min hour: {df_all['hour_bucket'].min()}")
    print(f"Max hour: {df_all['hour_bucket'].max()}")
    print(f"Output: {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Binance monthly kline archives to Parquet.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--sleep-sec", type=float, default=0.2)

    args = parser.parse_args()

    build_archive_parquet(
        symbol=args.symbol,
        interval=args.interval,
        start_month=args.start_month,
        end_month=args.end_month,
        output_path=args.output_path,
        sleep_sec=args.sleep_sec,
    )


if __name__ == "__main__":
    main()
