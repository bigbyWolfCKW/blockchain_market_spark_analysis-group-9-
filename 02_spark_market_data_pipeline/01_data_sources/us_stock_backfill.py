import yfinance as yf
import psycopg2
from datetime import timedelta

DB_CONFIG = {
    "host": "localhost",
    "database": "stock_db",
    "user": "admin",
    "password": "secret",
    "port": "5432"
}

US_STOCKS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "XOM", "OXY"]

def backfill_us_stocks():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    total_inserted = 0

    for symbol in US_STOCKS:
        try:
            print(f"\n📥 Backfilling {symbol} ...")

            ticker = yf.Ticker(symbol)

            # 過去 2 日，每 5 分鐘一筆，較穩陣
            hist = ticker.history(period="2d", interval="5m")

            if hist.empty:
                print(f"⚠️ No history found for {symbol}")
                continue

            inserted_count = 0

            for index, row in hist.iterrows():
                close_price = row.get("Close")

                if close_price is None:
                    continue

                # yfinance index 通常帶 timezone
                ts_utc = index.tz_convert("UTC") if index.tzinfo else index
                window_start = ts_utc.to_pydatetime().replace(tzinfo=None)
                window_end = window_start + timedelta(minutes=5)

                cursor.execute(
                    """
                    INSERT INTO realtime_prices (symbol, type, avg_price, window_start, window_end)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, type, window_start) DO NOTHING
                    """,
                    (symbol, "us_stock", float(close_price), window_start, window_end)
                )
                inserted_count += 1

            conn.commit()
            total_inserted += inserted_count
            print(f"✅ {symbol}: processed {inserted_count} rows")

        except Exception as e:
            print(f"⚠️ {symbol} backfill failed: {e}")

    cursor.close()
    conn.close()
    print(f"\n🎉 Done. Total processed rows: {total_inserted}")

if __name__ == "__main__":
    backfill_us_stocks()