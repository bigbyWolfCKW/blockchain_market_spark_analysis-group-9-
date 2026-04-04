import yfinance as yf
from pymongo import MongoClient
from datetime import datetime

# 1) 連 MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["mini_stock_platform"]
raw_ticks_col = db["raw_ticks"]

# 2) 選一隻股票
ticker = "AAPL"

# 3) 抓最近少量資料
df = yf.download(ticker, period="5d", interval="1h", auto_adjust=False)

# 4) 只取前 10 筆測試
df = df.reset_index().head(10)

# 5) 清空舊資料（方便測試）
raw_ticks_col.delete_many({})

# 6) 寫入 MongoDB
for _, row in df.iterrows():
    doc = {
        "symbol": ticker,
        "time": row["Datetime"].isoformat() if "Datetime" in df.columns else row["Date"].isoformat(),
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
        "volume": float(row["Volume"]) if row["Volume"] == row["Volume"] else 0
    }
    raw_ticks_col.insert_one(doc)

print("Done. Inserted", raw_ticks_col.count_documents({}), "rows.")