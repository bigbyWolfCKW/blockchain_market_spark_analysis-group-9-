import requests
import csv
from io import StringIO
from db import raw_ticks_col

url = "https://stooq.com/q/d/l/?s=aapl.us&i=d"

response = requests.get(url, timeout=10)
print("status_code =", response.status_code)
print("content-type =", response.headers.get("Content-Type"))

text_data = response.text
print("first 200 chars =")
print(text_data[:200])

raw_ticks_col.delete_many({})

reader = csv.DictReader(StringIO(text_data))

count = 0
for row in reader:
    if not row["Date"] or not row["Close"]:
        continue

    price = float(row["Close"])
    volume = float(row["Volume"]) if row["Volume"] else 0.0

    doc = {
        "stock_code": "AAPL",
        "price": price,
        "volume": volume,
        "amount": price * volume,
        "time": row["Date"]
    }

    raw_ticks_col.insert_one(doc)
    count += 1

    if count >= 10:
        break

print(f"Inserted {count} records into MongoDB.")