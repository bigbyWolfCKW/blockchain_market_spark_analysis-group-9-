import csv
import requests
from datetime import datetime, timezone

N = 200  # 先細細份，成功後再加大，例如 2000

latest = requests.get("https://blockchain.info/latestblock", timeout=30).json()
latest_height = latest["height"]

rows = []
for h in range(latest_height - N + 1, latest_height + 1):
    url = f"https://blockchain.info/block-height/{h}?format=json"
    j = requests.get(url, timeout=30).json()
    b = j["blocks"][0]
    ts = b["time"]  # unix seconds
    day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    n_tx = len(b.get("tx", []))
    rows.append((day, h, n_tx))
    if (h % 50) == 0:
        print("fetched height", h)

with open("data/blocks.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["time", "height", "n_tx"])
    w.writerows(rows)

print("written:", len(rows), "rows -> data/blocks.csv")