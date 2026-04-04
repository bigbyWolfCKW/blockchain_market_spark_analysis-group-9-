import os
import pandas as pd
from sklearn.ensemble import IsolationForest

INPUT_CSV = "data/blocks.csv"
OUT_DIR = "out"
OUT_CSV = os.path.join(OUT_DIR, "anomaly_blocks.csv")

# 讀取資料
df = pd.read_csv(INPUT_CSV)

# 基本檢查
required = {"time", "height", "n_tx"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing columns: {missing}. Found: {list(df.columns)}")

# 特徵：先用最簡單的 n_tx（你可以之後加 rolling/delta）
X = df[["n_tx"]].astype(float)

# Isolation Forest（contamination = 預期異常比例，可調）
model = IsolationForest(
    n_estimators=300,
    contamination=0.05,   # 先假設 5% block 是異常
    random_state=42
)
model.fit(X)

# 分數：越小越異常；pred = -1 表示異常
df["anomaly_score"] = model.decision_function(X)
df["is_anomaly"] = (model.predict(X) == -1)

# 排序：最異常（score 最低）排前
df_sorted = df.sort_values("anomaly_score", ascending=True)

# 輸出
os.makedirs(OUT_DIR, exist_ok=True)
df_sorted.to_csv(OUT_CSV, index=False)

# 印出 Top 20 異常（方便你即時睇）
print("Top 20 anomaly-like blocks (lowest score first):")
print(df_sorted[["time", "height", "n_tx", "anomaly_score", "is_anomaly"]].head(20).to_string(index=False))
print(f"\nSaved: {OUT_CSV}")