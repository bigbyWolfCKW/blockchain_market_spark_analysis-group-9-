import json
from pathlib import Path
from datetime import datetime
from db import raw_ticks_col, clusters_col, predictions_col

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
BACKUP_DIR = Path("backup") / timestamp
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

def backup_collection(collection, filename):
    docs = list(collection.find({}, {"_id": 0}))
    file_path = BACKUP_DIR / filename

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

    return len(docs), file_path

def clear_collection(collection):
    result = collection.delete_many({})
    return result.deleted_count

raw_count, raw_file = backup_collection(raw_ticks_col, "backup_raw_ticks.json")
cluster_count, cluster_file = backup_collection(clusters_col, "backup_clusters.json")
prediction_count, prediction_file = backup_collection(predictions_col, "backup_predictions.json")

deleted_raw = clear_collection(raw_ticks_col)
deleted_clusters = clear_collection(clusters_col)
deleted_predictions = clear_collection(predictions_col)

print(f"Backup folder: {BACKUP_DIR}")
print(f"Backed up raw_ticks: {raw_count} docs -> {raw_file}")
print(f"Backed up clusters: {cluster_count} docs -> {cluster_file}")
print(f"Backed up predictions: {prediction_count} docs -> {prediction_file}")
print()
print(f"Deleted raw_ticks: {deleted_raw}")
print(f"Deleted clusters: {deleted_clusters}")
print(f"Deleted predictions: {deleted_predictions}")
print("Backup and clear completed.")