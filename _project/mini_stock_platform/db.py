from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["mini_stock_platform"]

raw_ticks_col = db["raw_ticks"]
clusters_col = db["clusters"]
predictions_col = db["predictions"]