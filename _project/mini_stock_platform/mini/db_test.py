from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["mini_stock"]
col = db["test_collection"]

doc = {
    "name": "Bigby",
    "task": "MongoDB test",
    "status": "success"
}

result = col.insert_one(doc)

print("Inserted document id:", result.inserted_id)
print("MongoDB connection success")