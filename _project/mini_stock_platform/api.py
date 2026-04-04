from flask import Flask, jsonify
from db import raw_ticks_col, clusters_col, predictions_col

app = Flask(__name__)

@app.route("/")
def home():
    return "Mini stock platform API with MongoDB is running."

@app.route("/latest")
def latest():
    docs = list(raw_ticks_col.find({}, {"_id": 0}).sort("time", -1).limit(20))
    return jsonify(docs)

@app.route("/clusters")
def clusters():
    docs = list(clusters_col.find({}, {"_id": 0}).sort("time", -1).limit(20))
    return jsonify(docs)

@app.route("/predictions")
def predictions():
    docs = list(predictions_col.find({}, {"_id": 0}).sort("time", -1).limit(20))
    return jsonify(docs)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)