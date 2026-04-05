import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_data")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}


try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully.")
except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    sys.exit(1)


try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest"
    )
    print(f"Connected to Kafka topic '{KAFKA_TOPIC}' successfully.")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    sys.exit(1)


print("Kafka consumer started. Press Ctrl+C to stop.")


try:
    for message in consumer:
        data = message.value

        symbol = data.get("symbol")
        price = data.get("price")
        timestamp = data.get("timestamp")
        asset_type = data.get("type")

        if not all([symbol, price, timestamp, asset_type]):
            print(f"Skipped invalid message: {data}")
            continue

        cursor.execute(
            """
            INSERT INTO realtime_prices (window_start, symbol, avg_price, type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, type, window_start) DO NOTHING
            """,
            (timestamp, symbol, price, asset_type)
        )
        conn.commit()

        print(f"[Inserted] {symbol}: ${price} ({asset_type})")

except KeyboardInterrupt:
    print("\nConsumer stopped safely.")
finally:
    cursor.close()
    conn.close()
    consumer.close()