import socket
import time
from collections import defaultdict

HOST = "0.0.0.0"
PORT = 5003
CSV_FILE = "data/stocks.csv"
SEND_INTERVAL = 5

def load_grouped_rows(path):
    grouped = defaultdict(list)

    with open(path, "r", encoding="utf-8") as f:
        next(f)  # skip header

        for line in f:
            line = line.strip()
            if not line:
                continue

            ts = line.split(",")[0]
            grouped[ts].append(line)

    return grouped

def main():
    grouped = load_grouped_rows(CSV_FILE)
    ordered_times = sorted(grouped.keys())

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(1)

    print(f"Producer listening on {HOST}:{PORT}")
    print("Waiting for Spark client to connect...")

    client, addr = server.accept()
    print("Client connected:", addr)

    for ts in ordered_times:
        batch = "\n".join(grouped[ts]) + "\n"
        client.sendall(batch.encode("utf-8"))
        print(f"Sent batch for {ts}")
        time.sleep(SEND_INTERVAL)

if __name__ == "__main__":
    main()