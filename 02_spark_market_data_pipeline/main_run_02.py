import signal
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
processes = []


def start_process(name: str, relative_path: str, delay: int = 0) -> None:
    script_path = BASE_DIR / relative_path
    print(f"Starting {name}: {script_path}")

    process = subprocess.Popen(
        ["python3", str(script_path)],
        cwd=BASE_DIR
    )
    processes.append((name, process))

    if delay > 0:
        time.sleep(delay)


def shutdown_all() -> None:
    print("\nStopping all child processes...")

    for name, process in processes:
        if process.poll() is None:
            print(f"Terminating {name} (PID={process.pid})")
            process.terminate()

    time.sleep(3)

    for name, process in processes:
        if process.poll() is None:
            print(f"Killing {name} (PID={process.pid})")
            process.kill()


def signal_handler(sig, frame) -> None:
    shutdown_all()
    sys.exit(0)


def main() -> None:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        start_process(
            "Kafka Consumer",
            "02_processing_etl/kafka_consumer.py",
            delay=5
        )

        start_process(
            "HK Asia Market Producer",
            "01_data_sources/hk_asia_market_producer.py",
            delay=3
        )

        start_process(
            "WebSocket Engine",
            "01_data_sources/websocket_engine.py",
            delay=5
        )

        start_process(
            "Spark Crypto Analytics",
            "04_ml_and_analytics/spark_crypto_advanced.py",
            delay=5
        )

        start_process(
            "Spark US Analytics",
            "04_ml_and_analytics/spark_us_advanced.py",
            delay=5
        )

        print("\nAll processes started. Press Ctrl+C to stop everything.\n")

        while True:
            for name, process in processes:
                if process.poll() is not None:
                    print(f"Warning: {name} exited with return code {process.returncode}")
            time.sleep(5)

    except KeyboardInterrupt:
        shutdown_all()


if __name__ == "__main__":
    main()