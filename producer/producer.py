import csv
import glob
import json
import os
import time
from pathlib import Path

from confluent_kafka import KafkaException, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "mock-data")
DATA_DIR = os.getenv("DATA_DIR", "/data")
DELAY = float(os.getenv("SEND_DELAY_SECONDS", "0"))


def wait_producer() -> Producer:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})
    while True:
        try:
            producer.list_topics(timeout=5)
            return producer
        except KafkaException:
            print("Kafka is not ready, retrying...", flush=True)
            time.sleep(3)


def main() -> None:
    producer = wait_producer()
    files = sorted(glob.glob(str(Path(DATA_DIR) / "*.csv")))
    if not files:
        raise FileNotFoundError(f"No CSV files in {DATA_DIR}")

    total = 0
    for file_path in files:
        file_name = Path(file_path).name
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_number, row in enumerate(reader, start=1):
                # source_id нужен, чтобы повторный запуск producer не создавал дубли в fact_sales
                row["source_id"] = f"{file_name}:{row.get('id', row_number)}"
                value = json.dumps(row, ensure_ascii=False)
                producer.produce(TOPIC, key=row["source_id"].encode(), value=value.encode())
                producer.poll(0)
                total += 1

                if total % 1000 == 0:
                    producer.flush()
                    print(f"Sent {total} records", flush=True)
                if DELAY > 0:
                    time.sleep(DELAY)

    producer.flush()
    print(f"Finished. Sent {total} records to Kafka topic {TOPIC}", flush=True)


if __name__ == "__main__":
    main()
