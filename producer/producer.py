import csv
import time
import json
from kafka import KafkaProducer
import os

# Configuration
KAFKA_BROKER = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'hdd_events')
CSV_FILE_PATH = 'app/producer/data/raw_data_medium-utv_sorted.csv'
DELAY = 1  # Delay in seconds between sending each row

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def preprocess_data(row):
    preprocessed_row = {
        "date": row["date"],
        "serial_number": row["serial number"],
        "model": row["model"],
        "failure": row["failure"],
        "vault_id": int(row["vault id"]),
        "power_on_hours": int(row["s9 power on hours"]),
        "temperature_celsius": int(row["s194 temperature celsius"]) if row["s194 temperature celsius"] else None
    }
    return preprocessed_row

def send_to_kafka(row):
    producer.send(TOPIC_NAME, row)
    producer.flush()
    print(f"Successfully sent data to Kafka: {row}")

def main():
    with open(CSV_FILE_PATH, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            preprocessed_row = preprocess_data(row)
            send_to_kafka(preprocessed_row)
            time.sleep(DELAY)

if __name__ == '__main__':
    main()
