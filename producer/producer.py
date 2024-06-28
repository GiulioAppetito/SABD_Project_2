import csv
import time
import json
from kafka import KafkaProducer
import os
from datetime import datetime
from utils import parse_row, scale_interval

# Configuration
KAFKA_BROKER = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'hdd_events')
CSV_FILE_PATH = '/home/producer/data/raw_data_medium-utv_sorted.csv'
SCALE_FACTOR = 3600  # 1 hour becomes 1 second

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def parse_row(row):
    preprocessed_row = {
        "date": row[0],
        "serial_number": row[1],
        "model": row[2],
        "failure": row[3],
        "vault_id": int(row[4]),
        "power_on_hours": int(row[9]),
        "temperature_celsius": int(row[32]) if row[32] else None
    }
    return preprocessed_row


def scale_interval(interval, scale_factor):
    return interval / scale_factor


def send_to_kafka(row):
    producer.send(TOPIC_NAME, row)
    producer.flush()
    print(f"Successfully sent data to Kafka: {row}")


def main():
    with open(CSV_FILE_PATH, mode='r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip the header row

        previous_timestamp = None

        for row in csv_reader:
            current_timestamp = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f")

            if previous_timestamp:
                interval = (current_timestamp - previous_timestamp).total_seconds()
                scaled_interval = scale_interval(interval, SCALE_FACTOR)
                time.sleep(scaled_interval)

            preprocessed_row = parse_row(row)
            send_to_kafka(preprocessed_row)
            previous_timestamp = current_timestamp


if __name__ == '__main__':
    main()
