import csv
import time
import json
from kafka import KafkaProducer
import os
from datetime import datetime
from utils import parse_row, scale_interval

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'hdd_events')
CSV_FILE_PATH = '/home/producer/data/first_50_rows_per_date.csv'
SCALE_FACTOR = 3600  # 1 hour becomes 1 second

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


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
            # Check if temperature is not None
            current_timestamp = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f")

            if previous_timestamp:
                interval = (current_timestamp - previous_timestamp).total_seconds()
                scaled_interval = scale_interval(interval, SCALE_FACTOR)
                time.sleep(scaled_interval)

            preprocessed_row = parse_row(row)

            if preprocessed_row['s194_temperature_celsius'] is not None:
                send_to_kafka(preprocessed_row)
            previous_timestamp = current_timestamp


if __name__ == '__main__':
    main()