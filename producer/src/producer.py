
import csv
import time
import json
from kafka import KafkaProducer
import os
from datetime import datetime, timedelta
from utils import parse_row, scale_interval
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPIC_NAME = os.getenv('TOPIC_NAME')
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')
SCALE_FACTOR = int(os.getenv('SCALE_FACTOR'))

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
            try:
                current_timestamp = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f")

                if previous_timestamp:
                    interval = (current_timestamp - previous_timestamp).total_seconds()
                    scaled_interval = scale_interval(interval, SCALE_FACTOR)
                    time.sleep(scaled_interval)

                preprocessed_row = parse_row(row)

                if preprocessed_row['s194_temperature_celsius'] is not None:
                    send_to_kafka(preprocessed_row)
                previous_timestamp = current_timestamp
            except ValueError:
                print("Date not in the right format.")
    # Send one last row with a timestamp 7 days after the last row
    if previous_timestamp:
        dummy_timestamp = previous_timestamp + timedelta(days=7)
        dummy_row = row.copy()
        dummy_row[0] = dummy_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")
        preprocessed_dummy_row = parse_row(dummy_row)
        send_to_kafka(preprocessed_dummy_row)
        print(f"Sent final row with timestamp {dummy_row[0]}")


if __name__ == '__main__':
    main()