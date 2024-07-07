import os
import json
import csv
import logging
import sys
import time
from kafka import KafkaConsumer
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaQueryConsumer:
    def __init__(self, bootstrap_servers, group_id, output_dir, topics, max_retries=10, retry_delay=10):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.output_dir = output_dir
        self.topics = topics
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.topic_fieldnames = {
            'query1_1d_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query1_3d_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query1_all_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query2_1d_results': self.get_output_attributes_q2(),
            'query2_3d_results': self.get_output_attributes_q2(),
            'query2_all_results': self.get_output_attributes_q2()
        }
        logging.info("Consumer is alive. Ready to consume...")

    def get_output_attributes_q2(self):
        output_attributes_q2 = ["ts"]
        for i in range(10):
            output_attributes_q2.extend([f"vault_id{i + 1}", f"failures{i + 1}", f"failed_disks{i + 1}"])
        return output_attributes_q2

    def create_consumer(self):
        for attempt in range(self.max_retries):
            try:
                consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                logging.info(f"Connected to Kafka broker on attempt {attempt + 1}")
                return consumer
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} to connect to Kafka broker failed: {e}")
                time.sleep(self.retry_delay)
        raise Exception("Failed to connect to Kafka broker after multiple attempts")

    def write_csv_file(self, file_path, fieldnames, row, write_header):
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode, newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)

    def consume_results(self):
        kafka_consumer = self.create_consumer()
        try:
            while True:
                for message in kafka_consumer:
                    topic = message.topic
                    record = message.value
                    logging.info(f"Consumer received a record from the topic {topic}: {record}")

                    fieldnames = self.topic_fieldnames[topic]
                    csv_file_path = os.path.join(self.output_dir, f"{topic}.csv")

                    self.write_csv_file(csv_file_path, fieldnames, record, write_header=not os.path.exists(csv_file_path))
                    logging.info(f"Wrote record to file {csv_file_path}")
                    kafka_consumer.commit()
        except Exception as e:
            logging.error(f"Error while consuming messages from Kafka: {e}")
        finally:
            kafka_consumer.close()

    def empty_query_files(self):
        for topic in self.topics:
            file_path = os.path.join(self.output_dir, f"{topic}.csv")
            if os.path.exists(file_path):
                try:
                    os.unlink(file_path)
                    logging.info(f'Deleted file {file_path}')
                except Exception as e:
                    logging.error(f'Failed to delete {file_path} because of: {e}')

    def run(self):
        # Empty the files related to the specific queries
        self.empty_query_files()

        # Start consuming results
        self.consume_results()


if __name__ == '__main__':
    # Consumer init
    topics = sys.argv[1:]
    if not topics:
        logging.error("No topics specified for the consumer")
        sys.exit(1)

    logging.info(f"Starting consumer for topics: {topics}")
    consumer = KafkaQueryConsumer(
        bootstrap_servers='kafka:9092',
        group_id='kafka_consumer_group',
        output_dir='Results',
        topics=topics
    )

    consumer.run()
