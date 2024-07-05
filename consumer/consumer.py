import os
import json
import csv
import logging
import time
from threading import Thread
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaQueryConsumer:
    def __init__(self, bootstrap_servers, group_id, output_dir, max_retries=10, retry_delay=10):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.output_dir = output_dir
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        logging.info("Consumer is alive. Ready to consume...")

    def create_consumer(self, topic):
        for attempt in range(self.max_retries):
            try:
                consumer = KafkaConsumer(
                    topic,
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

    def write_csv_file(self, file_path, fieldnames, rows, write_header):
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode, newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def consume_results(self, topic, fieldnames):
        # Ensure each topic writes to a separate file
        csv_file_path = os.path.join(self.output_dir, f"{topic}.csv")
        logging.info(f"Consumer open file for the topic: {topic}")

        kafka_consumer = self.create_consumer(topic)
        try:
            rows = []
            write_header = True
            if os.path.exists(csv_file_path):
                write_header = False

            for message in kafka_consumer:
                record = message.value
                logging.info(f"Consumer received a record from the topic {topic}: {record}")
                rows.append(record)
                if rows:
                    self.write_csv_file(csv_file_path, fieldnames, rows, write_header)
                    write_header = False  # Header should be written only once
                    kafka_consumer.commit()
                    rows = []
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f"Error while consuming messages from this topic {topic}: {e}")
        finally:
            kafka_consumer.close()

    def empty_output_dir(self):
        for filename in os.listdir(self.output_dir):
            file_path = os.path.join(self.output_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f'Failed to delete {file_path}. Reason: {e}')

    def run(self):
        # Create the directory for output results
        self.empty_output_dir()
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        output_attributes_q2 = ["ts"]
        for i in range(10):
            output_attributes_q2.extend([f"vault_id{i + 1}", f"failures{i + 1}", f"failed_disks{i + 1}"])

        topic_fieldnames = {
            'query1_1d_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query1_3d_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query1_all_results': ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
            'query2_1d_results': output_attributes_q2,
            'query2_3d_results': output_attributes_q2,
            'query2_all_results': output_attributes_q2
        }

        topics_to_consume = ['query1_1d_results', 'query1_3d_results', 'query1_all_results',
                             'query2_1d_results', 'query2_3d_results', 'query2_all_results']

        threads = []
        for topic in topics_to_consume:
            fieldnames = topic_fieldnames[topic]
            thread = Thread(target=self.consume_results, args=(topic, fieldnames))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()


if __name__ == '__main__':
    # Consumer init
    consumer = KafkaQueryConsumer(
        bootstrap_servers='kafka:9092',
        group_id='kafka_consumer_group',
        output_dir='Results'
    )

    consumer.run()
