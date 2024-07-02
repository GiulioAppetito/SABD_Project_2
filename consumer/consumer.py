import argparse
import os
import json
import csv
import logging
import threading
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaQueryConsumer:
    def __init__(self, bootstrap_servers, group_id, output_dir):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.output_dir = output_dir

    def create_consumer(self, topic):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def write_csv(self, file_path, fieldnames, rows):
        with open(file_path, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def consume_results(self, topic, fieldnames):
        csv_file_path = os.path.join(self.output_dir, f"{topic}.csv")
        logging.info(f"Consumer open file for the topic: {topic}")

        kafka_consumer = self.create_consumer(topic)
        try:
            rows = []
            while True:
                messages = kafka_consumer.poll(timeout_ms=1000)
                if not messages:
                    logging.info("Consumer didn't receive any record in this poll!")
                for partition, msgs in messages.items():
                    for message in msgs:
                        record = message.value
                        logging.info(f"Consumer received this record from the topic {topic}: {record}")
                        rows.append(record)
                if rows:
                    self.write_csv(csv_file_path, fieldnames, rows)
                    kafka_consumer.commit()
                    rows = []

        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(f"Error while consuming messages from this topic {topic}: {e}")
        finally:
            kafka_consumer.close()

    def run(self, queries_to_execute):

        # Create the directory for output results
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        topic_fieldnames = {

            # Query 1 fieldnames
            'query1_1d_results': ["window_start", "vault_id", "count", "mean", "stddev"],
            'query1_3d_results': ["window_start", "vault_id", "count", "mean", "stddev"],
            'query1_global_results': ["window_start", "vault_id", "count", "mean", "stddev"],

            #Query2 fieldnames
            'query2_1d_results': ["ts", "vault_id1", "failures1", "details1", "vault_id2", "failures2", "details2",
                                  "vault_id3", "failures3", "details3", "vault_id4", "failures4", "details4",
                                  "vault_id5", "failures5", "details5", "vault_id6", "failures6", "details6",
                                  "vault_id7", "failures7", "details7", "vault_id8", "failures8", "details8",
                                  "vault_id9", "failures9", "details9", "vault_id10", "failures10", "details10"],
            'query2_3d_results': ["ts", "vault_id1", "failures1", "details1", "vault_id2", "failures2", "details2",
                                  "vault_id3", "failures3", "details3", "vault_id4", "failures4", "details4",
                                  "vault_id5", "failures5", "details5", "vault_id6", "failures6", "details6",
                                  "vault_id7", "failures7", "details7", "vault_id8", "failures8", "details8",
                                  "vault_id9", "failures9", "details9", "vault_id10", "failures10", "details10"],
            'query2_global_results': ["ts", "vault_id1", "failures1", "details1", "vault_id2", "failures2", "details2",
                                      "vault_id3", "failures3", "details3", "vault_id4", "failures4", "details4",
                                      "vault_id5", "failures5", "details5", "vault_id6", "failures6", "details6",
                                      "vault_id7", "failures7", "details7", "vault_id8", "failures8", "details8",
                                      "vault_id9", "failures9", "details9", "vault_id10", "failures10", "details10"]
        }

        topics_to_consume = []
        if 'query1' in queries_to_execute:
            topics_to_consume.extend(['query1_1d_results', 'query1_3d_results', 'query1_global_results'])
        if 'query2' in queries_to_execute:
            topics_to_consume.extend(['query2_1d_results', 'query2_3d_results', 'query2_global_results'])

        threads = []
        for topic in topics_to_consume:
            fieldnames = topic_fieldnames[topic]
            thread = threading.Thread(target=self.consume_results, args=(topic, fieldnames))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

# function to parse the argument (choice of the query)
def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka Consumer for Flink Queries")
    parser.add_argument('--queries', type=str, choices=['query1', 'query2', 'both'], required=True,
                        help='Queries to consume (query1, query2, both)')
    return parser.parse_args()


if __name__ == '__main__':
    # Parse the argument to choose between the two queries (or both)
    args = parse_arguments()

    # Consumer init
    consumer = KafkaQueryConsumer(
        bootstrap_servers='kafka:9092',
        group_id='kafka_consumer_group',
        output_dir='Results'
    )

    queries = []
    if args.queries == 'query1':
        queries.append('query1')
    elif args.queries == 'query2':
        queries.append('query2')
    elif args.queries == 'both':
        queries.extend(['query1', 'query2'])

    consumer.run(queries)
