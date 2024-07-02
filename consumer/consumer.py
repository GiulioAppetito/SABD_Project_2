from kafka import KafkaConsumer 
import json 
import csv
import os 
import threading
import logging
import time
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_consumer(topic, group_id, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    return consumer

def consume_results(consumer, topic, output_dir):
    csv_file_path = os.path.join(output_dir, f"{topic}.csv")
    logging.info(f"Consumer open file")
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = ["vault_id"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        try:
            for message in consumer:
                record = message.value
                logging.info(f"Received this record from topic {topic}: {record}")
                writer.writerow(record)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

def main():
    time.sleep(30)
    bootstrap_servers = 'kafka:9092'
    group_id = 'kafka_consumer_group'
    topics = ['query1_1d_results']
    output_dir = 'Results'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    time.sleep(10)
    
    threads=[]
    for topic in topics:
        consumer = create_consumer(topic, group_id, bootstrap_servers)
        thread  =threading.Thread(target=consume_results, args=(consumer, topic, output_dir))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()

