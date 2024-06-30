import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction


class PrintFunction(MapFunction):
    def map(self, value):
        print(f"Record received: {value}")
        return value


def main():

    # Obtain an execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    # Configuration of Source (KafkaConsumer)
    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'sabd_consumer_group'
    kafka_bootstrap_servers = 'kafka:9092'

    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group
        }
    )

    # Creating the DataStream from source
    kafka_stream = env.add_source(kafka_consumer)

    # Print the messages from Kafka
    kafka_stream.map(PrintFunction()).set_parallelism(1)

    # Execute the Flink Job
    env.execute("Flink Kafka Consumer Test")

if __name__ == '__main__':
    main()
