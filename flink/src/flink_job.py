import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    properties = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink_consumer'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics='hdd_events',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

    stream = env.add_source(kafka_consumer)

    # Print each message to the console log
    stream.map(lambda x: json.loads(x)).print()

    env.execute('Flink Streaming Job')


if __name__ == '__main__':
    main()
