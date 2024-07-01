import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
from datetime import datetime

class ParseJsonFunction(MapFunction):
    def map(self, value):
        return json.loads(value)

class PrintFunction(MapFunction):
    def map(self, value):
        print(f"Received message: {value}")
        return value

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.13.2.jar")

    kafka_consumer = FlinkKafkaConsumer(
        topics='hdd_events',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'flink_consumer_group',
            'security.protocol': 'PLAINTEXT'
        }
    )

    kafka_stream = env.add_source(kafka_consumer)

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

    kafka_stream = kafka_stream.assign_timestamps_and_watermarks(
        watermark_strategy.with_timestamp_assigner(lambda event, timestamp: datetime.strptime(event.split(",")[0], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000)
    )

    kafka_stream.map(ParseJsonFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
    kafka_stream.map(PrintFunction()).set_parallelism(1)

    env.execute("Flink Kafka Consumer Job")

if __name__ == '__main__':
    main()
