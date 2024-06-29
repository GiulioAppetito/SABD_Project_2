import json
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window_assigners import TumblingEventTimeWindows

from datetime import datetime

from flink.src.utils.welford import update, finalize


def process_window_function(key, context: ProcessWindowFunction.Context, elements, out):
    count, mean, M2 = 0, 0.0, 0.0
    for element in elements:
        count, mean, M2 = update((count, mean, M2), element[2])

    mean, variance, sample_variance = finalize((count, mean, M2))
    result = {
        "window_start": context.window().start,
        "vault_id": key,
        "count": count,
        "mean_temperature": mean,
        "stddev_temperature": sample_variance ** 0.5
    }
    out.collect(json.dumps(result))

def main():

    # Obtain an execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    #env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    # Configuration of Source (KafkaConsumer)
    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'sabd_consumer_group'
    kafka_bootstrap_servers = 'kafka:9092'
    security_protocol = 'PLAINTEXT'

    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group,
            'security.protocol': security_protocol
        }
    )

    # Creating the DataStream from source
    kafka_stream = env.add_source(kafka_consumer)

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    kafka_stream = kafka_stream.assign_timestamps_and_watermarks(
        watermark_strategy.with_timestamp_assigner(lambda event, timestamp: datetime.strptime(event.split(",")[0], "%Y-%m-%dT%H:%M:%S.%f").timestamp()*10000)
    )

    # Stampa
    kafka_stream.map(lambda x: f"Messaggio da Kafka: {x}", output_type=Types.STRING()).print()

    # Execute the Flink Job
    env.execute("Flink Kafka Consumer Test Giulio")

if __name__ == '__main__':
    main()
