import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from typing import List, Tuple, Dict, Iterable
from utils.q2_functions import VaultFailuresPerDayReduceFunction, FailuresAggregateFunction, \
    VaultsRankingProcessFunction, convert_to_row
from utils.utils import MyTimestampAssigner
from pyflink.common import Row
import sys

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    if len(sys.argv) < 3:
        print("Usage: python flink_job_q2.py <window_length> <evaluate>")
        print("<window_length>: one between '1d', '3d', 'all', 'all_three'")
        print("<evaluate>: true or false")
        sys.exit(1)

    window_length = sys.argv[1]
    evaluate = sys.argv[2].lower() == 'true'

    logging.info("Starting Flink job")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'flink_consumer_group'
    kafka_bootstrap_servers = 'kafka:9092'

    deserialization_schema = (JsonRowDeserializationSchema.Builder()
                              .type_info(Types.ROW_NAMED(
        ["date", "serial_number", "model", "failure", "vault_id", "s9_power_on_hours", "s194_temperature_celsius"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.INT(), Types.FLOAT(), Types.FLOAT()]
    )).build())

    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group
        }
    )

    # Sink topics
    kafka_sink_topic_1d = 'query2_1d_results'
    kafka_sink_topic_3d = 'query2_3d_results'
    kafka_sink_topic_all = 'query2_all_results'

    output_attributes = ["ts"]
    for i in range(10):
        output_attributes.extend([f"vault_id{i + 1}", f"failures{i + 1}", f"failed_disks{i + 1}"])

    output_types = [Types.LONG()] + [Types.INT(), Types.INT(), Types.STRING()] * 10

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(output_attributes, output_types)
    ).build()

    kafka_producer_1d = FlinkKafkaProducer(
        topic=kafka_sink_topic_1d,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )
    kafka_producer_3d = FlinkKafkaProducer(
        topic=kafka_sink_topic_3d,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )
    kafka_producer_all = FlinkKafkaProducer(
        topic=kafka_sink_topic_all,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())

    kafka_stream = env.add_source(kafka_consumer)

    partial_stream = (kafka_stream
                      .assign_timestamps_and_watermarks(watermark_strategy)
                      .filter(lambda x: x['failure'] is True)
                      .map(
        lambda x: (x['vault_id'], 1, [f"{x['model']},{x['serial_number']}"]),
        output_type=Types.TUPLE([Types.INT(), Types.INT(), Types.LIST(Types.STRING())])
    )
                      .key_by(lambda x: x[0])
                      .window(TumblingEventTimeWindows.of(Time.days(1)))
                      .reduce(VaultFailuresPerDayReduceFunction())
                      )

    if window_length in ('1d', 'all_three'):
        # Apply a tumbling window of 1 day
        windowed_stream_1d = partial_stream.window_all(TumblingEventTimeWindows.of(Time.days(1))).aggregate(
            FailuresAggregateFunction(), VaultsRankingProcessFunction()
        ).map(convert_to_row, output_type=Types.ROW_NAMED(output_attributes, output_types))

        windowed_stream_1d.add_sink(kafka_producer_1d)
        if evaluate:
            env.execute_async("Q2-1d")

    if window_length in ('3d', 'all_three'):
        # Apply a tumbling window of 3 days
        windowed_stream_3d = partial_stream.window_all(TumblingEventTimeWindows.of(Time.days(3))).aggregate(
            FailuresAggregateFunction(), VaultsRankingProcessFunction()
        ).map(convert_to_row, output_type=Types.ROW_NAMED(output_attributes, output_types))

        windowed_stream_3d.add_sink(kafka_producer_3d)
        if evaluate:
            env.execute_async("Q2-3d")

    if window_length in ('all', 'all_three'):
        # Apply a tumbling window of 23 days
        windowed_stream_all = partial_stream.window_all(TumblingEventTimeWindows.of(Time.days(23))).aggregate(
            FailuresAggregateFunction(), VaultsRankingProcessFunction()
        ).map(convert_to_row, output_type=Types.ROW_NAMED(output_attributes, output_types))

        windowed_stream_all.add_sink(kafka_producer_all)
        if evaluate:
            env.execute_async("Q2-all")

    if not evaluate:
        env.execute("Flink Job Q2")


if __name__ == '__main__':
    main()
