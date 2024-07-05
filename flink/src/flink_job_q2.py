import logging
from typing import Tuple, List, Iterable
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows, Time, SlidingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, ReduceFunction, ProcessAllWindowFunction
from pyflink.datastream.window import TimeWindow
from utils.Utils import MyTimestampAssigner

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VaultFailuresPerDayReduceFunction(ReduceFunction):
    def reduce(self, value1: Row, value2: Row) -> Row:
        vault_id = value1['vault_id']
        failures_count = value1['failures_count'] + value2['failures_count']
        failed_disks = value1['failed_disks'] + value2['failed_disks']
        return Row(vault_id=vault_id, failures_count=failures_count, failed_disks=failed_disks)


class VaultFailuresAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return []

    def add(self, value: Row, accumulator) -> List[Tuple[int, int, List[str]]]:
        vault_id, failures_count, failed_disks = value
        accumulator.append((vault_id, failures_count, failed_disks))
        accumulator.sort(key=lambda x: x[1], reverse=True)
        return accumulator[:10]

    def get_result(self, accumulator):
        return accumulator

    def merge(self, ranking_a, ranking_b):
        ranking = ranking_a + ranking_b
        ranking.sort(key=lambda x: x[1], reverse=True)
        return ranking[:10]

    def get_result(self, accumulator):
        return accumulator


class VaultFailuresProcessFunction(ProcessAllWindowFunction):
    def process(
            self,
            context: ProcessAllWindowFunction.Context,
            elements: Iterable[List[Tuple[int, int, List[str]]]],
    ) -> Iterable[Row]:
        ranking = next(iter(elements))
        window: TimeWindow = context.window()

        row_data = [window.start]
        for i in range(10):
            if i < len(ranking):
                vault_id, failures_count, failed_disks = ranking[i]
                row_data.extend([vault_id, failures_count, failed_disks])
            else:
                row_data.extend([None, None, None])

        yield Row(*row_data)

def main():
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
    output_types = [Types.LONG()] + [Types.INT(), Types.INT(), Types.LIST(Types.STRING())] * 10


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
                      .filter(lambda x: x['failure'] is True)
                      .assign_timestamps_and_watermarks(watermark_strategy)
                      .map(
        lambda x: Row(vault_id=x['vault_id'], failures_count=1, failed_disks=[f"{x['model']},{x['serial_number']}"]),
        output_type=Types.ROW_NAMED(
            ["vault_id", "failures_count", "failed_disks"],
            [Types.INT(), Types.INT(), Types.LIST(Types.STRING())]
        ))
                      .key_by(lambda x: x['vault_id'])
                      .window(TumblingEventTimeWindows.of(Time.days(1)))
                      .reduce(VaultFailuresPerDayReduceFunction())
                      )

    windowed_stream_1d = (partial_stream
                       .window_all(TumblingEventTimeWindows.of(Time.days(1)))
                       .aggregate(VaultFailuresAggregateFunction(), VaultFailuresProcessFunction(),
                                  output_type=Types.ROW_NAMED(output_attributes,output_types))
                       )
    windowed_stream_1d.add_sink(kafka_producer_1d)

    windowed_stream_3d = (partial_stream
                          .window_all(SlidingEventTimeWindows.of(Time.days(3), Time.days(1)))
                          .aggregate(VaultFailuresAggregateFunction(), VaultFailuresProcessFunction(),
                                     output_type=Types.ROW_NAMED(output_attributes, output_types))
                          )
    windowed_stream_3d.add_sink(kafka_producer_3d)

    windowed_stream_all = (partial_stream
                          .window_all(TumblingEventTimeWindows.of(Time.days(21)))
                          .aggregate(VaultFailuresAggregateFunction(), VaultFailuresProcessFunction(),
                                     output_type=Types.ROW_NAMED(output_attributes, output_types))
                          )
    windowed_stream_all.add_sink(kafka_producer_all)

    env.execute("Flink Job Q2")


if __name__ == '__main__':
    main()
