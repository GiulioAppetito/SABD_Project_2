from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import Time, TumblingEventTimeWindows, GlobalWindows
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.triggers import CountTrigger
import datetime
import math
from typing import Iterable, Tuple


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(datetime.datetime.strptime(value['date'], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000)


class TemperatureAggregate(AggregateFunction):
    def create_accumulator(self):
        return (0, 0.0, 0.0)  # count, mean, M2

    def add(self, value: dict, accumulator: tuple[int, float, float]) -> tuple[int, float, float]:
        count, mean, M2 = accumulator
        new_value = float(value['s194_temperature_celsius'])
        count += 1
        delta = new_value - mean
        mean += delta / count
        delta2 = new_value - mean
        M2 += delta * delta2
        return (count, mean, M2)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b) -> tuple[int, float, float]:
        pass


class ComputeStats(ProcessWindowFunction):

    def process(self, key, context: ProcessWindowFunction.Context, stats: Iterable[Tuple[int, float, float]]):
        count, mean, M2 = next(iter(stats))
        variance = M2 / count if count > 0 else float('nan')
        stddev = math.sqrt(variance) if count > 1 else float('nan')
        window = context.window()
        yield Row(window.start, key, count, mean, stddev)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    # Configurazione del consumatore Kafka
    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'flink_consumer_group'
    kafka_bootstrap_servers = 'kafka:9092'

    # Schema di deserializzazione
    deserialization_schema = (JsonRowDeserializationSchema.Builder()
                              .type_info(Types.ROW_NAMED(
                                ["date", "serial_number", "model", "failure", "vault_id", "s9_power_on_hours", "s194_temperature_celsius"],
                                [Types.STRING(), Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.INT(), Types.FLOAT(), Types.FLOAT()]
                              )
    ).build())

    # Configurazione del consumatore Kafka
    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group
        }
    )

    # Configurazione del produttore Kafka
    kafka_sink_topic_1d = 'query1_1d_results'
    kafka_sink_topic_3d = 'query1_3d_results'
    kafka_sink_topic_global = 'query1_global_results'

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
                        [Types.LONG(), Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT()])
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

    kafka_producer_global = FlinkKafkaProducer(
        topic=kafka_sink_topic_global,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )

    # Flusso di dati di esempio con assegnazione timestamp e watermark
    kafka_stream = env.add_source(kafka_consumer)

    watermark_strategy = (WatermarkStrategy
                          .for_monotonous_timestamps()
                          .with_timestamp_assigner(MyTimestampAssigner())
                          )

    filtered_stream = (kafka_stream
                       .filter(lambda x: 1000 <= x['vault_id'] <= 1020)
                       .assign_timestamps_and_watermarks(watermark_strategy)
                       .key_by(lambda x: x['vault_id'])
                       )

    # Finestra di 1 giorno
    windowed_stream_1d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(1)))
                          .aggregate(TemperatureAggregate(), ComputeStats()))

    windowed_stream_1d.add_sink(kafka_producer_1d)

    # Finestra di 3 giorni
    windowed_stream_3d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(3)))
                          .aggregate(TemperatureAggregate(), ComputeStats()))

    windowed_stream_3d.add_sink(kafka_producer_3d)

    # Finestra globale
    windowed_stream_global = (filtered_stream
                              .window(GlobalWindows.create())
                              .trigger(CountTrigger.of(1))
                              .aggregate(TemperatureAggregate(), ComputeStats()))

    windowed_stream_global.add_sink(kafka_producer_global)

    env.execute("Flink Kafka Job Test")


if __name__ == '__main__':
    main()
