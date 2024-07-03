import datetime
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows, Time, CountWindow
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TimeWindow
import math
from typing import Iterable, Tuple

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Funzioni update e finalize
def update(existing_aggregate, new_value):
    (count, mean, M2) = existing_aggregate
    count += 1
    delta = new_value - mean
    mean += delta / count
    delta2 = new_value - mean
    M2 += delta * delta2
    return (count, mean, M2)


def finalize(existing_aggregate):
    (count, mean, M2) = existing_aggregate
    if count < 2:
        return mean, float("nan"), float("nan")
    else:
        (mean, variance, sample_variance) = (mean, M2 / count, M2 / (count - 1))
        return (mean, variance, sample_variance)


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        logging.info("sono il log value.data", value)
        return datetime.datetime.strptime(value.date, "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000


class TemperatureAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0, 0.0  # count, mean, M2

    def add(self, value: Row, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        return update(accumulator, value.s194_temperature_celsius)

    def get_result(self, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        return accumulator

    def merge(self, a: Tuple[int, float, float], b: Tuple[int, float, float]) -> Tuple[int, float, float]:
        count_a, mean_a, M2_a = a
        count_b, mean_b, M2_b = b

        count = count_a + count_b
        delta = mean_b - mean_a
        mean = mean_a + delta * count_b / count
        M2 = M2_a + M2_b + delta * delta * count_a * count_b / count

        return count, mean, M2


class ComputeStats(ProcessWindowFunction):
    def process(
            self,
            key: int,
            context: ProcessWindowFunction.Context,
            stats: Iterable[Tuple[int, float, float]],
    ) -> Iterable[Row]:
        count, mean, M2 = next(iter(stats))
        mean, variance, sample_variance = finalize((count, mean, M2))
        stddev = math.sqrt(variance) if count > 1 else 0.0

        window: TimeWindow = context.window()

        result = Row(ts=window.start, vault_id=key, count=count, mean_s194=mean, stddev_s194=stddev)

        # Log the results before writing to Kafka
        yield result


def main():
    logging.info("Starting Flink job")
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
    )).build())

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
    kafka_sink_topic_1d = 'query2_1d_results'
    kafka_sink_topic_3d = 'query2_3d_results'
    kafka_sink_topic_all = 'query2_all_results'

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(["ts",
                         "vault_id1", "failures1", "models_serials1",
                         "vault_id2", "failures2", "models_serials2",
                         "vault_id3", "failures3", "models_serials3",
                         "vault_id4", "failures4", "models_serials4",
                         "vault_id5", "failures5", "models_serials5",
                         "vault_id6", "failures6", "models_serials6",
                         "vault_id7", "failures7", "models_serials7",
                         "vault_id8", "failures8", "models_serials8",
                         "vault_id9", "failures9", "models_serials9",
                         "vault_id10", "failures10", "models_serials10",],
                        [Types.LONG(),
                         Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
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
    # Watermark strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())

    # Source DataStream
    kafka_stream = env.add_source(kafka_consumer)

    # Filtered stream before windows
    filtered_stream = (kafka_stream
                       .filter(lambda x: 1000 <= x.vault_id <= 1020)
                       .assign_timestamps_and_watermarks(watermark_strategy)
                       .map(
        lambda x: Row(date=x.date, vault_id=x.vault_id, s194_temperature_celsius=x.s194_temperature_celsius),
        Types.ROW_NAMED(["date", "vault_id", "s194_temperature_celsius"], [Types.STRING(), Types.INT(), Types.FLOAT()]))
                       .key_by(lambda x: x.vault_id)
                       )

    # Apply a tumbling window of 1 minute for temperature aggregation
    windowed_stream_1d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(1)))
                          .aggregate(TemperatureAggregate(), ComputeStats(), output_type=Types.ROW_NAMED(
        ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
        [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                          )
    windowed_stream_1d.add_sink(kafka_producer_1d)

    # Apply a tumbling window of 3 minutes for temperature aggregation
    windowed_stream_3d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(3)))
                          .aggregate(TemperatureAggregate(), ComputeStats(), output_type=Types.ROW_NAMED(
        ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
        [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                          )
    windowed_stream_3d.add_sink(kafka_producer_3d)

    # Apply a global window for temperature aggregation
    windowed_stream_all = (filtered_stream
                           .window(TumblingEventTimeWindows.of(Time.days(22)))
                           .aggregate(TemperatureAggregate(), ComputeStats(), output_type=Types.ROW_NAMED(
        ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
        [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                           )
    windowed_stream_all.add_sink(kafka_producer_all)

    env.execute("Flink Kafka Filter and Forward Job")


if __name__ == '__main__':
    main()
