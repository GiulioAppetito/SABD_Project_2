import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream.window import TumblingEventTimeWindows, Time
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
        return float("nan")
    else:
        (mean, variance, sample_variance) = (mean, M2 / count, M2 / (count - 1))
        return (mean, variance, sample_variance)


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

        result = Row(window_start=window.start, vault_id=key, count=count, mean=mean, stddev=stddev)

        # Logga i risultati prima di scrivere su Kafka
        logging.info(f"Window Start: {window.start}, Vault ID: {key}, Count: {count}, Mean: {mean}, Stddev: {stddev}")

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
    kafka_sink_topic_1d = 'filtered_hdd_events'

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(["window_start", "vault_id", "count", "mean", "stddev"],
                        [Types.LONG(), Types.INT(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE()])
    ).build()

    kafka_producer_1d = FlinkKafkaProducer(
        topic=kafka_sink_topic_1d,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )


    # Source DataStream
    kafka_stream = env.add_source(kafka_consumer)

    # Filtered stream before windows
    filtered_stream = (kafka_stream
                       .filter(lambda x: 1000 <= x.vault_id <= 1020)
                       .map(lambda x: Row(vault_id=x.vault_id, s194_temperature_celsius=x.s194_temperature_celsius),
                            Types.ROW_NAMED(["vault_id", "s194_temperature_celsius"], [Types.INT(), Types.FLOAT()]))
                       .key_by(lambda x: x.vault_id)
                       )

    windowed_1d_stream = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(1)))
                          .aggregate(TemperatureAggregate(), ComputeStats(),
                                     accumulator_type=Types.TUPLE([Types.LONG(), Types.DOUBLE(), Types.DOUBLE()]),
                                     output_type=Types.ROW_NAMED(
                                         ["window_start", "vault_id", "count", "mean", "stddev"],
                                         [Types.LONG(), Types.INT(), Types.LONG(), Types.DOUBLE(),
                                          Types.DOUBLE()]))
                          )


    windowed_1d_stream.add_sink(kafka_producer_1d)

    env.execute("Flink Kafka Filter and Forward Job")


if __name__ == '__main__':
    main()
