import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows, Time, SlidingEventTimeWindows
from utils.q1_functions import TemperatureAggregateFunction, TemperatureProcessFunction
from utils.utils import MyTimestampAssigner

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    kafka_sink_topic_1d = 'query1_1d_results'
    kafka_sink_topic_3d = 'query1_3d_results'
    kafka_sink_topic_all = 'query1_all_results'

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
                        [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
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
                       .assign_timestamps_and_watermarks(watermark_strategy)
                       .filter(lambda x: 1000 <= x.vault_id <= 1020)
                       .map(
                            lambda x: Row(date=x.date, vault_id=x.vault_id, s194_temperature_celsius=x.s194_temperature_celsius),
                            Types.ROW_NAMED(["date", "vault_id", "s194_temperature_celsius"], [Types.STRING(), Types.INT(), Types.FLOAT()]))
                       .key_by(lambda x: x.vault_id)
                       )

    # Apply a tumbling window of 1 minute for temperature aggregation
    windowed_stream_1d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(1)))
                          .aggregate(TemperatureAggregateFunction(), TemperatureProcessFunction(), output_type=Types.ROW_NAMED(
                            ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
                            [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                          )
    windowed_stream_1d.add_sink(kafka_producer_1d)

    # Apply a tumbling window of 3 minutes for temperature aggregation
    windowed_stream_3d = (filtered_stream
                          .window(TumblingEventTimeWindows.of(Time.days(3)))
                          .aggregate(TemperatureAggregateFunction(), TemperatureProcessFunction(), output_type=Types.ROW_NAMED(
                            ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
                            [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                          )
    windowed_stream_3d.add_sink(kafka_producer_3d)

    # Apply a global window for temperature aggregation
    windowed_stream_all = (filtered_stream
                           .window(TumblingEventTimeWindows.of(Time.days(23)))
                           .aggregate(TemperatureAggregateFunction(), TemperatureProcessFunction(), output_type=Types.ROW_NAMED(
                            ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
                            [Types.LONG(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
                           )
    windowed_stream_all.add_sink(kafka_producer_all)

    env.execute("Flink Job Q1")


if __name__ == '__main__':
    main()
