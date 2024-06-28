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
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # Configuration of Source (KafkaConsumer)
    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'flink_consumer_group'
    kafka_bootstrap_servers = 'localhost:9092'

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

    # Timestamps and watermarks
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(20))

    kafka_stream = kafka_stream.assign_timestamps_and_watermarks(
        watermark_strategy
        # Probably you dont need assigner for kafka
        .with_timestamp_assigner(lambda event, timestamp: datetime.strptime(event.split(",")[0], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000)
    )


    # Processing stream
    result_stream = (kafka_stream
                     .map(lambda x: json.loads(x), output_type=Types.PICKLED_BYTE_ARRAY())
                     .filter(lambda x: 1000 <= x["vault_id"] <= 1020)  # Filtro per vault_id
                     .key_by(lambda x: x["vault_id"])  # Raggruppamento per vault_id
                     .window(TumblingEventTimeWindows.of(Time.days(1)))  # Applicazione di una finestra temporale di un giorno
                     .process(ProcessWindowFunction(process_window_function, output_type=Types.STRING()))  # Elaborazione della finestra
                     )

    # Configuration of Sink (KafkaProducer)
    kafka_sink_topic = 'result_topic'
    result_stream.add_sink(FlinkKafkaProducer(
        topic=kafka_sink_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    ))

    # Execute the Flink Job
    env.execute("Flink Streaming Job Q1")

if __name__ == '__main__':
    main()
