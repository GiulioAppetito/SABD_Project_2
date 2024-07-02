import json
import math
import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import Time, TumblingEventTimeWindows, GlobalWindows
from pyflink.datastream.triggers import CountTrigger


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(datetime.datetime.strptime(value['date'], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000)


def update(existing_aggregate, new_value):
    (count, mean, M2) = existing_aggregate
    count += 1
    delta = new_value - mean
    mean += delta / count
    delta2 = new_value - mean
    M2 += delta * delta2
    return count, mean, M2


def finalize(existing_aggregate):
    (count, mean, M2) = existing_aggregate
    if count < 2:
        return float("nan"), float("nan")
    else:
        (mean, variance, sample_variance) = (mean, M2 / count, M2 / (count - 1))
        return mean, math.sqrt(variance)


class TemperatureAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0, 0.0

    def add(self, value, accumulator):
        return update(accumulator, float(value['s194_temperature_celsius']))

    def get_result(self, accumulator):
        return accumulator


class ComputeStatistics(ProcessWindowFunction):
    def process(self, key, context: ProcessWindowFunction.Context, elements, out):
        count, mean, M2 = next(iter(elements))
        mean, stddev = finalize((count, mean, M2))
        window = context.window()
        result = Row(window.start, key, count, mean, stddev)
        out.collect(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Consumer configuration
    kafka_source_topic = 'hdd_events'
    kafka_consumer_group = 'flink_consumer_group'
    kafka_bootstrap_servers = 'kafka:9092'

    deserialization_schema = (JsonRowDeserializationSchema.Builder()
                              .type_info(Types.ROW_NAMED(
        ["date", "serial_number", "model", "failure", "vault_id", "s9_power_on_hours", "s194_temperature_celsius"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
    ))
                              .build())

    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group
        }
    )

    # Topics for each window result
    kafka_sink_topic_1d = 'query1_1d_results'
    kafka_sink_topic_3d = 'query1_3d_results'
    kafka_sink_topic_global = 'query1_global_results'

    serialization_schema = (JsonRowSerializationSchema.Builder()
                            .with_type_info(Types.ROW_NAMED(
        ["ts", "vault_id", "count", "mean_s194", "stddev_s194"],
        [Types.LONG(), Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT()]
    ))
                            .build())

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

    # Source stream from kafka
    kafka_stream = env.add_source(kafka_consumer)

    # Watermark strategy definition
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())

    # Partial stream before windows
    partial_stream = (kafka_stream
                      .filter(lambda x: 1000 <= int(x['vault_id']) <= 1020)
                      .assign_timestamps_and_watermarks(watermark_strategy)
                      )

    # 1-day window
    (partial_stream
     .key_by(lambda x: x['vault_id'])
     .window(TumblingEventTimeWindows.of(Time.days(1)))
     .aggregate(TemperatureAggregateFunction(), ComputeStatistics())
     .map(lambda row: json.dumps({
        "ts": row[0],
        "vault_id": row[1],
        "count": row[2],
        "mean_s194": row[3],
        "stddev_s194": row[4]
    }))
     .add_sink(kafka_producer_1d))

    # 3-days window
    (partial_stream
     .key_by(lambda x: x['vault_id'])
     .window(TumblingEventTimeWindows.of(Time.days(3)))
     .aggregate(TemperatureAggregateFunction(), ComputeStatistics())
     .map(lambda row: json.dumps({
        "window_start": row[0],
        "vault_id": row[1],
        "count": row[2],
        "mean": row[3],
        "stddev": row[4]
    }))
     .add_sink(kafka_producer_3d))

    # Global window
    (partial_stream
     .key_by(lambda x: x['vault_id'])
     .window(GlobalWindows.create())
     .trigger(CountTrigger.of(1))  # To ensure that each record is processed
     .aggregate(TemperatureAggregateFunction(), ComputeStatistics())
     .map(lambda row: json.dumps({
        "window_start": row[0],
        "vault_id": row[1],
        "count": row[2],
        "mean": row[3],
        "stddev": row[4]
    }))
     .add_sink(kafka_producer_global))

    env.execute("Flink Kafka Job 1 test")


if __name__ == '__main__':
    main()
