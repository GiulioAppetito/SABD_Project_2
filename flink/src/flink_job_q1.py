import json
import math
import datetime 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.datastream.functions import MapFunction, AggregateFunction, ProcessWindowFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema,JsonRowSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import Time, TumblingEventTimeWindows, GlobalWindows

class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return datetime.datetime.strptime(value[0], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000

def update(existing_aggregate, new_value):
    (count, mean, M2) = existing_aggregate
    count += 1
    delta = new_value - mean
    mean += delta / count
    delta2 = new_value - mean
    M2 += delta * delta2
    return count, mean, M2




# Retrieve the mean, variance and sample variance from an aggregate
def finalize(existing_aggregate):
    (count, mean, M2) = existing_aggregate
    if count < 2:
        return float("nan"),float("nan")
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
                                  ["date","serial_number","model","failure","vault_id","s9_power_on_hours","s194_temperature_celsius"],
                                  [Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING()]
                                ))
                              .build()
                              )

    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_source_topic,
        deserialization_schema=deserialization_schema,
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_consumer_group
        }
    )

    # Producers configuration
    kafka_sink_topic_1d = 'query1_1d_results'

    type_info = Types.ROW_NAMED(
                                  ["vault_id"],
                                  [Types.STRING()]
                                )
    
    serialization_schema = (JsonRowSerializationSchema.Builder()
                            .with_type_info(type_info)
                            .build()
                            )

    kafka_producer_1d = FlinkKafkaProducer(
        topic=kafka_sink_topic_1d,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )

    kafka_stream = env.add_source(kafka_consumer)

    # Watermark strategy definition
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner())

    # Partial stream before windows
    partial_stream = (kafka_stream
                      .filter(lambda x: 1000 <= int(x['vault_id']) <= 1020)
                      .assign_timestamps_and_watermarks(watermark_strategy)
                      .key_by(lambda x: x['vault_id'])
                    )
    
    result_stream_1d = (partial_stream
                        .window(TumblingEventTimeWindows.of(Time.days(1)))
                        .map(func=lambda f:f['vault_id'])
                        .sink_to(kafka_producer_1d)
                        )
    

    # Execute Flink job
    env.execute("Flink Kafka Job 1 test")

if __name__ == '__main__':
    main()