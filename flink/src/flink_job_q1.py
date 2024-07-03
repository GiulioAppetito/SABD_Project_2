from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types

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
                              ))
                              .build())

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
    kafka_sink_topic = 'filtered_hdd_events'

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(["date", "serial_number", "model", "failure", "vault_id", "s9_power_on_hours", "s194_temperature_celsius"],
                        [Types.STRING(), Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
    ).build()

    kafka_producer = FlinkKafkaProducer(
        topic=kafka_sink_topic,
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': kafka_bootstrap_servers}
    )

    # Flusso di dati di esempio
    kafka_stream = env.add_source(kafka_consumer)

    # Filtrare i dati
    filtered_stream = kafka_stream.filter(lambda x: 1000 <= x['vault_id'] <= 1020)
    filtered_stream.add_sink(kafka_producer)

    env.execute("Flink Kafka Filter and Forward Job")

if __name__ == '__main__':
    main()
