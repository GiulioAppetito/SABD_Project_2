# Usa l'immagine base di Apache Flink
FROM flink:latest

# Configurazione di Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Installare le dipendenze
RUN pip3 install apache-flink jproperties psquare tdigest

# Aggiungere dipendenza del connettore Kafka
RUN curl -o /KafkaConnectorDependencies.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

COPY ./src/ /job

# Comando predefinito per avviare il TaskManager
CMD ["taskmanager"]
