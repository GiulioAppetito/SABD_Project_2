FROM flink


# Imposta la directory di lavoro
WORKDIR /opt/flink

# Aggiorna i repository e installa Python
RUN apt-get update -y && apt-get install python3 -y
RUN apt-get update -y && apt-get install python3-pip -y

# Crea un collegamento simbolico per Python 3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copia i requirements e installa le dipendenze
COPY ./config/requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip3 install -r /opt/flink/usrlib/requirements.txt

# Install Kafka connector for Flink
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

# Copia il job di Flink
COPY src /opt/flink/jobs/src

# Expose ports
EXPOSE 6123 8081

# Configurazione di Flink
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Comando per avviare il TaskManager
CMD ["taskmanager"]
