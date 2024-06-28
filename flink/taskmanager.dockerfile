FROM apache/flink:1.13.2-scala_2.11

# Imposta la directory di lavoro
WORKDIR /opt/flink

# Aggiorna i repository e installa Python
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Crea un collegamento simbolico per Python 3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copia il job di Flink
COPY ./src /opt/flink/usrlib

# Configurazione di Flink per Prometheus
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Comando per avviare il TaskManager
CMD ["taskmanager"]
