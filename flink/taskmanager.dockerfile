FROM bitnami/flink:1.13.2

# Imposta la directory di lavoro
WORKDIR /opt/flink

# Aggiorna i repository e installa Python
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Crea un collegamento simbolico per Python 3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copia il job di Flink
COPY ./src /opt/flink/usrlib

# Configurazione di Flink
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Comando per avviare il TaskManager
CMD ["taskmanager"]
