FROM bitnami/flink:latest

# Imposta la directory di lavoro
WORKDIR /opt/flink

# Aggiorna i repository e installa Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Crea un collegamento simbolico per Python 3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Copia i requirements e installa le dipendenze
COPY ./config/requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip3 install -r /opt/flink/usrlib/requirements.txt

# Copia il job di Flink
COPY ./src /opt/flink/usrlib

# Configurazione di Flink
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Comando per avviare il JobManager
CMD ["jobmanager"]
