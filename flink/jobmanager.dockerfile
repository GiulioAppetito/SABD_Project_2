FROM flink:latest
# Aggiungi un file di configurazione per apt con un mirror locale
RUN echo "deb http://archive.ubuntu.com/ubuntu jammy main universe" > /etc/apt/sources.list
# Aggiorna i repository e installa Python
RUN apt-get update -y
RUN apt-get install python3 -y
RUN apt-get update -y
RUN apt-get install python3-pip -y

# Crea un collegamento simbolico per Python 3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Imposta la directory di lavoro
WORKDIR /opt/flink

# Copia i requirements e installa le dipendenze
COPY ./config/requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip3 install -r /opt/flink/usrlib/requirements.txt

# Copia il job di Flink
COPY ./src /opt/flink/usrlib

# Configurazione di Flink
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Comando per avviare il JobManager
CMD ["jobmanager"]
