# Usa l'immagine base di Apache Flink di Bitnami come punto di partenza
FROM bitnami/flink:latest

# Pulire e aggiornare la cache dei pacchetti, quindi installare Python e pip
RUN apt-get update -y
RUN apt install python3 -y
RUN apt-get update -y
RUN apt-get install python3-pip -y

# Install dependencies
COPY config/requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY src/ /job

# Comando predefinito per avviare il TaskManager
CMD ["taskmanager"]
