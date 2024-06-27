# Usa l'immagine base di Apache Flink di Bitnami come punto di partenza
FROM bitnami/flink:latest

# Aggiorna il gestore dei pacchetti e installa Python e pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install dependencies
COPY config/requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY src/ /job

# Esponi le porte necessarie per il JobManager
EXPOSE 6123 8081

# Comando predefinito per avviare il JobManager
CMD ["jobmanager"]
