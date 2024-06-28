FROM python:3.11

RUN apt-get update

# Imposta la directory di lavoro
WORKDIR /home/producer

# Copia i requirements e installa le dipendenze
COPY config/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY producer.py .
COPY utils.py .

# Copia i dati
COPY data /home/producer/data

# Comando per avviare il producer
CMD ["python", "producer.py"]
