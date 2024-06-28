FROM python:3.11

RUN apt-get update
RUN apt-get -y install python3-pip

# Imposta la directory di lavoro
WORKDIR /home/producer

# Copia i requirements e installa le dipendenze
COPY config/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY producer.py .
COPY utils.py .

# Copia i dati
COPY data /home/producer/data
