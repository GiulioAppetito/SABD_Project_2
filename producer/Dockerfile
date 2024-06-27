FROM python:3.9-slim

WORKDIR /home/producer

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py producer.py

#CMD ["python", "producer.py"]
