FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY send_to_kafka.py send_to_kafka.py
COPY data/ data/

CMD ["python", "send_to_kafka.py"]
