FROM python:latest

WORKDIR /home/producer

COPY config/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py producer.py

#CMD ["python", "producer.py"]
