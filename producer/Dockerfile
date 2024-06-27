FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py producer.py
#COPY data/ data/

CMD ["python", "producer.py"]
