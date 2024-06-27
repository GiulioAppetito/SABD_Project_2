FROM python:3.8-slim
WORKDIR /app
COPY config/requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY src/ .
CMD ["python", "producer.py"]
