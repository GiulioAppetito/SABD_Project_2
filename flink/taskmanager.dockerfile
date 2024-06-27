FROM bitnami/flink:latest
COPY config/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY src/ /job
