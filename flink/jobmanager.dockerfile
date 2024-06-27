FROM flink:latest

# Install Python dependencies
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install apache-flink

# Download Flink Kafka connector
RUN curl -o /opt/flink/lib/flink-connector-kafka_2.11-1.13.2.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka_2.11/1.13.2/flink-connector-kafka_2.11-1.13.2.jar

# Set the working directory to /opt/flink
WORKDIR /opt/flink

# Copy the Flink job to the Flink jobmanager
COPY src/flink_job.py /opt/flink/usrlib/flink_job.py

CMD ["jobmanager"]
