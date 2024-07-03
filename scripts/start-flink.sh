echo "Starting Flink job..."
if docker exec jobmanager /bin/bash -c "/opt/flink/bin/flink run -py /opt/flink/jobs/src/flink_job_q1.py";then
  echo "Flink job for q1 started succesfully!"
else
  echo "Failed to start Flink job for q1..."
  exit 1
fi