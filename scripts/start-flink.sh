#!/bin/bash

echo "Starting Flink job..."

# Read user input
read -p "Which query do you want to run? (query1/query2/both): " query_choice

case $query_choice in
  query1)
    if docker exec jobmanager /bin/bash -c "/opt/flink/bin/flink run -py /opt/flink/jobs/src/flink_job_q1.py"; then
      echo "Flink job for query1 started successfully!"
    else
      echo "Failed to start Flink job for query1..."
      exit 1
    fi
    ;;
  query2)
    if docker exec jobmanager /bin/bash -c "/opt/flink/bin/flink run -py /opt/flink/jobs/src/flink_job_q2.py"; then
      echo "Flink job for query2 started successfully!"
    else
      echo "Failed to start Flink job for query2..."
      exit 1
    fi
    ;;
  both)
    if docker exec jobmanager /bin/bash -c "/opt/flink/bin/flink run -py /opt/flink/jobs/src/flink_job_q1.py"; then
      echo "Flink job for query1 started successfully!"
    else
      echo "Failed to start Flink job for query1..."
      exit 1
    fi
    if docker exec jobmanager /bin/bash -c "/opt/flink/bin/flink run -py /opt/flink/jobs/src/flink_job_q2.py"; then
      echo "Flink job for query2 started successfully!"
    else
      echo "Failed to start Flink job for query2..."
      exit 1
    fi
    ;;
  *)
    echo "Invalid choice. Please select between query1, query2, or both."
    exit 1
    ;;
esac
