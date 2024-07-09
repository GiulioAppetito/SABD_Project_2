#!/bin/bash

echo "Starting Flink job..."

usage() {
  echo "Usage: $0 [--job1|--job2|--both] [--1d|--3d|--all|--all_three] [true|false]"
  exit 1
}

if [ $# -lt 2 ]; then
  usage
fi

run_flink_job() {
  job_name=$1
  script_path=$2
  window_type=$3
  flag=$4
  echo "Executing Flink job: $job_name with window type: $window_type"
  if docker exec -d jobmanager /bin/bash -c "/opt/flink/bin/flink run -py $script_path $window_type $flag"; then
    echo "Flink job for $job_name started successfully!"
  else
    echo "Failed to start Flink job for $job_name..."
    exit 1
  fi
}

run_consumer() {
  echo "Running Kafka consumer..."
  if docker exec consumer /bin/bash -c "python /app/src/consumer.py $@"; then
    echo "Kafka consumer started successfully!"
  else
    echo "Failed to start Kafka consumer..."
    exit 1
  fi
}

job_type=$1
window_type=$2
window=$(echo "$2" | sed 's/--//g')
flag=$3


case $job_type in
  job1)
    run_flink_job "query1" "/opt/flink/jobs/src/flink_job_q1.py" "$window" "$flag"
    ;;
  job2)
    run_flink_job "query2" "/opt/flink/jobs/src/flink_job_q2.py" "$window" $flag
    ;;
  both)
    run_flink_job "query1" "/opt/flink/jobs/src/flink_job_q1.py" "$window" $flag
    run_flink_job "query2" "/opt/flink/jobs/src/flink_job_q2.py" "$window" $flag
    ;;
  *)
    usage
    exit 1
    ;;
esac
case $window_type in
      1d)
        run_consumer "query1_1d_results" "query2_1d_results"
        ;;
      3d)
        run_consumer "query1_3d_results" "query2_3d_results"
        ;;
      all)
        run_consumer "query1_all_results" "query2_all_results"
        ;;
      all_three)
        run_consumer "query1_1d_results" "query1_3d_results" "query1_all_results" "query2_1d_results" "query2_3d_results" "query2_all_results"
        ;;
      *)
        echo "Consumer did not start up correctly"
        ;;
esac
