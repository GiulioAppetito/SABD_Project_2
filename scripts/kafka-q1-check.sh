docker exec -it kafka /bin/bash -c "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic query1_1d_results --from-beginning"