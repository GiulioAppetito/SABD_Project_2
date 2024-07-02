# Name of producer service in docker compose
PRODUCER_CONTAINER_NAME=$(docker-compose ps -q producer)

CONSUMER_CONTAINER_NAME=$(docker-compose ps -q consumer)

# Check if container is running
if [ "$(docker ps -q -f id=$PRODUCER_CONTAINER_NAME)" ]; then
  echo "Executing script producer.py inside container $PRODUCER_CONTAINER_NAME..."
  docker exec -it $PRODUCER_CONTAINER_NAME python /home/producer/producer.py
  echo "Starting consumer..."
  docker exec -it $CONSUMER_CONTAINER_NAME python /app/consumer.py
else
  echo "Producer container is not running."
fi