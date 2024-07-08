#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 1 ]; then
  echo "Usage: ./start-producer.sh <True|False>"
  exit 1
fi

# Validate the argument
if [ "$1" != "True" ] && [ "$1" != "False" ]; then
  echo "Invalid argument. Please use 'True' or 'False'."
  exit 1
fi

# Name of producer service in docker compose
PRODUCER_CONTAINER_NAME=$(docker-compose ps -q producer)

# Check if container is running
if [ "$(docker ps -q -f id=$PRODUCER_CONTAINER_NAME)" ]; then
  echo "Executing script producer.py inside container $PRODUCER_CONTAINER_NAME with parameter $1..."
  # Pass the fast argument to the script
  docker exec -it $PRODUCER_CONTAINER_NAME python /home/producer/producer.py "$1"
else
  echo "Producer container is not running."
fi
