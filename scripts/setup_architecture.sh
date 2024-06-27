#!/bin/bash

# Variables
DOCKER_NETWORK=project2-network
DATASET_RELATIVE_PATH=./producer/src/data

usage() {
    echo "Usage:"
    echo "       ./setup-architecture.sh --start: Starts the whole architecture."
    echo "       ./setup-architecture.sh --stop: Stops the whole architecture."
}

run_docker_compose_up() {
    echo "Starting the architecture..."
    docker-compose up -d
}

run_docker_compose_down() {
    echo "Stopping the architecture..."
    docker-compose down
}

execute() {
    if [ "$1" = "--help" ]; then
        usage
        exit 0
    elif [ "$1" = "--start" ]; then
        run_docker_compose_up
    elif [ "$1" = "--stop" ]; then
        run_docker_compose_down
    else
        usage
    fi
}

# Execute the script
execute $@
