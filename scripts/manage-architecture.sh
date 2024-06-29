#!/bin/bash

# Variables
DOCKER_NETWORK=project2-network

usage() {
    echo "Usage:"
    echo "       ./setup-architecture.sh --start: Starts the whole architecture."
    echo "       ./setup-architecture.sh --stop: Stops the whole architecture."
    echo "       ./setup-architecture.sh --restart: Restarts the whole architecture."
}

run_docker_compose() {
    echo "Starting the architecture..."
    docker-compose up -d
}

stop_docker_compose() {
    echo "Stopping the architecture..."
    docker-compose down
}

execute() {
    if [ "$1" = "--help" ]; then
        usage
        exit 0
    elif [ "$1" = "--start" ]; then
        run_docker_compose
    elif [ "$1" = "--stop" ]; then
        stop_docker_compose
    elif [ "$1" = "--restart" ]; then
        stop_docker_compose
        run_docker_compose
    else
        usage
    fi
}

# Execute the script
execute $@
