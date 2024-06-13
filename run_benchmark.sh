#!/bin/bash

# This script accepts parameters:
# 1. Request type: can be "compose-post", "home-timeline", "user-timeline"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Input parameters.
TYPE=$1

function build_benchmark {
    docker image rm deathstarbench/social-network-microservices:latest
    docker build --tag=deathstarbench/social-network-microservices .
}

# Monolith.
echo "Running Monolith..."
cd "$DIR"/socialNetwork &> /dev/null
build_benchmark
bash "$DIR"/socialNetwork/benchmark.sh $TYPE
cd - &> /dev/null
echo "Running Monolith... Finished!"

# Microservices.
echo "Running Microservices..."
cd "$DIR"/socialNetworkMicroservices &> /dev/null
build_benchmark
bash "$DIR"/socialNetworkMicroservices/benchmark.sh $TYPE
cd - &> /dev/null
echo "Running Microservices... Finished!"
