#!/bin/bash

# This script accepts parameters:
# 1. Request type: can be "compose-post", "home-timeline", "user-timeline"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Input parameters.
TYPE=$1

NCONNS_ARRAY=(2)

function build_benchmark {
    docker image rm deathstarbench/social-network-microservices:latest
    docker build --tag=deathstarbench/social-network-microservices .
}

# Monolith.
echo "Running Monolith..."
cd "$DIR"/socialNetwork &> /dev/null
build_benchmark
for max_threads in "${NCONNS_ARRAY[@]}"; do
    bash "$DIR"/socialNetwork/benchmark.sh $TYPE $max_threads
done
cd - &> /dev/null
echo "Running Monolith... Finished!"

# Microservices.
echo "Running Microservices..."
cd "$DIR"/socialNetworkMicroservices &> /dev/null
build_benchmark
for max_threads in "${NCONNS_ARRAY[@]}"; do
    bash "$DIR"/socialNetworkMicroservices/benchmark.sh $TYPE $max_threads
done
cd - &> /dev/null
echo "Running Microservices... Finished!"
