#!/bin/bash

# This script accepts parameters:
# 1. Request type: can be "compose-post", "home-timeline", "user-timeline"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DURATION=60
MAX_THREADS=52
OUTPUT_DIR=/tmp/benchmark_sn_res_mono

# Input parameters.
TYPE=$1

function run_benchmark {
    lua_script=$1
    url=$2
    rps=$3  # Requests per Second.
    threads=$4
    conns=$5

    output_file="$OUTPUT_DIR"/res_"$rps"

    "$DIR"/../wrk2/wrk -D exp -t $threads -c $conns -d $DURATION -L -s $lua_script $url -R $rps &> $output_file
}

# Prepare output directory.
rm -r $OUTPUT_DIR &> /dev/null
mkdir -p $OUTPUT_DIR

if [[ "$TYPE" == "compose-post" ]]; then
    LUA_SCRIPT="$DIR"/wrk2/scripts/social-network/compose-post.lua
    URL="http://localhost:8080/wrk2-api/post/compose"
elif [[ "$TYPE" == "home-timeline" ]]; then
    LUA_SCRIPT="$DIR"/wrk2/scripts/social-network/read-home-timeline.lua
    URL="http://localhost:8080/wrk2-api/home-timeline/read"
elif [[ "$TYPE" == "user-timeline" ]]; then
    LUA_SCRIPT="$DIR"/wrk2/scripts/social-network/read-user-timeline.lua
    URL="http://localhost:8080/wrk2-api/user-timeline/read"
fi

cd "$DIR" &> /dev/null

for rps in 100 200 400 600 800 1000 2000 3000 4000 5000 6000 7000 8000; do
    echo "Running for RPS=$rps"

    threads=$rps
    conns=$rps
    # Make sure not to create more than MAX_THREADS threads.
    if [ "$threads" -gt "$MAX_THREADS" ]; then
        threads=$MAX_THREADS
        conns=$MAX_THREADS
    fi

    echo "Starting Social Network..."
    docker-compose up -d &> /dev/null

    echo "Running benchmark..."
    run_benchmark $LUA_SCRIPT $URL $rps $threads $conns

    echo "Terminating Social Network..."
    docker-compose down &> /dev/null
    echo "Finished."
done

echo "Finished the experiment. Check the logs at $OUTPUT_DIR"

cd - &> /dev/null
