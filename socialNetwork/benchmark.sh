#!/bin/bash

# This script accepts parameters:
# 1. Request type: can be "compose-post", "home-timeline", "user-timeline"
# 2. <Optional> Max threads/connections to use.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DURATION=30

# Input parameters.
TYPE=$1
MAX_THREADS=$2
if [[ -z "$MAX_THREADS" ]]; then
    MAX_THREADS=52  # Default value for max threads and connections.
fi
OUTPUT_DIR="/tmp/nconns_$MAX_THREADS/benchmark_sn_res_mono"
RPS_ARRAY=
if [[ "$MAX_THREADS" -eq 1 ]]; then
    RPS_ARRAY=(125 150 175 200 225 250 275)
elif [[ "$MAX_THREADS" -eq 2 ]]; then
    RPS_ARRAY=(150 200 225 250 275 300 325 350)
elif [[ "$MAX_THREADS" -eq 4 ]]; then
    RPS_ARRAY=(150 200 225 250 275 300 325 350)
elif [[ "$MAX_THREADS" -eq 8 ]]; then
    RPS_ARRAY=(150 200 225 250 275 300 325 350)
elif [[ "$MAX_THREADS" -eq 16 ]]; then
    RPS_ARRAY=(250 500 750 1000)
elif [[ "$MAX_THREADS" -eq 32 ]]; then
    RPS_ARRAY=(500 1000 2000 4000)
else
    echo "Unknown MAX_THREADS value: $MAX_THREADS."
    exit 1
fi


function run_benchmark {
    url=$1
    rps=$2  # Requests per Second.
    threads=$3
    conns=$4
    iteration=$5

    output_file="$OUTPUT_DIR"/res_"$rps"_"$iteration"
    logs_file="$OUTPUT_DIR"/logs_"$rps"_"$iteration"

    "$DIR"/../wrk2/wrk -t $threads -c $conns -d $DURATION -L --u_latency $url -R $rps &> $output_file
    sleep 1
    docker logs socialnetwork-compose-post-service-1 >& $logs_file
}

# Prepare output directory.
rm -r $OUTPUT_DIR &> /dev/null
mkdir -p $OUTPUT_DIR

if [[ "$TYPE" == "compose-post" ]]; then
    URL="http://localhost:8080/wrk2-api/post/compose"
elif [[ "$TYPE" == "home-timeline" ]]; then
    URL="http://localhost:8080/wrk2-api/home-timeline/read?user_id=90&start=0&stop=10"
elif [[ "$TYPE" == "user-timeline" ]]; then
    URL="http://localhost:8080/wrk2-api/user-timeline/read?user_id=90&start=0&stop=10"
fi

cd "$DIR" &> /dev/null

for rps in "${RPS_ARRAY[@]}"; do
    echo "*** Running for RPS=$rps. ***"

    threads=$rps
    conns=$rps
    # Make sure not to create more than MAX_THREADS threads.
    if [ "$threads" -gt "$MAX_THREADS" ]; then
        threads=$MAX_THREADS
        conns=$MAX_THREADS
    fi

    for iteration in $(seq 1 5); do
        echo "Starting Social Network..."
        docker-compose up -d &> /dev/null
        sleep 5

        python3 "$DIR"/scripts/init_social_graph.py --graph=socfb-Reed98 --compose
        echo "Populating Social Network... Done!"

        sleep 0.5
        echo "Running benchmark..."
        run_benchmark $URL $rps $threads $conns $iteration

        echo "Terminating Social Network..."
        docker-compose down &> /dev/null
    done
    echo "*** Finished for RPS=$rps. ***"
done

echo "Finished the experiment. Check the logs at $OUTPUT_DIR"

cd - &> /dev/null
