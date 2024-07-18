TYPE=$1

if [[ "$TYPE" == "compose-post" ]]; then
    ../wrk2/wrk -D exp -t 1 -c 1 -d 1 -L -s ./wrk2/scripts/social-network/compose-post.lua http://localhost:8080/wrk2-api/post/compose -R 1
elif [[ "$TYPE" == "home-timeline" ]]; then
    ../wrk2/wrk -D exp -t 1 -c 1 -d 1 -L -s ./wrk2/scripts/social-network/read-home-timeline.lua http://localhost:8080/wrk2-api/home-timeline/read -R 1
elif [[ "$TYPE" == "user-timeline" ]]; then
    ../wrk2/wrk -D exp -t 1 -c 1 -d 1 -L -s ./wrk2/scripts/social-network/read-user-timeline.lua http://localhost:8080/wrk2-api/user-timeline/read -R 1
fi
