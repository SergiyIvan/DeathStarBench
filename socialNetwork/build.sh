#!/bin/bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
host_vol=$script_dir
dckr_vol=$script_dir
img=yg397/social-network-microservices:root_cause-del
cmd="mkdir -p $dckr_vol/build; cd $dckr_vol/build && cmake .. && make -j40 && make install && cp -r /usr/local/bin $dckr_vol/"

docker run -v $host_vol:$dckr_vol $img /bin/bash -c "$cmd"
