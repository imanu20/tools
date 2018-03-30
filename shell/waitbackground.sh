#! /bin/bash
############################################
#
# Author:
# Create time: 2018  1 06 16时37分29秒
# E-Mail: xiaojian.jia@163.com
# version 1.1
#
############################################
pids=()
timeout 10 bash -c 'sleep 5; echo "$(date +%H:%M:%S): job 1 terminated successfully"' &
pids+=($!)
timeout 2 bash -c 'sleep 5; echo "$(date +%H:%M:%S): job 2 terminated successfully"' &
pids+=($!)
wait "${pids[@]}"
