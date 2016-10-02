#!/bin/bash
NUM_SPAWNS=$1
SESSION=$2
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'python /home/ubuntu/dev/massieve/src/main/producer/pykafka_producer.py '"$ID"'' C-m
done
