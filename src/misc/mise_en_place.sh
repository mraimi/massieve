#!/usr/bin/env bash

/usr/local/hadoop/bin/hdfs dfs -rm -r /test/*
/usr/local/hadoop/bin/hdfs dfs -rm -r /train/*

/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /home/ubuntu/opt/realtimeAnomalies/src/main/training/unlabeled_full_numeric /intermediary/
/usr/local/hadoop/bin/hdfs dfs -mv /intermediary/unlabeled_full_numeric /train/
