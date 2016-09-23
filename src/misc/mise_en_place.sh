#!/usr/bin/env bash

/usr/local/hadoop/bin/hdfs dfs -rm -r /test/*
/usr/local/hadoop/bin/hdfs dfs -rm -r /train/*

/usr/local/hadoop/bin/hdfs dfs -mv /data/unlabeled_full_numeric /train/
/usr/local/hadoop/bin/hdfs dfs -mv /data/unlabeled_test_numeric /test/
/usr/local/hadoop/bin/hdfs dfs -copyToLocal /data/unlabeled_full_numeric /home/ubuntu/opt/realtimeAnomalies/src/main/training/
/usr/local/hadoop/bin/hdfs dfs -copyToLocal /data/unlabeled_test_numeric /home/ubuntu/opt/realtimeAnomalies/src/main/test/

/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /home/ubuntu/opt/realtimeAnomalies/src/main/training/unlabeled_full_numeric /intermediary/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /home/ubuntu/opt/realtimeAnomalies/src/main/test/unlabeled_test_numeric /intermediary/
/usr/local/hadoop/bin/hdfs dfs -mv /intermediary/unlabeled_full_numeric /train/
/usr/local/hadoop/bin/hdfs dfs -mv /intermediary/unlabeled_test_numeric /test/