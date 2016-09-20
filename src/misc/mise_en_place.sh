#!/usr/bin/env bash

/usr/local/hadoop/bin/hdfs dfs -rm -r /train
/usr/local/hadoop/bin/hdfs dfs -cp /data/labeled_full_numeric /test/

