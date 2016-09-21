#!/usr/bin/env bash

/usr/local/hadoop/bin/hdfs dfs -rm -r /test/*
/usr/local/hadoop/bin/hdfs dfs -rm -r /train/*
/usr/local/hadoop/bin/hdfs dfs -cp /data/unlabeled_full_numeric /train/
/usr/local/hadoop/bin/hdfs dfs -cp /data/unlabeled_test_numeric /test/


