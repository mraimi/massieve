#!/usr/bin/env bash

/usr/local/hadoop/bin/hdfs dfs -rm -r /test/*
/usr/local/hadoop/bin/hdfs dfs -cp /data/unlabeled_sample_numeric /test/

