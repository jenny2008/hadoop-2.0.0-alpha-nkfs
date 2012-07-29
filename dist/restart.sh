#!/bin/bash
./sbin/stop-dfs.sh
rm -rf /hadoop/*
./bin/hdfs namenode -format
./sbin/start-dfs.sh
./bin/hdfs dfs -mkdir nkfs:///data
./bin/hdfs dfs -put ~/books/143Mkjv.txt nkfs:///data/test
#./bin/hdfs dfs -put ~/movies/cimg-eva.1.0.avi nkfs:///data/test
#./bin/hdfs dfs -put ~/books/pgfmanual.pdf nkfs:///data/test


