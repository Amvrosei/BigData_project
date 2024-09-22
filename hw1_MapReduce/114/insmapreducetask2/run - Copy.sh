#! /usr/bin/env bash

OUT_DIR="out_tmp_1"

NUM_REDUCERS=8
  
hdfs dfs -rm -r -skipTrash ${OUT_DIR}.tmp > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="Task114_step1" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "/data/wiki/en_articles" \
    -output ${OUT_DIR}.tmp > /dev/null

hdfs dfs -rm -r -skipTrash ${OUT_DIR}.tmp > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Task114_step2" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.separator=\t \
    -D mapreduce.partition.keycomparator.options=-k1,1nr \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -files sort.py \
    -mapper cat \
    -reducer "python3 sort.py" \
    -input ${OUT_DIR}.tmp \
    -output ${OUT_DIR} > /dev/null
 
hdfs dfs -cat ${OUT_DIR}/part-00000 | head -n 10



