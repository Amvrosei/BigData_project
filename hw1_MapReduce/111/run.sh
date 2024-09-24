#!/usr/bin/env bash
 
OUT_DIR="out_dir"
NUM_REDUCERS=8
 
hdfs dfs -rm -r -skipTrash ${OUT_DIR}.tmp > /dev/null
 
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Task111 counting" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /data/wiki/en_articles \
    -output ${OUT_DIR}.tmp > /dev/null
 
 
hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null
 
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Task111 sorting" \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.reduces=1 \
    -D map.output.key.field.separator=\t \
    -D mapreduce.partition.keycomparator.options='-k1,1nr' \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -files sort.py \
    -mapper /bin/cat \
    -reducer "python3 sort.py" \
    -input ${OUT_DIR}.tmp \
    -output ${OUT_DIR} > /dev/null
 
hdfs dfs -cat ${OUT_DIR}/part-00000 | head -n 10

