#!/bin/bash

#需要建立lzo索引的目录
dir_name=$1
file_name=$2

#hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer /user/hive/warehouse/$dir_name$file_name

have_file=`hadoop fs -ls /user/hive/warehouse/${dir_name}${file_name}`

if [[ $have_file =~ "Found 1 items" ]]; then
    hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer /user/hive/warehouse/${dir_name}${file_name}
    echo "create index success"
else
    echo "don't have this file"
fi



