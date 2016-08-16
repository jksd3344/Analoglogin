#!/bin/bash

#需要建立lzo索引的目录
dir_name=$1
#file_name=$2

#hadoop fs -rmr /user/hive/warehouse/${dir_name}
have_file=`hadoop fs -ls /user/hive/warehouse/${dir_name}`

if [[ $have_file =~ "Found" ]]; then
    hadoop fs -rmr /user/hive/warehouse/${dir_name}
    echo "delete index success"
else
    echo "no index"
fi