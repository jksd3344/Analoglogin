#!/bin/bash

jar_path=$1
in_file=$2
out_file=$3

hadoop jar ${jar_path}/file2dw/playcrab-hdfs.jar ${in_file} ${out_file}
