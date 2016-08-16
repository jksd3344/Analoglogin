#!/bin/bash

#需要建立lzo索引的目录
dir_name=$1
file_name=$2

hadoop fs -rmr /user/hive/warehouse/${dir_name}${file_name}

hadoop fs -rmr /user/hive/warehouse/${dir_name}${file_name}.index