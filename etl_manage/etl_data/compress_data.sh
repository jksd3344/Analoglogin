#!/bin/bash

#文件所在路径
log_dir=$1
#文件名
log_name=$2

#压缩结果文件 lzo
cd $log_dir
lzop -v ${log_name}.txt

