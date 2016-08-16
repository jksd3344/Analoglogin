#!/bin/bash

#文件所在路径
log_dir=$1
#文件名
log_name=$2

#生成MD5文件
cd $log_dir
md5sum ${log_name}.txt.lzo > ${log_dir}/${log_name}.txt.lzo.md5

