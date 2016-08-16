#!/bin/bash

#要读取的文件所在路径
dir_name=$1
#最终打包生成的文件名
result_file=$2
#日志时间点(第几个五分钟)
log_time=$3

#获取路径下的所有文件/文件夹的名字
all_file=`ls ${dir_name} |grep ${log_time:0:2}*.txt`
#先生成结果文件，以防止当前时间点没有符合格式的数据时没有结果文件
touch ${dir_name}${result_file}_${log_time}.txt

#遍历所有的文件/文件夹,并逐行读取所有文件的内容
for file in $all_file
do
	#判断是否为文件
        if [ -f ${dir_name}${file} ]; then 
        	cat ${dir_name}${file} >> ${dir_name}${result_file}_${log_time}.txt
	fi
done

#压缩结果文件 lzo
#lzop -v $dir_name$result_file'_'$log_time.txt


