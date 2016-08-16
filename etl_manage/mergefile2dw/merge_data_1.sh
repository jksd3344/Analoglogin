#!/bin/bash

in_path=$1
out_path=$2
file_name=$3
do_rate=$4
log_time=$5

if [ "${do_rate}" == "1day" ]; then
    #合并数据
    #cat ${in_path}/${file_name}_*.txt > ${out_path}/${file_name}_${log_time}.txt
    files=`/bin/ls -l ${in_path} |awk '{print $9}' |grep "^${file_name}" |grep "txt$"`

    for file in ${files}
    do
        touch ${out_path}/result_${file_name}_${log_time}.txt
        cat ${in_path}/${file} >> ${out_path}/result_${file_name}_${log_time}.txt
        mv ${in_path}/${file} ${in_path}/cat_${file}
    done
elif [ "${do_rate}" == "1hour" ]; then
    files=`/bin/ls -l ${in_path} |awk '{print $9}' |grep "^${file_name}" |grep "txt$"`

    for file in ${files}
    do
        touch ${out_path}/result_${file_name}_${log_time}.txt
        cat ${in_path}/${file} >> ${out_path}/result_${file_name}_${log_time}.txt
        mv ${in_path}/${file} ${in_path}/cat_${file}
    done

elif [ "${do_rate}" == "5min" ]; then
    files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{print $NF,$0}' |grep -E "^${log_time}"|grep "txt$" |awk '{print $2}'`
    for file in ${files}
    do
        cat ${in_path}/${file} >> ${out_path}/${file_name}_${log_time}.txt
    done
fi

#合并之后的数据总条数
cat ${out_path}/result_${file_name}_${log_time}.txt |wc -l > ${out_path}/result_${file_name}_${log_time}.txt.row

#压缩结果文件 lzo
#lzop -v ${out_path}/${file_name}.txt



