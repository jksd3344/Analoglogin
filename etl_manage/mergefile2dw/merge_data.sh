#!/bin/bash

in_path=$1
out_path=$2
file_name=$3
do_rate=$4
log_time=$5

if [ "${do_rate}" == "1day" ]; then
    #合并数据
    cd ${in_path}
    cat ${file_name}_*.txt > ${file_name}_${log_time}_result.txt
    #cat ${in_path}/${file_name}_*.txt > ${out_path}/${file_name}_${log_time}_result.txt
elif [ "${do_rate}" == "1hour" ]; then
    hour=`expr ${log_time:0:2} - 1`

    if [ ${hour} -le 9 ]; then
        files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{print $NF,$0}' |grep -E "^0${hour}"|grep "txt$" |awk '{print $2}'`
    else
        files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{print $NF,$0}' |grep -E "^${hour}"|grep "txt$" |awk '{print $2}'`
    fi

    for file in ${files}
    do
        cat ${in_path}/${file} >> ${out_path}/${file_name}_${log_time}_result.txt
        mv ${in_path}/${file} ${in_path}/cat_${file}
    done

    if [ "$log_time" == "2400" ]; then
        #files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{print $NF,$0}' |grep -E "^2400"|grep "txt$" |awk '{print $2}'`
        files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{if($NF!="result.txt") print $NF,$0}' |awk '{print $2}' |grep -E "^${file_name}"|grep "txt$"`
        for file in ${files}
        do
            cat ${in_path}/${file} >> ${out_path}/${file_name}_${log_time}_result.txt
            mv ${in_path}/${file} ${in_path}/cat_${file}
        done
    fi
elif [ "${do_rate}" == "5min" ]; then
    files=`/bin/ls -l ${in_path} |awk '{print $9}'|awk 'BEGIN{FS="_"}{print $NF,$0}' |grep -E "^${log_time}"|grep "txt$" |awk '{print $2}'`
    for file in ${files}
    do
        cat ${in_path}/${file} >> ${out_path}/${file_name}_${log_time}.txt
    done
fi

#合并之后的数据总条数
cat ${out_path}/${file_name}_${log_time}_result.txt |wc -l > ${out_path}/${file_name}_${log_time}_result.txt.row

#压缩结果文件 lzo
#lzop -v ${out_path}/${file_name}.txt



