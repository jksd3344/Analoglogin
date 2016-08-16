#!/bin/bash

###
#auth: suguoxin
#mail: suguoxin@playcrab.com
#createtime: 2016-04-08 10:00:00
#usege: 下载wanpay数据到收集机上
###

day=`date '+%Y%m%d'`
game="wanpay"
platform="info"
url_port="http://112.124.116.44"
nameip="112.124.116.44"
log_time="0000"

#下载
function wget_data() {

    log_name=$1

    folder="/data/log_data/${game}/${platform}/${day}/${log_name}"
    file="${folder}/${log_name}_${nameip}_${log_time}.log"

    #如果要下载的文件已经存在，则不执行
    if [ ! -f "${file}" ]; then
        #如果文件夹不存在，则创建
        if [ ! -d "${folder}" ]; then
            mkdir -p ${folder}
        fi

        #正式下载，先下载MD5文件，再下载数据文件
        wget -o /tmp/log/wgetwanpay.log -c ${url_port}/${day}/${log_name}.log.md5 -O ${folder}/${log_name}_${nameip}_${log_time}.log.md5
        if [ "$?" == "0" ]; then
            wget -o /tmp/log/wgetwanpay.log -c ${url_port}/${day}/${log_name}.log -O ${folder}/${log_name}_${nameip}_${log_time}.log

            #校验MD5，如果失败删除文件
            md5_value=`md5sum ${folder}/${log_name}_${nameip}_${log_time}.log |cut -d ' ' -f 1`
            result=`cat ${folder}/${log_name}_${nameip}_${log_time}.log.md5 |grep "${md5_value}"`
            if [ "${result}" == "" ]; then
                rm -f  ${folder}/${log_name}_${nameip}_${log_time}.log*
            fi
        fi
    else
        echo "File already exists, do not need to download."
    fi


}

#启动下载
function start() {

    log_name_list=("bill" "apps" "paytypes")

    for log_name in ${log_name_list[@]}
    do {
        wget_data ${log_name}
    } &
    done
    wait
    echo "The End"
}

start