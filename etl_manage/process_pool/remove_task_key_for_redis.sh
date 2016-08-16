#!/bin/bash

###
#auth: suguoxin
#mail: suguoxin@playcrab.com
#createtime: 2016-06-15 16:16:00
#usege: 我们应用的celery 3.1.18版本，本身的机制存在漏洞，会将一个已经完成的任务再次分配给其他的worker，致使同一个任务执行多次
#       为防止此种现象，在任务开始执行时，会将任务的“唯一标示”写入redis中，标注已执行
#       此脚本为了迎合这件事而存在，目的是：用于删除redis中的任务“唯一标示”
###

game=$1
platform=$2
#任务类型
task_type=$3

#允许的任务类型：
param1="dw2dm"
param2="dm2report_new"

now_time=`date "+%Y-%m-%d %H:%M:%S"`

#该类型数据存储在 数据库：2
if [ $# == 0 ]; then
    redis-cli -n 2 flushdb
    echo "[${now_time}] clean db success"
elif [ $# == 1 ]; then
    redis-cli -n 2 keys "*_${game}_*" |xargs redis-cli -n 2 del
    echo "[${now_time}] clean ${game}_key success"
elif [ $# == 2 ]; then
    redis-cli -n 2 keys "*_${game}_${platform}_*" |xargs redis-cli -n 2 del
    echo "[${now_time}] clean ${game}_${platform}_key success"
else
    if [ "${task_type}" != "${param1}" ] && [ "${task_type}" != "${param2}" ]; then
        echo "[${now_time}] The third argument must be in the ('${param1}', '${param2}')"
    else
        redis-cli -n 2 keys "${task_type}_${game}_${platform}_*" |xargs redis-cli -n 2 del
        echo "[${now_time}] clean ${game}_${platform}_${task_type}_key success"
    fi
fi



