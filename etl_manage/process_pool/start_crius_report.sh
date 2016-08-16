#!/bin/bash

game="crius"
platform=$1
do_rate=$2

now_time=`date "+%Y-%m-%d %H:%M:%S"`

task_name="exec_dm2report_task"

PIDS=`ps -ef | grep "${task_name}.py ${game} ${platform} ${do_rate}" | grep -v grep | awk '{print $2}'`
if [ "$PIDS" != "" ]; then
    echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task is runing!"
else
    /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${now_time}" >> /tmp/log/${game}_${task_name}.log
    echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
fi
