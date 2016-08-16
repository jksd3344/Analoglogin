#!/bin/bash

###
#auth:suguoxin
#mail:suguoxin@playcrab.com
#createtime:2016-03-12 15:00:00
#usege: 用于启动kof相关任务
###

game="orsk"
do_rate=$1

now_time=`date "+%Y-%m-%d %H:%M:%S"`

for task_name in "exec_download_task" # "exec_file2mysql_task" "exec_file2dw_task"
#for task_name in "exec_etl_data_task"
do

for platform in "JP"
do
{
    PIDS=`ps -ef | grep "${task_name}.py ${game} ${platform} ${do_rate}" | grep -v grep | awk '{print $2}'`
    if [ "$PIDS" != "" ]; then
        echo "[${now_time}] ${task_name} ${game} ${platform} ${do_rate}'s task is runing!"
    else
        jetlag=`/usr/bin/python /data/etl_manage/handle_jetlag.py -g${game} -p${platform}`
        real_time=`date '+%Y-%m-%d %H:%M:%S' -d ${jetlag}' hours'`
        /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${real_time}" >> /tmp/log/${game}_${task_name}.log
        echo "[${real_time}] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
	fi
} &

done
done