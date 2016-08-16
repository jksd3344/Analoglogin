#!/bin/bash

game="master"
do_rate=$1

now_time=`date "+%Y-%m-%d %H:%M:%S"`
#echo $now_time

for task_name in "exec_download_task" #"exec_file2mysql_task" # "exec_file2dw_task"
#for task_name in "exec_etl_data_task"
do

for platform in "test"
do
{
	PIDS=`ps -ef | grep "${task_name}.py ${game} ${platform} ${do_rate}" | grep -v grep | awk '{print $2}'`
	if [ "$PIDS" != "" ]; then
    		echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task is runing!"
	else
        /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${now_time}" >> /tmp/log/${game}_${task_name}.log
        echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
	fi
} &

done
done
