#!/bin/bash

game="kof"
do_rate=$1

#vietnam 差1小时(比我们晚一个小时)，延时1小时执行合并
now_time=`date "+%Y-%m-%d %H:%M:%S"`
#echo $now_time

for task_name in "exec_mergefile2dw_task"
do

for platform in "vietnam"
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


