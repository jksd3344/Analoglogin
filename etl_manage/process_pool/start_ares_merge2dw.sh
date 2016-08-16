#!/bin/bash

game="ares"
do_rate=$1

now_time=`date "+%Y-%m-%d %H:%M:%S"`
now_time_before_8hour=`date '+%Y-%m-%d %H:%M:%S' -d '-8 hours'`
#echo $now_time

for task_name in "exec_mergefile2dw_task"
do

for platform in "gamecomb" "appstore" "mix" "winphone" "xp" "iosky" "iostb" "ios91" "appstoretw" "winphonetw" "qqandroid" "iospp" "kunlun" "appstoremix"
do
{
	PIDS=`ps -ef | grep "${task_name}.py ${game} ${platform} ${do_rate}" | grep -v grep | awk '{print $2}'`
	if [ "$PIDS" != "" ]; then
        echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task is runing!"
	else
	    if [ "${platform}" == "appstoremix" ]; then
            /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${now_time_before_8hour}" >> /tmp/log/${game}_${task_name}.log
            echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
        else
            /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${now_time}" >> /tmp/log/${game}_${task_name}.log
            echo "[$now_time] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
        fi
	fi
} &

done
done
