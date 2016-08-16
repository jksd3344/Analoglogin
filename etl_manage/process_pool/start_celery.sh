#!/bin/bash

#任务名
task_name=$1
#进程数
num=$2
time=`date "+%Y-%m-%d %H:%M:%S"`
minute=`date "+%H%M"`

if [ "$minute" == "0100" ]; then
	kill -9 `ps -ef |grep "celery" |grep -v "start_celery" |grep -v grep |grep "${task_name}" |grep -v "${task_name}_new"|awk '{print $2}'`
	cd /data/etl_manage/
    export C_FORCE_ROOT="true";celery -c ${num} -A ${task_name} worker -l info -Q ${task_name} -f /tmp/log/${task_name}_worker.log
    echo "[$time] $task_name's celery process start-up success."
else
	#获取etl_data进程id
    PIDS=`ps -ef |grep "celery" |grep -v "start_celery" |grep -v grep |grep "${task_name}" |grep -v "${task_name}_new" | awk '{print $0}'`
    if [ "$PIDS" != "" ]; then
    	echo "[$time] $task_name's celery process is running!"
	else
    	cd /data/etl_manage/
    	export C_FORCE_ROOT="true";celery -c ${num} -A ${task_name} worker -l info -Q ${task_name} -f /tmp/log/${task_name}_worker.log
    	echo "[$time] $task_name's celery process start-up success."
	fi
fi
