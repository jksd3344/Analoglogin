#!/bin/bash
game=$1
platform=$2
task_name="add_dm2report_new_task"
now_time=$(date "+%Y-%m-%d %H:%M:%S")
bin_dir="/data/etl_manage"
param="";
if [ $# -eq 0 ];then
	echo "[$now_time] game=ALL; platform=ALL;  task start-up success"
elif [ $# -eq 1 ];then
	game=$1
	param=" --game=${game}"
	echo "[$now_time] game=${game}; platform=ALL;  task start-up success"
elif [ $# -eq 2 ];then
	game=$1
	platform=$2
	jetlag=$(python  ${bin_dir}/handle_jetlag.py -g $game -p $platform)
	jetlag=$(expr $jetlag \* 60 \* 60)
	if [ ${jetlag} -ne 0 ];then
		[ ${jetlag} -ge 0 ] && jetlag="+${jetlag}"
		param=" --game=${game} --platform=${platform} --time_zone=${jetlag}"
		echo "[$now_time] game=${game}; platform=${platform}; jetlag=${jetlag} task start-up success"
	else
		param=" --game=${game} --platform=${platform}"
		echo "[$now_time] game=${game}; platform=${platform}; task start-up success"
	fi
fi
/usr/bin/python ${bin_dir}/${task_name}.py ${param}  >> /tmp/log/add_dm2report_task.log 2>&1

