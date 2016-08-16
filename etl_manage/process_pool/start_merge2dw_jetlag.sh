#!/bin/bash

#auth: suguoxin
#mail: suguoxin@playcrab.com
#createtime: 2016-03-31 23:00:00
#usege: 处理时差，合并本地数据录入hdfs启动脚本

#接收参数，执行频次
do_rate=$1
#任务名称
task_name="exec_mergefile2dw_task"
#时间点常量，指早4点30分
RUN_TIME="0430"

#获取所有时差游戏平台
jetlag_tmp=`/usr/bin/python /data/etl_manage/handle_jetlag.py`
#替换所有无用字符
jetlag_text=`echo ${jetlag_tmp} |sed -e "s/'//g;s/ //g;s/\[//g;s/\]//g;"`
#切割后数组长度
len=`echo ${jetlag_text} |awk -F '},{' '{print NF}' |tr -d ' '`

#循环输出每一个值
i=1
while ((${i} <= ${len}))
do
    #切割时差数据组，并替换无用字符
    jetlag_value=`echo ${jetlag_text} |awk -F '},{' '{print $'${i}'}' |tr -d ' ' |sed -e "s/}//g;s/{//g;"`
    i=$((${i}+1))

    #定义变量用于存储解析字典之后的值
    game=""
    platform=""
    jetlag=""

    for result in ${jetlag_value//,/" "}
    do
        if [ "${result%:*}" == "game" ];then
            game=${result#*:}
        elif [ "${result%:*}" == "platform" ];then
            platform=${result#*:}
        else
            jetlag=${result#*:}
        fi
    done
    #echo $game , $platform , $jetlag
    #获取处理时差后的当前平台时间
    platform_real_time=`date '+%H%M' -d ${jetlag}' hours'`
    #如果当前平台时间等于启动时间常量，则开始运行
    if [ "${platform_real_time}" == "${RUN_TIME}" ];then
        PIDS=`ps -ef | grep "${task_name}.py ${game} ${platform} ${do_rate}" | grep -v grep | awk '{print $2}'`
        if [ "$PIDS" != "" ]; then
            echo "[${platform_real_time}] ${task_name} ${game} ${platform} ${do_rate}'s task is runing!"
        else
            /usr/bin/python /data/etl_manage/${task_name}.py ${game} ${platform} ${do_rate} "${platform_real_time}" >> /tmp/log/${game}_${task_name}.log
            echo "[${platform_real_time}] ${task_name} ${game} ${platform} ${do_rate}'s task start-up success."
        fi
    fi
done


