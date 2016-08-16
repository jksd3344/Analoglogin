#!/bin/bash

#清理历史数据

logdir="/data/home/user00/log/datacenter/logic"
kuaizhaodir="/data/home/user00/log/datacenter/snap/"
cutlog=`find ${logdir} -type d -mtime +7 |grep '[0-9]'`
delkuaizhao=`find ${kuaizhaodir} -type d -mtime +7 |grep '[0-9]'`

rm -rf $cutlog
rm -rf $delkuaizhao