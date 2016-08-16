#!/bin/bash

#同步快照数据到收集机

date=`date '+%Y%m%d'`
game=`cat /etc/sysinfo |awk -F "_" '{print $1}'`
platform=`cat /etc/sysinfo |awk -F "_" '{print $2}'`
sip=`/sbin/ip a|grep "eth0"|grep "inet"|awk '{print $2}'|awk -F "/" '{print $1}' `

dip="172.16.110.249"

info="/data/home/user00/log/datacenter/snap/${date}/role_day_info"
ddir="/data/log_data/"
time=`date '+%H%M'`
################################
for file in `ls $info/*.log`
   do
      if [[ -e $file ]] && [[ -e $file.md5 ]];then
          rsync -avzP $file $dip:${ddir}/${game}/${platform}/${date}/role_day_info/ && rsync -avzP $file.md5 $dip:${ddir}/${game}/${platform}/${date}/role_day_info/
      fi
   done

