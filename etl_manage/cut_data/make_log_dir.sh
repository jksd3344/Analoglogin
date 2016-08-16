#!/bin/bash

#创建任务所需的文件夹

account_info="account_info"
ation="action_log"
login="role_login_log"
uplevel="role_uplevel_status"
info="role_info"
upvip="role_upvip_status"
gold_recharge="gold_recharge_log"
gold_consume="gold_consume_log"
second_gold_recharge="second_gold_recharge_log"
second_gold_consume="second_gold_consume_log"
role_day_info="role_day_info"
fileList=($role_day_info $account_info $login $uplevel $info $upvip $gold_recharge $gold_consume $second_gold_recharge $second_gold_consume $ation)

targetDir="/data/log_data"
dateTomorrow=$(date -d "1 day" "+%Y%m%d")
game=$(cat /etc/sysinfo |awk -F "_" '{print $1}')
platform=$(cat /etc/sysinfo |awk -F "_" '{print $2}')

for i in ${fileList[@]};do
    if [ ! -d ${targetDir}/${dateTomorrow}/${i} ];then
        mkdir -p ${targetDir}/${game}/${platform}/${dateTomorrow}/${i}
        mkdir -p /data/home/user00/log/datacenter/snap/${dateTomorrow}/${role_day_info}
    fi
done