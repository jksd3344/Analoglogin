#!/bin/bash

#切割日志文件，生成5分钟时间点文件，并同步到收集机

# 日志目录
logPath="/data/home/user00/log/datacenter/logic"

# 切割行数存放目录
lineDir="/data/home/user00/log/datacenter/logic/tmp"

# 目标目录
targetDir="/data/log_data"

# 获取外网ip
wIp=$(cat /home/playcrab/ip.txt)

# 时间日期
yesterday=$(date "+%Y%m%d" -d yesterday)
fileyesterday=$(date "+%Y-%m-%d" -d yesterday)
dateNameDir=$(date "+%Y%m%d")
dateTime=$(date "+%H%M")
dateFormat=$(date "+%Y-%m-%d")
dateTimeLog=$(date "+%Y/%m/%d %H:%M")

# 获取游戏名和平台
game=`cat /etc/sysinfo |awk -F "_" '{print $1}'`
platform=`cat /etc/sysinfo |awk -F "_" '{print $2}'`

# 收集机ip
dip="172.16.110.249"

# 日志列表
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
fileList=($account_info $login $uplevel $info $upvip $gold_recharge $gold_consume $second_gold_recharge $second_gold_consume $ation)

# 检查存储处理后的目录与每天零点主动创建生产log文件
function checkFileDir(){
    if [[ "${dateTime}" -eq 0000 ]] || [[ ! -d "${logPath}/${dateNameDir}" ]];then
        for i in ${fileList[@]};do
            mkdir -p "${logPath}/${dateNameDir}/${i}"
        done
    fi


    if [[ "${dateTime}" -ge 0000 ]] && [[ "${dateTime}" -lt 0005 ]];then
        for x in ${fileList[@]};do
            touch "${logPath}/${x}_${dateFormat}.log"
        done
    fi
}

# 记录行数切割日志并发送到收集机
function cutSendFile(){
    for list in ${fileList[@]};do
        if [[ "${dateTime}" -eq "0000" ]];then
            echo "[${dateTimeLog}] Record_line: 0" >> ${lineDir}/${list}_${dateFormat}.txt
            echo  "" > ${logPath}/${dateNameDir}/${list}/${list}_${wIp}_${dateTime}.log
        else
            lineNum=$(wc -l ${logPath}/${list}_${dateFormat}.log | cut -d" " -f1)
            echo "[${dateTimeLog}] Record_line: ${lineNum}" >> ${lineDir}/${list}_${dateFormat}.txt
            oldline=$(tail -2 ${lineDir}/${list}_${dateFormat}.txt |awk 'NR==1{print $4}')
            newline=$(tail -1 ${lineDir}/${list}_${dateFormat}.txt |awk '{print $4}')
            prline=$(expr ${oldline} + 1)

            sed -n "${prline}, ${newline}p" ${logPath}/${list}_${dateFormat}.log > ${logPath}/${dateNameDir}/${list}/${list}_${wIp}_${dateTime}.log
            md5sum ${logPath}/${dateNameDir}/${list}/${list}_${wIp}_${dateTime}.log > ${logPath}/${dateNameDir}/${list}/${list}_${wIp}_${dateTime}.log.md5
        fi

        if [[ "${dateTime}" -eq "0000" ]];then

            yesttday=$(tail -1 ${lineDir}/${list}_${fileyesterday}.txt |awk '{print $4}')
            ttfile=`wc -l ${logPath}/${list}_${fileyesterday}.log |awk 'END{print}'|awk '{print $1}'`
            echo "[${dateTimeLog}] Record_line: ${ttfile}" >> ${lineDir}/${list}_${fileyesterday}.txt
            epline=$(expr ${yesttday} + 1)

            sed -n "$epline,$ttfile p" ${logPath}/${list}_${fileyesterday}.log > ${logPath}/${yesterday}/${list}/${list}_${wIp}_2400.log
            md5sum ${logPath}/${yesterday}/${list}/${list}_${wIp}_2400.log > ${logPath}/${yesterday}/${list}/${list}_${wIp}_2400.log.md5
            rsync -avzP  ${logPath}/${yesterday}/${list}/ ${targetDir}/${game}/${platform}/${yesterday}/${list}
        fi

        rsync -avzP ${logPath}/${dateNameDir}/${list}/ ${targetDir}/${game}/${platform}/${dateNameDir}/${list}
    done
}

function main(){
    checkFileDir
    cutSendFile
}
main