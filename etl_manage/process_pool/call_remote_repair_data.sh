#!/bin/bash

#游戏
game=$1
#需要修复的数据的日期
data_day=$2

#远程服务器ip列表
ip_list=""
user="root"
port="22"
current_day=`date "+%Y-%m-%d"`

#要执行的远程服务器上的脚本
remote_cmd="/bin/bash /data/etl_manage/process_pool/repair_data_local.sh ${game} ${data_day} > /tmp/repair_data_${current_day}.log 2>&1"

#标记，用来判断是否需要先跳转至hadoops1服务器
flag="0"

#本地通过ssh执行远程服务器上的脚本
function call_remote_cmd() {

    get_ip_by_game ${game}

    for ip in ${ip_list[@]}
    do
    {
        get_user_and_port_by_ip ${ip}

        if [ "flag" == "0" ]; then
            # -f 后台运行，一般与 -n 一起使用
            # -n 将标准输入重定向到 /dev/null，防止读取标准输入
            ssh -f -n -p ${port} -l ${user} ${ip} "${remote_cmd}"
        else
            ssh -f -n -p "22" -l "hadoop" "10.0.0.2" "${remote_cmd}"
        fi

        pid=$(ps aux |grep "ssh -f -n -p ${port} -l ${user} ${ip} ${remote_cmd}" |awk '{print $2}' |sort -n |head -n 1) # 获取进程号

        echo "ssh ${ip} command is running, pid: ${pid}"

        #延迟10秒后执行kill命令，关闭ssh进程
        #时间应依所执行脚本而定，确保此时远程服务器上的脚本已开始执行
        sleep 10 && kill -9 ${pid} && echo "ssh ${ip} command is complete, pid: ${pid}"

    } &
    done
}

#获取游戏收集机ip列表
function get_ip_by_game() {
    game=$1

    if [ "${game}" == "ares" ]; then
        ip_list=("120.26.19.206" "120.26.3.95" "120.26.1.224" "54.178.87.113" "119.29.7.244" "42.62.67.233" "203.69.146.54")

    elif [ "${game}" == "hebe" ]; then
        ip_list=("120.26.19.206" "120.26.1.224")

    elif [ "${game}" == "crius" ]; then
        ip_list=("120.26.19.206" "119.29.7.244" "122.147.50.230" "103.227.130.114" "120.27.198.143")

    elif [ "${game}" == "kof" ]; then
        ip_list=("210.71.227.40" "121.78.51.72" "52.76.74.163" "103.206.212.2")

    elif [ "${game}" == "kok" ]; then
        ip_list=("113.196.114.210")

    elif [ "${game}" == "orsk" ]; then
        ip_list=("52.192.47.91")

    fi
}

#获取收集机的用户
function get_user_and_port_by_ip() {
    remote_ip=$1
    remote_user=""
    remote_port="22"
    flag="1"

    if [ "${remote_ip}" == "210.71.227.40" ] || [ "${remote_ip}" == "121.78.51.72" ] || [ "${remote_ip}" == "52.76.74.163" ]; then
        remote_user="service"

    elif [ "${remote_ip}" == "103.206.212.2" ]; then
        remote_user="user00"

    elif [ "${remote_ip}" == "122.147.50.230" ] || [ "${remote_ip}" == "122.147.50.230" ] || [ "${remote_ip}" == "103.227.130.114" ] || [ "${remote_ip}" == "203.69.146.54" ]; then
        remote_user="playcrab"

    else
        flag="0"
        return

    fi

    remote_cmd="/bin/bash /opt/script/call_remote_repair_data.sh ${remote_user} ${remote_ip} ${remote_port} ${game} ${data_day} > /tmp/call_remote_repair_data_${current_day}.log 2>&1"
}

call_remote_cmd