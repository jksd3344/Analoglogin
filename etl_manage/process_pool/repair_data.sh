#!/bin/bash


game=$1
day_param=$2

# 如果传入参数，仅一天，如果想执行多天最好是修改此处.
# 因为考虑到日期可能不连续 或日期太多，就不当做参数传递了.
# 注意：此处日期格式为：xxxx-xx-xx
if [ "${day_param}" == "" ]; then
    day_list=("2012-01-01" "2012-01-01")
else
    day_list=("${day_param}")
fi

log_list=("role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info")

platform_list=""
if [ "${game}" == "ares" ]; then
    platform_list=("gamecomb" "appstore" "mix" "winphone" "xp" "iosky" "iostb" "ios91" "appstoremix" "appstoretw" "qqandroid" "iospp" "kunlun")
    log_list=("role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "account_info")
elif [ "${game}" == "hebe" ]; then
    platform_list=("android" "appstore" "android91" "iosky" "iostb" "ios91" "iospp" "appstoretw")
elif [ "${game}" == "crius" ]; then
    platform_list=("mix" "andmix" "efun" "efunxm" "tencent" "ourpalm")
elif [ "${game}" == "kof" ]; then
    platform_list=("vietnam" "traditional" "thailand" "korea")
elif [ "${game}" == "orsk" ]; then
    platform_list=("JP")
elif [ "${game}" == "kok" ]; then
    platform_list=("traditional")
fi

#合并本地数据并去除空行，运行在各内网收集机上
function repair_etl() {
    for day_y in ${day_list[@]}
    do
        day=${day_y//-/}
        for platform in ${platform_list[@]}
        do
            for log in ${log_list[@]}
            do
                {
                    if [ "${log}" == "role_day_info" ]; then
                        day=`date -d "${day_y//-/} 1 days" "+%Y%m%d"`
                    fi

                    cd /data/log_data/${game}/${platform}/${day}/${log}/
                    rm -f result_all*

                    cat ${log}_*.log > result_all_tmp.txt

                    cat result_all_tmp.txt |sed -e '/^$/d' > result_all.txt

                    lzop -v result_all.txt

                    echo ${day}_${platform}_${log}
                } &
            done
        done
    done
    wait
    echo "The End"
}

ip=""
disk=""
# 下载各内网收集机的数据到hadoop收集机本地，运行在hadoop收集机上
function repair_download() {

    for day_y in ${day_list[@]}
    do
        day=${day_y//-/}
        for platform in ${platform_list[@]}
        do
            for log in ${log_list[@]}
            do
                {
                    if [ "${log}" == "role_day_info" ]; then
                        day=`date -d "${day_y//-/} 1 days" "+%Y%m%d"`
                    fi

                    get_disk_and_ip ${game} ${platform}

                    if [ ! -d "/${disk}/data/${game}/${platform}/${day}/${log}/" ]; then
                        mkdir -p /${disk}/data/${game}/${platform}/${day}/${log}/
                    fi

                    if [ -f "/${disk}/data/${game}/${platform}/${day}/${log}/result_all.txt.lzo" ]; then
                        rm -f /${disk}/data/${game}/${platform}/${day}/${log}/result_all.txt.lzo
                    fi

                    wget -o wget_log -O /${disk}/data/${game}/${platform}/${day}/${log}/result_all.txt.lzo http://${ip}/${game}/${platform}/${day}/${log}/result_all.txt.lzo

                    echo ${day}_${game}_${platform}_${log}
                } &
            done
        done
    done
    wait
    repair_file2dw
    echo "The End"
}

# 将hadoop收集机本地的数据上传到hdfs上，运行在hadoop收集机上
function repair_file2dw() {
    for day_y in ${day_list[@]}
    do
        day=${day_y//-/}
        for platform in ${platform_list[@]}
        do
            for log in ${log_list[@]}
            do
                {
                    if [ "${log}" == "role_day_info" ]; then
                        day=`date -d "${day_y//-/} 1 days" "+%Y%m%d"`
                    fi

                    get_disk_and_ip ${game} ${platform}

                    /opt/cloudera/parcels/CDH/bin/hive -e "load data local inpath '/${disk}/data/${game}/${platform}/${day}/${log}/result_all.txt.lzo' overwrite into table ${game}_dw.${log} partition (plat_form='$platform',log_date='${day_y}')"
                    hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer /user/hive/warehouse/${game}_dw.db/${log}/plat_form=${platform}/log_date=${day_y}/result_all.txt.lzo

                    echo ${day}_${game}_${platform}_${log}
                } &
            done
        done
    done
    wait
    echo "The End"
}

# 获取游戏、平台对应的内网收集机IP地址及本地存储的磁盘名
function get_disk_and_ip() {

    game=$1
    platform=$2

    if [ "${game}" == "ares" ]; then

        disk="disk1"
        if [ "${platform}" == "gamecomb" ] || [ "${platform}" == "winphone" ] || [ "${platform}" == "xp" ]; then
            ip="120.26.19.206"
        elif [ "${platform}" == "appstore" ] || [ "${platform}" == "iosky" ] || [ "${platform}" == "iostb" ]; then
            ip="120.26.3.95"
        elif [ "${platform}" == "mix" ] || [ "${platform}" == "ios91" ]; then
            ip="120.26.1.224"
        elif [ "${platform}" == "appstoretw" ] || [ "${platform}" == "appstoremix" ]; then
            ip="54.178.87.113"
        elif [ "${platform}" == "qqandroid" ]; then
            ip="119.29.7.244"
        elif [ "${platform}" == "iospp" ]; then
            ip="42.62.67.233"
        elif [ "${platform}" == "kunlun" ]; then
            ip="203.69.146.54"
        fi

    elif [ "${game}" == "hebe" ]; then

        disk="disk2"
        if [ "${platform}" == "appstore" ] || [ "${platform}" == "android" ]; then
            ip="120.26.19.206"
        elif [ "${platform}" == "iostb" ] || [ "${platform}" == "ios91" ] || [ "${platform}" == "iosky" ] || [ "${platform}" == "iospp" ] || [ "${platform}" == "android91" ]; then
            ip="120.26.1.224"
        fi

    elif [ "${game}" == "crius" ]; then

        disk="disk3"
        if [ "${platform}" == "mix" ] || [ "${platform}" == "andmix" ]; then
            ip="120.26.3.95"
        elif [ "${platform}" == "tencent" ]; then
            ip="119.29.7.244"
        elif [ "${platform}" == "efun" ]; then
            ip="122.147.50.230:8080"
        elif [ "${platform}" == "efunxm" ]; then
            ip="103.227.130.114"
        elif [ "${platform}" == "ourpalm" ]; then
            ip="120.27.198.143"
        fi

    elif [ "${game}" == "kof" ]; then

        disk="disk1"
        if [ "${platform}" == "traditional" ]; then
            ip="210.71.227.40"
        elif [ "${platform}" == "korea" ]; then
            ip="121.78.51.72"
        elif [ "${platform}" == "thailand" ]; then
            ip="52.76.74.163"
        elif [ "${platform}" == "vietnam" ]; then
            ip="103.206.212.2"
        fi

    fi
}

repair_etl
#repair_download
#repair_file2dw