#!/bin/bash

dayy=$1
day=${dayy//-/''}
game="kof"

function repair_etl() {
    for platform in "korea" "traditional"
    do
        #for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
        for log in "gold_recharge_log"
        do
            {
                cat /data/log_data/${game}/${platform}/${day}/${log}/${log}_*.log > /data/log_data/${game}/${platform}/${day}/${log}/result_all.txt

                cd /data/log_data/${game}/${platform}/${day}/${log}/
                lzop -v result_all.txt

                echo ${platform}_${log}
            } &
        done
    done
    wait
    echo "The End"
}

function repair_download() {

    #for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
    for log in "gold_recharge_log"
    do
        {
            wget -o wget_log -O /disk1/data/kof/korea/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/kof/korea/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/data/kof/traditional/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/kof/traditional/${day}/${log}/result_all.txt.lzo

            echo ${platform}_${log}
        } &
    done
    wait
    repair_file2dw
    echo "The End"
}

function repair_file2dw() {

    for platform in "korea" "traditional"
    do
        #for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
        for log in "gold_recharge_log"
        do
            {
                /opt/cloudera/parcels/CDH/bin/hive -e "load data local inpath '/disk1/data/${game}/${platform}/${day}/${log}/result_all.txt.lzo' into table ${game}_dw.${log} partition (plat_form='$platform',log_date='${dayy}')"
                hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer /user/hive/warehouse/${game}_dw.db/${log}/plat_form=${platform}/log_date=${dayy}/result_all.txt.lzo

                echo ${platform}_${log}
            } &
        done
    done
    wait
    echo "The End"
}


repair_etl
#repair_download
#repair_file2dw