#!/bin/bash

dayy=$1
day=${dayy//-/''}
game="hebe"

function repair_etl() {
    for platform in "android" "appstore" "android91" "iosky" "iostb" "ios91" "iospp" "appstoretw"
    do
        for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
        #for log in "action_log"
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

    for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
    #for log in "action_log"
    do
        {
            wget -o wget_log -O /disk1/tmp_data/${game}/appstore/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/${game}/appstore/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/android/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/${game}/android/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/iostb/${day}/${log}/result_all.txt.lzo http://120.26.1.224/${game}/iostb/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/ios91/${day}/${log}/result_all.txt.lzo http://120.26.1.224/${game}/ios91/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/iosky/${day}/${log}/result_all.txt.lzo http://120.26.1.224/${game}/iosky/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/iospp/${day}/${log}/result_all.txt.lzo http://120.26.1.224/${game}/iospp/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/android91/${day}/${log}/result_all.txt.lzo http://120.26.1.224/${game}/android91/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/${game}/appstoretw/${day}/${log}/result_all.txt.lzo http://54.178.87.113/${game}/appstoretw/${day}/${log}/result_all.txt.lzo

            echo ${platform}_${log}
        } &
    done
    wait
    repair_file2dw
    echo "The End"
}

function repair_file2dw() {

    for platform in "android" "appstore" "android91" "iosky" "iostb" "ios91" "iospp" "appstoretw"
    do
        for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "account_info"
        #for log in "action_log"
        do
            {
                /opt/cloudera/parcels/CDH/bin/hive -e "load data local inpath '/disk1/tmp_data/${game}/${platform}/${day}/${log}/result_all.txt.lzo' into table ${game}_dw.${log} partition (plat_form='$platform',log_date='${dayy}')"
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