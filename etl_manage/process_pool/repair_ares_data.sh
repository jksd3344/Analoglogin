#!/bin/bash

dayy=$1
day=${dayy//-/''}
game="ares"

function repair_etl() {
    for platform in "gamecomb" "appstore" "mix" "winphone" "xp" "iosky" "iostb" "ios91" "appstoremix" "appstoretw" "winphonetw" "qqandroid" "iospp" "kunlun"
    do
        for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "account_info"
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

    for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "account_info"
    #for log in "action_log"
    do
        {
            wget -o wget_log -O /disk1/tmp_data/ares/gamecomb/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/ares/gamecomb/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/winphone/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/ares/winphone/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/xp/${day}/${log}/result_all.txt.lzo http://120.26.19.206/log/ares/xp/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/appstore/${day}/${log}/result_all.txt.lzo http://120.26.3.95/ares/appstore/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/iosky/${day}/${log}/result_all.txt.lzo http://120.26.3.95/ares/iosky/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/iostb/${day}/${log}/result_all.txt.lzo http://120.26.3.95/ares/iostb/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/mix/${day}/${log}/result_all.txt.lzo http://120.26.1.224/ares/mix/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/ios91/${day}/${log}/result_all.txt.lzo http://120.26.1.224/ares/ios91/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/appstoretw/${day}/${log}/result_all.txt.lzo http://54.178.87.113/ares/appstoretw/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/appstoremix/${day}/${log}/result_all.txt.lzo http://54.178.87.113/ares/appstoremix/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/winphonetw/${day}/${log}/result_all.txt.lzo http://54.178.87.113/ares/winphonetw/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/qqandroid/${day}/${log}/result_all.txt.lzo http://119.29.7.244/ares/qqandroid/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/iospp/${day}/${log}/result_all.txt.lzo http://42.62.67.233/ares/iospp/${day}/${log}/result_all.txt.lzo
            wget -o wget_log -O /disk1/tmp_data/ares/kunlun/${day}/${log}/result_all.txt.lzo http://203.69.146.54/ares/kunlun/${day}/${log}/result_all.txt.lzo

            echo ${platform}_${log}
        } &
    done
    wait
    repair_file2dw
    echo "The End"
}

function repair_file2dw() {

    for platform in "gamecomb" "appstore" "mix" "winphone" "xp" "iosky" "iostb" "ios91" "appstoremix" "appstoretw" "winphonetw" "qqandroid" "iospp" "kunlun"
    do
        for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "account_info"
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