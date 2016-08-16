#!/bin/bash

date=`date -d "+1 day" +%Y%m%d`
#date="20151121"

for platform in "qqandroid"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/ares/$platform/$date/$log/
done
done 

chown -R playcrab:playcrab /data/log_data/ares/qqandroid/$date/


for platform in "tencent"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/crius/$platform/$date/$log/
done
done

chown -R playcrab:playcrab /data/log_data/crius/tencent/$date/
