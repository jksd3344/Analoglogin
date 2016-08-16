#!/bin/bash

date=`date -d "+1 day" +%Y%m%d`
#date="20151121"

for platform in "appstore" "iosky" "iostb"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/ares/$platform/$date/$log/
done
done 

chown -R playcrab:playcrab /data/log_data/ares/iosky/$date/
chown -R playcrab:playcrab /data/log_data/ares/iostb/$date/
chown -R playcrab:playcrab /data/log_data/ares/appstore/$date/

for platform in "mix" "andmix"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/crius/$platform/$date/$log/
done
done

chown -R playcrab:playcrab /data/log_data/crius/mix/$date/
chown -R playcrab:playcrab /data/log_data/crius/andmix/$date/
