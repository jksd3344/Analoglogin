#!/bin/bash

date=`date -d "+1 day" +%Y%m%d`
#date="20151121"

for platform in "gamecomb" "winphone" "xp"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/ares/$platform/$date/$log/
done
done 

chown -R playcrab:playcrab /data/log_data/ares/xp/$date/
chown -R playcrab:playcrab /data/log_data/ares/gamecomb/$date/
chown -R playcrab:playcrab /data/log_data/ares/winphone/$date/

for platform in "appstore" "android"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/hebe/$platform/$date/$log/
done
done

chown -R playcrab:playcrab /data/log_data/hebe/appstore/$date/
chown -R playcrab:playcrab /data/log_data/hebe/android/$date/
