#!/bin/bash

date=`date -d "+1 day" +%Y%m%d`
#date="20151121"

for platform in "mix" "ios91"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "second_gold_recharge_log" "second_gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/ares/$platform/$date/$log/
done
done 

chown -R playcrab:playcrab /data/log_data/ares/mix/$date/
chown -R playcrab:playcrab /data/log_data/ares/ios91/$date/

for platform in "iostb" "ios91" "iosky" "iospp" "android91"
do
for log in "role_info" "role_login_log" "role_uplevel_status" "role_upvip_status" "gold_recharge_log" "gold_consume_log" "role_day_info" "action_log" "account_info"
do
mkdir -p /data/log_data/hebe/$platform/$date/$log/
done
done

chown -R playcrab:playcrab /data/log_data/hebe/iostb/$date/
chown -R playcrab:playcrab /data/log_data/hebe/ios91/$date/
chown -R playcrab:playcrab /data/log_data/hebe/iospp/$date/
chown -R playcrab:playcrab /data/log_data/hebe/iosky/$date/
chown -R playcrab:playcrab /data/log_data/hebe/android91/$date/
