#!/bin/bash

project_path="/mnt/collect_data"

day=`date '+%Y%m%d'`
today=`date '+%Y-%m-%d'`
beforeday=`date '+%Y-%m-%d' -d '-1 days'`
today_zero=`date -d "${today} 00:00:00" +%s`
beforeday_zero=`date -d "${beforeday} 00:00:00" +%s`

host="localhost"
port="58500"
db="wanpaybill"
table="bill"
where="{'_t':{\$gte:${beforeday_zero},\$lt:${today_zero}}}"
field="_id,uid,username,sec,extern_trade_no,product_id,product_cnt,product_name,product_price,appid,extra_info,source,pid,_t,status,third_trade_no,paytype,paid_t,finish_t"

sh ${project_path}/script/dump_mongo_data.sh ${day} ${host} ${port} ${db} ${table} ${where} ${field}