#!/bin/bash

project_path="/mnt/collect_data"

day=`date '+%Y%m%d'`
#today=`date '+%Y-%m-%d'`
#today_zero=`date -d "${today} 00:00:00" +%s`

host="localhost"
port="58300"
db="wanpay"
table="paytypes"
where="{}"
field="_id,name,desc,source,paytype,enable,payid,sort"

sh ${project_path}/script/dump_mongo_data.sh ${day} ${host} ${port} ${db} ${table} ${where} ${field}