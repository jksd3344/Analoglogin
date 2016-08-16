
#采集游戏基础数据
01 00 * * * /bin/bash /data/collect_data/script/mysql2file.sh /data/collect_data/basic_data > /tmp/log/mysql2file.log
01 00 * * * /bin/bash /data/collect_data/script/channel_mysql2file.sh > /tmp/log/channel_mysql2file.log
*/5 * * * * /usr/bin/python /data/collect_data/script/dumpdata2file.py -n >> /tmp/log/dumpdata.log

*/10 03,04 * * * /bin/bash /data/etl_manage/process_pool/start_web_local.sh 1day >> /tmp/log/start_web_1day.log 2>&1
*/5 * * * * /bin/bash /data/etl_manage/process_pool/start_web_local.sh 5min >> /tmp/log/start_web_5min.log 2>&1

#创建日志文件夹（为运维操作提前准备）
50 23 * * * /bin/bash /data/etl_manage/makedir/mkdir_206.sh

#删除临时数据
01 00 * * * /bin/bash /data/etl_manage/clean_data/delete_log.sh >> /tmp/log/ares_delete_log.log 2>&1
32 00 * * * /bin/bash /data/etl_manage/clean_data/delete_etl_data.sh web >> /tmp/log/web_delete_local_data.log 2>&1