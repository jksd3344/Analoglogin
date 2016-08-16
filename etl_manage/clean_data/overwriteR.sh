#!/bin/bash
##########
###针对主公和掌门的大R用户,一共保留60天Action_log数据,将第30天后的数据只保留大R用户的Action.
###删除第61天的Action_log数据.
##########
hadoop="/usr/bin/hadoop"
hive="/usr/bin/hive"
function overwrite()
{
    one_day_ago=$(date -d "-1 day" +%Y-%m-%d)
    thirty_day_ago=$(date -d "-31 day" +%Y-%m-%d)
    sixty_day_ago=$(date -d "-60 day" +%Y-%m-%d)
    game_name=$1
    database=$2
    hdfs_database=$3
    echo $sixty_day_ago
    exce_sql $game_name $database $thirty_day_ago
    delete_hdfs $hdfs_database $sixty_day_ago
    add_index $thirty_day_ago
}
function delete_hdfs()
{
    hdfs_database=$1
    sixty_day_ago=$2
    for platform in $($hadoop fs -ls /user/hive/warehouse/$hdfs_database/action_log/|awk '{print $8}'|awk 'BEGIN{FS="/"}{split($7,a,"=");print a[2]}')
    do
        sql="use $database;alter table action_log drop if exists partition (plat_form='$platform',log_date='$sixty_day_ago')"
	echo $sql
	$hive -e "$sql"
        hdfs_path="/user/hive/warehouse/$hdfs_database/action_log/plat_form=$platform/log_date=$sixty_day_ago" 
        echo $hdfs_path
        $hadoop fs -rmr $hdfs_path 
    done
}
function exce_sql()
{
    game_name=$1
    database=$2
    thirty_day_ago=$3
    if [ $game_name == "ares" ];then
        dzm="use $database;set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.compress.output=true;set mapred.output.compress=true;set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec;insert overwrite table action_log partition(plat_form,log_date)
        select uid,platform,dist,role_id,level,vip,ip,system,action,gain,cost,result,device_id,log_time,plat_form,log_date
        from action_log
        where vip >=13 and log_date='$thirty_day_ago' distribute by plat_form"
        echo "$dzm" 
        $hive -e "$dzm"
    elif [ $game_name == "crius" ];then
        crius="use $database;set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.compress.output=true;set mapred.output.compress=true;set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec;insert overwrite table action_log partition(plat_form,log_date)
        select uid,platform,dist,role_id,level,vip,ip,system,action,gain,cost,result,device_id,log_time,plat_form,log_date
        from action_log
        where vip >=15 and log_date='$thirty_day_ago' distribute by plat_form"
        echo "$crius"
        $hive -e "$crius"
    fi
}
function add_index()
{
    thirty_day_ago=$1
    for platform in "gamecomb" "appstore" "mix" "winphone" "xp" "iosky" "iostb" "ios91" "appstoretw" "qqandroid" "iospp" "kunlun" "appstoremix"
    do
	echo $platform
	hdfs_path="/user/hive/warehouse/ares_dw.db/action_log/plat_form=$platform/log_date=$thirty_day_ago"
	$hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer $hdfs_path
    done
}
if [ $# -ne 1 ]; then
    echo "[ERROR] Please usage : sh overwriteR.sh game_name"
    exit 1
fi
game_name=$1
#game_name="ares"
hdfs_database=""
database=""
if [ $game_name == "ares" ];then
    hdfs_database="ares_dw.db"
    database="ares_dw"
elif [ $game_name == "crius" ];then
    hdfs_database="crius_dw.db"
    database="crius_dw"
fi

overwrite $game_name $database $hdfs_database

