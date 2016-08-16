#!/bin/bash
hive="/usr/bin/hive"
hadoop="/usr/bin/hadoop"


#删除hdfs数据
function delete_hdfs()
{
    hdfs_database=$1
    hive_database=$2
    one_day_ago=$(date -d "-20 day" +'%Y-%m-%d')
    #获取数据库下所平台和日期
    for i in  $(hadoop fs -ls /user/hive/warehouse/$hdfs_database/*/*/|grep '^d'|grep -v 'action_log'|awk '{print $8}'|awk -vds=$one_day_ago 'BEGIN{FS="/";OFS=","}{split($8,a,"=");if(a[2] <=ds)print $0}')
    do
	tablename=$(echo $i|awk 'BEGIN{FS="/"}{split($6,a,"=");print a[2]}')
	platform=$(echo $i|awk 'BEGIN{FS="/"}{split($7,a,"=");print a[2]}')
	log_date=$(echo $i|awk 'BEGIN{FS="/"}{split($8,a,"=");print a[2]}')
	hdfs_path=$i
	#删除hive中partition
	sql="use $hive_database;alter table $tablename drop partition(plat_form='$platform',log_date='$log_date');"
	echo $sql
	#删除hdfs中的数据
	cmd="$hadoop -rmr $hdfs_path"
	echo $cmd
    done
}

#删除磁盘本地数据
function delete_local()
{
    game_name=$1
    one_day_ago=$(date -d "-7 day" +'%Y%m%d')
    #获取所有平台的文件夹
    disks="disk1,disk2,disk3,disk4,disk5,disk6,disk7,disk8,disk9,disk10,disk11"
    arr=(${disks//,/ })
    for disk in ${arr[@]}
    do
	echo $disk
        if [ -d /$disk/data/$game_name/ ];then
            for i in $(ls /$disk/data/$game_name/)
            do
        	path_1="/$disk/data/$game_name/$i/"
                echo "$path_1"
                for log_date in $(ls $path_1|awk -vds=$one_day_ago '{if($1 <= ds)print $1}')
                do
        	        path_2="$path_1"$log_date
         	        if [ -d "$path_2" ];then
        	    	    echo $path_2
        	    	    rm -rf $path_2
        	        fi
            	#获取所有表文件夹
           	        #for j in $(ls $path_2|grep -E -v "action_log|channel|main_category|platform|sub_category")
        	    	#do
        		    #echo "$path_2/$j/"
        	            #ls $path_2/$j/
        	            #rm -rf $(ls $path/$j/)
        	        #done
        	    done
            done
        fi
    done
}

#删除mysql数据
function delete_mysql()
{
    game_name=$1
    database=$2
    result=$(mysql -uroot -p'832ure$DGH98w' -N -e "use $database;show tables;")
    three_day_ago=$(date -d "-1 day" +%Y-%m-%d)
    for table_name in ${result}
    do
        if [ ${game_name} == "ares" ] || [ ${game_name} == "crius" ] || [ ${game_name} == "hebe" ];then
            if [ ${table_name} == "account_info" ];then
      	        sql="delete from $table_name where from_unixtime(first_login_time,'%Y-%m-%d')<='$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
#	        elif [ ${table_name} == "gold_consume_log" ];then
#      	        sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<'$three_day_ago'"
#                echo ${sql}
#                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
	        elif [ ${table_name} == "gold_recharge_log" ];then
      	        sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<='$three_day_ago'"
		        echo ${sql}
		        mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            elif [ ${table_name} == "role_info" ];then
                sql="delete from $table_name where from_unixtime(create_time,'%Y-%m-%d')<='$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            elif [ ${table_name} == "role_login_log" ];then
                sql="delete from $table_name where from_unixtime(login_time,'%Y-%m-%d')<='$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
#            elif [ ${table_name} == "role_uplevel_status" ];then
#                sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<'$three_day_ago'"
#                echo ${sql}
#                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
#            elif [ ${table_name} == "role_upvip_status" ];then
#                sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<'$three_day_ago'"
#                echo ${sql}
#                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
#            elif [ ${table_name} == "second_gold_consume_log" ];then
#                sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<'$three_day_ago'"
#                echo ${sql}
#                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
#            elif [ ${table_name} == "second_gold_recharge_log" ];then
#                sql="delete from $table_name where from_unixtime(log_time,'%Y-%m-%d')<'$three_day_ago'"
#                echo ${sql}
#                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            fi
        elif [ ${game_name} == "mostsdk" ];then
            if [ ${table_name} == "account_info" ];then
                sql="delete from $table_name where substring(first_login_time,1,10)<'$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            elif [ ${table_name} == "account_login_log" ];then
                sql="delete from $table_name where substring(login_time,1,10)<'$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            elif [ ${table_name} == "recharge_mostsdk_log" ];then
                sql="delete from $table_name where substring(create_date,1,10)<'$three_day_ago'"
                echo ${sql}
                mysql -uroot -p'832ure$DGH98w' -e "use $database;$sql;"
            fi
        fi
    done
}
if [ $# -ne 2 ]; then
    echo "[ERROR] Please usage : sh delete_data.sh local game_name"
    echo "[ERROR] Please usage : sh delete_data.sh hdfs game_name"
    echo "[ERROR] Please usage : sh delete_data.sh mysql game_name"
    exit 1
fi
data_type=$1
game_name=$2
#game_name="ares"
#game_name="mostsdk"
hdfs_database=""
database=""
databasee=""
if [ ${game_name} == "ares" ];then
    hdfs_database="ares_dw.db"
    database="ares_dw"
    databasee="realtime_ares"
elif [ ${game_name} == "mostsdk" ];then
    game_name="web"
    hdfs_database="mostsdk.db"
    database="mostsdk"
    databasee="realtime_mostsdk"
elif [ ${game_name} == "hebe" ];then
    hdfs_database="hebe_dw.db"
    database="hebe_dw"
    databasee="realtime_hebe"
elif [ ${game_name} == "crius" ];then
    hdfs_database="crius_dw.db"
    database="crius_dw"
    databasee="realtime_crius"
elif [ ${game_name} == "kof" ];then
    hdfs_database="kof_dw.db"
    database="kof_dw"
    databasee="realtime_kof"
fi
if [ ${data_type} == "local" ];then
    echo "delete local data!"
    delete_local ${game_name}
elif [ ${data_type} == "hdfs" ];then
    delete_hdfs ${hdfs_database} ${database}
elif [ ${data_type} == "mysql" ];then
    delete_mysql ${game_name} ${databasee}
fi
