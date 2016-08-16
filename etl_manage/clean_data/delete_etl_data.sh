#!/bin/bash

function delete_local()
{
    game_name=$1
    one_day_ago=$(date -d "-15 day" +'%Y%m%d')
    #获取所有平台的文件夹
    for i in $(ls /data/log_data/${game_name}/)
    do
	    path_1="/data/log_data/${game_name}/${i}/"
        for log_date in $(ls ${path_1}|awk -vds=${one_day_ago} '{if($1 <= ds)print $1}')
        do
            path_2="${path_1}"${log_date}
            if [ -d "${path_2}" ];then
                echo ${path_2}
                rm -rf ${path_2}
            fi
	    done
    done
}

game_name=$1

delete_local ${game_name}