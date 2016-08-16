#!/bin/bash

if [ $# -ne 2 ]; then
    echo "[ERROR] Please usage : sh mysql2file exec_sql_file output_file"
    exit 1
fi

#要执行的sql文件
exec_sql_file=$1
#输出的文件名(包含路径)
output_file=$2



#连接mysql执行查询并输出到文件
mysql -h120.26.1.250 -P3306 -uetl_manage -p'eGOZdv4|y2iLOiB' -N < $exec_sql_file | tr "TAB" ":" > $output_file


