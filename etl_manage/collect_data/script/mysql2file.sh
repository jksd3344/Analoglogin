#!/bin/bash

if [ $# -ne 1 ]; then
    echo "[ERROR] Please usage : sh mysql2file sql_path"
    exit 1
fi

#要执行的sql文件所在路径
sql_path=$1
#当前日期
today=`date +%Y%m%d`
#结果输出的主路径
output_path="/data/log_data/basic/info"
#需要执行的所有sql文件
all_file=`ls $sql_path`

#遍历所有需要执行的sql文件
for file in $all_file
do
    #判断要输出到的文件夹是否存在，如果不存在则创建
    if [ ! -d "$output_path/$today/${file%.*}" ]; then
        mkdir -p $output_path/$today/${file%.*}
    fi
    #连接mysql，执行查询，并将结果按照\t分割输出到文件
    mysql -h120.26.1.250 -P3306 -uetl_manage -p'eGOZdv4|y2iLOiB' -N < $sql_path/$file > $output_path/$today/${file%.*}/${file%.*}_120.26.1.250_0000.log
    #mysql -h120.26.1.250 -P3306 -uetl_manage -p'eGOZdv4|y2iLOiB' -N < $sql_path/$file | sed -e "s/TAB/:/g" > $output_path/$today/${file%.*}/${file%.*}_120.26.1.250_0000.txt
    cd $output_path/$today/${file%.*}/
    md5sum ${file%.*}_120.26.1.250_0000.log > ${file%.*}_120.26.1.250_0000.log.md5
done

#cd $output_path/$today/${file%.*}/
#md5sum ${file%.*}_120.26.1.250_0000.log > ${file%.*}_120.26.1.250_0000.log.md5
