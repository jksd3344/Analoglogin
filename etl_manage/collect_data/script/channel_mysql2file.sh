#!/bin/bash

#要执行的sql文件所在路径
sql_path="/data/collect_data/mostsdk_data"
file_names="channel.sql,app.sql,channel_app.sql"
#当前日期
today=`date +%Y%m%d`
#结果输出的主路径
output_path="/data/log_data/mostsdk/info"
arr=(${file_names//,/ })
for file_name in ${arr[@]}
do
    #判断要输出到的文件夹是否存在，如果不存在则创建
    if [ ! -d "$output_path/$today/${file_name%.*}" ]; then
    	mkdir -p $output_path/$today/${file_name%.*}
    fi
    #连接mysql，执行查询，并将结果按照\t分割输出到文件
    mysql -h120.26.13.150 -P3306 -uread_mostsdk -p'TEST_READ_MOSTSDK' -N < $sql_path/$file_name > $output_path/$today/${file_name%.*}/${file_name%.*}_120.26.13.150_0000.log
    #mysql -h120.26.13.150 -P3306 -uread_mostsdk -p'TEST_READ_MOSTSDK' -N -e "set names 'utf8';select id,name,alias from mostsdk.channel;" > $output_path/$today/${file_name%.*}/${file_name%.*}_120.26.13.150_0000.log
    cd $output_path/$today/${file_name%.*}/
    md5sum ${file_name%.*}_120.26.13.150_0000.log > ${file_name%.*}_120.26.13.150_0000.log.md5
done
