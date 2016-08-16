#!/bin/bash

###
#auth: suguoxin
#mail: suguoxin@playcrab.com
#createtime: 2016-04-07 11:00:00
#usege: 查询mongo，并将结果输出到指定文件，并控制格式
###

day=$1
host=$2
port=$3
db=$4
table=$5
where=$6
field=$7

function dump_data() {

    folder="/mnt/log_data/${day}"

    #如果文件夹不存在，则创建
    if [ ! -d "${folder}" ]; then
        mkdir -p ${folder}
    fi

    #调用mongo命令，导出数据到临时文件，内容格式为："a","b","c"
    /usr/bin/mongoexport -h ${host}:${port} -d ${db} -c ${table} -q ${where} --csv -f ${field} -o ${folder}/${table}_tmp.log

    #删除第一行，第一行为：字段名
    sed -i '1d' ${folder}/${table}_tmp.log

    #替换字符串，并重定向到结果文件
    sed -e 's/"//g;s/,/\t/g' ${folder}/${table}_tmp.log > ${folder}/${table}.log

    #删除临时文件
    rm -f ${folder}/${table}_tmp.log

    #生成MD5文件
    cd ${folder}
    md5sum ${folder}/${table}.log > ${folder}/${table}.log.md5
}

dump_data