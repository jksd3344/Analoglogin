#!/bin/bash

#要读取的文件所在路径
dir_name=$1
#最终打包生成的文件名
result_file=$2
#逐行检验用，每行文件的列数
field_num=$3
#日志时间点(第几个五分钟)
log_time=$4

#dir_name="/Users/playcrab/Work/script/data/"
#result_file="result_file.txt"
#field_num="33"
#log_time="0005"

#获取路径下的所有文件/文件夹的名字
all_file=`ls ${dir_name} |grep $log_time.txt`
#先生成结果文件，以防止当前时间点没有符合格式的数据时没有结果文件
touch $dir_name$result_file'_'$log_time.txt
#用于记录文件总行数
row_num=0

#遍历所有的文件/文件夹,并逐行读取所有文件的内容
for file in $all_file
do
    #判断是否为当前时间点文件(默认格式为.txt)
#    if [[ $dir_name$file =~ $log_time.txt ]]; then
        #判断是否为文件
        if [ -f $dir_name$file ]; then 
            while read line
            do
                #按照\t拆分
                array=(${line/\t/})
                num=${#array[@]}
                #校验每行内容的列数，如果正确将其写入结果文件，同时记录条数 
                if [ $num -eq $field_num ]; then
                    ((row_num++))
                    echo "$line" >> $dir_name$result_file'_'$log_time.txt
                fi
            done < $dir_name$file
        fi
#    fi
done

#生成记录行数的文件
echo $row_num > $dir_name$result_file'_'$log_time.txt.row

#压缩结果文件 lzo
#lzop -v $dir_name$result_file'_'$log_time.txt

#生成MD5文件
#cd $dir_name
#md5sum $result_file'_'$log_time.txt.lzo > $dir_name$result_file'_'$log_time.txt.lzo.md5


