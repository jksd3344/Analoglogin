#!/bin/bash

#要读取的文件所在路径
dir_name=$1
#最终打包生成的文件名
file_name=$2
#逐行检验用，每行文件的列数
field_num=$3

#提前建立文件，防止没有符合条件的数据时，无文件生成
#touch ${dir_name}/${file_name}_0000.txt

cd ${dir_name}/

tmp=`ls |grep "${file_name}_" |head -5`

if [ "${tmp}" != "" ]; then
    #以\t分割字符串，判断符合格式的输出到结果文件
    cat ${file_name}_*.log |awk 'BEGIN{FS="\t";OFS="\t"}{if(NF=='${field_num}') print $0}' > ${file_name}_0000.txt
    #记录结果文件条数
    cat ${file_name}_0000.txt |awk 'END{ print NR}' > ${file_name}_0000.txt.row
fi

