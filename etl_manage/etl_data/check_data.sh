#!/bin/bash

#要读取的文件所在路径
dir_name=$1
#最终打包生成的文件名
file_name=$2
#逐行检验用，每行文件的列数
field_num=$3

row_num=0
#提前建立文件，防止没有符合条件的数据时，无文件生成
touch ${dir_name}/${file_name}.txt

if [ -f ${dir_name}/${file_name}.log ]; then 
	#以\t分割字符串，判断符合格式的输出到结果文件
	cat ${dir_name}/${file_name}.log |awk 'BEGIN{FS="\t";OFS="\t"}{if(NF=='${field_num}') print $0}' > ${dir_name}/${file_name}.txt
	#记录结果文件条数
	cat ${dir_name}/${file_name}.txt |awk 'END{ print NR}' > ${dir_name}/${file_name}.txt.row
fi
