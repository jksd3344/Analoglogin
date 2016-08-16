#!/bin/bash
##mysql -h120.26.13.167 -uetl_manage -p'1qaz!##DR2' -e "show processlist" |grep -i 'etl_manage'| grep -i "Locked" >> locked_log.txt
mysql -h120.26.1.180 -uetl_manage -p'@J8Xj7be2q9V' -e "show processlist" |grep -i 'etl_manage'| grep -i "Locked" >> locked_log.txt

for line in `cat locked_log.txt | awk '{print $1}'`
do 
   echo "kill $line;" >> kill_thread_id.sql
done

echo "End"