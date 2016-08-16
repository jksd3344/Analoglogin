#!/bin/bash

kill -9 `ps -ef |grep celery |grep -v grep |awk '{print $2}'`
#kill -9 `ps -ef |grep exec_ |grep -v grep |awk '{print $2}'`
#kill -9 `ps -ef |grep wget |grep -v grep |awk '{print $2}'`
#清理redis中的数据
#redis-cli keys  "*" | while read LINE ; do TTL=`redis-cli ttl $LINE`; if [ $TTL -eq -1 ]; then echo "Del $LINE"; RES=`redis-cli del $LINE`; fi; done;

cd /data/etl_manage/
export C_FORCE_ROOT="true";celery -c 80 -A download worker -l info -Q download
export C_FORCE_ROOT="true";celery -c 50 -A file2mysql worker -l info -Q file2mysql
export C_FORCE_ROOT="true";celery -c 40 -A mergefile2dw worker -l info -Q mergefile2dw