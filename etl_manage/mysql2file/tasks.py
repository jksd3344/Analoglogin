#!/usr/bin/python
#coding=utf-8

"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2015-9-17 10:00:00
usege: 用于hadoop下载文件任务操作

"""


from __future__ import absolute_import
import os
import uuid
import time
import random
import celery
import datetime


from mysql2file.celery import app
from custom.command import Custom_Command as cmd
from custom.db.mysql import Custom_MySQL
from custom.db.hive import Custom_Hive


'''
将文件加载到hive的dw中
'''
@app.task(bind=True,max_retries=3,default_retry_delay=1 * 6)
def run_task(self, task_param):



    mysql = Custom_MySQL(using='etl_manage')
    mysql.begin()

    try:

        '''
        业务代码块放下方
        '''
        dir_param ={'game':task_param['game'],
                    'platform':task_param['platform'],
                    'log_date':task_param['log_date'],
                    'log_name':task_param['log_name']}

        filename_dict = {'log_name':task_param['log_name'],'log_time':task_param['log_time']}
        
        '''
        游戏＼平台＼日期＼业务日志名＼日志或者md5文件
        '''
        log_dir =  "/%(game)s/%(platform)s/%(log_date)s/%(log_name)s/" % dir_param

        lzo_file_name = "%(log_name)s_%(log_time)s.txt"% filename_dict

        local_log_dir = '/tmp'+log_dir


        dump_sql = task_param['dump_sql']
        dump_sql = dump_sql.replace('{table_name}',task_param['table_name'])
        dump_sql = dump_sql.replace('{partition_name}',task_param['partition_name'])
        dump_sql = dump_sql.replace('{db_name}',task_param['db_name'])
        print(dump_sql)

        result = mysql.dump(sql,local_log_dir+lzo_file_name)
        #print(result)



        '''
        将任务标识为加载文件完成：2
        '''
        datas = {'load_status':2}
        where = {}
        where['id'] = int(task_param['id'])

        mysql.update('etl_data_log',
                          ' id = %(id)d' % where,
                            **datas)
        mysql.commit()
        mysql.close()
        return True

    except Exception as exc:
        print (exc)
        mysql.rollback()
        raise self.retry(exc=exc, countdown=60)
