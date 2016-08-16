#!/usr/bin/python
#coding=utf-8

"""
auth: wuqichao、suguoxin
mail: wuqichao@playcrab.com、suguoxin@playcrab.com
create_time: 2015-9-17 10:00:00
used: 用于上传数据到hdfs

last_update: 2016-04-28 14:45:00
"""

from __future__ import absolute_import
import os
import datetime

from file2dw.celery import app
from custom.command import Custom_Command as cmd
from custom.db.mysql import Custom_MySQL
from custom.db.hive import Custom_Hive
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

'''
将文件加载到hive的dw中
'''
@app.task(bind=True, max_retries=5, default_retry_delay=6, soft_time_limit=270, time_limit=280)
def run_task(self, task_param):

    mysql = Custom_MySQL(using='etl_manage')
    mysql.begin()

    where = {'id': int(task_param['id'])}

    try:
        dir_param = {'game': task_param['game'], 'platform': task_param['platform'],
                     'log_date': task_param['log_date'], 'log_name': task_param['log_name']}
        filename_dict = {'log_name': task_param['log_name'], 'log_time': task_param['log_time'], 'source_ip':task_param['source_ip']}
        index_dict = {'db_name': task_param['db_name'], 'table_name': task_param['table_name'], 'platform': task_param['platform'],
                      'log_date': datetime.datetime.strptime(task_param['log_date'], '%Y%m%d').strftime("%Y-%m-%d")}
        partition = {'platform': task_param['platform'], 'log_date': datetime.datetime.strptime(task_param['log_date'], '%Y%m%d').strftime("%Y-%m-%d")}

        log_dir = "/%(game)s/%(platform)s/%(log_date)s/%(log_name)s/" % dir_param

        lzo_file_name = "%(log_name)s_%(source_ip)s_%(log_time)s.txt.lzo" % filename_dict
        index_dir_name = "%(db_name)s.db/%(table_name)s/plat_form=%(platform)s/log_date=%(log_date)s/" % index_dict
        partition_name = "plat_form='%(platform)s',log_date='%(log_date)s'" % partition
        project_path = os.getcwd()

        local_log_dir = '/disk1/tmp_data'+log_dir
        logger.info('local_log_dir: {0}'.format(local_log_dir))

        #判断要录入hive 中的文件知否存在，存在则执行
        if os.path.exists('%s%s' % (local_log_dir, lzo_file_name)):

            '''
            将任务标识为开始执行：1
            '''
            datas = {'load_status': 1}
            mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)

            #执行load之前，删除同名文件，防止同一个文件出现两次的可能
            cmd_remove = '/bin/bash %s/file2dw/remove_damaged_file.sh %s %s' % (project_path, index_dir_name, lzo_file_name)
            logger.info('remove damaged files: {0}'.format(cmd_remove))
            remove_result = cmd.run(cmd_remove)

            if remove_result['status'] != 0:
                logger.error('Error Code %s : %s  Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

            '''
            文件加载到hive中
            '''
            hive = Custom_Hive(using='ares_dw')
        
            load_sql = task_param['load_sql']
            load_sql = load_sql.replace('{dir_path}', local_log_dir+lzo_file_name)
            load_sql = load_sql.replace('{table_name}', task_param['table_name'])
            load_sql = load_sql.replace('{partition_name}', '%s' % partition_name)
            load_sql = load_sql.replace('{db_name}', task_param['db_name'])

            logger.info('hive load SQL: {0}'.format(load_sql))
            result = hive.load(load_sql)
            logger.info('hive load result {0}'.format(result['output']))

            if result['status'] == 0:
                '''
                将任务标识为加载文件完成：2
                '''
                datas = {'load_status': 2}
                mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)

                '''
                建立索引，否则lzo将不支持split
                '''
                #print index_dir_name
                cmd_index = '/bin/bash %s/file2dw/create_lzo_indexer.sh %s %s' % (project_path, index_dir_name, lzo_file_name)
                logger.info('create lzo index: {0}'.format(cmd_index))
                index_result = cmd.run(cmd_index)

                if index_result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (index_result['status'], index_result['output'], cmd_index))
                else:
                    if "create index success" in index_result['output']:
                        '''
                        将任务标识为建立lzo索引完成：3
                        '''
                        datas = {'load_status': 3}
                        mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                    else:
                        '''
                        如果load数据失败，则删除半途出现错误的文件，方式hive查询的时候报错
                        '''
                        cmd_remove = '/bin/bash %s/file2dw/remove_damaged_file.sh %s %s' % (project_path, index_dir_name, lzo_file_name)
                        logger.info('remove damaged files: {0}'.format(cmd_remove))
                        remove_result = cmd.run(cmd_remove)

                        if remove_result['status'] != 0:
                            logger.error('Error Code %s : %s Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

            else:
                '''
                将任务标识为未启动，重新执行：0
                '''
                datas = {'load_status': 0}
                mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)

        '''
        将任务标示为：(模拟) 已从任务队列中移除
        '''
        datas = {'in_queue': 0}
        update_result = mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
        # 如果更新失败，则再调用一次，如果还是失败，则等待自动修复机制，但这种概率很小了
        if update_result != 1:
            mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        mysql.close()
        return True

    except Exception as exc:
        print (exc)
        mysql.rollback()
        
        datas = {'in_queue': 0}
        mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
        mysql.commit()

        mysql.close()
        raise self.retry(exc=exc, countdown=60)
