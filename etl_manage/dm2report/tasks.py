#!/usr/bin/python
#coding=utf-8

"""
auth: wuqichao、suguoxin
mail: wuqichao@playcrab.com、suguoxin@playcrab.com
create_time: 2016-01-07 12:20:00
used: 用于dm层执行计算report数据

last_update:2016-04-28 14:10:00
"""

from __future__ import absolute_import
import os
import time
import datetime
import random

from dm2report.celery import app
from custom.db.mysql import Custom_MySQL
from custom.db.hive import Custom_Hive

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

'''
将数据仓库中的数据导入到数据集市中，中间可能会涉及清洗数据或合并数据
'''
#, soft_time_limit=7200, time_limit=7201
@app.task(bind=True, max_retries=3, default_retry_delay=6)
def run_task(self, task_param):

    mysql = Custom_MySQL(using='hadoops2')
    mysql_etl = Custom_MySQL(using='etl_manage')
    mysql.begin()
    mysql_etl.begin()
    where = {'id': int(task_param['id'])}
    try:
        hive = Custom_Hive(using='ares_dw')

        game = task_param['game']
        platform = task_param['platform']
        table_name = task_param['table_name']
        log_date = task_param['log_date']
        prefix_sql = task_param['prefix_sql']
        exec_sql = task_param['exec_sql']
        post_sql = task_param['post_sql']
        date_cycle = task_param['date_cycle']
        random_str = str(random.randint(0, 999999999))
        stimes = str(int(time.time()))

        tmp_file_dir = "/tmp/tmp/%s/%s/%s" % (game, platform, log_date)
        #创建本地目录
        if not os.path.exists(tmp_file_dir):
            os.makedirs(tmp_file_dir)

        tmp_file = "%s/%s_%s_%s_%s.txt" % (tmp_file_dir, table_name, date_cycle, random_str, stimes)
        hql_conf = "SET hive.support.concurrency=false;SET hive.exec.compress.output=true;" \
                   "SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; "
        '''
        将任务标示为开始执行：1
        '''
        datas = {'status': 1, 'start_time': str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}
        mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
        mysql_etl.commit()

        #执行前置sql，将数据临时写入本地，用于mysql load数据
        if prefix_sql is not None:
            result = mysql.delete_by_sql(prefix_sql)
            logger.info('exec prefix_sql: delete old data {0}'.format(result['output']))

            if result['status'] == 0:

                '''
                将任务标示为删除临时文件完成：2
                '''
                datas = {'status': 2}
                mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
                mysql_etl.commit()

                '''
                开始执行hive ql,将数据dump到本地
                '''
                result = hive.dump(hql_conf+exec_sql, tmp_file)
                logger.info('exec exec_sql: dump data {0}'.format(result['output']))

                if result['status'] == 0 and True == os.path.exists('%s' % tmp_file):

                    '''
                    将任务标示为dump hive数据完成：3
                    '''
                    datas = {'status': 3, 'tmp_file_name': tmp_file}
                    mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
                    mysql_etl.commit()

                    #执行后置sql
                    if post_sql is not None:
                        post_sql = post_sql.replace('{dir_path}', tmp_file)
                        post_sql = post_sql.replace('{table_name}', task_param['table_name'])
                        post_sql = post_sql.replace('{db_name}', task_param['db_name'])

                        result = mysql.load(post_sql)
                        logger.info('exec post_sql: load data to hdfs {0}'.format(result['output']))

                        if result['status'] == 0:
                            '''
                            将任务标识为录入mysql完成：4
                            '''
                            datas = {'status': 4, 'end_time': str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}
                            mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
                        else:
                            logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], post_sql))
                else:
                    logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], exec_sql))
            else:
                logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], prefix_sql))
                '''
                执行失败，将其状态标为未执行：0
                '''
                datas = {'status': 0}
                mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)

        '''
        将任务标示为：(模拟) 已从任务队列中移除
        '''
        datas = {'in_queue': 0}
        update_result = mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
        # 如果数据库更新失败，再调用一次。 如果还是失败，等待自动修复机制，但这样的概率应该很小了。
        if update_result != 1:
            mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)

        mysql_etl.commit()
        mysql.commit()
        mysql_etl.close()
        mysql.close()

        return True

    except Exception as exc:
        logger.error('dm2report error: %s' % exc)
        mysql_etl.rollback()
        mysql.rollback()

        datas = {'in_queue': 0, 'status': 0}
        mysql_etl.update('dm2report_log', ' id = %(id)d' % where, **datas)
        mysql_etl.commit()

        mysql_etl.close()
        mysql.close()
        raise self.retry(exc=exc, countdown=60)



