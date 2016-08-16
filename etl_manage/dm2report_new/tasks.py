#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-03-11 15:00:00
used: 用于计算dm层的数据，生成report相关数据

last_update: 2016-06-15 14:55:00
"""

from __future__ import absolute_import
import os
import time
import datetime
import random
import hashlib

from dm2report_new.celery import app
from custom.db.mysql import Custom_MySQL
from custom.db.hive import Custom_Hive
from custom.db.redis_tools import Custom_Redis

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
    redis = Custom_Redis(using='etl_task')

    mysql.begin()
    mysql_etl.begin()

    datas = {'status': 0}
    where = {'id': int(task_param['id'])}

    task_key_exc = ""

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

        task_date = task_param['task_date']
        task_name = task_param['task_name']
        do_rate = task_param['do_rate']

        #task_key_tmp = str(game)+str(platform)+str(task_name)+str(date_cycle)+str(do_rate)+str(log_date)+str(task_date)
        task_key_tmp = str(task_name)+str(date_cycle)+str(do_rate)+str(log_date)+str(task_date)

        task_key_md5 = hashlib.md5()
        task_key_md5.update(task_key_tmp)
        task_key_md5_result = task_key_md5.hexdigest()

        task_key = "dm2report_new_"+str(game)+"_"+str(platform)+"_"+str(task_key_md5_result)
        task_key_exc = task_key

        '''
        celery 本身的机制存在漏洞，会将一个已经完成任务再次分配给其他的worker，致使同一个任务执行多次
        为防止此种现象，在任务开始执行时，将任务的“唯一标示”写入redis中，标注已执行
        '''
        #如果task_key is None, 则表示该条任务没有执行过，正常执行即可
        #如果task_key = 0, 则表示该条任务上次执行失败，允许重复执行
        if redis.get(task_key) == "0" or redis.get(task_key) is None:

            tmp_file_dir = "/tmp/tmp/%s/%s/%s" % (game, platform, log_date)
            #创建本地目录
            if not os.path.exists(tmp_file_dir):
                os.makedirs(tmp_file_dir)

            tmp_file = "%s/%s_%s_%s_%s.txt" % (tmp_file_dir, table_name, date_cycle, random_str, stimes)
            hql_conf = "SET hive.support.concurrency=false;" \
                       "SET hive.exec.compress.output=true;" \
                       "SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec; "
            '''
            将任务标示为开始执行：1
            '''
            datas['status'] = 1
            datas['start_time'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)
            mysql_etl.commit()
            #在redis标注 任务正在执行
            redis.set(task_key, 1)

            #执行前置sql，将数据临时写入本地，用于mysql load数据
            if prefix_sql is not None:
                result = mysql.delete_by_sql(prefix_sql)
                logger.info('exec prefix_sql: delete old data {0}'.format(result['output']))

                if result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], prefix_sql))
                    '''
                    执行失败，将其状态标为未执行：0
                    '''
                    datas['status'] = 0
                    #在redis标注 任务未开始执行
                    redis.set(task_key, 0)
                else:
                    '''
                    将任务标示为删除临时文件完成：2
                    '''
                    datas['status'] = 2
                    datas.pop('start_time')
                    mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)
                    mysql_etl.commit()

                    '''
                    开始执行hive ql,将数据dump到本地
                    '''
                    result = hive.dump(hql_conf+exec_sql, tmp_file)
                    logger.info('exec exec_sql: dump data {0}'.format(result['output']))

                    if result['status'] != 0 or False == os.path.exists('%s' % tmp_file):
                        logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], exec_sql))
                        #在redis标注 任务未开始执行
                        redis.set(task_key, 0)
                    else:

                        '''
                        将任务标示为dump hive数据完成：3
                        '''
                        datas['status'] = 3
                        datas['tmp_file_name'] = tmp_file
                        mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)
                        mysql_etl.commit()

                        #执行后置sql
                        if post_sql is not None:
                            post_sql = post_sql.replace('{dir_path}', tmp_file)
                            post_sql = post_sql.replace('{table_name}', task_param['table_name'])
                            post_sql = post_sql.replace('{db_name}', task_param['db_name'])

                            result = mysql.load(post_sql)
                            logger.info('exec post_sql: load data to hdfs {0}'.format(result['output']))

                            if result['status'] != 0:
                                logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], post_sql))
                                #在redis标注 任务未开始执行
                                redis.set(task_key, 0)
                            else:
                                '''
                                将任务标识为录入mysql完成：4
                                '''
                                datas['status'] = 4
                                datas['end_time'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                datas.pop('tmp_file_name')
                                #在redis标注 任务已成功执行
                                redis.set(task_key, 2)
            else:
                logger.error('prefix_sql is null')
                datas['status'] = 0
                #在redis标注 任务未开始执行
                redis.set(task_key, 0)

        #如果task_key=2, 则标示该条任务已经运行成功
        elif redis.get(task_key) == "2":
            datas['status'] = 4
        #该条任务正在运行中
        else:
            return True

        '''
        将任务标示为：(模拟) 已从任务队列中移除
        '''
        datas['in_queue'] = 0
        update_result = mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)
        # 如果数据库更新失败，再调用一次。 如果还是失败，等待自动修复机制，但这样的概率应该很小了。
        if update_result != 1:
            mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)

        mysql_etl.commit()
        mysql.commit()
        mysql_etl.close()
        mysql.close()

        return True

    except Exception as exc:
        logger.error('dm2report error: %s' % exc)
        mysql_etl.rollback()
        mysql.rollback()
        redis.set(task_key_exc, 0)

        datas = {'in_queue': 0, 'status': 0}
        mysql_etl.update('dm2report_new_log', ' id = %(id)d' % where, **datas)
        mysql_etl.commit()

        mysql_etl.close()
        mysql.close()
        raise self.retry(exc=exc, countdown=60)



