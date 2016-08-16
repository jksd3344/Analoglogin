#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-02-29 17:30:00
used: 抽取dw的数据，并将其录入到mart中

last_update: 2016-06-16 15:35:00
"""

from __future__ import absolute_import

import datetime
import os
import hashlib

from dw2dm.celery import app
from custom.command import Custom_Command as cmd
from custom.db.hive import Custom_Hive
from custom.db.mysql import Custom_MySQL
from custom.db.redis_tools import Custom_Redis
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

'''
将数据仓库中的数据导入到数据集市中，中间可能会涉及清洗数据或合并数据
'''
@app.task(bind=True, max_retries=3, default_retry_delay=1 * 6, soft_time_limit=7200, time_limit=7201)
def run_task(self, task_param):

    redis = Custom_Redis(using='etl_task')
    mysql = Custom_MySQL(using='etl_manage')
    mysql.begin()

    datas = {'status': 0}
    where = {'id': int(task_param['id'])}
    hive = Custom_Hive(using='hadoops2')

    task_key_exc = ""

    try:

        game = task_param['game']
        platform = task_param['platform']

        task_dict = {'log_name': task_param['log_name'], 'do_rate': task_param['do_rate'],
                     'log_date': task_param['log_date'], 'task_date': task_param['task_date']}

        task_key_tmp = "%(log_name)s%(do_rate)s%(log_date)s%(task_date)s" % task_dict

        task_key_md5 = hashlib.md5()
        task_key_md5.update(task_key_tmp)
        task_key_md5_result = task_key_md5.hexdigest()

        task_key = "dw2dm_"+str(game)+"_"+str(platform)+"_"+str(task_key_md5_result)
        task_key_exc = task_key

        '''
        celery 本身的机制存在漏洞，会将一个已经完成任务再次分配给其他的worker，致使同一个任务执行多次
        为防止此种现象，在任务开始执行时，将任务的“唯一标示”写入redis中，标注已执行
        '''
        #如果task_key is None, 则表示该条任务没有执行过，正常执行即可
        #如果task_key = 0, 则表示该条任务上次执行失败，允许重复执行
        if redis.get(task_key) == "0" or redis.get(task_key) is None:

            exec_sql = task_param['exec_sql']
            log_date = (datetime.datetime.strptime(task_param['log_date'], '%Y%m%d')).strftime("%Y-%m-%d")

            #prefix_sql = task_param['prefix_sql']
            #post_sql = task_param['post_sql']

            index_dict = {'db_name': task_param['db_name'], 'table_name': task_param['table_name'],
                          'platform': task_param['platform'], 'log_date': log_date}
            index_dir_name = "%(db_name)s.db/%(table_name)s/plat_form=%(platform)s/log_date=%(log_date)s/" % index_dict
            #用于删除索引
            del_index_dir_name = "%(db_name)s.db/%(table_name)s/plat_form=%(platform)s/log_date=%(log_date)s/" % index_dict

            if platform == 'all':
                index_dict = {'db_name': task_param['db_name'], 'table_name': task_param['table_name'],
                              'log_date': log_date}
                #用于建立索引(建立索引时，不能使用*通配符，所以仅指定到表名)
                index_dir_name = "%(db_name)s.db/%(table_name)s/" % index_dict
                #用于删除索引，虽然建立的时候指定到表名，但删除的时候，仅删除当天的
                del_index_dir_name = "%(db_name)s.db/%(table_name)s/*/log_date=%(log_date)s/*" % index_dict

            hql_conf = "set hive.exec.dynamic.partition.mode=nonstrict;" \
                       "set hive.exec.compress.output=true;" \
                       "set mapred.output.compress=true;" \
                       "set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;" \
                       "set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec; "

            #获取项目根路径：/data/etl_manage
            project_path = os.getcwd()

            '''
            将任务标示为开始执行：1
            '''
            datas['status'] = 1
            datas['start_time'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            mysql.update('dw2dm_log', ' id = %(id)d' % where, **datas)
            mysql.commit()
            #在redis标注 任务正在执行
            redis.set(task_key, 1)

            #执行正式的 select xx insert xx 语句
            result = hive.select_insert(hql_conf+exec_sql)
            logger.info('exec exec_sql: select xxx insert xxx {0}'.format(result['output']))

            if result['status'] != 0:
                logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], exec_sql))
                '''
                执行失败，将其状态标为未执行：0
                '''
                datas['status'] = 0
                #在redis标注 任务未执行
                redis.set(task_key, 0)
            else:

                '''
                将任务标示为执行hive ql, select xxx insert xxx完成：2
                '''
                datas['status'] = 2
                datas.pop('start_time')
                mysql.update('dw2dm_log', ' id = %(id)d' % where, **datas)
                mysql.commit()

                '''
                建立索引，否则lzo将不支持split
                '''
                cmd_index = '/bin/bash %s/dw2dm/create_lzo_indexer.sh %s' % (project_path, index_dir_name)
                logger.info('create lzo index: {0}'.format(cmd_index))
                index_result = cmd.run(cmd_index)

                if index_result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (index_result['status'], index_result['output'], cmd_index))
                    #在redis标注 任务未执行
                    redis.set(task_key, 0)
                else:
                    if "create index success" in index_result['output']:
                        '''
                        将任务标识为建立lzo索引完成：3
                        '''
                        datas['status'] = 3
                        datas['end_time'] = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        #在redis标注 任务已完成且成功
                        redis.set(task_key, 2)
                    else:
                        '''
                        如果load数据失败，则删除半途出现错误的文件，防止hive查询的时候报错
                        '''
                        cmd_remove = '/bin/bash %s/dw2dm/remove_damaged_file.sh %s' % (project_path, del_index_dir_name)
                        logger.info('remove damaged files: {0}'.format(cmd_remove))
                        remove_result = cmd.run(cmd_remove)

                        if remove_result['status'] != 0:
                            logger.error('Error Code %s : %s Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))
                            #在redis标注 任务未执行
                            redis.set(task_key, 0)

        #如果task_key=2, 则标示该条任务已经运行成功
        elif redis.get(task_key) == "2":
            datas['status'] = 3
        #该条任务正在运行中
        else:
            return True

        '''
        执行完毕，模拟从队列中清除任务:0
        '''
        datas['in_queue'] = 0
        update_result = mysql.update('dw2dm_log', ' id = %(id)d' % where, **datas)
        # 如果更新失败，则再调用一次，如果还是失败，则等待自动修复机制，但这种概率很小了
        if update_result != 1:
            mysql.update('dw2dm_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        mysql.close()
        return True

    except Exception as exc:
        logger.error('dw2dm_log error: %s' % exc)
        redis.set(task_key_exc, 0)
        mysql.rollback()

        datas = {'in_queue': 0, 'status': 0}
        mysql.update('dw2dm_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        mysql.close()
        raise self.retry(exc=exc, countdown=60)