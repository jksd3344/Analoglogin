#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2015-10-12 15:00:00
used: 用于清洗游戏原始日志数据(校验、压缩、MD5)

last_update:2016-04-28 14:47:00
"""

from __future__ import absolute_import
import os

from etl_data.celery import app
from custom.command import Custom_Command as cmd
from custom.db.mysql import Custom_MySQL
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

'''
启动对游戏日志原始文件的清洗，由此脚本完成
'''
@app.task(bind=True, max_retries=5, default_retry_delay=6, soft_time_limit=270, time_limit=280)
def run_task(self, task_param):

    mysql = Custom_MySQL(using='etl_manage')
    mysql.begin()
    datas = {'etl_status': 0}
    where = {'id': int(task_param['id'])}

    try:
        log_param = {'game': task_param['game'], 'platform': task_param['platform'],
                     'log_date': task_param['log_date'], 'log_name': task_param['log_name'],
                     'log_dir': task_param['log_dir'], 'col_num': task_param['col_num']}
        log_name_param = {'log_name': task_param['log_name'], 'source_ip': task_param['source_ip'], 'log_time': task_param['log_time']}

        do_rate = task_param['do_rate']
        flag = task_param['flag']

        log_dir = '%(log_dir)s/%(game)s/%(platform)s/%(log_date)s/%(log_name)s' % log_param
        log_name = '%(log_name)s_%(source_ip)s_%(log_time)s' % log_name_param
        log_name_notime = '%(log_name)s_%(source_ip)s' % log_name_param
        col_num = task_param['col_num']

        project_path = os.getcwd()

        #判断如果有md5文件和数据文件同时存在，则开始执行
        if (do_rate == "1day" and flag == "log") or (os.path.exists('%s/%s.log.md5' % (log_dir, log_name)) is True and os.path.exists('%s/%s.log' % (log_dir, log_name)) is True):

            #排除同名文件存在的可能，同时为修复执行提供方便
            if os.path.exists('%s/%s.txt' % (log_dir, log_name)):
                cmd_remove = 'rm -f %s/%s.txt*' % (log_dir, log_name)
                logger.info('remove history file: {0}'.format(cmd_remove))
                remove_result = cmd.run(cmd_remove)
                if remove_result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

            '''
            将任务标识为开始执行：1
            '''
            datas['etl_status'] = 1
            mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
            mysql.commit()

            '''
            校验数据
            '''
            #如果是“天”频次的日志数据，则特殊处理，直接将同一ip的合成一个文件，同时执行校验
            if do_rate == "1day" and flag == "log":
                cmd_merge = '/bin/bash %s/etl_data/merge_data.sh %s %s %s' % (project_path, log_dir, log_name_notime, col_num)
                logger.info('check data: {0}'.format(cmd_merge))
                merge_result = cmd.run(cmd_merge)
            else:
                cmd_merge = '/bin/bash %s/etl_data/check_data.sh %s %s %s' % (project_path, log_dir, log_name, col_num)
                logger.info('check data: {0}'.format(cmd_merge))
                merge_result = cmd.run(cmd_merge)

            if merge_result['status'] != 0:
                logger.error('Error Code %s : %s  Cmd: %s' % (merge_result['status'], merge_result['output'], cmd_merge))
                datas['etl_status'] = 0
            else:
                '''
                读取校验格式后的文件总条数
                '''
                row = open('%s/%s.txt.row' % (log_dir, log_name)).read()

                '''
                将文件总条数写入数据库，并将任务标识为为校验已完成：2
                '''
                datas['etl_status'] = 2
                datas['row_num'] = int(row)
                #datas = {'etl_status': 2, 'row_num': int(row)}
                mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                mysql.commit()

                '''
                压缩数据
                '''
                cmd_compress = '/bin/bash %s/etl_data/compress_data.sh %s %s' % (project_path, log_dir, log_name)
                logger.info('compress data: {0}'.format(cmd_compress))
                compress_result = cmd.run(cmd_compress)
                if compress_result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (compress_result['status'], compress_result['output'], cmd_compress))
                    datas['etl_status'] = 0
                else:
                    '''
                    将任务标识为压缩完成：4
                    '''
                    datas['etl_status'] = 4
                    datas.pop('row_num')
                    mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                    mysql.commit()

                    '''
                    生成MD5文件
                    '''
                    cmd_md5 = '/bin/bash %s/etl_data/md5_data.sh %s %s' % (project_path, log_dir, log_name)
                    logger.info('md5 data: {0}'.format(cmd_md5))
                    md5_result = cmd.run(cmd_md5)

                    if md5_result['status'] != 0:
                        logger.error('Error Code %s : %s  Cmd: %s' % (md5_result['status'], md5_result['output'], cmd_md5))
                        datas['etl_status'] = 0
                    else:
                        '''
                        将任务标识为生成MD5完成(即为校验、合并、压缩均已完成)：6
                        '''
                        datas['etl_status'] = 6

        '''
        执行完毕，模拟从队列中清楚任务:0
        '''
        datas['in_etl_queue'] = 0
        update_result = mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
        # 如果更新失败，则再调用一次，如果还是失败，则等待自动修复机制，但这种概率很小了
        if update_result != 1:
            mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        mysql.close()
        return True

    except Exception as exc:
        logger.error('etl_data error: %s' % exc)
        mysql.rollback()

        datas = {'in_etl_queue': 0, 'etl_status': 0}
        mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
        mysql.commit()

        mysql.close()
        raise self.retry(exc=exc, countdown=30)
