#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2015-10-12 15:00:00
usege: 用于合并本地数据，并加载到hdfs中

last_update:2016-04-28 14:49:00
"""

from __future__ import absolute_import
import os
import datetime

from mergefile2dw.celery import app
from custom.command import Custom_Command as cmd
from custom.db.mysql import Custom_MySQL
from custom.db.hive import Custom_Hive
from custom.db.redis_tools import Custom_Redis
from celery.utils.log import get_task_logger

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

logger = get_task_logger(__name__)

'''
启动对游戏日志原始文件的清洗，由此脚本完成
'''
#, soft_time_limit=600, time_limit=601
@app.task(bind=True, max_retries=5, default_retry_delay=6)
def run_task(self, task_param):

    mysql = Custom_MySQL(using='etl_manage')
    hive = Custom_Hive(using='ares_dw')
    redis = Custom_Redis(using='etl_manage')
    mysql.begin()
    where = {'id': int(task_param['id'])}

    try:
        log_date = datetime.datetime.strptime(task_param['log_date'], '%Y%m%d').strftime("%Y-%m-%d")
        log_date_1 = (datetime.datetime.strptime(task_param['log_date'], '%Y%m%d')-datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        dir_param = {'game': task_param['game'], 'platform': task_param['platform'],
                     'log_date': task_param['log_date'], 'log_name': task_param['log_name']}
        filename_dict = {'log_name': task_param['log_name'], 'log_time': task_param['log_time']}
        index_dict = {'db_name': task_param['db_name'], 'table_name': task_param['table_name'],
                      'platform': task_param['platform'], 'log_date': log_date}
        partition = {'platform': task_param['platform'], 'log_date': log_date}

        index_dict_1 = {'db_name': task_param['db_name'], 'table_name': task_param['table_name'],
                        'platform': task_param['platform'], 'log_date': log_date_1}
        partition_1 = {'platform': task_param['platform'], 'log_date': log_date_1}

        log_dir = "/%(game)s/%(platform)s/%(log_date)s/%(log_name)s" % dir_param

        flag = task_param['flag']

        file_name = "%(log_name)s" % filename_dict
        file_name_txt = "%(log_name)s_%(log_time)s_result.txt" % filename_dict
        file_name_lzo = "%(log_name)s_%(log_time)s_result.txt.lzo" % filename_dict
        file_name_row = "%(log_name)s_%(log_time)s_result.txt.row" % filename_dict
        index_dir_name = "%(db_name)s.db/%(table_name)s/plat_form=%(platform)s/log_date=%(log_date)s/" % index_dict
        partition_name = "plat_form='%(platform)s',log_date='%(log_date)s'" % partition

        project_path = os.getcwd()

        log_time = task_param['log_time']
        do_rate = task_param['do_rate']

        #if flag == "snap" or (do_rate == "1day" and flag == "log")
        if flag == "snap":
            index_dir_name = "%(db_name)s.db/%(table_name)s/plat_form=%(platform)s/log_date=%(log_date)s/" % index_dict_1
            partition_name = "plat_form='%(platform)s',log_date='%(log_date)s'" % partition_1

        #从redis中，获取当前数据对应存储到哪块磁盘
        if redis.get("disk_xml") is None:
            disk_tmp = open('/data/etl_manage/conf/disk_game.xml', 'r')
            redis.set("disk_xml", str(disk_tmp.read()))

        disk_list = str(redis.get("disk_xml"))
        root = ET.fromstring(disk_list)
        disk = ""
        for gameinfo in root.findall('game'):
            if gameinfo.get('name') == task_param['game']:
                disk = gameinfo.get('disk')
                continue

        #local_log_dir = '/disk1/tmp_data'+log_dir
        local_log_dir = '/'+disk+'/data'+log_dir
        logger.info('local_log_dir: {0}'.format(local_log_dir))

        #判断目录是否存在
        if os.path.exists('%s' % local_log_dir):

            #排除同名文件存在的可能，同时为修复执行提供方便
            if os.path.exists('%s/%s' % (local_log_dir, file_name_txt)):
                cmd_remove = 'rm -f %s/%s*' % (local_log_dir, file_name_txt)
                logger.info('remove history file: {0}'.format(cmd_remove))
                remove_result = cmd.run(cmd_remove)
                if remove_result['status'] != 0:
                    logger.error('Error Code %s : %s  Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

            datas = {'load_status': 1}
            mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
            mysql.commit()

            cmd_merge = '/bin/bash %s/mergefile2dw/merge_data.sh %s %s %s %s %s' \
                        '' % (project_path, local_log_dir, local_log_dir, file_name, do_rate, log_time)
            logger.info('merge data: {0}'.format(cmd_merge))
            merge_result = cmd.run(cmd_merge)
            logger.info('merge data result {0}'.format(merge_result['output']))

            if merge_result['status'] == 0:

                #读取总条数
                row = open('%s/%s' % (local_log_dir, file_name_row)).read()

                '''
                合并数据完成：2
                '''
                datas = {'load_status': 2, 'row_num': int(row)}
                mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                mysql.commit()

                '''
                压缩数据
                '''
                cmd_compress = '/bin/bash %s/mergefile2dw/compress_data.sh %s %s' % (project_path, local_log_dir, file_name_txt)
                logger.info('compress data: {0}'.format(cmd_compress))
                compress_result = cmd.run(cmd_compress)
                if compress_result['status'] != 0:
                    logger.error('Error Code %s : %s Cmd: %s' % (compress_result['status'], compress_result['output'], cmd_compress))
                else:
                    '''
                    压缩数据完成：3
                    '''
                    datas = {'load_status': 3}
                    mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                    mysql.commit()

                    #执行load之前，删除同名文件，防止同一个文件出现两次的可能
                    cmd_remove = '/bin/bash %s/mergefile2dw/remove_damaged_file.sh %s %s' % (project_path, index_dir_name, file_name_lzo)
                    logger.info('remove damaged files: {0}'.format(cmd_remove))
                    remove_result = cmd.run(cmd_remove)

                    if remove_result['status'] != 0:
                        logger.error('Error Code %s : %s  Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

                    '''
                    文件加载到hive中
                    '''
                    load_sql = task_param['load_sql']
                    load_sql = load_sql.replace('{dir_path}', local_log_dir+"/"+file_name_lzo)
                    load_sql = load_sql.replace('{table_name}', task_param['table_name'])
                    load_sql = load_sql.replace('{partition_name}', '%s' % partition_name)
                    load_sql = load_sql.replace('{db_name}', task_param['db_name'])

                    logger.info('hive load SQL: {0}'.format(load_sql))
                    result = hive.load(load_sql)
                    logger.info('hive load result {0}'.format(result['output']))

                    if result['status'] == 0:
                        '''
                        将任务标识为加载文件完成：4
                        '''
                        datas = {'load_status': 4}
                        mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                        mysql.commit()

                        '''
                        建立索引，否则lzo将不支持split
                        '''
                        #print index_dir_name
                        cmd_index = '/bin/bash %s/mergefile2dw/create_lzo_indexer.sh %s %s' % (project_path, index_dir_name, file_name_lzo)
                        logger.info('create lzo index: {0}'.format(cmd_index))
                        index_result = cmd.run(cmd_index)

                        if index_result['status'] != 0:
                            logger.error('Error Code %s : %s  Cmd: %s' % (index_result['status'], index_result['output'], cmd_index))
                        else:
                            if "create index success" in index_result['output']:
                                '''
                                将任务标识为建立lzo索引完成：5
                                '''
                                datas = {'load_status': 5}
                                mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                            else:
                                '''
                                如果load数据失败，则删除半途出现错误的文件，防止hive查询的时候报错
                                '''
                                cmd_remove = '/bin/bash %s/mergefile2dw/remove_damaged_file.sh %s %s' % (project_path, index_dir_name, file_name_lzo)
                                logger.info('remove damaged files: {0}'.format(cmd_remove))
                                remove_result = cmd.run(cmd_remove)

                                if remove_result['status'] != 0:
                                    logger.error('Error Code %s : %s Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))
                    else:
                        logger.error('Error Code %s : %s Cmd: %s' % (result['status'], result['output'], load_sql))
            else:
                '''
                合并数据失败
                '''
                datas = {'load_status': 0}
                mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                logger.error('Error Code %s : %s Cmd: %s' % (merge_result['status'], merge_result['output'], merge_result))

        '''
        执行完毕，模拟从队列中清楚任务:0
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
        logger.error('mergefile2dw error: %s' % exc)
        mysql.rollback()

        datas = {'in_queue': 0, 'load_status': 0}
        mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
        mysql.commit()

        mysql.close()
        raise self.retry(exc=exc, countdown=30)
