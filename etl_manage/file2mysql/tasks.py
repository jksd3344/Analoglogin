#!/usr/bin/python
#coding=utf-8

"""
auth:wuqichao、suguoxin
mail:wuqichao@playcrab.com、suguoxin@playcrab.com
createtime:2015-9-17 10:00:00
usege: 用于将本地文件加载到mysql当中

lastupdate:2016-03-21 10:35:00
"""

from __future__ import absolute_import
import os

from file2mysql.celery import app
from custom.db.mysql import Custom_MySQL

from custom.db.redis_tools import Custom_Redis
from celery.utils.log import get_task_logger

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

logger = get_task_logger(__name__)

'''
将文件加载到mysql中
'''
@app.task(bind=True, max_retries=5, default_retry_delay=6, soft_time_limit=270, time_limit=280)
def run_task(self, task_param):

    redis = Custom_Redis(using='etl_manage')
    mysql = Custom_MySQL(using='hadoops2')
    mysql_etl = Custom_MySQL(using='etl_manage')
    mysql.begin()
    mysql_etl.begin()

    where = {'id': int(task_param['id'])}
    datas = {'load_status': 0}

    try:
        '''
        业务代码块放下方
        '''
        dir_param = {'game': task_param['game'], 'platform': task_param['platform'],
                     'log_date': task_param['log_date'], 'log_name': task_param['log_name']}

        filename_dict = {'log_name': task_param['log_name'], 'log_time': task_param['log_time'], 'source_ip': task_param['source_ip']}
        prefix_sql = task_param['prefix_sql']
        post_sql = task_param['post_sql']

        log_dir = "/%(game)s/%(platform)s/%(log_date)s/%(log_name)s/" % dir_param
        file_name = "%(log_name)s_%(source_ip)s_%(log_time)s.txt" % filename_dict

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

        #判断要录入的文件是否存在，如果存在则执行，否则不执行
        if os.path.exists('%s%s' % (local_log_dir, file_name)):
            '''
            将任务标识为开始执行：1
            '''
            datas['load_status'] = 1
            mysql_etl.update('file2mysql_log', ' id = %(id)d' % where, **datas)
            mysql_etl.commit()
            logger.info('start load data')
            #执行前置sql
            if prefix_sql is not None and prefix_sql != '':
                mysql.query(prefix_sql)
 
            '''
            执行load数据进mysql
            '''
            load_sql = task_param['load_sql']
            load_sql = load_sql.replace('{dir_path}', local_log_dir+file_name)
            load_sql = load_sql.replace('{table_name}', task_param['table_name'])
            load_sql = load_sql.replace('{db_name}', task_param['db_name'])
        
            result = mysql.load(load_sql)
            logger.info('load data to mysql: {0}'.format(result['output']))

            #判断录入mysql是否成功
            if result['status'] == 0:
                #执行后置sql
                if post_sql is not None and post_sql != '':
                    post_sql = post_sql.replace('{table_name}', task_param['table_name'])
                    post_sql = post_sql.replace('{db_name}', task_param['db_name'])
                    mysql.query(post_sql)

                '''
                将任务标识为录入mysql完成：3
                '''
                datas['load_status'] = 3

            else:

                logger.error('Error Code %s : %s  Cmd: %s' % (result['status'], result['output'], load_sql))
                '''
                录入mysql失败，将任务标示为未执行：0
                '''
                datas['load_status'] = 0

        '''
        将任务标示为：(模拟) 已从任务队列中移除
        '''
        datas['in_queue'] = 0
        update_result = mysql_etl.update('file2mysql_log', ' id = %(id)d' % where, **datas)
        # 如果更新失败，则再调用一次，如果还是失败，则等待自动修复机制，但这种概率很小了
        if update_result != 1:
            mysql_etl.update('file2mysql_log', ' id = %(id)d' % where, **datas)

        mysql_etl.commit()
        mysql.commit()
        mysql_etl.close()
        mysql.close()
        return True

    except Exception as exc:
        logger.error('file2mysql error: %s' % exc)
        mysql_etl.rollback()
        mysql.rollback()
        
        datas = {'in_queue': 0, 'load_status': 0}
        mysql_etl.update('file2mysql_log', ' id = %(id)d' % where, **datas)
        mysql_etl.commit()
        
        mysql_etl.close()
        mysql.close()

        raise self.retry(exc=exc, countdown=60)
