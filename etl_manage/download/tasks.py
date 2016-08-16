#!/usr/bin/python
#coding=utf-8

"""
auth: wuqichao、suguoxin
mail: wuqichao@playcrab.com、suguoxin@playcrab.com
create_time: 2015-9-17 10:00:00
usege: 用于hadoop下载文件任务操作

last_update: 2016-04-28 14:45:00
"""

from __future__ import absolute_import
import os

from download.celery import app
from custom.command import Custom_Command as cmd
from custom.db.mysql import Custom_MySQL
from custom.db.redis_tools import Custom_Redis
from celery.utils.log import get_task_logger

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

logger = get_task_logger(__name__)

'''
hadoop服务器集群中启动下载任务程池，由此脚本完成
'''
@app.task(bind=True, max_retries=5, default_retry_delay=6, soft_time_limit=270, time_limit=280)
def run_task(self, task_param):

    redis = Custom_Redis(using='etl_manage')
    mysql = Custom_MySQL(using='etl_manage')
    mysql.begin()
    datas = {'download_status': 0}
    where = {'id': int(task_param['id'])}

    local_log_dir = ""
    lzo_file_name = ""

    try:
        dir_param = {'game': task_param['game'], 'platform': task_param['platform'],
                     'log_date': task_param['log_date'], 'log_name': task_param['log_name']}
        filename_dict = {'log_name': task_param['log_name'], 'log_time': task_param['log_time'], 'source_ip': task_param['source_ip']}
        log_dir = "/%(game)s/%(platform)s/%(log_date)s/%(log_name)s/" % dir_param

        txt_file_name = "%(log_name)s_%(source_ip)s_%(log_time)s.txt" % filename_dict
        lzo_file_name = "%(log_name)s_%(source_ip)s_%(log_time)s.txt.lzo" % filename_dict
        md5_file_name = "%(log_name)s_%(source_ip)s_%(log_time)s.txt.lzo.md5" % filename_dict

        lzo_download_url = task_param['download_url'].rstrip('/') + log_dir + lzo_file_name
        md5_download_url = task_param['download_url'].rstrip('/') + log_dir + md5_file_name

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

        #local_log_dir = '/disk1/tmp_data' + log_dir
        local_log_dir = '/'+disk+'/data' + log_dir
        #创建本地目录
        if not os.path.exists(local_log_dir):
            os.makedirs(local_log_dir)

        #排除同名文件存在的可能，同时为修复执行提供方便
        if os.path.exists('%s%s' % (local_log_dir, txt_file_name)):
            cmd_remove = 'rm -f %s%s*' % (local_log_dir, txt_file_name)
            logger.info('remove history file: {0}'.format(cmd_remove))
            remove_result = cmd.run(cmd_remove)
            if remove_result['status'] != 0:
                logger.error('Error Code %s : %s  Cmd: %s' % (remove_result['status'], remove_result['output'], cmd_remove))

        '''
        下载md5文件,如果md5文件不存在则退出，不再继续执行程序，同时不向数据库写入任何标示
        '''
        md5_line = ' wget -o /tmp/log/wget_log -O %s%s %s' % (local_log_dir, md5_file_name, md5_download_url)
        logger.info('md5 info: {0}'.format(md5_line))
        md5_result = cmd.run(md5_line)

        if md5_result['status'] != 0:
            logger.error('Error Code %s : %s  Cmd: %s' % (md5_result['status'], md5_result['output'], md5_line))

        else:
            '''
            将任务标识为开始执行：1
            '''
            datas['download_status'] = 1
            mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
            mysql.commit()

            '''
            下载数据文件
            '''
            lzo_line = ' wget -o /tmp/log/wget_log -O %s%s %s' % (local_log_dir, lzo_file_name, lzo_download_url)
            logger.info('file info: {0}'.format(lzo_line))
            lzo_result = cmd.run(lzo_line)

            if lzo_result['status'] != 0:
                logger.error('Error Code %s : %s  Cmd: %s' % (lzo_result['status'], lzo_result['output'], lzo_line))
                datas['download_status'] = 0
            else:
                '''
                将任务标识为下载完成：2
                '''
                datas['download_status'] = 2
                mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                mysql.commit()

                '''
                md5校验，如果未通过则不再继续执行程序
                '''
                check_line = "cat %s%s |grep `md5sum %s%s|cut -d ' ' -f 1`" % (local_log_dir, md5_file_name, local_log_dir, lzo_file_name)
                logger.info('md5 or md5 info: {0}'.format(check_line))
                check_result = cmd.run(check_line)
        
                if check_result['status'] != 0:
                    logger.error('Error Code %s : %s Cmd: %s' % (check_result['status'], check_result['output'], check_line))
                    datas['download_status'] = 0
                else:
                    '''
                    lzop解压缩
                    '''
                    cmd_line = ' lzop -dP %s%s' % (local_log_dir, lzo_file_name)
                    logger.info('file info: {0}'.format(cmd_line))
                    cmd_result = cmd.run(cmd_line)
                    if cmd_result['status'] != 0:
                        logger.error('Lzop Code %s : %s Cmd: %s' % (cmd_result['status'], cmd_result['output'], cmd_line))
                        datas['download_status'] = 0
                    else:
                        '''
                        将任务标识md5一致，完成下载任务：3
                        '''
                        datas['download_status'] = 3

        '''
        将任务标示为：(模拟) 已从任务队列中移除
        '''
        datas['in_download_queue'] = 0
        update_result = mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
        # 如果更新失败，则再调用一次，如果还是失败，则等待自动修复机制，但这种概率很小了
        if update_result != 1:
            mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        mysql.close()
        return True
    
    except Exception as exc:
        print (exc)
        logger.error('download error : %s' % exc)
        mysql.rollback()

        kill_proces = "kill -9 `ps -ef |grep wget |grep -v grep |grep '%s%s'|awk '{print $2}'`" % (local_log_dir, lzo_file_name)
        cmd.run(kill_proces)
        
        datas = {'in_download_queue': 0, 'download_status': 0}
        mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
        mysql.commit()

        mysql.close()
        raise self.retry(exc=exc, countdown=60)
