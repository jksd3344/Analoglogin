#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-06-23 17:03:00
used: 执行下载任务，为处理服务器过多的有游戏所建立

last_update: 2016-06-23 17:03:00
"""

import datetime
import sys
from download_new.tasks import run_task
from custom.db.mysql import Custom_MySQL

mysql = Custom_MySQL(using='etl_manage')

#执行频次，通过命令行参数获得
game = sys.argv[1]
platform = sys.argv[2]
do_rate = sys.argv[3]
now_time = sys.argv[4]
now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')

#数据日期，格式如：20151015
log_date = now_time.strftime('%Y%m%d')
#数据时间点(每五分钟)，格式如：0005、2400
log_time = now_time.strftime("%H%M")


def task_5min():
    #查询所有符合条件的任务，按游戏、平台、人物名、日期等重要字段去重
    task_sql = 'select distinct game,platform,log_name,log_dir,md5_name,md5_dir,download_url,col_num,do_rate,`group`,' \
               'priority,from_id,target_id,log_date,log_time,task_date from etl_data_log ' \
               'where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 and download_status=0 ' \
               'and task_date="%s" and in_download_queue =0 and download_exec_num < 4 ' \
               'and download_retry_num < 6 and log_time <="%s"' \
               '' % (game, platform, do_rate, log_date, log_time)

    if log_time <= '0115':
        log_time_old = '2400'
        log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')
        task_sql = '(select distinct game,platform,log_name,log_dir,md5_name,md5_dir,download_url,col_num,do_rate,' \
                   '`group`,priority,from_id,target_id,log_date,log_time,task_date from etl_data_log ' \
                   'where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 and download_status=0 ' \
                   'and task_date="%s" and in_download_queue =0 and download_exec_num < 4 and download_retry_num < 6 ' \
                   'and log_time <="%s") union (select distinct game,platform,log_name,log_dir,md5_name,md5_dir,' \
                   'download_url,col_num,do_rate,`group`,priority,from_id,target_id,log_date,log_time,task_date ' \
                   'from etl_data_log where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 ' \
                   'and download_status=0 and task_date="%s" and in_download_queue =0 and download_exec_num < 4 ' \
                   'and download_retry_num < 6 and log_time <="%s")' \
                   '' % (game, platform, do_rate, log_date, log_time,
                         game, platform, do_rate, log_date_old, log_time_old)

    task_results = mysql.query(task_sql)

    for task in task_results:
        try:
            mysql.begin()
            where = {'game': task['game'], 'platform': task['platform'], 'log_name': task['log_name'],
                     'do_rate': task['do_rate'], 'log_date': task['log_date'], 'log_time': task['log_time'],
                     'task_date': task['task_date']}

            results = mysql.query('select distinct etl_status,download_exec_num,in_download_queue,download_retry_num '
                                  'from etl_data_log where game="%(game)s" '
                                  'and platform="%(platform)s" and log_name="%(log_name)s" '
                                  'and do_rate="%(do_rate)s" and log_date="%(log_date)s" '
                                  'and log_time="%(log_time)s" and task_date="%(task_date)s"' % where)

            #如果当前时间点的所有任务均已完成，则开始执行
            if len(results) == 1 and results[0]['etl_status']:

                download_exec_num = int(results[0]['download_exec_num'])
                in_download_queue = int(results[0]['in_download_queue'])
                download_retry_num = int(results[0]['download_retry_num'])

                if download_retry_num < 5:
                    if in_download_queue == 0:
                        if download_exec_num < 3:
                            if download_retry_num == 0:
                                download_retry_num += 1

                            download_exec_num += 1
                            in_download_queue = 1

                            datas = {'download_exec_num': download_exec_num, 'in_download_queue': in_download_queue,
                                     'download_retry_num': download_retry_num}
                            update_result = mysql.update('etl_data_log',
                                                         ' game = "%(game)s" and platform="%(platform)s" '
                                                         'and log_name="%(log_name)s" and log_date="%(log_date)s" '
                                                         'and log_time="%(log_time)s"' % where, **datas)
                            if update_result == 1:
                                run_task.apply_async((task,), queue='download_new')
                            else:
                                print "update data{'download_exec_num', 'in_download_queue', 'download_retry_num'} error"
                        else:
                            datas = {'download_status': -1}
                            mysql.update('etl_data_log', ' game = "%(game)s" and platform="%(platform)s" '
                                                         'and log_name="%(log_name)s" and log_date="%(log_date)s" '
                                                         'and log_time="%(log_time)s"' % where, **datas)
                else:
                    #任务执行彻底失败，不再自动重试
                    datas = {'download_status': -2}
                    mysql.update('etl_data_log', ' game = "%(game)s" and platform="%(platform)s" '
                                                 'and log_name="%(log_name)s" and log_date="%(log_date)s" '
                                                 'and log_time="%(log_time)s"' % where, **datas)

            mysql.commit()
            #mysql.close()

        except Exception as exc:
            #回滚
            print(exc)
            mysql.rollback()
            print "db rollback success"
            mysql.close()

task_5min()