#!/usr/bin/python
#coding=utf-8

"""
auth: wuqichao、suguoxin
mail: wuqichao@playcrab.com、suguoxin@playcrab.com
create_time: 2015-9-17 10:00:00
used: 执行下载任务

last_update: 2016-04-28 15:15:00
"""

import datetime
import sys
from download.tasks import run_task
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

sql = 'select * from etl_data_log ' \
      'where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 and download_status=0 and task_date="%s" ' \
      'and in_download_queue=0 and download_exec_num<4 and download_retry_num<6 and log_time<="%s"' \
      '' % (game, platform, do_rate, log_date, log_time)

if log_time <= '0115':
    log_time_old = '2400'
    log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

    sql = '(select * from etl_data_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 and download_status=0 and task_date="%s" ' \
          'and in_download_queue=0 and download_exec_num<4 and download_retry_num<6 and log_time<="%s") ' \
          'union (select * from etl_data_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and etl_status=6 and download_status=0 and task_date="%s" ' \
          'and in_download_queue=0 and download_exec_num<4 and download_retry_num<6 and log_time<="%s")' \
          '' % (game, platform, do_rate, log_date, log_time, game, platform, do_rate, log_date_old, log_time_old)

tasks = mysql.query(sql)

for task in tasks:
    #print(task)
    try:
        mysql.begin()
        where = {'id': int(task['id'])}

        '''
        照理说for里面不会有人抢快照数据，以防万一起动排他锁(使用主键启动行锁)，兄弟们最好别瞎用
        '''
        #result = mysql.get('select * from etl_data_log where etl_status = 6  and id = %(id)s for update' % where)
        result = mysql.get('select * from etl_data_log where etl_status = 6  and id = %(id)s ' % where)
        if result is not None:
            download_exec_num = int(result['download_exec_num'])
            in_download_queue = int(result['in_download_queue'])
            download_retry_num = int(result['download_retry_num'])

            if download_retry_num < 5:
                if in_download_queue == 0:
                    if download_exec_num < 3:
                        if download_retry_num == 0:
                            download_retry_num += 1

                        download_exec_num += 1
                        in_download_queue = 1

                        datas = {'download_exec_num': download_exec_num, 'in_download_queue': in_download_queue,
                                 'download_retry_num': download_retry_num}
                        update_result = mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                        if update_result == 1:
                            run_task.apply_async((result,), queue='download')
                        else:
                            print "update data{'download_exec_num', 'in_download_queue', 'download_retry_num'} error"
                    else:
                        datas = {'download_status': -1}
                        mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
            else:
                #任务执行彻底失败，不再自动重试
                datas = {'download_status': -2}
                mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        #mysql.close()

    except Exception as exc:
        #回滚
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()
