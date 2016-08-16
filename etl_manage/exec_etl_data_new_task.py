#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-06-25 15:45:00
used: 用于清洗游戏日志原始数据

last_update:2016-06-23 16:40:00
"""

import datetime
import sys
from etl_data_new.tasks import run_task
from custom.db.mysql import Custom_MySQL

mysql = Custom_MySQL(using='etl_manage')

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
               'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
               'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s"' \
               '' % (game, platform, do_rate, log_date, log_time)

    if log_time <= '0100':
        log_time_old = '2400'
        log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')
        task_sql = '(select distinct game,platform,log_name,log_dir,md5_name,md5_dir,download_url,col_num,do_rate,' \
                   '`group`,priority,from_id,target_id,log_date,log_time,task_date from etl_data_log ' \
                   'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
                   'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s") ' \
                   'union (select distinct game,platform,log_name,log_dir,md5_name,md5_dir,download_url,col_num,' \
                   'do_rate,`group`,priority,from_id,target_id,log_date,log_time,task_date from etl_data_log ' \
                   'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
                   'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s")' \
                   '' % (game, platform, do_rate, log_date, log_time,
                         game, platform, do_rate, log_date_old, log_time_old)

    task_results = mysql.query(task_sql)

    for task in task_results:
        try:
            mysql.begin()
            where = {'game': task['game'], 'platform': task['platform'], 'log_name': task['log_name'],
                     'do_rate': task['do_rate'], 'log_date': task['log_date'], 'log_time': task['log_time'],
                     'task_date': task['task_date']}

            results = mysql.query('select f.*, s.flag from etl_data_log as f left join structure as s '
                                  'on f.target_id=s.id where f.game="%(game)s" '
                                  'and f.platform="%(platform)s" and f.log_name="%(log_name)s" '
                                  'and f.do_rate="%(do_rate)s" and f.log_date="%(log_date)s" '
                                  'and f.log_time="%(log_time)s" and f.task_date="%(task_date)s"' % where)
            task_set = set()
            ip_set = set()
            ips = ""
            flag = 0
            ids = ""
            if results is not None:
                for result in results:
                    tmp_task = {'game': result['game'], 'platform': result['platform'], 'log_name': result['log_name'],
                                'log_dir': result['log_dir'], 'col_num': result['col_num'],
                                'do_rate': result['do_rate'],
                                'log_date': result['log_date'], 'log_time': result['log_time'],
                                'task_date': result['task_date'], 'flag': result['flag']}
                    task_set.add(str(tmp_task))

                    tmp_ip = result['source_ip']
                    ip_set.add(tmp_ip)

                    ips = "'" + "','".join(list(ip_set)) + "'"

                    etl_exec_num = int(result['etl_exec_num'])
                    in_etl_queue = int(result['in_etl_queue'])
                    etl_retry_num = int(result['etl_retry_num'])
                    pid = int(result['id'])
                    ids += str(result['id']) + ","

                    if etl_retry_num < 5:
                        if in_etl_queue == 0:
                            if etl_exec_num < 3:
                                if etl_exec_num == 0:
                                    etl_retry_num += 1

                                etl_exec_num += 1
                                in_etl_queue = 1

                                datas = {'etl_exec_num': etl_exec_num, 'in_etl_queue': in_etl_queue,
                                         'etl_retry_num': etl_retry_num}
                                update_result = mysql.update('etl_data_log', ' id = %s' % pid, **datas)
                                if update_result != 1:
                                    flag = 1
                                    print "update data{'etl_exec_num', 'in_etl_queue', 'etl_retry_num'} error"
                            else:
                                flag = 2
                                #任务在该次自动重试中执行失败
                                datas = {'etl_status': -1}
                                mysql.update('etl_data_log', ' id = %s' % pid, **datas)
                        else:
                            flag = 2
                            #任务执行彻底失败，不再自动重试
                            datas = {'etl_status': -2}
                            mysql.update('etl_data_log', ' id = %s' % pid, **datas)

            #flag=0表示所有任务均修改状态成功，则加入队列继续执行
            #flag=1表示有任务修改状态失败，标示全部为未执行
            #flag=2表示重试次数已经到了
            if flag == 0:
                task_result = "".join(list(task_set))
                run_task.apply_async((task_result, ips, "5min"), queue='etl_data_new')
            elif flag == 1:
                datas = {'in_etl_queue': 0, 'etl_status': 0}
                mysql.update('etl_data_log', ' id in (%s)' % ids[:-1], **datas)
            else:
                datas = {'in_etl_queue': 0}
                mysql.update('etl_data_log', ' id in (%s)' % ids[:-1], **datas)

            mysql.commit()

        except Exception as exc:
            print(exc)
            mysql.rollback()
            print "db rollback success"
            mysql.close()


def task_1day():
    sql = 'select * from etl_data_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
          'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s"' \
          '' % (game, platform, do_rate, log_date, log_time)

    if log_time <= '0100':
        log_time_old = '2400'
        log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')
        sql = '(select * from etl_data_log ' \
              'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
              'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s") ' \
              'union (select * from etl_data_log ' \
              'where game="%s" and platform="%s" and do_rate="%s" and etl_status=0 and task_date="%s" ' \
              'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s")' \
              '' % (game, platform, do_rate, log_date, log_time, game, platform, do_rate, log_date_old, log_time_old)

    tasks = mysql.query(sql)

    for task in tasks:
        try:
            mysql.begin()
            where = {'id': int(task['id'])}

            '''
            照理说for里面不会有人抢快照数据，以防万一起动排他锁(使用主键启动行锁)，兄弟们最好别瞎用
            '''
            result = mysql.get('select f.*,s.flag from etl_data_log as f left join structure as s '
                               'on f.target_id=s.id where f.id = %(id)s ' % where)

            if result is not None:
                etl_exec_num = int(result['etl_exec_num'])
                in_etl_queue = int(result['in_etl_queue'])
                etl_retry_num = int(result['etl_retry_num'])

                if etl_retry_num < 5:
                    if in_etl_queue == 0:
                        if etl_exec_num < 3:
                            if etl_exec_num == 0:
                                etl_retry_num += 1

                            etl_exec_num += 1
                            in_etl_queue = 1

                            datas = {'etl_exec_num': etl_exec_num, 'in_etl_queue': in_etl_queue,
                                     'etl_retry_num': etl_retry_num}
                            update_result = mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                            if update_result == 1:
                                run_task.apply_async((result, "", "1day"), queue='etl_data_new')
                            else:
                                print "update data{'etl_exec_num', 'in_etl_queue', 'etl_retry_num'} error"
                        else:
                            #任务在该次自动重试中执行失败
                            datas = {'etl_status': -1}
                            mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                else:
                    #任务执行彻底失败，不再自动重试
                    datas = {'etl_status': -2}
                    mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)

            mysql.commit()

        except Exception as exc:
            print(exc)
            mysql.rollback()
            print "db rollback success"
            mysql.close()


if do_rate == "5min":
    task_5min()
else:
    task_1day()