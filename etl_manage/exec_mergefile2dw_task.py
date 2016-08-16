#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-01-17 24:00:00
used: 执行mergefile2dw任务

last_update: 2016-04-28 14:56:00
"""

import datetime
import sys
from mergefile2dw.tasks import run_task
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

sql = 'select * from file2dw_log ' \
      'where game="%s" and platform="%s" and do_rate="%s" and load_status=0 and task_date="%s" ' \
      'and in_queue =0 and exec_num < 4 and log_time <="%s"' \
      '' % (game, platform, do_rate, log_date, log_time)

if log_time == '0020':
    log_time = '2400'
    log_date = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')
    sql = 'select * from file2dw_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and load_status=0 and task_date="%s" ' \
          'and in_queue =0 and exec_num < 4 and log_time <="%s"' \
          '' % (game, platform, do_rate, log_date, log_time)

tasks = mysql.query(sql)

for task in tasks:
    #print(task)
    try:
        mysql.begin()
        where = {'id': int(task['id'])}

        '''
        照理说for里面不会有人抢快照数据，以防万一起动排他锁(使用主键启动行锁)，兄弟们最好别瞎用
        '''
        #result = mysql.get('select f.*,s.db_name,s.table_name from file2dw_log as f left join structure as s '
        result = mysql.get('select f.*,s.db_name,s.table_name,s.flag from file2dw_log as f left join structure as s '
                           'on f.target_id=s.id where f.id = %(id)s ' % where)
        if result is not None:
            exec_num = int(result['exec_num'])
            in_queue = int(result['in_queue'])
            if in_queue == 0:
                if exec_num < 3:
                    exec_num += 1
                    in_queue = 1

                    datas = {'exec_num': exec_num, 'in_queue': in_queue}
                    update_result = mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)
                    # 为防止修改数据库时出现异常，调整为：确认修改字段状态成功后，再加入队列
                    if update_result == 1:
                        run_task.apply_async((result,), queue='mergefile2dw')
                    else:
                        print "update data{'exec_num', 'in_queue'} error"
                else:
                    datas = {'load_status': -1}
                    mysql.update('file2dw_log', ' id = %(id)d' % where, **datas)

        mysql.commit()

    except Exception as exc:
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()

