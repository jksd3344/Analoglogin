#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-01-01 11:20:00
used: dm2report 任务执行

last_update:2016-04-28 14:48:00
"""

import datetime
import sys
from dm2report.tasks import run_task
from custom.db.mysql import Custom_MySQL

mysql = Custom_MySQL(using='etl_manage')

game = sys.argv[1]
platform = sys.argv[2]
do_rate = sys.argv[3]
now_time = sys.argv[4]
now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')

#数据日期，格式如：20151015
log_date = now_time.strftime('%Y%m%d')

tasks = mysql.query('select * from dm2report_log where do_rate="%s" and task_date="%s" and game="%s" '
                    'and platform="%s" and exec_num<4 and in_queue=0 and status=0'
                    '' % (do_rate, log_date, game, platform))

for task in tasks:

    try:
        mysql.begin()
        where = {'id': int(task['id'])}

        #result = mysql.get('select f.*,s.db_name,s.table_name from dm2report_log as f left join structure as s '
        result = mysql.get('select f.*,s.db_name,s.table_name from dm2report_log as f left join structure as s '
                           'on f.target_id=s.id where f.id = %(id)s ' % where)

        if result is not None:
            exec_num = int(result['exec_num'])
            in_queue = int(result['in_queue'])

            if in_queue == 0:
                if exec_num < 3:
                    exec_num += 1
                    in_queue = 1

                    datas = {'exec_num': exec_num, 'in_queue': in_queue}
                    update_result = mysql.update('dm2report_log', ' id = %(id)d' % where, **datas)

                    # 为防止修改数据库时出现异常，调整为：确认修改字段状态成功后，再加入队列
                    if update_result == 1:
                        run_task.apply_async((result,), queue='dm2report')
                    else:
                        print "update data{'exec_num', 'in_queue'} error"
                else:
                    datas = {'status': -1}
                    mysql.update('dm2report_log', ' id = %(id)d' % where, **datas)

        mysql.commit()	

    except Exception as exc:
        #回滚
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()
