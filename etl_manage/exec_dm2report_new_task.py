#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-03-11 15:20:00
used: dm2report_new 任务执行

last_update:2016-04-28 14:48:00
"""

import datetime
import sys
from dm2report_new.tasks import run_task
from custom.db.mysql import Custom_MySQL

mysql = Custom_MySQL(using='etl_manage')

game = sys.argv[1]
platform = sys.argv[2]
do_rate = sys.argv[3]
now_time = sys.argv[4]
now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')

#用于判断，是否需要校验对于上层任务(dw2dm任务)的依赖，onrely: 需要，offrely: 不需要
is_rely = ""
try:
    is_rely = sys.argv[5]
except Exception as exc:
    is_rely = "onrely"

#数据日期，格式如：20151015
task_date = now_time.strftime('%Y%m%d')

# 查找当天未开始的任务
sql = "select  id,from_id,log_date,grouped,priority from dm2report_new_log d " \
      "where d.task_date='%s' and game='%s' and platform='%s' and do_rate='%s' " \
      "and d.status=0 and d.in_queue=0 and d.exec_num < 3" \
      "" % (task_date, game, platform, do_rate)

tasks = mysql.query(sql)

for task in tasks:
    try:
        mysql.begin()
        pid = task['id']
        from_id = task['from_id']
        log_date = task['log_date']
        grouped = task['grouped']
        priority = task['priority']

        sql = ""
        result = ""
        if is_rely == "onrely":
            # 查找该条任务所依赖的上层dw2dm 或是 本层dm2report 任务是否完成
            # dm层不再处理时差，所以此处仅查询platform='all'的 #and f.platform = a.platformm
            sql = "select distinct a.* from dw2dm_log f,(select s.game gamee,s.platform platformm," \
                  "s.table_name table_namee,d.task_name log_namee,s.flag flagg, d.* " \
                  "from structure s,dm2report_new_log d " \
                  "where s.type='dm' and s.id in ({0}) and d.id={1}) a " \
                  "where f.log_date=a.log_date and f.game=a.gamee and f.platform='all' " \
                  "and f.log_name=a.table_namee and f.status!=3 " \
                  "".format(from_id, pid)

            result = mysql.query_by_sql(sql)
        elif is_rely == "offrely":
            result = {'output': ''}
        else:
            result = ""

        # 如果为空，则全部完成
        if result['output'] is None or result['output'] == '':

            # 查找该条任务
            formal_sql = "select d.*,s.db_name,s.table_name from dm2report_new_log d, structure s " \
                         "where d.id = %s and d.target_id = s.id" % pid
            formal_result = ""

            if grouped == 1 and priority == 1:
                formal_result = mysql.get(formal_sql)
            elif grouped == 1 and priority != 1:
                # 查找组别为1且优先级小于该条任务优先级的其他dm2report任务是否完成
                gpsql = "select d.* from dm2report_new_log d,(select priority from dm2report_new_log where id = %s) a " \
                        "where d.log_date = '%s' and d.game = '%s' and d.platform = '%s' and d.grouped = 1 " \
                        "and d.priority<a.priority and d.status != 3" % (pid, log_date, game, platform)
                gpresult = mysql.query(gpsql)
                if not gpresult:
                    formal_result = mysql.get(formal_sql)
            elif grouped != 1 and priority == 1:
                # 查找组别小于该条任务组别的其他dm2report任务是否完成
                gpsql = "select d.* from dm2report_new_log d,(select grouped from dm2report_new_log where id = %s) a " \
                        "where d.log_date = '%s' and d.game = '%s' and d.platform = '%s' and d.grouped<a.grouped " \
                        "and d.status != 3" % (pid, log_date, game, platform)
                gpresult = mysql.query(gpsql)
                if not gpresult:
                    formal_result = mysql.get(formal_sql)
            else:
                # 查找组别小于该条任务组别的其他dm2report任务 或者 组别与该条任务相同且优先级小于该条任务优先级的其他dm2report任务是否完成
                gpsql = "select d.* from dm2report_new_log d,(select grouped,priority from dm2report_new_log where id = %s) a " \
                        "where ((d.grouped=a.grouped and d.priority<a.priority) or d.grouped<a.grouped) " \
                        "and d.log_date = '%s' and d.game = '%s' and d.platform = '%s' and d.status != 3" \
                        "" % (pid, log_date, game, platform)
                gpresult = mysql.query(gpsql)
                if not gpresult:
                    formal_result = mysql.get(formal_sql)

            # 如果为空，则全部完成
            if formal_result is not None and formal_result != "":
                exec_num = int(formal_result['exec_num'])
                in_queue = int(formal_result['in_queue'])
                if in_queue == 0:
                    if exec_num < 3:
                        exec_num += 1
                        in_queue = 1

                        datas = {'exec_num': exec_num, 'in_queue': in_queue}
                        update_result = mysql.update('dm2report_new_log', ' id = %s' % pid, **datas)

                        # 为防止修改数据库时出现异常，调整为：确认修改字段状态成功后，再加入队列
                        if update_result == 1:
                            run_task.apply_async((formal_result,), queue='dm2report_new')
                        else:
                            print "update data{'exec_num', 'in_queue'} error"
                    else:
                        datas = {'status': -1}
                        mysql.update('dm2report_new_log', ' id = %s' % pid, **datas)
        mysql.commit()

    except Exception as exc:
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()
