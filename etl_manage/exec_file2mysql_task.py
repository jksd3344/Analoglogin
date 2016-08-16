#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2015-11-16 15:28:00
used: 执行file2mysql任务

last_update: 2016-04-28 14:57:00
"""

import sys
import datetime
from file2mysql.tasks import run_task
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

sql = "select distinct a.* from (select * from file2mysql_log where game='%s' and platform='%s' and load_status=0 " \
      "and do_rate='%s' and task_date='%s' and in_queue=0 and exec_num<4 and retry_num <6 and log_time<='%s') as a left outer join " \
      "(select * from etl_data_log where game='%s' and platform='%s' and etl_status=6 and download_status=3 " \
      "and do_rate='%s' and task_date='%s' and log_time<='%s') as b on a.game=b.game and a.platform=b.platform " \
      "and a.log_name=b.log_name and a.task_date = b.task_date and a.do_rate=b.do_rate and a.log_time=b.log_time" \
      "" % (game, platform, do_rate, log_date, log_time, game, platform, do_rate, log_date, log_time)

if log_time < '0100':
    log_time_old = '2400'
    log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

    sql = "(select distinct a.* from (select * from file2mysql_log where game='%s' and platform='%s' " \
          "and load_status=0 and do_rate='%s' and task_date='%s' and in_queue=0 and exec_num<4 and retry_num <6 and log_time<='%s') " \
          "as a left outer join (select * from etl_data_log where game='%s' and platform='%s' and etl_status=6 " \
          "and download_status=3 and do_rate='%s' and task_date='%s' and log_time<='%s') as b on a.game=b.game " \
          "and a.platform=b.platform and a.log_name=b.log_name and a.task_date = b.task_date and a.do_rate=b.do_rate " \
          "and a.log_time=b.log_time) union (select distinct a.* from (select * from file2mysql_log where game='%s' " \
          "and platform='%s' and load_status=0 and do_rate='%s' and task_date='%s' and in_queue=0 and exec_num<4 and retry_num <6 " \
          "and log_time<='%s') as a left outer join (select * from etl_data_log where game='%s' and platform='%s' " \
          "and etl_status=6 and download_status=3 and do_rate='%s' and task_date='%s' and log_time<='%s') as b " \
          "on a.game=b.game and a.platform=b.platform and a.log_name=b.log_name and a.task_date = b.task_date " \
          "and a.do_rate=b.do_rate and a.log_time=b.log_time)" \
          "" % (game, platform, do_rate, log_date, log_time, game, platform, do_rate, log_date, log_time,
                game, platform, do_rate, log_date_old, log_time_old, game, platform, do_rate, log_date_old, log_time_old)
       
tasks = mysql.query(sql)

for task in tasks:

    try:
        mysql.begin()
        where = {'id': int(task['id'])}

        '''
        照理说for里面不会有人抢快照数据，以防万一起动排他锁(使用主键启动行锁)，
        业务上 1.压缩完毕 2.下载到本地 3.验证md5完毕 4.未录入到hive的dw库中
        '''
        #result = mysql.get('select f.*,s.db_name,s.table_name from file2mysql_log as f left join structure as s '
        result = mysql.get('select f.*,s.db_name,s.table_name from file2mysql_log as f left join structure as s '
                           'on f.target_id=s.id where f.id = %(id)s ' % where)

        if result is not None:
            exec_num = int(result['exec_num'])
            in_queue = int(result['in_queue'])
            retry_num = int(result['retry_num'])

            if retry_num < 5:
                if in_queue == 0:
                    if exec_num < 3:
                        if exec_num == 0:
                            retry_num += 1

                        exec_num += 1
                        in_queue = 1

                        datas = {'exec_num': exec_num, 'in_queue': in_queue, 'retry_num': retry_num}
                        update_result = mysql.update('file2mysql_log', ' id = %(id)d' % where, **datas)
                        # 为防止修改数据库时出现异常，调整为：确认修改字段状态成功后，再加入队列
                        if update_result == 1:
                            run_task.apply_async((result,), queue='file2mysql')
                        else:
                            print "update data{'exec_num', 'in_queue', 'retry_num'} error"
                    else:
                        datas = {'load_status': -1}
                        mysql.update('file2mysql_log', ' id = %(id)d' % where, **datas)
            else:
                datas = {'load_status': -2}
                mysql.update('file2mysql_log', ' id = %(id)d' % where, **datas)

        mysql.commit()
        #mysql.close()

    except Exception as exc:
        #回滚
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()
